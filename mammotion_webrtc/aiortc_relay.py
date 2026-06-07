"""Agora → RTSP H265 passthrough relay.

Owns one upstream aiortc ``RTCPeerConnection`` that subscribes to the
mower's Agora channel as a passive viewer, and forwards every received
H265 RTP packet to an embedded :class:`Go2RtcRtspStream` that go2rtc
consumes via ``rtsp://``. Path:

    Mower → Agora SFU → aiortc upstream PC → RTP tap → RTSP → go2rtc

The aiortc codec stack is monkey-patched in :mod:`.h265_patch` so the SDP
offer/answer can carry H265 PT 100 and the receiver does not drop packets
or crash its decoder thread. Once the patches are in place aiortc is happy
to ferry the H265 bytes through SRTP and out the other side; we never
decode anything.

Lifecycle:

* Eager start: ``await start()`` kicks off the supervisor task. Frigate is
  always-on, so a lazy/idle-grace teardown adds no value and risks the
  upstream not being ready when the first viewer asks.
* Self-healing: the supervisor reconnects on upstream failure with a
  bounded backoff. Each retry refetches credentials via the provider so
  expired Agora tokens are handled implicitly.
* ``await stop()`` tears down both the PC and the RTSP server cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time

from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.mediastreams import MediaStreamTrack
from aiortc.rtp import RtpPacket, unwrap_rtx

from . import h265_patch
from .agora_edge import AgoraWebSocketHandler
from .agora_session import (
    AgoraContextProvider,
    CredentialsProvider,
    PublisherWakeup,
)
from .rtsp_server import Go2RtcRtspStream

# Apply the H265 patches once at import time. The bridge entrypoint imports
# this module before constructing any RTCPeerConnection, so this is safe.
h265_patch.apply()

LOGGER = logging.getLogger(__name__)


class AgoraToRtspRelay:
    """Bridge from Agora WebRTC to a local RTSP server.

    One instance owns one upstream PC and one RTSP server. The RTSP server
    is shared across all go2rtc consumers (typically just one — go2rtc
    multiplexes its own viewers internally).
    """

    def __init__(
        self,
        *,
        credentials_provider: CredentialsProvider,
        agora_context_provider: AgoraContextProvider,
        rtsp_server: Go2RtcRtspStream,
        publisher_wakeup: PublisherWakeup = None,
        cheap_recovery: PublisherWakeup = None,
        upstream_track_timeout_seconds: float = 15.0,
        reconnect_backoff_seconds: float = 1.0,
        max_reconnect_backoff_seconds: float = 60.0,
        no_rtp_watchdog_seconds: float = 5.0,
        cheap_recovery_wait_seconds: float = 5.0,
    ) -> None:
        self._credentials_provider = credentials_provider
        self._agora_context_provider = agora_context_provider
        self._rtsp_server = rtsp_server
        self._publisher_wakeup = publisher_wakeup
        # Best-effort "nudge the publisher without tearing down the upstream"
        # callback. The watchdog fires this first; only if RTP doesn't resume
        # within cheap_recovery_wait_seconds do we escalate to full teardown.
        self._cheap_recovery = cheap_recovery
        self._upstream_track_timeout = upstream_track_timeout_seconds
        self._reconnect_backoff = reconnect_backoff_seconds
        self._max_reconnect_backoff = max_reconnect_backoff_seconds
        self._no_rtp_watchdog = no_rtp_watchdog_seconds
        self._cheap_recovery_wait = cheap_recovery_wait_seconds

        self._stop_event = asyncio.Event()
        self._upstream_pc: RTCPeerConnection | None = None
        self._upstream_handler: AgoraWebSocketHandler | None = None
        self._supervisor_task: asyncio.Task[None] | None = None
        # The receiver associated with the inbound video track. Cached so
        # the RTSP server can ask us to PLI upstream when a new client joins.
        self._video_receiver = None
        # SSRC of the inbound H265 stream — needed for the PLI we send on
        # behalf of new RTSP clients. None until the first packet arrives.
        self._video_ssrc: int | None = None
        # Monotonic-ns timestamp of the last forwarded H265 RTP packet. Used
        # by the in-relay no-RTP watchdog (tears down + reconnects) to detect
        # mid-stream stalls. The bridge-level dryness watchdog instead keys off
        # _last_healthy_ns below (see seconds_since_healthy) so it isn't fooled
        # by churn trickle. Initialised to "now" so a bootstrap that never
        # produces video also ages out, instead of looking stale from t=0.
        self._last_rtp_ns: int = time.monotonic_ns()
        # Monotonic-ns timestamp of the last *sustained* (healthy) RTP. Unlike
        # ``_last_rtp_ns``, a lone trickle packet does NOT advance this — only
        # a cycle that has forwarded at least ``_healthy_packet_threshold``
        # packets counts. The bridge-level dryness watchdog keys off this so
        # the "publisher is join/quit churning and dribbling one IDR every
        # 60-90s" failure mode (which kept resetting ``_last_rtp_ns`` and so
        # defeated the watchdog) still ages out and triggers a full
        # in-process restart. Initialised to "now" so a bootstrap that never
        # streams also ages out instead of looking healthy from t=0.
        self._last_healthy_ns: int = time.monotonic_ns()
        # Packets forwarded in the current upstream cycle. Reset at the top of
        # each ``_run_one_upstream``. Used to classify a cycle as healthy (a
        # real stream) vs a trickle (churn) for both ``_last_healthy_ns`` and
        # the supervisor's backoff regime.
        self._cycle_packets: int = 0
        # ~4s of video at the mower's ~10-40 pps. Above this a cycle is a
        # genuine stream; at or below it's churn (a stray IDR or two from a
        # publisher that quit ~1s after joining).
        self._healthy_packet_threshold: int = 150
        # Throughput counters incremented in the RTP tap and sampled by the
        # heartbeat. Lifetime totals (across all reconnect cycles) so the
        # heartbeat can show "yes the process is still doing useful work"
        # even after a recent stall.
        self._packets_forwarded: int = 0
        self._bytes_forwarded: int = 0
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_interval: float = 60.0
        # Snapshot of (packets, bytes, monotonic_ns) at the previous
        # heartbeat tick — used to derive interval-local pps and kbps so
        # the heartbeat shows the *current* rate, not the lifetime average.
        self._heartbeat_prev: tuple[int, int, int] = (0, 0, time.monotonic_ns())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def seconds_since_last_rtp(self) -> float:
        """Wall-clock seconds since the last forwarded H265 RTP packet.

        Survives reconnect cycles: the timestamp is only updated by the RTP
        tap (and once at construction), never reset by teardown. The bridge
        uses this to drive a process-level dryness watchdog that escapes the
        "publisher unreachable for hours" failure mode the in-relay
        reconnect loop can't dig itself out of (stale pymammotion session,
        stuck MQTT, mower needing a full cloud-side wake).
        """
        return (time.monotonic_ns() - self._last_rtp_ns) / 1e9

    @property
    def seconds_since_healthy(self) -> float:
        """Wall-clock seconds since we last had a *sustained* H265 stream.

        Advances only while a cycle is streaming real video (>=
        ``_healthy_packet_threshold`` packets), so a publisher that joins and
        quits ~1s later — dribbling a stray IDR each time — does NOT keep this
        clock fresh. That is the distinction the bridge dryness watchdog needs:
        ``seconds_since_last_rtp`` is reset by those trickle packets and so
        never trips, while this keeps climbing until a genuine stream returns.
        """
        return (time.monotonic_ns() - self._last_healthy_ns) / 1e9

    async def start(self) -> None:
        if self._supervisor_task is not None and not self._supervisor_task.done():
            return
        self._stop_event.clear()
        self._heartbeat_prev = (
            self._packets_forwarded,
            self._bytes_forwarded,
            time.monotonic_ns(),
        )
        self._supervisor_task = asyncio.create_task(self._supervise())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        for task in (self._supervisor_task, self._heartbeat_task):
            if task is not None:
                task.cancel()
        for task in (self._supervisor_task, self._heartbeat_task):
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._supervisor_task = None
        self._heartbeat_task = None
        await self._teardown_upstream()

    async def _heartbeat_loop(self) -> None:
        """Periodic INFO line summarising steady-state health.

        Most failure modes show up as the absence of expected events; this
        is the matching presence-of-success log. If you tail the log and
        see one of these every minute, the bridge is working. If they go
        silent, you've got an outage. Each line is small (< 200 chars) so
        it doesn't drown the rest of the logs.
        """
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self._heartbeat_interval
                    )
                    return
                except asyncio.TimeoutError:
                    pass

                now_ns = time.monotonic_ns()
                prev_pkts, prev_bytes, prev_ns = self._heartbeat_prev
                elapsed = max(1e-6, (now_ns - prev_ns) / 1e9)
                d_pkts = self._packets_forwarded - prev_pkts
                d_bytes = self._bytes_forwarded - prev_bytes
                pps = d_pkts / elapsed
                kbps = (d_bytes * 8 / 1000) / elapsed
                self._heartbeat_prev = (
                    self._packets_forwarded,
                    self._bytes_forwarded,
                    now_ns,
                )

                pc = self._upstream_pc
                upstream_state = pc.connectionState if pc is not None else "down"
                idle_s = (now_ns - self._last_rtp_ns) / 1e9
                rtsp_clients = self._rtsp_server.active_session_count

                LOGGER.info(
                    "Heartbeat upstream=%s last_rtp=%.1fs pps=%.0f kbps=%.0f "
                    "lifetime_pkts=%d rtsp_clients=%d",
                    upstream_state,
                    idle_s,
                    pps,
                    kbps,
                    self._packets_forwarded,
                    rtsp_clients,
                )
        except asyncio.CancelledError:
            return

    async def request_keyframe(self) -> None:
        """Send a PLI to Agora asking for an IDR.

        Called by the RTSP server when a new client connects, so the viewer
        does not have to wait for the next naturally-scheduled keyframe.
        Silently no-ops if upstream is not ready or the SSRC is unknown.
        """
        receiver = self._video_receiver
        ssrc = self._video_ssrc
        if receiver is None or ssrc is None:
            return
        try:
            await receiver._send_rtcp_pli(ssrc)
        except Exception:  # noqa: BLE001
            LOGGER.debug("PLI to upstream failed", exc_info=True)

    # ------------------------------------------------------------------
    # Supervisor loop — reconnect-with-backoff
    # ------------------------------------------------------------------

    async def _supervise(self) -> None:
        # Two backoff regimes, picked per cycle by whether any H265 RTP packet
        # arrived this round:
        #
        #   * Publisher stall (we did get RTP, then it stopped) — fast recovery
        #     at the immediate ``reconnect_backoff_seconds``. The mower is
        #     reachable, just paused; we want a sub-15s gap.
        #   * Mower offline (no RTP at all — Agora signaling timeouts, "device
        #     not responding" from cloud, etc.) — exponential ramp up to
        #     ``max_reconnect_backoff_seconds``. Hammering MQTT/Agora doesn't
        #     wake a sleeping mower and burns the pymammotion 300/24h budget.
        offline_backoff = self._reconnect_backoff
        while not self._stop_event.is_set():
            healthy_this_cycle = False
            try:
                await self._run_one_upstream()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                LOGGER.exception("Upstream connection failed")
            finally:
                # _cycle_packets is reset at the top of _run_one_upstream and
                # not touched by teardown, so it's still valid here. We treat a
                # cycle as healthy only if it carried a *sustained* stream —
                # not a stray IDR or two from a publisher that quit ~1s after
                # joining. Fast-retrying those trickle cycles is what used to
                # hammer the cloud (a wake + a stream-token fetch every ~1s),
                # which on an old-firmware device burns pymammotion's 300/24h
                # MQTT send budget and ends in a 12h self-imposed ban.
                healthy_this_cycle = (
                    self._cycle_packets >= self._healthy_packet_threshold
                )
                await self._teardown_upstream()

            if healthy_this_cycle:
                # Genuine mid-stream stall after real video — reset the ramp
                # and retry fast; the mower is reachable, just paused.
                backoff = self._reconnect_backoff
                offline_backoff = self._reconnect_backoff
                LOGGER.info(
                    "Publisher stalled mid-stream; retrying in %.1fs", backoff
                )
            else:
                # No sustained video this cycle — mower offline, refusing to
                # publish, or join/quit churning. Ramp the backoff so we don't
                # poke the cloud (and spend MQTT budget) on a tight loop.
                backoff = offline_backoff
                offline_backoff = min(
                    offline_backoff * 2, self._max_reconnect_backoff
                )
                LOGGER.warning(
                    "No sustained video this cycle (mower offline or "
                    "join/quit churning); backing off %.1fs "
                    "(next attempt's backoff: %.1fs)",
                    backoff,
                    offline_backoff,
                )

            if self._stop_event.is_set():
                return
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                # If we got here without timing out, stop was requested.
                return
            except asyncio.TimeoutError:
                pass

    async def _run_one_upstream(self) -> None:
        """Bring up one Agora subscription and pump RTP until it dies."""
        LOGGER.info("Starting upstream Agora subscription")
        # Zero the per-cycle tally up front so a cycle that fails before the
        # track arrives (e.g. signaling timeout) can't inherit the previous
        # cycle's count and be misclassified as healthy by the supervisor.
        self._cycle_packets = 0

        if self._publisher_wakeup is not None:
            try:
                await self._publisher_wakeup()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Publisher wakeup failed; continuing anyway")

        credentials = await self._credentials_provider()
        agora_response = await self._agora_context_provider(credentials)

        pc = RTCPeerConnection()
        pc.addTransceiver("video", direction="recvonly")
        pc.addTransceiver("audio", direction="recvonly")

        # Per-attempt state for the track and connection-state events.
        track_received = asyncio.Event()
        connection_failed = asyncio.Event()
        video_track_ref: dict[str, MediaStreamTrack] = {}

        @pc.on("track")
        def _on_track(track: MediaStreamTrack) -> None:
            LOGGER.info(
                "Upstream aiortc track received kind=%s id=%s", track.kind, track.id
            )
            if track.kind != "video" or "video" in video_track_ref:
                return
            video_track_ref["video"] = track
            self._install_rtp_tap(pc, track)
            track_received.set()

        @pc.on("connectionstatechange")
        def _on_state() -> None:
            state = pc.connectionState
            LOGGER.info("Upstream aiortc connectionState=%s", state)
            if state in ("failed", "closed", "disconnected"):
                connection_failed.set()

        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        handler = AgoraWebSocketHandler(
            prefer_instant_video=True,
            subscribe_retry_delay=1.0,
            subscribe_retry_attempts=3,
            declare_remote_video_ssrc=True,
        )
        session_id = secrets.token_hex(16)

        # Track these so _teardown_upstream can clean them up even if we
        # bail out partway through this method.
        self._upstream_pc = pc
        self._upstream_handler = handler

        answer_sdp = await handler.connect_and_join(
            live_feed=credentials.to_agora_credentials(),
            offer_sdp=pc.localDescription.sdp,
            session_id=session_id,
            app_id=credentials.app_id,
            agora_response=agora_response,
        )
        if not answer_sdp:
            raise RuntimeError("Agora signaling returned no answer SDP")

        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer_sdp, type="answer")
        )

        try:
            await asyncio.wait_for(
                track_received.wait(), timeout=self._upstream_track_timeout
            )
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Upstream video track did not arrive within "
                f"{self._upstream_track_timeout:.1f}s"
            )

        LOGGER.info("Upstream ready (session=%s); pumping RTP to RTSP server", session_id)

        # Reset the watchdog clock at the moment we're ready to receive. Some
        # initial silence is normal while the publisher uploads parameter sets
        # and the first IDR; the watchdog only fires if the silence persists.
        self._last_rtp_ns = time.monotonic_ns()

        async def _no_rtp_watchdog() -> None:
            # Poll twice per window so the worst-case detection latency is
            # ~0.5 * window and we don't spin too aggressively.
            interval = max(1.0, self._no_rtp_watchdog / 2)
            threshold_ns = int(self._no_rtp_watchdog * 1e9)
            tried_cheap_recovery = False
            while True:
                await asyncio.sleep(interval)
                idle_ns = time.monotonic_ns() - self._last_rtp_ns
                if idle_ns < threshold_ns:
                    # Stream healthy; if we'd previously tried a cheap
                    # recovery, reset the flag so a future stall gets one too.
                    tried_cheap_recovery = False
                    continue

                # Stalled. First try a cheap recovery — the bridge wires this
                # to refresh_stream_subscription, which re-fetches a token and
                # tells the mower to rejoin without us tearing down the
                # upstream PC. Only escalate to a full teardown if that
                # doesn't restore RTP within cheap_recovery_wait_seconds.
                if (
                    not tried_cheap_recovery
                    and self._cheap_recovery is not None
                ):
                    LOGGER.info(
                        "No upstream RTP for %.1fs; trying cheap recovery "
                        "before teardown",
                        idle_ns / 1e9,
                    )
                    tried_cheap_recovery = True
                    try:
                        await self._cheap_recovery()
                    except Exception:  # noqa: BLE001
                        LOGGER.debug("cheap recovery raised", exc_info=True)
                    # Give the mower a chance to react. Check RTP after the
                    # configured wait — if it resumed, the next loop iter
                    # sees idle_ns under threshold and resets the flag.
                    await asyncio.sleep(self._cheap_recovery_wait)
                    if time.monotonic_ns() - self._last_rtp_ns < threshold_ns:
                        LOGGER.info("Cheap recovery worked — RTP resumed")
                        tried_cheap_recovery = False
                        continue
                    LOGGER.warning(
                        "Cheap recovery didn't restore RTP; escalating to "
                        "full teardown"
                    )

                LOGGER.warning(
                    "No upstream RTP for %.1fs (publisher likely stalled); "
                    "tearing down to force reconnect",
                    (time.monotonic_ns() - self._last_rtp_ns) / 1e9,
                )
                connection_failed.set()
                return

        # Hold until the connection dies, the watchdog fires, or we are asked
        # to stop. The actual RTP forwarding happens in the receiver-hook
        # coroutine; this task just supervises.
        stop_wait = asyncio.create_task(self._stop_event.wait())
        failed_wait = asyncio.create_task(connection_failed.wait())
        watchdog_task = asyncio.create_task(_no_rtp_watchdog())
        try:
            await asyncio.wait(
                {stop_wait, failed_wait, watchdog_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (stop_wait, failed_wait, watchdog_task):
                if not task.done():
                    task.cancel()

    # ------------------------------------------------------------------
    # RTP tap — the actual passthrough
    # ------------------------------------------------------------------

    def _install_rtp_tap(self, pc: RTCPeerConnection, track: MediaStreamTrack) -> None:
        """Hook the receiver's RTP handler so each H265 packet goes to RTSP.

        We wrap ``receiver._handle_rtp_packet`` rather than tapping deeper
        because that point lets us still call the original handler — which
        keeps RTCP RR / NACK / PLI flowing back to Agora. Without those,
        Agora's edge eventually decides the subscription is stale and
        either stops sending or drops the session.

        The original handler is left in charge of RTX unwrapping; we mirror
        that work locally so our tap sees the recovered packet too. Losing
        RTX recovery would translate to visible glitches on any path with
        non-trivial loss.
        """
        receiver = None
        for transceiver in pc.getTransceivers():
            if transceiver.receiver is not None and transceiver.receiver.track is track:
                receiver = transceiver.receiver
                break
        if receiver is None:
            LOGGER.error("Could not locate RTCRtpReceiver for video track")
            return

        self._video_receiver = receiver
        original_handler = receiver._handle_rtp_packet
        rtsp_server = self._rtsp_server

        # Pull the receiver's private name-mangled attributes once. These
        # are stable across aiortc 1.13.x — if a future version reorganises
        # them, the patch reapplication step in h265_patch.py is where we
        # would notice.
        codecs_by_pt = receiver._RTCRtpReceiver__codecs  # noqa: SLF001
        rtx_ssrc_map = receiver._RTCRtpReceiver__rtx_ssrc  # noqa: SLF001

        relay_self = self

        async def tap(packet: RtpPacket, arrival_time_ms: int) -> None:
            try:
                effective = _unwrap_rtx_if_needed(packet, codecs_by_pt, rtx_ssrc_map)
                codec = codecs_by_pt.get(effective.payload_type)
                if codec is not None and codec.mimeType.lower() in h265_patch.H265_MIMETYPES:
                    ssrc_just_learned = relay_self._video_ssrc is None
                    if ssrc_just_learned:
                        relay_self._video_ssrc = effective.ssrc
                    if effective.payload:
                        now_ns = time.monotonic_ns()
                        relay_self._last_rtp_ns = now_ns
                        relay_self._cycle_packets += 1
                        # Only a sustained stream advances the health clock;
                        # the first trickle packets of a churn cycle do not.
                        if (
                            relay_self._cycle_packets
                            >= relay_self._healthy_packet_threshold
                        ):
                            relay_self._last_healthy_ns = now_ns
                        relay_self._packets_forwarded += 1
                        relay_self._bytes_forwarded += len(effective.payload)
                        rtsp_server.push_rtp(
                            payload=bytes(effective.payload),
                            timestamp=effective.timestamp,
                            marker=bool(effective.marker),
                        )
                    if ssrc_just_learned:
                        # First H265 packet of a new upstream session. The
                        # mower is mid-GOP and we got handed P-frames — no
                        # parameter sets in them, so RTSP DESCRIBE would
                        # block its 10 s wait and go2rtc would time out at
                        # 5 s. Fire one PLI immediately to force the mower
                        # to emit a fresh IDR (which prepends VPS/SPS/PPS).
                        # Detached task so the tap stays non-blocking.
                        LOGGER.info(
                            "First H265 RTP received (ssrc=%d); requesting IDR",
                            effective.ssrc,
                        )
                        asyncio.create_task(relay_self.request_keyframe())
            except Exception:  # noqa: BLE001
                LOGGER.exception("RTP tap failed; dropping packet")
            await original_handler(packet, arrival_time_ms=arrival_time_ms)

        receiver._handle_rtp_packet = tap

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    async def _teardown_upstream(self) -> None:
        handler = self._upstream_handler
        pc = self._upstream_pc
        self._upstream_handler = None
        self._upstream_pc = None
        self._video_receiver = None
        self._video_ssrc = None

        if handler is not None:
            try:
                await handler.disconnect()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Agora handler disconnect failed")
        if pc is not None:
            try:
                await pc.close()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Upstream PC close failed")


def _unwrap_rtx_if_needed(
    packet: RtpPacket,
    codecs_by_pt: dict,
    rtx_ssrc_map: dict,
) -> RtpPacket:
    """Mirror :meth:`RTCRtpReceiver._handle_rtp_packet`'s RTX unwrap.

    Returns the original packet unchanged if it is not RTX or if any input
    needed for unwrap is missing. The receiver's own copy of this logic
    still runs (we did not disturb the original handler), so this is purely
    for our tap.
    """
    codec = codecs_by_pt.get(packet.payload_type)
    if codec is None or codec.name.lower() != "rtx":
        return packet
    original_ssrc = rtx_ssrc_map.get(packet.ssrc)
    apt = codec.parameters.get("apt")
    if (
        original_ssrc is None
        or not isinstance(apt, int)
        or apt not in codecs_by_pt
        or len(packet.payload) < 2
    ):
        return packet
    try:
        return unwrap_rtx(packet, payload_type=apt, ssrc=original_ssrc)
    except Exception:  # noqa: BLE001
        return packet
