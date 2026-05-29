"""aiortc-based Agora ↔ go2rtc passthrough relay.

Bridges around Pion's interop issue with Agora's edge by putting a
libwebrtc-equivalent stack (aiortc, Python) in the path:

  Mower → Agora SFU → [aiortc upstream PC in bridge] ─MediaRelay─→
                                                         │
              [per-downstream aiortc PC] → go2rtc (Pion) → Frigate

Both ends now speak standards-compliant WebRTC against aiortc. Pion never
talks to Agora's edge directly. No transcoding, no ffmpeg: H265 RTP packets
flow through aiortc as a relayed MediaStreamTrack.

Lifecycle:

* The upstream Agora subscription is lazily started on the first downstream
  request and shared across all current downstream consumers via
  :class:`aiortc.contrib.media.MediaRelay`.
* When the last downstream PC disconnects, the upstream is kept warm for
  ``grace_seconds`` then torn down so we are not paying for an idle Agora
  session.
* If the upstream PC fails (connectionState=failed/closed), it is dropped;
  the next downstream request triggers a fresh subscription.

This module intentionally keeps no fancy session reuse, no health tracking,
no replacement guards. The aiortc-side connections do their own health
management; we just spin them up/down.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable

from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaRelay
from aiortc.mediastreams import MediaStreamTrack
from aiortc.sdp import candidate_from_sdp

from .agora_edge import (
    AgoraResponse,
    AgoraWebSocketHandler,
)

LOGGER = logging.getLogger(__name__)


# Type aliases for the callbacks the bridge entrypoint provides.
CredentialsProvider = Callable[[], Awaitable["object"]]
AgoraContextProvider = Callable[["object"], Awaitable[AgoraResponse]]
PublisherWakeup = Callable[[], Awaitable[None]] | None


class AgoraAiortcRelay:
    """One stream of Mammotion video, relayed through aiortc.

    A single instance owns at most one upstream Agora ``RTCPeerConnection``
    and synthesises a fresh downstream ``RTCPeerConnection`` per consumer
    that subscribes to the same upstream tracks via ``MediaRelay``.
    """

    def __init__(
        self,
        credentials_provider: CredentialsProvider,
        agora_context_provider: AgoraContextProvider,
        *,
        publisher_wakeup: PublisherWakeup = None,
        upstream_track_timeout_seconds: float = 15.0,
        idle_grace_seconds: float = 30.0,
    ) -> None:
        self._credentials_provider = credentials_provider
        self._agora_context_provider = agora_context_provider
        self._publisher_wakeup = publisher_wakeup
        self._upstream_track_timeout = upstream_track_timeout_seconds
        self._idle_grace = idle_grace_seconds

        self._lock = asyncio.Lock()
        self._upstream_pc: RTCPeerConnection | None = None
        self._upstream_handler: AgoraWebSocketHandler | None = None
        self._upstream_session_id: str | None = None
        self._video_track: MediaStreamTrack | None = None
        self._audio_track: MediaStreamTrack | None = None
        self._tracks_ready = asyncio.Event()
        self._relay = MediaRelay()
        self._downstream_pcs: set[RTCPeerConnection] = set()
        self._idle_cleanup_task: asyncio.Task[None] | None = None

    @property
    def active_downstream_count(self) -> int:
        return len(self._downstream_pcs)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def negotiate_downstream(
        self, offer_sdp: str
    ) -> tuple[RTCPeerConnection, str]:
        """Handle one downstream offer, return (pc, answer_sdp).

        The PC handle is returned so the caller can feed trickled ICE
        candidates from the downstream client via :meth:`add_remote_candidate`.
        """
        await self._ensure_upstream()
        if self._video_track is None:
            raise RuntimeError("Upstream video track unavailable; cannot relay")

        pc = RTCPeerConnection()
        pc.addTrack(self._relay.subscribe(self._video_track))
        if self._audio_track is not None:
            pc.addTrack(self._relay.subscribe(self._audio_track))

        @pc.on("connectionstatechange")
        async def _on_state() -> None:
            LOGGER.info("Downstream PC connectionState=%s", pc.connectionState)
            if pc.connectionState in ("failed", "closed", "disconnected"):
                await self._detach_downstream(pc)

        await pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)

        self._downstream_pcs.add(pc)
        self._cancel_idle_cleanup()
        LOGGER.info(
            "Relayed downstream attached; active_downstream=%d",
            len(self._downstream_pcs),
        )
        return pc, pc.localDescription.sdp

    async def add_remote_candidate(
        self,
        pc: RTCPeerConnection,
        candidate_line: str,
        sdp_m_line_index: int | None = None,
        sdp_mid: str | None = None,
    ) -> None:
        """Parse one ``candidate:`` line and add it to a downstream PC.

        ``candidate_line`` may begin with ``a=candidate:`` or just
        ``candidate:``; both are accepted. Empty string is treated as
        end-of-candidates and silently ignored.
        """
        if pc not in self._downstream_pcs:
            return
        raw = (candidate_line or "").strip()
        if raw.startswith("a="):
            raw = raw[2:]
        if not raw or not raw.startswith("candidate:"):
            return
        try:
            candidate = candidate_from_sdp(raw[len("candidate:"):])
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to parse downstream ICE candidate: %s", raw)
            return
        if sdp_m_line_index is not None:
            candidate.sdpMLineIndex = sdp_m_line_index
        if sdp_mid is not None:
            candidate.sdpMid = sdp_mid
        try:
            await pc.addIceCandidate(candidate)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Failed to add downstream ICE candidate to PC")

    async def close_downstream(self, pc: RTCPeerConnection) -> None:
        """Explicit teardown for one downstream PC (e.g. WS disconnect)."""
        await self._detach_downstream(pc)

    async def close(self) -> None:
        """Drop all downstream PCs and the upstream subscription."""
        async with self._lock:
            for pc in list(self._downstream_pcs):
                try:
                    await pc.close()
                except Exception:  # noqa: BLE001
                    LOGGER.exception("Failed to close downstream PC during shutdown")
            self._downstream_pcs.clear()
            await self._close_upstream_locked()
            self._cancel_idle_cleanup()

    # ------------------------------------------------------------------
    # Upstream lifecycle (lock-held)
    # ------------------------------------------------------------------

    async def _ensure_upstream(self) -> None:
        async with self._lock:
            if self._upstream_alive():
                return
            await self._start_upstream_locked()

    def _upstream_alive(self) -> bool:
        pc = self._upstream_pc
        if pc is None:
            return False
        return pc.connectionState in ("new", "connecting", "connected")

    async def _start_upstream_locked(self) -> None:
        LOGGER.info("Starting aiortc upstream subscription to Agora")

        if self._publisher_wakeup is not None:
            try:
                await self._publisher_wakeup()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Publisher wakeup failed; continuing")

        credentials = await self._credentials_provider()
        agora_response = await self._agora_context_provider(credentials)

        pc = RTCPeerConnection()
        pc.addTransceiver("video", direction="recvonly")
        pc.addTransceiver("audio", direction="recvonly")

        self._tracks_ready.clear()
        self._video_track = None
        self._audio_track = None

        @pc.on("track")
        def _on_track(track: MediaStreamTrack) -> None:
            LOGGER.info("Upstream aiortc track received kind=%s id=%s", track.kind, track.id)
            if track.kind == "video" and self._video_track is None:
                self._video_track = track
                self._tracks_ready.set()
            elif track.kind == "audio" and self._audio_track is None:
                self._audio_track = track

        @pc.on("connectionstatechange")
        async def _on_state() -> None:
            LOGGER.info("Upstream aiortc connectionState=%s", pc.connectionState)
            if pc.connectionState in ("failed", "closed", "disconnected"):
                async with self._lock:
                    if self._upstream_pc is pc:
                        await self._close_upstream_locked()

        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        handler = AgoraWebSocketHandler(
            prefer_instant_video=True,
            subscribe_retry_delay=1.0,
            subscribe_retry_attempts=3,
            declare_remote_video_ssrc=True,
        )
        session_id = secrets.token_hex(16)
        try:
            answer_sdp = await handler.connect_and_join(
                live_feed=credentials.to_agora_credentials(),
                offer_sdp=pc.localDescription.sdp,
                session_id=session_id,
                app_id=credentials.app_id,
                agora_response=agora_response,
            )
        except Exception:
            await pc.close()
            raise

        if not answer_sdp:
            await pc.close()
            try:
                await handler.disconnect()
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError("Agora signaling returned no answer SDP")

        try:
            await pc.setRemoteDescription(
                RTCSessionDescription(sdp=answer_sdp, type="answer")
            )
        except Exception:
            await pc.close()
            try:
                await handler.disconnect()
            except Exception:  # noqa: BLE001
                pass
            raise

        self._upstream_pc = pc
        self._upstream_handler = handler
        self._upstream_session_id = session_id

        try:
            await asyncio.wait_for(self._tracks_ready.wait(), timeout=self._upstream_track_timeout)
        except asyncio.TimeoutError:
            LOGGER.error(
                "Upstream video track did not arrive within %.1fs; closing",
                self._upstream_track_timeout,
            )
            await self._close_upstream_locked()
            raise

        LOGGER.info(
            "Upstream aiortc subscription ready session=%s", self._upstream_session_id
        )

    async def _close_upstream_locked(self) -> None:
        if self._upstream_handler is not None:
            try:
                await self._upstream_handler.disconnect()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Failed to disconnect Agora handler")
            self._upstream_handler = None

        if self._upstream_pc is not None:
            try:
                await self._upstream_pc.close()
            except Exception:  # noqa: BLE001
                LOGGER.exception("Failed to close upstream PC")
            self._upstream_pc = None

        self._upstream_session_id = None
        self._video_track = None
        self._audio_track = None
        self._tracks_ready.clear()

    # ------------------------------------------------------------------
    # Downstream lifecycle
    # ------------------------------------------------------------------

    async def _detach_downstream(self, pc: RTCPeerConnection) -> None:
        if pc not in self._downstream_pcs:
            return
        self._downstream_pcs.discard(pc)
        LOGGER.info(
            "Downstream PC detached; active_downstream=%d",
            len(self._downstream_pcs),
        )
        try:
            await pc.close()
        except Exception:  # noqa: BLE001
            pass
        if not self._downstream_pcs:
            self._schedule_idle_cleanup()

    def _schedule_idle_cleanup(self) -> None:
        if self._idle_grace <= 0:
            asyncio.create_task(self._idle_cleanup_now())
            return

        if self._idle_cleanup_task is not None and not self._idle_cleanup_task.done():
            return

        async def _later() -> None:
            try:
                await asyncio.sleep(self._idle_grace)
            except asyncio.CancelledError:
                return
            async with self._lock:
                if not self._downstream_pcs:
                    LOGGER.info(
                        "Idle grace expired (%.1fs); closing upstream",
                        self._idle_grace,
                    )
                    await self._close_upstream_locked()

        self._idle_cleanup_task = asyncio.create_task(_later())

    async def _idle_cleanup_now(self) -> None:
        async with self._lock:
            if not self._downstream_pcs:
                await self._close_upstream_locked()

    def _cancel_idle_cleanup(self) -> None:
        task = self._idle_cleanup_task
        if task is not None and not task.done():
            task.cancel()
        self._idle_cleanup_task = None
