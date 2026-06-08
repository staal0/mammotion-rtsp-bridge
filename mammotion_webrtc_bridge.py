#!/usr/bin/env python3
"""Mammotion Agora → RTSP passthrough bridge.

Single-process bridge for delivering the Mammotion mower's H265 camera into
go2rtc/Frigate without ffmpeg, transcoding, or a second WebRTC hop. Flow:

1. Log into Mammotion via pymammotion and fetch the Agora stream
   subscription (appid/channelName/token/uid/areaCode) for one device.
2. Run a supervisor that brings up an aiortc upstream peer connection to
   Agora as a passive viewer and taps the inbound H265 RTP packets
   (:class:`mammotion_webrtc.AgoraToRtspRelay`).
3. Serve those packets verbatim over a minimal RTSP server bound on
   ``MAMMOTION_RTSP_PORT`` (default 8554), mount ``/mammotion``.
4. Register ``rtsp://<self>:<port>/mammotion`` with go2rtc via its REST
   API so Frigate discovers the stream automatically.
5. Keep the mower in the Agora channel by sending MQTT
   ``send_todev_ble_sync`` every ~10s. The publisher times out without
   this nudge; Mammotion has no RTM heartbeat.

Environment variables:
  MAMMOTION_EMAIL / MAMMOTION_PASSWORD     - cloud credentials (required)
  MAMMOTION_DEVICE_NAME                    - device to stream ("" / "first" = first)
  MAMMOTION_RTSP_PORT                      - RTSP listen port (default 8554)
  MAMMOTION_RTSP_HOST                      - host go2rtc uses to reach us
                                             (default: container hostname)
  MAMMOTION_RTSP_BIND                      - bind address (default 0.0.0.0)
  MAMMOTION_STREAM_NAME                    - go2rtc stream name (default mammotion)
  GO2RTC_API_URL                           - go2rtc REST base (default http://frigate:1984)
  MAMMOTION_GO2RTC_RECONCILE_SECONDS       - periodic re-register interval (default 20)
  MAMMOTION_KEEPALIVE_SECONDS              - MQTT keep-alive interval (default 10).
                                             Must be well under the mower's
                                             publisher-idle timeout (~50s) or
                                             the publisher will leave the
                                             channel between keep-alives and
                                             we'll churn on cheap recovery.
  MAMMOTION_NO_RTP_WATCHDOG_SECONDS        - tear down upstream + reconnect if no
                                             H265 RTP packet for this many seconds
                                             (default 5)
  MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS    - on stall, wait this long after the
                                             cheap-recovery call
                                             (refresh_stream_subscription)
                                             before escalating to a full
                                             teardown (default 5)
  MAMMOTION_DRY_RESTART_SECONDS            - if no *sustained* H265 stream
                                             (i.e. ``seconds_since_healthy``,
                                             which only advances during a real
                                             stream and is unaffected by churn
                                             trickle) for this many consecutive
                                             seconds, tear down the WHOLE
                                             bridge in-process (relay + RTSP +
                                             pymammotion login) and
                                             re-bootstrap from scratch. Escapes
                                             the "stuck for hours" failure mode
                                             where the in-relay reconnect loop
                                             can't wake the publisher because
                                             of a stale cloud session — and
                                             resets pymammotion's per-client
                                             send counter so we get out from
                                             under the 300-sends/24h MQTT ban.
                                             Default 90 (v0.1.19+ keys this
                                             off seconds_since_healthy, which
                                             only advances during real
                                             streams, so a 90 s threshold is
                                             as honest as 180 s used to be on
                                             the old churn-defeated clock).
                                             Works without any docker restart
                                             policy — no process exit, just
                                             an in-process reset.
  MAMMOTION_RECONNECT_BACKOFF_SECONDS      - login retry backoff (default 8)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
from typing import Any

from mammotion_webrtc import __version__ as BRIDGE_VERSION
from mammotion_webrtc.agora_session import (
    StreamCredentials,
    refresh_agora_context,
)
from mammotion_webrtc.aiortc_relay import AgoraToRtspRelay
from mammotion_webrtc.go2rtc_register import Go2RTCStreamRegistrar
from mammotion_webrtc.rtsp_server import Go2RtcRtspStream

LOGGER = logging.getLogger("mammotion_webrtc_bridge")

# Minimum seconds between get_stream_subscription calls. Each call re-triggers
# the mower's publish but also hits the cloud, so we debounce rapid retries
# to avoid an account lockout.
CREDS_DEBOUNCE_S = 15.0

# Minimum seconds between refresh_stream_subscription calls. On old-firmware
# devices this sends a budgeted MQTT join command (see _refresh_stream_subscription),
# so re-poking faster than this on a churning mower would exhaust pymammotion's
# 300-sends/24h budget and trip a 12h self-imposed ban. Kept comfortably above
# the watchdog's stall cadence so a flapping publisher can't drive it.
REFRESH_SUB_DEBOUNCE_S = 20.0

# Mirrors the AREA_CODE_MAP in the main-branch bridge, but maps to the
# "CN,GLOBAL"-style strings the REST choose_server API expects.
AREA_CODE_STRING_MAP = {
    "AREA_CODE_CN": "CN",
    "AREA_CODE_NA": "NA",
    "AREA_CODE_EU": "EU",
    "AREA_CODE_AS": "AS",
    "AREA_CODE_JP": "JP",
    "AREA_CODE_IN": "IN",
    "AREA_CODE_GLOB": "GLOBAL",
}


def resolve_area_code_string(value: Any) -> str:
    """Map a Mammotion areaCode to a choose_server area_code string."""
    if isinstance(value, str) and value in AREA_CODE_STRING_MAP:
        region = AREA_CODE_STRING_MAP[value]
        return "CN,GLOBAL" if region in ("GLOBAL", "CN") else f"{region},GLOBAL"
    return "CN,GLOBAL"


async def fetch_stream_fields(mammotion: Any, device_name: str) -> dict[str, Any]:
    """Fetch Agora stream subscription fields for one device."""
    selected_name = (device_name or "").strip()
    if not selected_name or selected_name.lower() == "first":
        all_devices = mammotion.device_registry.all_devices
        if not all_devices:
            raise RuntimeError("No devices found in Mammotion account")
        device_handle = all_devices[0]
        selected_name = device_handle.device_name
        LOGGER.info("Auto-selected first device: %s", selected_name)
    else:
        device_handle = mammotion.device_registry.get_by_name(selected_name)
        if device_handle is None:
            raise RuntimeError(f"Device not found: {selected_name}")

    iot_id = device_handle.iot_id
    stream_response = await mammotion.get_stream_subscription(selected_name, iot_id)
    data = getattr(stream_response, "data", None)
    if data is None:
        raise RuntimeError(f"Stream response has no data: {stream_response}")

    return {
        "appid": getattr(data, "appid", None),
        "channelName": getattr(data, "channelName", None),
        "token": getattr(data, "token", None),
        "uid": getattr(data, "uid", None),
        "areaCode": getattr(data, "areaCode", None),
        "iot_id": iot_id,
        "device_name": selected_name,
    }


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


class _DryWatchdogTripped(Exception):
    """Internal signal: no H265 RTP for too long → full in-process restart.

    Raised from the keep-alive loop when ``relay.seconds_since_healthy``
    exceeds ``MAMMOTION_DRY_RESTART_SECONDS``. The outer ``main`` loop
    catches it, tears down the current session (relay, RTSP server,
    pymammotion client), and re-enters :func:`_run_bridge_session` with
    a fully fresh state. Never propagates out of ``main``.
    """


async def main() -> None:
    log_level = os.getenv("MAMMOTION_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    LOGGER.info("Mammotion RTSP bridge v%s starting", BRIDGE_VERSION)

    email = os.getenv("MAMMOTION_EMAIL", "")
    password = os.getenv("MAMMOTION_PASSWORD", "")
    device_name = os.getenv("MAMMOTION_DEVICE_NAME", "")
    if not email or not password:
        raise SystemExit(
            "Missing Mammotion credentials. Set MAMMOTION_EMAIL/MAMMOTION_PASSWORD."
        )

    config = {
        "email": email,
        "password": password,
        "device_name": device_name,
        "rtsp_port": _env_int("MAMMOTION_RTSP_PORT", 8554),
        "rtsp_bind": os.getenv("MAMMOTION_RTSP_BIND", "0.0.0.0"),
        "rtsp_host": os.getenv("MAMMOTION_RTSP_HOST", socket.gethostname()),
        "go2rtc_api_url": os.getenv("GO2RTC_API_URL", "http://frigate:1984"),
        "stream_name": os.getenv("MAMMOTION_STREAM_NAME", "mammotion"),
        "go2rtc_reconcile_interval": float(
            _env_int("MAMMOTION_GO2RTC_RECONCILE_SECONDS", 20)
        ),
        # 10 s keeps us well under Mammotion's ~50 s publisher-idle timeout.
        # A 300 s default (a previous regression) left a ~4-minute window in
        # every cycle where the mower received no "viewer present" signal
        # and dropped its Agora publish, forcing the watchdog to recover
        # every keepalive cycle — visible as constant cheap-recovery churn.
        "keepalive_interval": float(_env_int("MAMMOTION_KEEPALIVE_SECONDS", 10)),
        "reconnect_backoff": _env_int("MAMMOTION_RECONNECT_BACKOFF_SECONDS", 8),
        "no_rtp_watchdog_seconds": float(
            _env_int("MAMMOTION_NO_RTP_WATCHDOG_SECONDS", 5)
        ),
        # 5 s gives refresh_stream_subscription enough time to round-trip
        # (HTTP token fetch + MQTT publish to mower + mower rejoin + first
        # H265 packet back). The previous 3 s sometimes timed out before
        # the recovery actually worked, leading to spurious full teardowns.
        "cheap_recovery_wait_seconds": float(
            _env_int("MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS", 5)
        ),
        # Dryness watchdog: when the in-relay reconnect loop has been unable
        # to fetch a single H265 packet for this many consecutive seconds,
        # we tear the WHOLE bridge down in-process (relay, RTSP server,
        # pymammotion client) and re-bootstrap from a fresh login. The
        # in-relay watchdog already handles short publisher stalls
        # (~5-10s); this one is the escape from "stuck for hours" failure
        # modes — typically a stale pymammotion/MQTT session that needs a
        # clean re-login. Set high enough that ordinary publisher stalls
        # don't trigger it.
        #
        # In-process restart works without any container restart policy.
        # We deliberately do NOT call sys.exit / os._exit here, so users
        # without ``restart: unless-stopped`` don't end up with a crashed
        # container.
        "dry_restart_seconds": float(
            _env_int("MAMMOTION_DRY_RESTART_SECONDS", 90)
        ),
    }

    LOGGER.info("Loading Mammotion SDK modules")
    from pymammotion.client import MammotionClient

    stop_async = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_async.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_async.set())

    # Outer retry loop: each iteration is one fully-isolated session. On
    # graceful stop the inner function returns and we exit. On dryness
    # watchdog trip the inner function raises _DryWatchdogTripped, all
    # resources are torn down inside it, and we re-enter for a fresh start.
    while not stop_async.is_set():
        try:
            await _run_bridge_session(stop_async, MammotionClient, config)
            return
        except _DryWatchdogTripped as exc:
            LOGGER.warning(
                "Dry watchdog tripped (%s) — restarting bridge in-process", exc
            )
            # Tiny pause so we don't tight-loop if something is permanently
            # broken upstream. The relay's own backoff is the primary brake;
            # this is just for the outer cycle.
            try:
                await asyncio.wait_for(stop_async.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass


async def _run_bridge_session(
    stop_async: asyncio.Event,
    MammotionClient: Any,
    config: dict[str, Any],
) -> None:
    """One full bootstrap → run → cleanup cycle.

    Returns normally on graceful stop. Raises :class:`_DryWatchdogTripped`
    if the relay reports no H265 RTP for ``config['dry_restart_seconds']``
    consecutive seconds — the outer ``main`` loop catches it and re-enters.
    All resources allocated here (relay, RTSP server, pymammotion client)
    are released in the ``finally`` block before the exception propagates.
    """
    email = config["email"]
    password = config["password"]
    device_name = config["device_name"]
    rtsp_port = config["rtsp_port"]
    rtsp_bind = config["rtsp_bind"]
    rtsp_host = config["rtsp_host"]
    go2rtc_api_url = config["go2rtc_api_url"]
    stream_name = config["stream_name"]
    go2rtc_reconcile_interval = config["go2rtc_reconcile_interval"]
    keepalive_interval = config["keepalive_interval"]
    reconnect_backoff = config["reconnect_backoff"]
    dry_restart_seconds = config["dry_restart_seconds"]

    rtsp_source = f"rtsp://{rtsp_host}:{rtsp_port}/{stream_name}"
    loop = asyncio.get_running_loop()

    # Shared state used by the credentials provider and keep-alive loop.
    state: dict[str, Any] = {
        "mammotion": None,
        "device_name": device_name,
        "iot_id": None,
        "credentials": None,
        "creds_fetched_at": 0.0,
        "refresh_sub_at": 0.0,
    }

    async def _fresh_client() -> Any | None:
        # A fresh client + full login every cycle. The pymammotion refresh
        # token goes stale after a few hours; starting fresh sidesteps a
        # dead session.
        while not stop_async.is_set():
            client = MammotionClient(ha_version="3.4.23")
            try:
                LOGGER.info("Logging in to Mammotion cloud")
                await client.login_and_initiate_cloud(email, password)
                return client
            except Exception:
                LOGGER.exception(
                    "Mammotion login failed; retrying in %ss", reconnect_backoff
                )
                try:
                    await client.stop()
                except Exception:
                    pass
                await asyncio.sleep(reconnect_backoff)
        return None

    def _creds_from_fields(fields: dict[str, Any]) -> StreamCredentials:
        return StreamCredentials(
            app_id=str(fields["appid"]),
            channel=str(fields["channelName"]),
            rtc_token=str(fields["token"]),
            uid=int(fields["uid"]),
            area_code=resolve_area_code_string(fields.get("areaCode")),
        )

    async def credentials_provider() -> StreamCredentials:
        """Return Agora credentials, debounced.

        ``get_stream_subscription`` does double duty: it returns the RTC
        token AND sends the MQTT command that tells the mower to (re)join
        Agora. The relay calls us on every reconnect attempt; without a
        debounce we would hammer the cloud and risk a lockout.
        """
        mammotion = state["mammotion"]
        now = loop.time()
        last = state.get("creds_fetched_at", 0.0)
        cached = state.get("credentials")
        if mammotion is None or (cached is not None and now - last < CREDS_DEBOUNCE_S):
            if cached is None:
                raise RuntimeError("Mammotion credentials not ready")
            return cached
        try:
            fields = await fetch_stream_fields(mammotion, state["device_name"])
            for key in ("appid", "channelName", "token", "uid"):
                if not fields.get(key):
                    raise RuntimeError(f"Missing {key} in stream subscription payload")
            state["device_name"] = fields["device_name"]
            state["iot_id"] = fields["iot_id"]
            state["credentials"] = _creds_from_fields(fields)
            state["creds_fetched_at"] = now
            LOGGER.info("Re-triggered mower publish via get_stream_subscription")
        except Exception:
            LOGGER.exception("Credential refresh failed; using cached if available")
            if cached is None:
                raise
        return state["credentials"]

    async def _refresh_stream_subscription() -> bool:
        """Renew the Agora stream token and force the mower to (re)join.

        Wraps pymammotion's ``refresh_stream_subscription`` — this is the
        documented way to reconnect user 1 (the publisher) and works on both
        WiFi and 4G (unlike the now-removed ``refresh_fpv``, which only ever
        took effect over 4G). On success we also update the cached credentials
        so the next supervisor reconnect uses the fresh token instead of the
        stale one we already burned.

        DEBOUNCED. For old-firmware devices pymammotion's
        ``refresh_stream_subscription`` sends an MQTT
        ``device_agora_join_channel_with_position`` command that counts
        against the 300-sends/24h budget; firing it on every ~5s watchdog
        stall (the prior behaviour) burned the whole budget in well under an
        hour on a churning WiFi mower, after which the transport self-imposed
        a 12h ban and the stream died until a container restart. The debounce
        caps how fast we can re-poke the cloud; the bridge dryness watchdog is
        the real escape hatch for a wedged session (it re-logs in fresh, which
        resets the in-process budget counter).

        Returns True on success (or when skipped by the debounce), False on
        any error.
        """
        mammotion = state["mammotion"]
        device = state.get("device_name")
        iot_id = state.get("iot_id")
        if mammotion is None or not device or not iot_id:
            return False
        now = loop.time()
        last = state.get("refresh_sub_at", 0.0)
        if now - last < REFRESH_SUB_DEBOUNCE_S:
            LOGGER.debug(
                "Skipping refresh_stream_subscription (debounced, %.0fs < %.0fs)",
                now - last,
                REFRESH_SUB_DEBOUNCE_S,
            )
            return True
        state["refresh_sub_at"] = now
        try:
            response = await mammotion.refresh_stream_subscription(device, iot_id)
        except AttributeError:
            LOGGER.debug(
                "refresh_stream_subscription not available in this pymammotion version",
                exc_info=True,
            )
            return False
        except Exception:
            LOGGER.debug("refresh_stream_subscription failed", exc_info=True)
            return False
        # Best-effort: pull fresh credentials out of the response and cache
        # them so the next reconnect uses the new RTC token.
        data = getattr(response, "data", None) if response is not None else None
        if data is not None:
            fields = {
                "appid": getattr(data, "appid", None),
                "channelName": getattr(data, "channelName", None),
                "token": getattr(data, "token", None),
                "uid": getattr(data, "uid", None),
                "areaCode": getattr(data, "areaCode", None),
                "iot_id": iot_id,
                "device_name": device,
            }
            if all(fields[k] for k in ("appid", "channelName", "token", "uid")):
                state["credentials"] = _creds_from_fields(fields)
                state["creds_fetched_at"] = loop.time()
        return True

    async def _send_ble_sync_heartbeat(sync_type: int = 2) -> bool:
        """Send a ``ble_sync`` ping that doesn't burn the cloud budget.

        ``sync_type=2`` is the steady-state keepalive; ``sync_type=3`` is the
        wake nudge used during recovery. Both go through pymammotion's
        heartbeat path so neither counts against the 300-sends/24h MQTT budget.

        pymammotion's MQTT transports cap user-initiated sends at 300 per
        24 h ("self-imposing rate limit" warning). Calling
        ``send_command_with_args`` would count against that budget — at our
        10 s keepalive cadence we'd exhaust it in ~25 minutes and then the
        publisher idle-times out (no signal reaches it) for hours until the
        rolling window slides forward.

        pymammotion has a dedicated heartbeat path on each transport
        (``Transport.send_heartbeat``) which intentionally skips
        ``record_send()`` — its docstring explicitly says "periodic
        ble_sync pings don't burn the 300-sends/24 h budget". We go through
        that path here. Same MQTT message hits the mower, no budget cost.

        Falls back to ``send_command_with_args`` if anything in the
        pymammotion API surface has shifted in a future release.
        """
        mammotion = state["mammotion"]
        device = state.get("device_name")
        if mammotion is None or not device:
            return False
        try:
            # Lazy import so a pymammotion version without these symbols
            # only fails at the first heartbeat tick, not at bridge startup.
            from pymammotion.proto import TransportType
        except Exception:  # noqa: BLE001
            return await _fallback_ble_sync_via_command(mammotion, device, sync_type)
        try:
            handle = mammotion.device_registry.get_by_name(device)
            if handle is None:
                return False
            cmd_bytes = handle.commands.send_todev_ble_sync(sync_type=sync_type)
            # Prefer Aliyun MQTT (the rate-limited transport we're worried
            # about); fall back to the other cloud transport if Aliyun isn't
            # currently connected. Either accepts ble_sync just fine.
            for tt in (TransportType.CLOUD_ALIYUN, TransportType.CLOUD_MAMMOTION):
                transport = handle.get_transport(tt)
                if transport is not None and transport.is_connected:
                    await transport.send_heartbeat(cmd_bytes, iot_id=handle.iot_id)
                    return True
            return False
        except AttributeError:
            # pymammotion internals shifted — fall back to the command-args path.
            LOGGER.debug(
                "heartbeat path unavailable in this pymammotion; using send_command_with_args",
                exc_info=True,
            )
            return await _fallback_ble_sync_via_command(mammotion, device, sync_type)
        except Exception:
            LOGGER.debug("ble_sync heartbeat failed", exc_info=True)
            return False

    async def _fallback_ble_sync_via_command(
        mammotion: Any, device: str, sync_type: int
    ) -> bool:
        """Legacy ble_sync path — counts against the 300/24 h budget.

        Only invoked if the heartbeat path isn't available in the installed
        pymammotion version. Same wire effect, just rate-limited.
        """
        try:
            await mammotion.send_command_with_args(
                device, "send_todev_ble_sync", sync_type=sync_type
            )
            return True
        except Exception:
            LOGGER.debug("Fallback ble_sync failed", exc_info=True)
            return False

    async def wake_publisher() -> None:
        """Force the mower into the Agora channel with video on.

        Order matters and is intentional:

        1. ``send_todev_ble_sync sync_type=3`` — BLE-over-MQTT wake. Useful
           if the mower itself has gone to sleep (not just the publisher).
           Sent via the heartbeat path so it doesn't count against the
           300-sends/24h MQTT budget — this runs on every reconnect, so on a
           flapping WiFi mower the old budgeted send was a steady drain.
        2. ``refresh_stream_subscription`` — pymammotion's documented way to
           reconnect user 1. Works on both WiFi and 4G. This is the actual
           workhorse and replaces what we used to do with an explicit
           ``device_agora_join_channel_with_position`` call (which is now
           sent for us by ``refresh_stream_subscription`` on old-firmware
           devices and unnecessary on new ones). Debounced internally.

        ``refresh_fpv`` used to be a third step here; it was removed because
        it's a no-op on WiFi (per upstream) yet still spent a budgeted MQTT
        send on every reconnect — pure drain for a mower without 4G.

        Both are best-effort; failures don't block negotiation.
        """
        mammotion = state["mammotion"]
        device = state.get("device_name")
        if mammotion is None or not device:
            return
        await _send_ble_sync_heartbeat(sync_type=3)
        await _refresh_stream_subscription()

    # Construct the RTSP server now so the relay can hold a reference even
    # before we know the port is bound — start()/stop() are explicit below.
    rtsp_server = Go2RtcRtspStream(
        bind=rtsp_bind,
        port=rtsp_port,
        mount_point=stream_name,
    )
    relay = AgoraToRtspRelay(
        credentials_provider=credentials_provider,
        agora_context_provider=refresh_agora_context,
        rtsp_server=rtsp_server,
        publisher_wakeup=wake_publisher,
        # cheap_recovery is fired by the in-relay watchdog when RTP stops
        # mid-stream; refresh_stream_subscription is the only mechanism
        # confirmed by upstream to actually reconnect user 1 (the publisher)
        # on both WiFi and 4G. refresh_fpv used to live here but is 4G-only,
        # which is why recovery used to fail silently for WiFi mowers.
        cheap_recovery=_refresh_stream_subscription,
        no_rtp_watchdog_seconds=config["no_rtp_watchdog_seconds"],
        cheap_recovery_wait_seconds=config["cheap_recovery_wait_seconds"],
    )
    # Wire the RTSP server's "new viewer connected" hook back to the relay
    # so we can opportunistically PLI Agora for a fresh keyframe — without
    # this the new viewer waits up to one full GOP for picture.
    rtsp_server._on_keyframe_request = lambda: asyncio.create_task(relay.request_keyframe())

    relay_started = False
    try:
        # ---- Login + initial credential fetch ----
        # The supervisor cannot start until we have *some* credentials cached,
        # because credentials_provider's first run feeds the initial PC.
        while not stop_async.is_set() and state["mammotion"] is None:
            mammotion = await _fresh_client()
            if mammotion is None:
                break
            state["mammotion"] = mammotion
            try:
                fields = await fetch_stream_fields(mammotion, device_name)
                for key in ("appid", "channelName", "token", "uid"):
                    if not fields.get(key):
                        raise RuntimeError(
                            f"Missing {key} in stream subscription payload"
                        )
                state["device_name"] = fields["device_name"]
                state["iot_id"] = fields["iot_id"]
                state["credentials"] = _creds_from_fields(fields)
                state["creds_fetched_at"] = loop.time()
                LOGGER.info(
                    "Stream subscription ready for device %s (channel=%s)",
                    fields["device_name"],
                    fields["channelName"],
                )
            except Exception:
                LOGGER.exception("Initial stream fetch failed; retrying")
                try:
                    await mammotion.stop()
                except Exception:
                    pass
                state["mammotion"] = None
                await asyncio.sleep(reconnect_backoff)

        if stop_async.is_set():
            return

        # ---- Start RTSP server + relay supervisor ----
        await rtsp_server.start()
        LOGGER.info(
            "RTSP server up: %s (bind=%s:%d)",
            rtsp_source,
            rtsp_bind,
            rtsp_port,
        )
        await relay.start()
        relay_started = True

        # ---- Register the go2rtc stream + reconcile loop + keep-alive ----
        next_keepalive = loop.time()
        next_go2rtc_reconcile = loop.time()
        while not stop_async.is_set():
            # Dryness check before anything else so a stuck session does not
            # waste a keep-alive cycle on a doomed cloud client. ``finally``
            # below handles teardown of everything we built in this session.
            #
            # Keyed off seconds_since_*healthy*, not seconds_since_last_rtp:
            # a join/quit-churning publisher dribbles a stray IDR every
            # 60-90s, which kept resetting the last-RTP clock and so the
            # watchdog never fired — the bridge sat wedged "for all eternity"
            # while the 300/24h MQTT budget stayed exhausted. The health clock
            # only advances on a sustained stream, so it ages out under churn
            # and we re-login fresh (which resets pymammotion's in-process
            # send-budget counter — the same thing a container restart does).
            dryness = relay.seconds_since_healthy
            if dryness > dry_restart_seconds:
                raise _DryWatchdogTripped(
                    f"no sustained H265 RTP for {dryness:.0f}s "
                    f"(threshold {dry_restart_seconds:.0f}s)"
                )

            now = loop.time()
            mammotion = state["mammotion"]
            if mammotion is None:
                # Lost cloud session — re-login and continue. The relay's
                # own supervisor will keep retrying with cached creds (or
                # fail loudly on token expiry), so we do not need to tear
                # it down here.
                mammotion = await _fresh_client()
                if mammotion is None:
                    break
                state["mammotion"] = mammotion

            if now >= next_go2rtc_reconcile:
                try:
                    async with Go2RTCStreamRegistrar(go2rtc_api_url) as registrar:
                        ok = await registrar.ensure_stream(stream_name, rtsp_source)
                    if ok:
                        LOGGER.debug(
                            "go2rtc stream %s wired to %s", stream_name, rtsp_source
                        )
                    else:
                        LOGGER.warning(
                            "go2rtc stream %s not confirmed; will retry in %.0fs",
                            stream_name,
                            go2rtc_reconcile_interval,
                        )
                except Exception:
                    LOGGER.warning(
                        "go2rtc reconciliation failed; retrying in %.0fs",
                        go2rtc_reconcile_interval,
                        exc_info=True,
                    )
                next_go2rtc_reconcile = now + go2rtc_reconcile_interval

            if now >= next_keepalive:
                # Periodic keep-alive via pymammotion's *heartbeat* path
                # (transport.send_heartbeat). Same ble_sync sync_type=2
                # message as before but bypasses pymammotion's 300/24h
                # MQTT budget — without that bypass, our 10s cadence ate
                # the whole budget in ~25 minutes, after which the mower
                # silently stopped getting keepalives and idle-timed out.
                # No refresh_fpv on this path: it counts against the
                # budget AND is a no-op on WiFi (per upstream), so it
                # bought us nothing while costing 50% of our 24h budget.
                sent = await _send_ble_sync_heartbeat(sync_type=2)
                if not sent:
                    LOGGER.debug(
                        "Keepalive heartbeat failed; dropping client to re-login"
                    )
                    try:
                        await mammotion.stop()
                    except Exception:
                        pass
                    state["mammotion"] = None
                next_keepalive = now + keepalive_interval

            try:
                await asyncio.wait_for(stop_async.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
    finally:
        LOGGER.info("Shutting down")
        if relay_started:
            try:
                await relay.stop()
            except Exception:
                LOGGER.exception("Relay stop failed")
        try:
            await rtsp_server.stop()
        except Exception:
            LOGGER.exception("RTSP server stop failed")
        if state["mammotion"] is not None:
            try:
                await state["mammotion"].stop()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
