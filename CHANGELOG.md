# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[SemVer](https://semver.org/) (pre-1.0 — expect changes).

## [Unreleased]

## [0.1.17] - 2026-06-03

### Added
- **RTCP Sender Report generation** (RFC 3550 §6.4.1) — every 5 s on
  the existing TCP-interleaved RTCP channel. SR carries the
  NTP-timestamp ↔ RTP-timestamp pair the receiver's playout pacer
  needs to render frames against a real-world clock. Without SR,
  Chrome MSE / WebRTC players guess pacing from arrival times and the
  jitter buffer thrashes — which produced the bursty
  "3 fast flashes / pause / catch-up" rendering visible in Frigate's
  inbound-rtp stats (`interFrameDelayStDev_in_ms` ~30-50 at 10 fps,
  `pauseCount` >10).
- **RTP pacer in the RTSP writer** — instead of forwarding packets the
  moment they arrive (which mirrors Agora's bursty delivery to our
  consumers), each packet is held until its RTP-timestamp-derived
  wall-clock target. ~200 ms initial delay anchors the timeline;
  packets within the same frame still flush back-to-back. Re-anchors
  automatically on upstream session restart or timestamp wraparound.

Both fixes operate entirely on the RTP transport layer — no
transcoding, no codec touching, no upstream changes. Pure timing.

## [0.1.16] - 2026-06-03

### Removed
- Dead code in `agora_edge.py` left over from the WHEP era:
  - Duplicate `_send_renew_token` (the older variant calling
    `rtc_token_provider`; superseded by the v0.1.14 port from PyAgora).
  - `add_ice_candidate`, `self.candidates`, `_convert_candidates_to_ortc`
    — the candidate-trickle path required by browser-driven WHEP
    signaling. aiortc gathers its own candidates upstream now.
  - `_fire_connection_lost` and its `on_connection_lost` constructor
    callback — never wired by any caller in this codebase.
  - `pion_compat` / `disable_audio_answer` / `rtc_token_provider`
    constructor parameters — defaulted to off and never overridden;
    code paths consuming them were unreachable.
  - `_upstream_ready` event on the relay — set/cleared but never
    awaited by anything.
- About 100 lines net out of `agora_edge.py`.

### Changed
- `MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS` constructor default on the
  relay aligned to 5 s (was 3 s) — matches the bridge's env default
  shipped in v0.1.13.
- README rewritten to reflect the current architecture: drops stale
  references to `refresh_fpv`, the 300 s keepalive default, the
  pre-watchdog reliability story, and the WHEP path. Adds
  `MAMMOTION_DRY_RESTART_SECONDS` and the new three-layer recovery
  description.

## [0.1.15] - 2026-06-03

### Added
- Periodic heartbeat INFO line every 60 s from the relay summarising
  steady-state health:
  ```
  Heartbeat upstream=connected last_rtp=0.4s pps=84 kbps=512
  lifetime_pkts=15234 rtsp_clients=2
  ```
  If you tail the log and see one of these every minute, the bridge is
  working. If they go silent, you've got an outage. Complements the
  existing "I only log when something breaks" event style.
- RTP throughput counters (`_packets_forwarded`, `_bytes_forwarded`) on
  the relay, sampled by the heartbeat to derive interval-local pps/kbps.

### Changed
- `Publisher uid=X is now online` logged at INFO when the mower (a
  non-self uid) appears via `on_user_online`. Our own uid joining stays
  silent (already implied by the join-success log).
- Demoted the per-message `WS-join <- type=… body=…` dump in the join
  loop from INFO to DEBUG. It was 3-5 KB per message (dominated by
  Agora's rtpCapabilities JSON) and only useful while debugging the
  signaling itself. Typed events (`on_user_online`, `on_add_video_stream`,
  `on_p2p_lost`, etc.) still log at INFO via their handlers, so normal
  operation visibility is unchanged but the log volume drops sharply.

## [0.1.14] - 2026-06-03

### Added
- Three Agora event handlers / helpers backported from mikey0000's
  [PyAgora](https://github.com/mikey0000/PyAgora) (the upstream
  extraction of the integration's Agora WebSocket client we'll switch
  to as a dep once it's HA-agnostic and published):
  - `_handle_user_offline` — fires when the publisher (mower) leaves
    the channel. Clears stale `_online_users` / `_video_streams`
    entries and sends `renew_token` so the Agora session stays warm,
    so a rejoin can subscribe immediately without renegotiating the WS.
  - `_handle_p2p_ok` — logs the SFU peer-confirmation event for
    visibility when debugging slow joins.
  - `_send_renew_token` — sends Agora's `renew_token` over the
    existing WebSocket. Now invoked by `_handle_user_offline`; can
    also be used to extend a long-running session before the cached
    token's 24 h lifetime expires.
- `AgoraWebSocketHandler` now tracks `self._uid` (our own uid on the
  channel) so the offline handler can distinguish "publisher left" from
  "we left".

## [0.1.13] - 2026-06-03

### Fixed
- **Keepalive default restored to 10 s** (was a regressed 300 s). With a
  300 s interval, we sent no "viewer present" signal for ~4 minutes out
  of every 5, so the mower idle-dropped its publisher inside that window
  and the watchdog had to fire cheap recovery every cycle. Visible in
  logs as ~55 s of video followed by a stall, on repeat.
- Cheap-recovery wait extended from 3 s → 5 s default.
  `refresh_stream_subscription` is heavier than the old `refresh_fpv`
  (HTTP token fetch + MQTT publish + mower rejoin) so the previous 3 s
  window sometimes timed out before recovery actually completed,
  triggering a spurious full teardown.
- Cheap-recovery log line no longer references `refresh_fpv` (the
  callback hasn't been `refresh_fpv` since v0.1.12).

## [0.1.12] - 2026-06-03

### Fixed
- Recovery and wake paths no longer rely on `refresh_fpv`, which per
  upstream (mikey0000 in [#repo]) is a no-op for mowers on WiFi. This is
  why the in-relay cheap-recovery and wake_publisher steps appeared to
  do nothing for WiFi-connected mowers and we kept falling through to
  full upstream teardowns. The bridge now uses pymammotion's
  `refresh_stream_subscription` (the documented "reconnect user 1" path,
  works on both WiFi and 4G) as the primary mechanism for cheap-recovery
  and wake_publisher, with `refresh_fpv` kept only as a best-effort
  secondary call.
- Keep-alive loop simplified to send `send_todev_ble_sync sync_type=2`
  unconditionally (was previously preferring `refresh_fpv` and only
  falling back to ble_sync). The BLE sync works on both transports;
  refresh_fpv now follows as a best-effort secondary.

## [0.1.11] - 2026-06-01

### Added
- Log the bridge version on startup. `mammotion_webrtc.__version__` is
  the single source of truth and is bumped per release together with the
  git tag, so the running container's version is visible in the very
  first INFO line.

## [0.1.10] - 2026-06-01

### Added
- In-process dryness watchdog: if `relay.seconds_since_last_rtp` exceeds
  `MAMMOTION_DRY_RESTART_SECONDS` (default `180`), the bridge tears the
  whole session down (relay, RTSP server, pymammotion client) and
  re-bootstraps from a fresh cloud login inside the same process. Closes
  the "stuck for hours, only `docker restart` fixes it" failure mode that
  the in-relay reconnect loop could not escape because cred-refresh on a
  stale pymammotion/MQTT session no longer wakes the mower.

### Changed
- `main()` refactored into an outer retry loop plus a new
  `_run_bridge_session()` inner function. All resources allocated in a
  session are released in its `finally` block before the watchdog
  exception propagates, so the in-process restart never leaks the
  upstream PC, RTSP listener, or cloud client. No `sys.exit` / `os._exit`
   — recovery works regardless of the container's restart policy.
- `AgoraToRtspRelay._last_rtp_ns` now initialised to `monotonic_ns()` at
  construction (was `0`) so a bootstrap that never produces video also
  ages out, and a public `seconds_since_last_rtp` property exposes the
  age in seconds for the bridge-level watchdog to consume.

## [0.1.9] - 2026-05-31

### Changed
- Split the relay supervisor's backoff into two regimes based on whether
  any H265 RTP packet arrived during the cycle:
  - **Publisher stall** (we had RTP, then it stopped) — always retry at the
    immediate `reconnect_backoff_seconds` (1 s). Sub-15 s recovery preserved.
  - **Mower offline** (no RTP this cycle — Agora signaling timeouts,
    "device not responding" from cloud, no `on_user_online`, etc.) —
    exponential ramp up to `max_reconnect_backoff_seconds`. We don't burn
    the pymammotion 300/24h MQTT budget hammering a sleeping mower.
- Raised `max_reconnect_backoff_seconds` from 10 s back to 60 s. The cap
  is now only relevant for the offline path; stalls always use 1 s, so
  this no longer slows down legitimate recovery.

### Added
- New log lines that explicitly distinguish the two failure modes:
  - `INFO  Publisher stalled mid-stream; retrying in 1.0s`
  - `WARNING Mower appears offline (no video received this cycle); backing off 4.0s (next attempt's backoff: 8.0s)`

### Why
v0.1.8's tuning made stall recovery fast, but the same fast path was also
used when the mower itself was offline — burning MQTT budget and stack-
tracing every cycle without any hope of recovery. The phone app is
genuinely unreachable in the same scenarios; sustained Agora hammering
doesn't make our bridge more capable than the official app.

## [0.1.8] - 2026-05-30

### Changed
- **Aggressive recovery defaults out of the box.** The previous defaults
  (30 s watchdog, 10 s cheap-recovery wait, 5 s reconnect backoff) made
  stall recovery take ~47 s with no env-var tuning. Re-baselined so a
  default-config bridge recovers in ~11 s:
  - `MAMMOTION_NO_RTP_WATCHDOG_SECONDS`: `30` → `5`
  - `MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS`: `10` → `3`
  - Relay `reconnect_backoff_seconds`: `5.0` → `1.0` (internal, not env-tunable)
  - Relay `max_reconnect_backoff_seconds`: `60.0` → `10.0` (internal)
- Demoted `agora_edge` `Generated answer SDP` log from INFO to DEBUG.
  Every Agora reconnect was dumping ~50 SDP lines into the log; with the
  more aggressive recovery defaults that's a lot of noise. Re-enable with
  `MAMMOTION_LOG_LEVEL=DEBUG`.

### Why
With v0.1.7's proactive PLI eliminating the GOP wait, the dominant
remaining freeze contributor was the watchdog / cheap-recovery / backoff
defaults — all generous holdovers from earlier debugging. The new
defaults are tuned for "10 fps H265 stream, want sub-15 s gaps."

## [0.1.7] - 2026-05-30

### Added
- Proactive PLI when the first H265 RTP packet of a new upstream session
  arrives. The mower is typically mid-GOP at that moment and the first
  packets we receive are P-frames — which carry no parameter sets, so
  RTSP `DESCRIBE` blocks waiting for VPS/SPS/PPS and go2rtc times out at
  5 s. Firing one PLI as soon as we know the publisher's SSRC pushes the
  mower onto a fresh IDR (which prepends parameter sets), so DESCRIBE
  completes in ~1 s instead of ~6 s (next natural GOP boundary).

  Cuts black-screen time after every reconnect — token expiry, no-RTP
  watchdog fire, network blip, etc. Most visible at startup and after
  any of v0.1.6's cheap-recovery escalations.

## [0.1.6] - 2026-05-30

### Added
- Wire pymammotion's `refresh_fpv` (`MulSetEncode(encode=True)`) in as the
  camera-specific keep-alive. Three integration points:
  - `wake_publisher` now sends `refresh_fpv` after the
    `device_agora_join_channel_with_position`, so every upstream reconnect
    explicitly re-enables the encoder.
  - The periodic keep-alive loop calls `refresh_fpv` instead of the generic
    `send_todev_ble_sync sync_type=2` (which only kept the cloud session
    warm and didn't touch the encoder). Falls back to the old command if
    pymammotion is too old to expose `refresh_fpv`.
  - The no-RTP watchdog now tries `refresh_fpv` first as a cheap recovery
    (`MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS`, default 10 s) and only
    escalates to full teardown + reconnect if RTP doesn't resume. This
    eliminates most of the reconnect churn that was burning the MQTT budget.

### Why
`send_todev_ble_sync sync_type=2` is a generic Aliyun MQTT heartbeat, not
a camera command. The mower's multimedia SoC was going idle even while we
were sending those pings. `refresh_fpv` targets the actual encoder.
Surfaced by reading Mikey's recent pymammotion additions
([`pymammotion/mammotion/commands/messages/video.py:48`](pymammotion/mammotion/commands/messages/video.py#L48)).

## [0.1.5] - 2026-05-30

### Fixed
- Drop the early-SDP fallback in `agora_edge` that fired on
  `on_add_video_stream` timeout. The fallback built an answer SDP without the
  publisher's actual SSRC/PT, which silently broke codec routing in the
  aiortc receiver (`codecs_by_pt` lookup misses → RTP tap drops every
  packet → watchdog fires → reconnect → same timeout → infinite loop).
  Raising on timeout instead lets the supervisor back off and retry cleanly.
- Re-tag `:stable` to the actual newest code. After the v0.1.2/3/4 history
  rewrite, parallel CI builds for all three retagged versions raced for the
  `:stable` floating tag and v0.1.2 (WHEP bridge code) won, leaving `:stable`
  serving the old WHEP signaling bridge. v0.1.5 is unambiguously the newest
  tag so CI publishes the aiortc relay code as `:stable`.

## [0.1.4] - 2026-05-30

### Added
- No-RTP watchdog (`MAMMOTION_NO_RTP_WATCHDOG_SECONDS`, default 30 s). If the
  upstream ICE/DTLS session stays "connected" but no H265 RTP packet arrives
  for the configured window, the bridge tears down the upstream and lets the
  supervisor reconnect — which refetches credentials and re-triggers the
  mower to publish. Fixes the silent-stall mode where the publisher would
  back off after a few minutes and the bridge would happily sit on a dead
  session until the hourly token expiry forced a reconnect.

### Changed
- `MAMMOTION_KEEPALIVE_SECONDS` default raised from 10 s to 300 s. The old
  cadence sent ~8640 MQTT messages per day; pymammotion's Aliyun MQTT
  transport self-imposes a ~300/24 h cap, so the bridge would hit the limit
  ~50 min in and then silently stop nudging the mower. 5 min cadence keeps
  the bridge well under the cap and the no-RTP watchdog handles the case
  where the publisher times out anyway.

## [0.1.3] - 2026-05-29

### Changed
- **Replaced the architecture.** The bridge is now an aiortc-based passive
  WebRTC subscriber that taps Agora's H265 RTP packets and republishes them
  byte-for-byte from an embedded RTSP server. No ffmpeg, no transcoding, no
  WHEP/WS signaling step — Frigate's go2rtc consumes the bridge with a plain
  `rtsp://` source. See the rewritten README for the rationale.
- Default (and only) entrypoint is `mammotion_webrtc_bridge.py`. Image no
  longer needs `BRIDGE_SCRIPT` to select the relay.

### Removed
- Legacy ffmpeg transcode bridge (`mammotion_go2rtc_bridge.py`) — the new
  passthrough path supersedes it.
- WebRTC signaling passthrough mode (WHEP/WS) — Pion's interop with Agora's
  edge is unreliable (`p2p_lost: Timeout` ~10 s into every session), so this
  path is effectively unusable for Mammotion and has been removed.
- `ffmpeg` apt package, the `docker-entrypoint.sh` wrapper, and the
  `MAMMOTION_GO2RTC_SIGNALING` / `MAMMOTION_WHEP_*` env vars.
- Stale `DESIGN-webrtc-passthrough.md` design notes.

### Added
- aiortc H265 patch (`mammotion_webrtc/h265_patch.py`) — registers H265 with
  aiortc as opaque RTP so the SDP/codec-routing/decoder-spawn paths accept it
  without a native H265 decoder dependency.
- Minimal asyncio RTSP server (`mammotion_webrtc/rtsp_server.py`) with
  TCP-interleaved transport, parameter-set-aware SDP, and per-session queue
  back-pressure.
- Single combined `docker-compose.example.yml` modelled on a real working
  Frigate + bridge setup.

## [0.1.2] - 2026-05-28

### Added
- Experimental Mammotion -> go2rtc WebRTC passthrough bridge (`mammotion_webrtc_bridge.py`) and standalone signaling server (`mammotion_webrtc/whep_server.py`) for direct Agora edge negotiation.
- go2rtc-native WebSocket signaling endpoint (`/api/ws?src=...`) as an alternative to one-shot WHEP client mode.
- Runtime signaling mode switch with `MAMMOTION_GO2RTC_SIGNALING` (`http` or `ws`).
- Periodic stream-registration reconciliation (`MAMMOTION_GO2RTC_RECONCILE_SECONDS`) to auto-recover registration after go2rtc/Frigate restarts.
- Dedicated passthrough example compose file and docs updates.

### Changed
- SDP answer generation now includes a Pion/go2rtc compatibility profile for better interop with Frigate-bundled go2rtc.
- WebRTC passthrough now enforces go2rtc-native WS signaling; WHEP mode is treated as unsupported for Frigate/go2rtc.

### Fixed
- Improved reconnect behavior when closing/reopening a viewer by handling trickle ICE candidates throughout the WS session lifecycle.
- Added explicit WHEP client logging for easier diagnostics of active compatibility mode.

## [0.1.1] - 2026-05-27

First public release. Experimental.

### Added
- Bridge that subscribes to a Mammotion mower's Agora video and publishes it to
  go2rtc over RTSP, transcoded to a steady 10 fps H.264 stream.
- Automatic recovery from the mower leaving the Agora channel (~50 s cycle):
  proactive 20 s keep-alive plus reactive wake-up.
- Fresh cloud login per cycle so an expired session token can't wedge the bridge.
- Watchdogs: startup-frame timeout, frame-stall, publisher-gone-too-long, and an
  ffmpeg subprocess health check with auto-restart.
- Docker healthcheck driven by a frame-ingress heartbeat file (defaults to
  `/tmp/mammotion_heartbeat`).
- Multi-arch image (amd64 + arm64) published to GHCR with `:latest`, `:stable`,
  and semver tags via GitHub Actions.
- Example configs for Frigate and standalone go2rtc, plus a documented HA
  advanced-camera-card snippet.

[Unreleased]: https://github.com/Bleialf/mammotion-rtsp-bridge/compare/v0.1.9...HEAD
[0.1.9]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.9
[0.1.8]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.8
[0.1.7]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.7
[0.1.6]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.6
[0.1.5]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.5
[0.1.4]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.4
[0.1.3]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.3
[0.1.2]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.2
[0.1.1]: https://github.com/Bleialf/mammotion-rtsp-bridge/releases/tag/v0.1.1
