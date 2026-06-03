# Mammotion -> Frigate / go2rtc RTSP bridge

Single-container bridge that gets a Mammotion mower's camera into Frigate (or
any RTSP consumer) by joining the mower's Agora WebRTC channel as a passive
subscriber and re-exposing the H265 stream over plain RTSP.

```
Mammotion cloud -> Agora SFU -> bridge (aiortc subscriber)
                                       |
                                       v
                              RTSP server :8554
                                       |
                                       v
                          Frigate / go2rtc / VLC / ffplay
```

No ffmpeg, no transcoding. The Agora RTP packets are forwarded byte-for-byte;
the bridge only rewrites the 12-byte RTP header (SSRC, sequence number,
payload type) and leaves the H265 payload untouched.

> Unofficial. Not endorsed by Mammotion. Use at your own risk.

> Use a separate Mammotion account shared from your main account. Mammotion
> is effectively single-session, so using one account for both the app and the
> bridge will cause one to log the other out.

## Why this architecture

Earlier versions tried two other approaches; both lost to upstream realities:

- **ffmpeg transcode (0.1.1).** Worked but added CPU load, GOP-rewrite
  artefacts, and a per-frame jitter problem. Replaced.
- **WebRTC signaling passthrough (0.1.2).** The bridge brokered SDP only and
  go2rtc was meant to be the WebRTC peer to Agora directly. go2rtc's Pion
  stack does not interoperate cleanly with Agora's edge — `p2p_lost: Timeout`
  ~10 s into every session — so the path was effectively dead on arrival for
  this stream. Removed.

The current path runs aiortc as the WebRTC peer (which talks to Agora's edge
without issue), monkey-patches it to accept H265 as opaque bytes
([mammotion_webrtc/h265_patch.py](mammotion_webrtc/h265_patch.py)), taps the
inbound RTP, and republishes it as a minimal RTSP server. Frigate's bundled
go2rtc then consumes it like any normal IP camera.

## Quick start

1. Copy [docker-compose.example.yml](docker-compose.example.yml) into your
   Frigate stack (or paste just the `mammotion-bridge` service into your
   existing compose so it shares Frigate's network).
2. Fill in `MAMMOTION_EMAIL`, `MAMMOTION_PASSWORD`, and (optionally)
   `MAMMOTION_DEVICE_NAME`.
3. Start:

```bash
docker compose pull mammotion-bridge
docker compose up -d mammotion-bridge
```

4. The bridge logs in to the Mammotion cloud, joins the Agora channel,
   starts its RTSP server, and registers itself with Frigate's go2rtc.
5. Open `http://<frigate-host>:1984/stream.html?src=mammotion` to view.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `MAMMOTION_EMAIL` | — | Mammotion cloud account email (**required**) |
| `MAMMOTION_PASSWORD` | — | Mammotion cloud password (**required**) |
| `MAMMOTION_DEVICE_NAME` | `first` | Device name in the app, or `first` to auto-pick |
| `GO2RTC_API_URL` | `http://frigate:1984` | go2rtc REST API base for stream auto-registration |
| `MAMMOTION_STREAM_NAME` | `mammotion` | Name go2rtc registers the stream under |
| `MAMMOTION_RTSP_HOST` | container hostname | Hostname go2rtc uses to dial back to the bridge |
| `MAMMOTION_RTSP_PORT` | `8554` | RTSP listen port inside the container |
| `MAMMOTION_RTSP_BIND` | `0.0.0.0` | RTSP bind address |
| `MAMMOTION_GO2RTC_RECONCILE_SECONDS` | `20` | Periodic go2rtc registration self-heal interval |
| `MAMMOTION_KEEPALIVE_SECONDS` | `10` | MQTT keepalive to the mower. Stays well under the publisher-idle timeout (~50 s) so the mower keeps publishing between recoveries |
| `MAMMOTION_NO_RTP_WATCHDOG_SECONDS` | `5` | If no H265 RTP arrives for this long, trigger an in-relay cheap recovery (`refresh_stream_subscription`); escalate to a full teardown if that doesn't restore packets |
| `MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS` | `5` | How long to wait after a cheap-recovery call before judging it failed |
| `MAMMOTION_DRY_RESTART_SECONDS` | `180` | If no H265 RTP at all for this many seconds, tear the whole bridge down in-process (relay + RTSP + pymammotion client) and re-bootstrap from a fresh cloud login. Escape from "stale cloud session, in-relay recovery isn't working" failure modes. No process exit — works regardless of Docker restart policy |
| `MAMMOTION_RECONNECT_BACKOFF_SECONDS` | `8` | Retry delay on cloud/session failure |
| `MAMMOTION_LOG_LEVEL` | `INFO` | `DEBUG` exposes the per-message join-loop body dumps and RTSP method exchange |

## Viewer compatibility

The bridge serves a plain RTP-over-RTSP H265 stream. Consumers tested:

- **Frigate / go2rtc (RTSP source)** — works (this is the primary target).
- **ffplay / VLC** — works (`rtsp://<host>:8554/<stream>`).
- **go2rtc `stream.html?mode=webrtc`** — works smoothly when the browser
  supports H265 over WebRTC (Safari / recent Chrome do; Firefox does not).
- **go2rtc `stream.html` (MSE default)** — choppy. Mammotion encodes at
  ~10 fps and Chrome's MSE low-delay renderer stalls on low-framerate H265.
  Use `&mode=webrtc` or play the RTSP URL directly.

## Reliability

Three layers of recovery, each handling a different failure class:

1. **In-relay cheap recovery.** If no H265 RTP arrives for
   `MAMMOTION_NO_RTP_WATCHDOG_SECONDS` (default 5 s), the relay fires
   `pymammotion.refresh_stream_subscription` — that re-fetches a token AND
   tells the mower to rejoin Agora, all over the existing MQTT session
   without tearing down the upstream PC. Recovers from "publisher idle-timed
   out" in 1-2 s.
2. **Full upstream teardown.** If cheap recovery didn't restore packets
   within `MAMMOTION_CHEAP_RECOVERY_WAIT_SECONDS` (default 5 s), the relay
   tears down the upstream PC and reconnects from scratch with fresh
   credentials. Recovers from genuine ICE/DTLS or token-expiry failures.
3. **In-process bridge restart.** If those still haven't brought RTP back
   after `MAMMOTION_DRY_RESTART_SECONDS` (default 180 s), the bridge tears
   down everything *including* the pymammotion client and re-bootstraps from
   a fresh cloud login inside the same process. Escapes "stale session"
   failure modes the in-relay loop can't recover from. No `sys.exit`, so it
   works without any Docker restart policy.

Other reliability bits:

- A **heartbeat INFO line** every 60 s summarises steady-state health
  (upstream state, last-RTP age, pps, kbps, lifetime packet count, RTSP
  client count). If you tail the log and these go silent, something's wrong.
- **go2rtc registration is reconciled periodically**, so the stream comes
  back on its own after a Frigate restart.
- **Cloud-login refresh per cycle** so a stale pymammotion refresh token
  can't wedge the bridge for hours.

## Limitations

- **H265 only.** Mammotion's camera publishes H265; this bridge does not
  transcode. Consumers that need H264 should put a transcoder downstream (or
  use the Frigate `ffmpeg`-based hwaccel preset to record/restream as H264).
- **No RTCP Sender Reports.** go2rtc gets RTP timestamps without a wall-clock
  anchor. This is fine for Frigate (which timestamps on arrival) but can
  cause MSE-mode browser playback to drift over long sessions.
- **Single device per container.** Run one container per mower.
- **Local-only is not possible.** All Mammotion video is brokered through
  Agora's cloud; there is no documented LAN-direct path today.

## Integration with Mammotion-HA

mikey0000 is extracting the Agora WebRTC client used here into a standalone
library, [PyAgora](https://github.com/mikey0000/PyAgora). Once that's
published and HA-agnostic (it currently has a hard `homeassistant.core`
import), the ~2 kLOC of duplicated Agora code in
[mammotion_webrtc/agora_edge.py](mammotion_webrtc/agora_edge.py) and
[mammotion_webrtc/sdp.py](mammotion_webrtc/sdp.py) goes away in favour of
`pip install pyagora`.

The longer-term direction discussed upstream is to push the H265-passthrough
+ RTSP server *into* mikey's Mammotion HA integration so users get a
Frigate-ready RTSP camera without running this separate container at all.
That's a larger rework — for now this bridge stays the dedicated path.

## Releases

| Tag | Meaning |
| --- | --- |
| `:stable` | Latest tagged release |
| `:latest` | `main` branch HEAD |
| `:x.y.z` | Pinned release |
| `:sha-<short>` | Pinned commit |

See [CHANGELOG.md](CHANGELOG.md).

## Credits

Built on [pymammotion](https://github.com/mikey0000/PyMammotion) for cloud
auth and Agora token fetch. The H265 patch + relay design borrows ideas from
the PetKit HA integration's Agora WebRTC client (MIT,
© 2024-2026 @Jezza34000) and from mikey0000's
[PyAgora](https://github.com/mikey0000/PyAgora) (`on_user_offline`,
`on_p2p_ok`, `renew_token` handlers).
