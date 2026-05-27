# WIP: WebRTC passthrough rewrite

> Branch `webrtc-passthrough`. Experimental, incomplete. The `main` branch
> (ffmpeg → RTSP) is the working version; this is an attempt at something better.

## Goal

Eliminate the ffmpeg transcode/remux hop. Instead of
`Agora → encoded frames → ffmpeg → RTSP → go2rtc`, let **go2rtc terminate the
Agora WebRTC media session directly**:

```
Agora edge ──RTP/WebRTC (DTLS-SRTP)──▶ go2rtc ──▶ RTSP / WebRTC / HLS
                    ▲
   this bridge only brokers the SDP handshake (signaling); media never
   flows through Python.
```

This preserves Agora's RTP timing, RTX retransmission, and the consumer's
native jitter buffer — which is why it should fix the choppy/fast-forward
playback that the encoded-frame→ffmpeg pipe causes.

## Approach (ported from the PetKit HA integration, MIT licensed)

Reference: `homeassistant_petkit-main/custom_components/petkit/` (MIT,
© 2024–2026 @Jezza34000). We port its Agora-edge client and WHEP bridge into a
standalone async service, swapping PetKit's cloud API for pymammotion.

Components to build:

1. **Agora edge client** (port of `agora_websocket.py` + `agora_sdp.py`):
   joins the Agora channel as audience over Agora's private WebSocket protocol,
   subscribes to the publisher's video SSRC, and synthesizes an SDP answer
   whose ICE/DTLS points at the Agora edge gateway.
2. **WHEP endpoint** (port of `whep_proxy.py`): an aiohttp server that receives
   an SDP offer (from go2rtc) and returns the Agora-derived answer.
3. **go2rtc registration** (port of `go2rtc_stream.py`): POST to go2rtc's REST
   API to register `mammotion` with source `webrtc:<our-whep-url>`, so go2rtc
   dials our WHEP endpoint and then DTLS-SRTPs straight to Agora.
4. **Credentials**: reuse pymammotion `get_stream_subscription` for
   appid/channel/rtc_token/uid (already implemented on `main`).
5. **Keep-alive**: Mammotion has no RTM token like PetKit, so the publisher
   keep-alive stays the existing MQTT `send_todev_ble_sync` path.

## Key risk — H.265 over WebRTC (GO/NO-GO)

PetKit streams **H.264**, which WebRTC supports universally. Mammotion streams
**H.265**. The port itself is codec-adaptable (PetKit's SDP layer is generic;
only the subscribe codec is hardcoded `h264` → change to `h265`). The open
question is whether **go2rtc can negotiate/ingest H.265 over a WebRTC/WHEP
source**. If it can't, this approach dead-ends and we stay on `main`.

Quick test (no porting needed): open `http://<host>:1984/webrtc.html?src=mammotion`
against the current H.265 stream. Plays = green light. Fails (only MSE/HLS
works) = H.265-over-WebRTC unsupported = abandon this branch.

## Status

- [x] Feasibility assessment of PetKit code (codec-agnostic; port is tractable)
- [ ] **GO/NO-GO: confirm go2rtc H.265 over WebRTC**
- [ ] Port agora_sdp.py
- [ ] Port agora_websocket.py
- [ ] WHEP server + go2rtc registration
- [ ] Wire pymammotion credentials + keep-alive
- [ ] Integration test with a real mower

## Port notes

This section documents the standalone port that lives in the new package
`mammotion_webrtc/` plus the entrypoint `mammotion_webrtc_bridge.py`. None of the
main-branch files were modified.

### What was ported (file by file)

| New file | Ported from (PetKit) | Notes |
|---|---|---|
| `mammotion_webrtc/sdp.py` | `agora_sdp.py` | Near-verbatim. Two additions (below). |
| `mammotion_webrtc/agora_edge.py` | `agora_websocket.py` + the needed parts of `agora_api.py` | The whole join_v3 client, the SDP-answer synth, plus `AgoraAPIClient` / `AgoraResponse` / `EdgeAddress` / `RESPONSE_FLAGS` / `SERVICE_IDS`. |
| `mammotion_webrtc/whep_server.py` | `whep_proxy.py` (upstream half) + `camera.py` `_refresh_agora_context` / `_filter_candidates` | HA `HomeAssistantView` classes replaced with plain `aiohttp.web` handlers; manager replaced with `MammotionWhepManager`. |
| `mammotion_webrtc/go2rtc_register.py` | `go2rtc_stream.py` (REST registration core) | Just the `POST/PUT/PATCH api/streams` ladder + idempotency check via `GET api/streams`. |
| `mammotion_webrtc_bridge.py` | new; reuses `mammotion_go2rtc_bridge.py` `fetch_stream_fields()` / `_fresh_client()` | Login, WHEP server, go2rtc registration, MQTT keep-alive. |

### HA → standalone adaptations made

- Removed all `homeassistant.*` / `custom_components.*` imports.
- `LOGGER` / `_LOGGER` → `logging.getLogger(__name__)` everywhere.
- `HomeAssistantView` POST/PATCH/DELETE → `aiohttp.web` route handlers.
- HA auth (`_check_external_auth`, signed-request JWT) → dropped. Replaced with
  an **optional** static bearer token (`MAMMOTION_WHEP_TOKEN`); default is open,
  since the WHEP endpoint is meant to be reachable only by the co-located go2rtc.
- PetKit credential sources (`LiveFeed`, coordinator, `AGORA_APP_ID` constant)
  → Mammotion stream-subscription fields (`appid`/`channelName`/`token`/`uid`/
  `areaCode`) via a `StreamCredentialsProvider` callback wired to pymammotion
  `get_stream_subscription` (same pattern as the main-branch bridge).
- `AGORA_APP_ID` was a hardcoded PetKit constant; Mammotion's app id comes from
  the subscription payload (`fields["appid"]`) and is passed through to both
  `choose_server` and `join_v3`.

### Dependencies replaced (so we don't pull PetKit's libs)

- `sdp_transform.parse` → the local `SDPParser` (it already emits the same field
  names the answer builder reads). To make it a true drop-in I added two things
  to `SDPParser` vs. the verbatim `agora_sdp.py`:
  1. parse `a=extmap-allow-mixed` → `parsed["extmapAllowMixed"]` (the offer-info
     extractor needs it; the reference got it from `sdp_transform`).
  2. populate the `rtcpFb` list from `a=rtcp-fb` lines (the reference declared
     the list but only `sdp_transform` filled it; without this the ORTC
     `rtcpFeedbacks` for recv codecs would be empty).
  Verified: parse + ORTC translation + answer generation round-trip in a local
  test (extmap-allow-mixed, rtcp-fb nack/pli, H265 fmtp all propagate).
- `webrtc_models.RTCIceCandidateInit` → a local dataclass with the same three
  attributes the code touches (`candidate`, `sdp_mid`, `sdp_m_line_index`).
- `pypetkitapi.LiveFeed` (in the join flow) → `AgoraCredentials` dataclass with
  just `rtc_token` + `channel_id` (the only two attributes `connect_and_join` /
  `_create_join_message` read).

### Mammotion-specific changes

- **Codec h264 → h265.** PetKit hardcoded `"h264"` in the three subscribe calls
  and the `join_v3` `codec` field. `agora_edge.py` now has
  `DEFAULT_VIDEO_CODEC = "h265"`; `_send_subscribe`, `_subscribe_video_stream`,
  `_subscribe_retry_loop`, and the join message all use it. The SDP builders
  stay codec-agnostic (they echo whatever codecs the offer/ORTC declare).

### RTM / keep-alive (important — partly unverified)

- PetKit used `AgoraRTMSignaling` (`agora_rtm.py`) `start_live` + a 500 ms
  `live_heartbeat` to keep the publisher streaming, using an `rtm_token` carried
  in `LiveFeed`. **Mammotion has no rtm_token**, so `agora_rtm.py` was *not*
  ported and the RTM start_live/heartbeat is omitted entirely (there is a
  `# NOTE(mammotion)` where PetKit started it in `whep_server.py`).
- Instead the publisher is kept alive over MQTT, exactly like the main-branch
  ffmpeg bridge: `send_command_with_args(device, "send_todev_ble_sync",
  sync_type=2)` on a ~10 s loop (`MAMMOTION_KEEPALIVE_SECONDS`).
- **UNVERIFIED:** whether RTM is actually required for *media* on Mammotion, or
  whether it was only PetKit's keep-alive. The main-branch bridge sustains
  Mammotion video with only the MQTT sync (no RTM), which is strong evidence the
  MQTT path is sufficient — but that was proven for the native-SDK audience
  path, not for this WHEP/edge-WebSocket path. If the publisher still drops with
  this bridge, RTM-equivalent signaling may be needed and there is no Mammotion
  token for it.

### Things stubbed or guessed (search for `TODO(mammotion)` / `NOTE(mammotion)`)

1. **Area code** (`mammotion_webrtc_bridge.py: resolve_area_code_string`). The
   main bridge maps `areaCode` to the native SDK's integer bitmask. The REST
   `choose_server` API instead wants a string like `"CN,GLOBAL"`. We default to
   `"CN,GLOBAL"` (Agora's own default). Mapping of the non-CN regions to the
   exact comma-list Agora expects is a guess; if the app id is region-locked
   this may need tuning.
2. **RTC token renewal** (`whep_server.py: refresh_rtc_token`). PetKit refreshed
   via RTM `update_tokens`; we instead re-call the credentials provider (which
   re-fetches the stream subscription) to feed Agora's `renew_token`. Untested
   against a real expiry event.
3. **`_on_connection_lost`** schedules `close_session` on the running loop so a
   dropped Agora session frees go2rtc to redial — adapted from PetKit's
   `hass.async_create_task`.

### What still needs doing before it can work

- **GO/NO-GO on H.265-over-WebRTC in go2rtc is still open** (see the risk
  section above). If go2rtc can't ingest H.265 over a WHEP/WebRTC source, this
  whole branch dead-ends regardless of the port being correct.
- **Real-mower integration test.** All logic is unit-verified locally
  (SDP parse → ORTC → answer synth, AgoraResponse parsing, candidate filtering,
  WHEP routes, go2rtc REST ladder) but nothing has talked to a live Agora edge
  or a real go2rtc yet.
- **Verify Mammotion's offer/answer shape.** The answer synth assumes go2rtc
  sends a BUNDLE'd recvonly offer with audio+video m-lines (PetKit's shape). If
  go2rtc's WHEP offer differs (e.g. video-only, different mids), the
  `_build_media_section_lines` / BUNDLE handling may need adjustment.
- **Confirm `send_todev_ble_sync` sync_type for "viewer present".** The main
  bridge uses `sync_type=2` for keep-alive and `sync_type=3` for wake-up; we use
  `2`. Reuse is intentional but unverified for this path.
- **Decide on auth.** Currently open by default. If the WHEP port is exposed
  beyond the go2rtc host, set `MAMMOTION_WHEP_TOKEN`.
- **Dockerfile / compose wiring** for the new entrypoint + the
  `MAMMOTION_WHEP_*` / `GO2RTC_API_URL` env vars (not done; main-branch
  Dockerfile still targets `mammotion_go2rtc_bridge.py`).
