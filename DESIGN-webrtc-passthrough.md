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
