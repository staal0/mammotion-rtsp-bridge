"""Mammotion Agora → RTSP passthrough bridge package.

The bridge subscribes to the mower's Agora WebRTC channel using a patched
aiortc stack (see :mod:`.h265_patch`), taps the inbound H265 RTP packets
in :mod:`.aiortc_relay`, and serves them over an embedded RTSP server in
:mod:`.rtsp_server` that go2rtc/Frigate consumes with a plain ``rtsp://``
source.

Why this layering: Pion (go2rtc's WebRTC stack) can complete ICE/DTLS with
many SFUs but not Agora's edge — Agora reports ``p2p_lost: Timeout`` ~10s
after every attempt. aiortc handles the same handshake without issue but
has no H265 codec; the patches in :mod:`.h265_patch` make it accept H265
PT 100 as opaque bytes so we can forward the RTP payload verbatim. No
transcoding, no ffmpeg.
"""

from __future__ import annotations

# Single source of truth for the bridge version. Bump this together with
# the git tag (e.g. tagging ``v0.1.11`` requires this string to read
# ``"0.1.11"``). The bridge entrypoint logs it on startup so the running
# container's version is obvious from the logs without inspecting the
# image digest.
__version__ = "0.1.20"

__all__ = [
    "AgoraAPIClient",
    "AgoraResponse",
    "AgoraWebSocketHandler",
    "AgoraToRtspRelay",
    "Go2RTCStreamRegistrar",
    "Go2RtcRtspStream",
    "SDPParser",
    "StreamCredentials",
    "__version__",
    "parse_offer_to_ortc",
]

from .agora_edge import (
    AgoraAPIClient,
    AgoraResponse,
    AgoraWebSocketHandler,
)
from .agora_session import StreamCredentials
from .aiortc_relay import AgoraToRtspRelay
from .go2rtc_register import Go2RTCStreamRegistrar
from .rtsp_server import Go2RtcRtspStream
from .sdp import SDPParser, parse_offer_to_ortc
