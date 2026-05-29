FROM python:3.13-slim
# Pinned to 3.13 to thread the needle between two upstream constraints:
#   - PyAV (av) has no prebuilt wheel for Python 3.14 yet.
#   - pymammotion 0.7.x sets python_requires>=3.13 (on 3.12 only the legacy
#     0.2.x line installs, with an incompatible API).
# 3.13 has wheels for both.

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/app

WORKDIR /app

# libsrtp2-1: required by aiortc's SRTP layer.
# PyAV's manylinux wheel statically links its own ffmpeg + libvpx + libopus,
# so we don't need any ffmpeg-dev packages here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates libsrtp2-1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY mammotion_webrtc_bridge.py /app/mammotion_webrtc_bridge.py
COPY mammotion_webrtc/ /app/mammotion_webrtc/

ENTRYPOINT ["python", "-u", "/app/mammotion_webrtc_bridge.py"]
