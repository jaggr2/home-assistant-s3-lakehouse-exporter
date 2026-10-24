ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.20
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_BREAK_SYSTEM_PACKAGES=1

# Python 3.12 on the Alpine-based Home Assistant base image.
RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY exporter /app/exporter
COPY rootfs /

RUN chmod +x /etc/s6-overlay/s6-rc.d/exporter/run \
    && chmod +x /etc/s6-overlay/s6-rc.d/exporter/finish

WORKDIR /app
