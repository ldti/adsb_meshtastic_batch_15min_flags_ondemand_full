# =============================
# Stage 1 — Builder (Alpine)
# =============================
FROM python:3.11-alpine AS builder

# Install build dependencies for numpy & pyModeS
RUN apk add --no-cache build-base linux-headers

WORKDIR /build

# Preinstall Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
 && pip wheel --no-cache-dir --wheel-dir /wheels \
      numpy pyModeS pycountry meshtastic

# =============================
# Stage 2 — Runtime (Alpine, tiny)
# =============================
FROM python:3.11-alpine AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy wheels from builder and install them
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels numpy pyModeS pycountry meshtastic \
 && rm -rf /wheels

WORKDIR /app
COPY adsb_meshtastic_batch_15min_flags_ondemand_full.py /app/app.py

# Add non-root user
RUN adduser -D appuser && chown -R appuser /app
USER appuser

# Default environment variables
ENV DUMP1090_HOST=10.200.10.18 \
    DUMP1090_PORT=30103 \
    MESHTASTIC_TCP_HOST=10.200.10.16 \
    MESHTASTIC_CHANNEL_INDEX=4 \
    BATCH_MINUTES=15 \
    MAX_MESSAGE_CHARS=200 \
    LOG_LEVEL=INFO

# Default command
CMD ["python", "/app/app.py"]
