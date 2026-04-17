# ── Stage 1: dependency builder ───────────────────────────────────────────────
FROM python:3.11-slim AS builder

# Install build tools for compiled packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Stage 2: production image ─────────────────────────────────────────────────
FROM python:3.11-slim

# Optimised for AWS us-east-1 latency:
#   • slim base keeps the image small (faster ECR pull on cold start)
#   • no extra daemons competing for CPU/network
#   • TZ set to America/New_York — matches exchange time, avoids DST bugs

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=America/New_York \
    DB_PATH=/data/orb_trades.db

RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app
COPY bot.py dashboard.py ./

# Mount a volume at /data so the SQLite file survives container restarts
VOLUME ["/data"]

# Expose Streamlit port (only needed when running dashboard service)
EXPOSE 8501

# Default: run the trading bot
CMD ["python", "bot.py"]
