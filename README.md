# ORB — Opening Range Breakout Trading Bot

Production-ready automated trading system built with Python and the Alpaca Markets API (`alpaca-py`).

---

## Architecture Overview

```
Alpaca DataStream (WebSocket)
        │  1-min bars
        ▼
    bot.py
  ┌───────────────────────────────────┐
  │  State Machine (per symbol)       │
  │  WAITING_RANGE → SCANNING         │
  │       → IN_TRADE → COMPLETED      │
  │                                   │
  │  ORB Logic Engine                 │
  │  • Collects first N 1-min bars    │
  │  • Validates bar count (≥13/15)   │
  │  • Sets High/Low/Mid              │
  │  • Detects close breakout         │
  │                                   │
  │  Execution Engine                 │
  │  • Bracket order (Entry+TP+SL)    │
  │  • Submitted atomically to Alpaca │
  │                                   │
  │  Risk Manager                     │
  │  • Kill switch (daily loss %)     │
  │  • Auto-flatten at 3:55 PM EST    │
  └───────────────────────────────────┘
        │  writes
        ▼
   orb_trades.db  (SQLite)
        │  reads
        ▼
   dashboard.py  (Streamlit + Plotly)
```

---

## Quick Start

### 1. Clone and configure

```bash
cd orb/
cp .env.example .env
# Edit .env — add your Alpaca paper keys and choose your symbols
```

### 2. Run locally (paper trading)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Terminal 1 — bot
python bot.py

# Terminal 2 — dashboard
streamlit run dashboard.py
# Open http://localhost:8501
```

---

## Deploy on AWS EC2 (Recommended — us-east-1)

> Hosting in `us-east-1` (N. Virginia) puts your bot in the same data-centre neighbourhood as Alpaca's order routing, cutting execution latency from ~200 ms (home) to ~5–10 ms.

### Instance sizing

| Use-case     | Instance    | Why                          |
|--------------|-------------|------------------------------|
| Paper/dev    | t3.micro    | Free-tier eligible           |
| Live trading | t3.medium   | Headroom for Python GC spikes |
| High-volume  | c6i.xlarge  | Consistent CPU burst          |

### Option A — Docker Compose (simplest)

```bash
# On your EC2 instance (Amazon Linux 2023 / Ubuntu 22.04)
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git

git clone <your-repo> orb && cd orb
cp .env.example .env && nano .env   # paste live/paper keys

sudo docker compose up -d           # starts bot + dashboard
sudo docker compose logs -f bot     # tail logs
```

Dashboard is available at `http://<EC2-public-IP>:8501`.
Lock port 8501 to your IP in the EC2 Security Group.

### Option B — systemd (no Docker)

```bash
# Install Python 3.11
sudo apt install -y python3.11 python3.11-venv

# Create a dedicated system user
sudo useradd -m -s /bin/bash orbbot
sudo -u orbbot bash -c "
  cd /home/orbbot &&
  git clone <your-repo> orb &&
  cd orb &&
  python3.11 -m venv .venv &&
  .venv/bin/pip install -r requirements.txt &&
  cp .env.example .env
"
sudo nano /home/orbbot/orb/.env    # add keys
```

Create `/etc/systemd/system/orb-bot.service`:

```ini
[Unit]
Description=ORB Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=orbbot
WorkingDirectory=/home/orbbot/orb
EnvironmentFile=/home/orbbot/orb/.env
ExecStart=/home/orbbot/orb/.venv/bin/python bot.py
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/orb-dashboard.service`:

```ini
[Unit]
Description=ORB Streamlit Dashboard
After=network-online.target

[Service]
Type=simple
User=orbbot
WorkingDirectory=/home/orbbot/orb
EnvironmentFile=/home/orbbot/orb/.env
ExecStart=/home/orbbot/orb/.venv/bin/streamlit run dashboard.py \
          --server.port=8501 --server.address=0.0.0.0 \
          --server.headless=true --browser.gatherUsageStats=false
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now orb-bot orb-dashboard
sudo journalctl -u orb-bot -f    # follow logs
```

---

## Configuration Reference

| Variable              | Default         | Description                                      |
|-----------------------|-----------------|--------------------------------------------------|
| `ALPACA_API_KEY`      | *required*      | Alpaca API key                                   |
| `ALPACA_API_SECRET`   | *required*      | Alpaca API secret                                |
| `ALPACA_PAPER`        | `true`          | `true` = paper, `false` = live                   |
| `ALPACA_DATA_FEED`    | `iex`           | `iex` (free) or `sip` (paid)                    |
| `SYMBOLS`             | `SPY,QQQ,AAPL`  | Comma-separated watchlist                        |
| `ORB_MINUTES`         | `15`            | Length of opening range window                   |
| `RISK_RATIO`          | `2.0`           | TP = entry ± (range × ratio)                    |
| `POSITION_SIZE_USD`   | `1000.0`        | Notional per trade in USD                        |
| `STOP_LOSS_TYPE`      | `midpoint`      | `midpoint` or `hard` (opposite range extreme)    |
| `MIN_BARS_REQUIRED`   | `13`            | Min bars to validate range (rejects gappy opens) |
| `MAX_DAILY_LOSS_PCT`  | `2.0`           | Kill-switch threshold (% of starting equity)     |
| `DB_PATH`             | `orb_trades.db` | SQLite path (shared with dashboard)              |

---

## Safety Protocols

| Protocol         | Behaviour                                                           |
|------------------|---------------------------------------------------------------------|
| **Kill Switch**  | Checked every 60 s; if daily loss ≥ `MAX_DAILY_LOSS_PCT`, flattens all positions and halts new trades |
| **Auto-Flatten** | At exactly 3:55 PM EST, closes all open positions (avoids overnight risk) |
| **Bracket Orders** | Entry + TP + SL submitted as a single atomic instruction; broker manages exits even if bot crashes |
| **Data Validation** | ORB range is only accepted if ≥ `MIN_BARS_REQUIRED` of `ORB_MINUTES` bars arrived (guards the chaotic 9:30 crush) |
| **Graceful Shutdown** | SIGINT/SIGTERM triggers position flatten before exit |

---

## Paper → Live Checklist

- [ ] Tested for at least 2 weeks on paper with real market hours
- [ ] Kill-switch threshold tuned to your account size
- [ ] `ALPACA_PAPER=false` and live keys set in `.env`
- [ ] `ALPACA_DATA_FEED=sip` (live SIP feed subscribed in Alpaca dashboard)
- [ ] EC2 instance in `us-east-1`
- [ ] EC2 Security Group: port 8501 restricted to your IP only
- [ ] Systemd `Restart=on-failure` confirmed working (kill the process, watch it restart)
- [ ] CloudWatch alarm on EC2 StatusCheckFailed metric
