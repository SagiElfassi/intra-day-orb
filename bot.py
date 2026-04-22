#!/usr/bin/env python3
"""
ORB Dynamic Momentum Trading Bot
Production-ready automated trading system using Alpaca Markets API (alpaca-py).

Strategy Overview:
  - A 1-minute Opening Range is built from 9:30–9:45 AM (configurable).
  - Breakout is only confirmed when three additional filters pass simultaneously:
      1. VWAP filter   — close must be above (LONG) or below (SHORT) intraday VWAP.
      2. Volume Surge  — breakout bar volume must exceed 1.5× the average ORB volume.
      3. Spread filter — bid-ask spread must be ≤ max_spread_pct at order time.
  - Position sizing is volatility-adjusted: risk_pct_equity % of account equity is
    divided by the per-share distance to stop-loss, so every trade risks the same
    dollar amount regardless of price or volatility.
  - Execution uses a single bracket order:
      Full position exits at TP = entry ± ORB_range × risk_ratio.
      Alpaca's OCO mechanism auto-cancels the SL when TP fills.

Architecture:
  - StockDataStream (WebSocket) for real-time 1-min bar data.
  - Asyncio-native event loop with executor-wrapped blocking REST calls.
  - Per-symbol state machine:
      WAITING_RANGE → SCANNING → SUBMITTING → IN_TRADE → COMPLETED
  - SQLite for trade/event logging (read by dashboard.py).
  - Kill switch + auto-flatten (3:55 PM) safety protocols.
  - WebSocket reconnection with exponential back-off + REST resync.
  - Crash recovery: open trades restored from DB on startup.
  - Orphan order cleanup: bracket legs cancelled if position disappears externally.
"""

import asyncio
import logging
import math
import os
import smtplib
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import aiosqlite
import json
from dotenv import load_dotenv

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.models import Bar
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import (
    MarketOrderRequest,
    StopLimitOrderRequest,
    StopLossRequest,
    TakeProfitRequest,
)

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("orb_bot.log"),
    ],
)
logger = logging.getLogger("ORB")


# ── Configuration ──────────────────────────────────────────────────────────────

def _resolve_paper_mode() -> bool:
    try:
        with open("live_config.json") as f:
            cfg = json.load(f)
        if "alpaca_paper" in cfg:
            return bool(cfg["alpaca_paper"])
    except Exception:
        pass
    return os.environ.get("ALPACA_PAPER", "true").lower() == "true"


def _resolve_credentials(paper: bool) -> tuple:
    if paper:
        key    = os.environ.get("ALPACA_API_KEY_PAPER")    or os.environ.get("ALPACA_API_KEY",    "")
        secret = os.environ.get("ALPACA_API_SECRET_PAPER") or os.environ.get("ALPACA_API_SECRET", "")
    else:
        key    = os.environ.get("ALPACA_API_KEY_LIVE")     or os.environ.get("ALPACA_API_KEY",    "")
        secret = os.environ.get("ALPACA_API_SECRET_LIVE")  or os.environ.get("ALPACA_API_SECRET", "")
    return key, secret


@dataclass
class Config:
    paper: bool = field(default_factory=_resolve_paper_mode)
    api_key: str = ""
    api_secret: str = ""

    def __post_init__(self):
        if not self.api_key or not self.api_secret:
            self.api_key, self.api_secret = _resolve_credentials(self.paper)
        try:
            with open("live_config.json") as f:
                saved = json.load(f)
            scalar_fields = [
                ("min_orb_volume", int),    ("min_atr_pct", float),     ("max_spread_pct", float),
                ("risk_pct_equity", float), ("max_position_usd", float),("risk_ratio", float),
                ("volume_surge_mult", float),("vwap_filter", bool),
            ]
            for attr, cast in scalar_fields:
                if attr in saved:
                    setattr(self, attr, cast(saved[attr]))
            if "symbols" in saved and isinstance(saved["symbols"], list):
                self.symbols = [s.strip().upper() for s in saved["symbols"] if s.strip()]
            if "orb_minutes" in saved:
                self.orb_minutes = int(saved["orb_minutes"])
            if "stop_loss_type" in saved:
                self.stop_loss_type = str(saved["stop_loss_type"])
            if "trading_window_end_hour" in saved:
                self.trading_window_end_hour = int(saved["trading_window_end_hour"])
            if "trading_window_end_minute" in saved:
                self.trading_window_end_minute = int(saved["trading_window_end_minute"])
        except Exception:
            pass

    # Trading universe
    symbols: List[str] = field(
        default_factory=lambda: [
            s.strip().upper()
            for s in os.environ.get("SYMBOLS", "SPY,QQQ,AAPL,META,TSLA").split(",")
        ]
    )

    # ORB parameters
    orb_minutes: int   = field(default_factory=lambda: int(os.environ.get("ORB_MINUTES", "15")))
    risk_ratio: float  = field(default_factory=lambda: float(os.environ.get("RISK_RATIO", "2.0")))
    stop_loss_type: str = field(default_factory=lambda: os.environ.get("STOP_LOSS_TYPE", "midpoint"))
    min_bars_required: int = field(default_factory=lambda: int(os.environ.get("MIN_BARS_REQUIRED", "13")))

    # Multi-factor confirmation filters
    vwap_filter: bool         = field(default_factory=lambda: os.environ.get("VWAP_FILTER", "true").lower() == "true")
    volume_surge_mult: float  = field(default_factory=lambda: float(os.environ.get("VOLUME_SURGE_MULT", "1.5")))
    min_orb_volume: int       = field(default_factory=lambda: int(os.environ.get("MIN_ORB_VOLUME", "1000000")))
    min_atr_pct: float        = field(default_factory=lambda: float(os.environ.get("MIN_ATR_PCT", "2.0")))
    max_spread_pct: float     = field(default_factory=lambda: float(os.environ.get("MAX_SPREAD_PCT", "0.05")))

    # Institutional risk management
    risk_pct_equity: float  = field(default_factory=lambda: float(os.environ.get("RISK_PCT_EQUITY", "1.0")))
    max_position_usd: float = field(default_factory=lambda: float(os.environ.get("MAX_POSITION_USD", "0")))

    # Legacy fixed size (kept for backtest compatibility; bot uses risk_pct_equity)
    position_size_usd: float = field(default_factory=lambda: float(os.environ.get("POSITION_SIZE_USD", "1000.0")))

    # Time-of-day filter — no new entries after this time
    trading_window_end_hour:   int   = 11
    trading_window_end_minute: int   = 0

    # ATR stop-loss multiplier (used when stop_loss_type="atr")
    atr_sl_mult: float = 1.5

    # Stop-limit entry: limit_price offset from stop_price (fraction, e.g. 0.002 = 0.2%)
    entry_limit_offset_pct: float = 0.002

    # Hard leverage cap: max position value as % of account equity (0 = unlimited)
    max_leverage_pct: float = 25.0

    # Kill-switch + flatten
    max_daily_loss_pct: float = field(default_factory=lambda: float(os.environ.get("MAX_DAILY_LOSS_PCT", "2.0")))
    flatten_hour: int   = 15
    flatten_minute: int = 55

    data_feed: DataFeed = field(
        default_factory=lambda: (
            DataFeed.SIP
            if os.environ.get("ALPACA_DATA_FEED", "iex").lower() == "sip"
            else DataFeed.IEX
        )
    )
    db_path: str = field(default_factory=lambda: os.environ.get("DB_PATH", "orb_trades.db"))
    tz: ZoneInfo = field(default_factory=lambda: ZoneInfo("America/New_York"))

    # Email notifications
    email_sender:    str = field(default_factory=lambda: os.environ.get("EMAIL_SENDER", ""))
    email_password:  str = field(default_factory=lambda: os.environ.get("EMAIL_PASSWORD", ""))
    email_recipient: str = field(default_factory=lambda: os.environ.get("EMAIL_RECIPIENT", ""))


# ── Per-Symbol State ───────────────────────────────────────────────────────────

STATUS_WAITING    = "WAITING_RANGE"
STATUS_SCANNING   = "SCANNING"
STATUS_SUBMITTING = "SUBMITTING"   # order REST call in flight — blocks re-entry
STATUS_IN_TRADE   = "IN_TRADE"     # bracket order live
STATUS_COMPLETED  = "COMPLETED"
STATUS_INVALID    = "INVALID"


@dataclass
class TickerState:
    symbol: str
    status: str = STATUS_WAITING
    bars: List[Bar] = field(default_factory=list)   # ORB window bars
    range_high: Optional[float] = None
    range_low:  Optional[float] = None
    range_mid:  Optional[float] = None
    avg_orb_volume: float = 0.0                     # average volume per ORB bar

    order_id:  Optional[str] = None
    order1_id: Optional[str] = None              # kept for DB/resume compat

    trade_db_id: Optional[int] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    trade_date: Optional[str] = None

    # Position details
    side: Optional[str] = None                   # "LONG" | "SHORT"
    entry_price: Optional[float] = None
    sl_price:    Optional[float] = None
    tp_price:    Optional[float] = None
    entry_qty_requested: int = 0
    entry_qty_filled:    int = 0

    # Running VWAP (accumulated from 9:30 AM)
    vwap_num: float = 0.0   # Σ(typical_price × volume)
    vwap_den: float = 0.0   # Σ(volume)

    # ATR (% of price) cached at range-finalization for use in SL calculation
    atr_pct: float = 0.0

    # 1-entry-per-day guardrail: True once an order is successfully submitted
    has_entered_today: bool = False


# ── Database Manager ───────────────────────────────────────────────────────────

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    date         TEXT    NOT NULL,
                    symbol       TEXT    NOT NULL,
                    side         TEXT    NOT NULL,
                    qty          REAL    NOT NULL,
                    entry_price  REAL,
                    take_profit  REAL    NOT NULL,
                    stop_loss    REAL    NOT NULL,
                    status       TEXT    NOT NULL,
                    exit_price   REAL,
                    realized_pnl REAL,
                    order_id     TEXT,
                    created_at   TEXT    NOT NULL,
                    closed_at    TEXT
                );
                CREATE TABLE IF NOT EXISTS orb_ranges (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    date          TEXT NOT NULL,
                    symbol        TEXT NOT NULL,
                    range_high    REAL NOT NULL,
                    range_low     REAL NOT NULL,
                    range_mid     REAL NOT NULL,
                    bars_received INTEGER NOT NULL,
                    valid         INTEGER NOT NULL,
                    created_at    TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS price_bars (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol    TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    open      REAL,
                    high      REAL,
                    low       REAL,
                    close     REAL,
                    volume    INTEGER,
                    UNIQUE(symbol, timestamp)
                );
                CREATE TABLE IF NOT EXISTS bot_events (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp  TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message    TEXT
                );
            """)
            # Schema migration — add new columns to existing DBs
            migrations = [
                "ALTER TABLE trades ADD COLUMN order2_id TEXT",
                "ALTER TABLE trades ADD COLUMN tp1_price REAL",
                "ALTER TABLE trades ADD COLUMN tp2_price REAL",
                "ALTER TABLE trades ADD COLUMN partial_pnl REAL DEFAULT 0",
                "ALTER TABLE trades ADD COLUMN gap_pct REAL",
                "ALTER TABLE trades ADD COLUMN rvol REAL",
                "ALTER TABLE trades ADD COLUMN entry_vwap REAL",
            ]
            for stmt in migrations:
                try:
                    await db.execute(stmt)
                except Exception:
                    pass  # column already exists
            await db.commit()
        logger.info("Database initialized at %s", self.db_path)

    async def log_event(self, event_type: str, message: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO bot_events (timestamp, event_type, message) VALUES (?,?,?)",
                (datetime.now(timezone.utc).isoformat(), event_type, message),
            )
            await db.commit()

    async def save_orb_range(self, symbol, date, high, low, mid, bars_received, valid):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO orb_ranges
                   (date, symbol, range_high, range_low, range_mid, bars_received, valid, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (date, symbol, high, low, mid, bars_received, int(valid),
                 datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()

    async def insert_trade(
        self,
        date: str, symbol: str, side: str, qty: float,
        take_profit: float, stop_loss: float, order_id: str,
        order2_id: Optional[str] = None,
        tp1_price: Optional[float] = None,
        tp2_price: Optional[float] = None,
        gap_pct: float = 0.0,
        rvol: float = 0.0,
        entry_vwap: float = 0.0,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO trades
                   (date, symbol, side, qty, take_profit, stop_loss, status,
                    order_id, created_at, order2_id, tp1_price, tp2_price,
                    partial_pnl, gap_pct, rvol, entry_vwap)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (date, symbol, side, qty, take_profit, stop_loss, "PENDING",
                 order_id, datetime.now(timezone.utc).isoformat(),
                 order2_id, tp1_price, tp2_price, 0.0, gap_pct, rvol, entry_vwap),
            )
            await db.commit()
            return cursor.lastrowid

    async def update_trade(self, trade_id: int, **kwargs):
        if not kwargs:
            return
        sets   = ", ".join(f"{k}=?" for k in kwargs)
        values = list(kwargs.values()) + [trade_id]
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(f"UPDATE trades SET {sets} WHERE id=?", values)
            await db.commit()

    async def save_bar(self, bar: Bar):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR IGNORE INTO price_bars
                   (symbol, timestamp, open, high, low, close, volume)
                   VALUES (?,?,?,?,?,?,?)""",
                (bar.symbol, bar.timestamp.isoformat(),
                 bar.open, bar.high, bar.low, bar.close, bar.volume),
            )
            await db.commit()

    async def fetch_orb_bars(self, symbol: str, today: str, orb_end_minute: int) -> list:
        """Return bars from 9:30 AM up to orb_end_minute for range seeding."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT high, low, close, volume FROM price_bars
                   WHERE symbol=? AND DATE(timestamp)=?
                     AND (strftime('%H','timestamp') = '09'
                          AND CAST(strftime('%M','timestamp') AS INTEGER) BETWEEN 30 AND ?)
                   ORDER BY timestamp""",
                (symbol, today, orb_end_minute - 1),
            )
            return [dict(r) for r in await cursor.fetchall()]

    async def fetch_all_bars_today(self, symbol: str, today: str) -> list:
        """Return all bars for today at or after 9:30 AM for VWAP reconstruction."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT high, low, close, volume FROM price_bars
                   WHERE symbol=? AND DATE(timestamp)=?
                     AND (strftime('%H','timestamp') > '09'
                          OR (strftime('%H','timestamp') = '09'
                              AND CAST(strftime('%M','timestamp') AS INTEGER) >= 30))
                   ORDER BY timestamp""",
                (symbol, today),
            )
            return [dict(r) for r in await cursor.fetchall()]

    async def load_active_trades(self, today: str) -> List[dict]:
        """Return today's trades that were still open at last shutdown."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades WHERE date=? AND status IN "
                "('PENDING','OPEN')",
                (today,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ── Core Bot ───────────────────────────────────────────────────────────────────

class ORBBot:
    def __init__(self, config: Config):
        self.config = config
        self.db     = DatabaseManager(config.db_path)

        self.trading_client = TradingClient(
            api_key=config.api_key, secret_key=config.api_secret, paper=config.paper,
        )
        self.stream = StockDataStream(
            api_key=config.api_key, secret_key=config.api_secret, feed=config.data_feed,
        )
        self.historical_client = StockHistoricalDataClient(
            api_key=config.api_key, secret_key=config.api_secret,
        )

        self.states: Dict[str, TickerState] = {
            sym: TickerState(symbol=sym) for sym in config.symbols
        }

        self.active_symbols: set = set(config.symbols)

        self.starting_equity: float = 0.0
        self.kill_switch_triggered: bool = False
        self._last_bar_time: float = 0.0
        self._stream_reconnect_lock = asyncio.Lock()

    # ── Initialisation ─────────────────────────────────────────────────────────

    async def _fetch_starting_equity(self):
        loop    = asyncio.get_running_loop()
        account = await loop.run_in_executor(None, self.trading_client.get_account)
        self.starting_equity = float(account.equity)
        logger.info("Starting equity: $%.2f", self.starting_equity)

    # ── Crash Recovery ─────────────────────────────────────────────────────────

    async def _resume_from_db(self):
        today  = datetime.now(self.config.tz).strftime("%Y-%m-%d")
        active = await self.db.load_active_trades(today)
        if not active:
            return

        logger.info("Resume: %d active trade(s) in DB for %s", len(active), today)
        loop = asyncio.get_running_loop()

        for trade in active:
            symbol   = trade["symbol"]
            order_id = trade["order_id"]
            trade_id = trade["id"]
            db_status = trade["status"]

            if symbol not in self.states:
                continue
            state = self.states[symbol]

            try:
                order = await loop.run_in_executor(
                    None, lambda oid=order_id: self.trading_client.get_order_by_id(oid)
                )
                alpaca_status = str(order.status).lower()

                if alpaca_status in ("filled", "partially_filled", "accepted",
                                     "pending_new", "new"):
                    filled_qty = int(float(order.filled_qty or trade["qty"]))
                    async with state.lock:
                        state.status              = STATUS_IN_TRADE
                        state.order_id            = order_id
                        state.order1_id           = order_id
                        state.trade_db_id         = trade_id
                        state.trade_date          = today
                        state.entry_qty_requested = int(float(trade["qty"]))
                        state.entry_qty_filled    = filled_qty
                        state.tp_price            = trade.get("tp1_price") or trade.get("take_profit")
                        state.sl_price            = trade.get("stop_loss")
                        state.entry_price         = trade.get("entry_price")
                        state.side                = trade.get("side")
                        state.has_entered_today   = True
                    logger.info("%s | RESUMED → IN_TRADE (order=%s)", symbol, order_id)
                    await self.db.log_event("RESUMED", f"{symbol}: IN_TRADE order={order_id}")
                    self.active_symbols.add(symbol)

                elif alpaca_status in ("canceled", "expired", "done_for_day",
                                       "stopped", "rejected"):
                    await self.db.update_trade(
                        trade_id, status="CLOSED_CANCELLED",
                        closed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    async with state.lock:
                        state.trade_date = today
                else:
                    logger.warning(
                        "%s | Unknown status '%s' on resume — assuming IN_TRADE", symbol, alpaca_status
                    )
                    async with state.lock:
                        state.status          = STATUS_IN_TRADE
                        state.order_id        = order_id
                        state.order1_id       = order_id
                        state.trade_db_id     = trade_id
                        state.trade_date      = today
                        state.entry_qty_requested = int(float(trade["qty"]))

            except Exception as exc:
                logger.error("%s | Resume verify failed: %s — assuming IN_TRADE", symbol, exc)
                async with state.lock:
                    state.status          = STATUS_IN_TRADE
                    state.order_id        = order_id
                    state.order1_id       = order_id
                    state.trade_db_id     = trade_id
                    state.trade_date      = today
                    state.entry_qty_requested = int(float(trade["qty"]))

    # ── WebSocket Bar Handler ──────────────────────────────────────────────────

    async def on_bar(self, bar: Bar):
        if self.kill_switch_triggered:
            return

        symbol = bar.symbol
        if symbol not in self.active_symbols:
            return

        self._last_bar_time = datetime.now(timezone.utc).timestamp()
        state = self.states[symbol]
        await self.db.save_bar(bar)

        bar_time_est = bar.timestamp.astimezone(self.config.tz)
        today        = bar_time_est.strftime("%Y-%m-%d")

        async with state.lock:
            # Daily reset
            if state.trade_date != today:
                state.status              = STATUS_WAITING
                state.bars                = []
                state.range_high = state.range_low = state.range_mid = None
                state.avg_orb_volume      = 0.0
                state.order_id = state.order1_id = None
                state.trade_db_id         = None
                state.entry_price = state.sl_price = state.tp_price = None
                state.side                = None
                state.entry_qty_requested = 0
                state.entry_qty_filled    = 0
                state.vwap_num            = 0.0
                state.vwap_den            = 0.0
                state.has_entered_today   = False
                state.trade_date          = today

            # Block new signals for these terminal/transitional states
            if state.status in (
                STATUS_IN_TRADE, STATUS_SUBMITTING,
                STATUS_COMPLETED, STATUS_INVALID,
            ):
                return

            orb_start = bar_time_est.replace(hour=9, minute=30, second=0, microsecond=0)
            orb_end   = orb_start + timedelta(minutes=self.config.orb_minutes)

            # Accumulate running VWAP from market open
            if bar_time_est >= orb_start:
                typical = (bar.high + bar.low + bar.close) / 3.0
                state.vwap_num += typical * float(bar.volume)
                state.vwap_den += float(bar.volume)

            if orb_start <= bar_time_est < orb_end:
                await self._process_range_bar(state, bar, today)
            elif bar_time_est >= orb_end and state.status == STATUS_SCANNING:
                # Time-of-day filter: no new entries after trading_window_end
                window_end = bar_time_est.replace(
                    hour=self.config.trading_window_end_hour,
                    minute=self.config.trading_window_end_minute,
                    second=0, microsecond=0,
                )
                if bar_time_est >= window_end:
                    state.status = STATUS_COMPLETED
                    logger.info(
                        "%s | TIME FILTER: past %02d:%02d EST — no new entries",
                        state.symbol,
                        self.config.trading_window_end_hour,
                        self.config.trading_window_end_minute,
                    )
                    await self.db.log_event(
                        "FILTER_SKIP",
                        f"{state.symbol}: TIME_OF_DAY (past "
                        f"{self.config.trading_window_end_hour:02d}:"
                        f"{self.config.trading_window_end_minute:02d})",
                    )
                    return
                await self._check_breakout(state, bar, today)

    async def _process_range_bar(self, state: TickerState, bar: Bar, today: str):
        state.bars.append(bar)
        logger.debug(
            "%s | ORB bar %d/%d  H=%.2f L=%.2f C=%.2f V=%d",
            state.symbol, len(state.bars), self.config.orb_minutes,
            bar.high, bar.low, bar.close, bar.volume,
        )
        if len(state.bars) >= self.config.orb_minutes:
            await self._finalize_range(state, today)

    async def _finalize_range(self, state: TickerState, today: str):
        bars_received = len(state.bars)
        valid         = bars_received >= self.config.min_bars_required

        if not valid:
            state.status = STATUS_INVALID
            logger.warning("%s | INVALID range — %d/%d bars", state.symbol, bars_received, self.config.orb_minutes)
            await self.db.save_orb_range(state.symbol, today, 0, 0, 0, bars_received, False)
            await self.db.log_event("INVALID_RANGE", f"{state.symbol}: {bars_received}/{self.config.orb_minutes}")
            return

        state.range_high    = max(b.high   for b in state.bars)
        state.range_low     = min(b.low    for b in state.bars)
        state.range_mid     = (state.range_high + state.range_low) / 2
        state.avg_orb_volume = sum(float(b.volume) for b in state.bars) / bars_received

        # Volume filter
        orb_volume = sum(int(b.volume) for b in state.bars)
        if orb_volume < self.config.min_orb_volume:
            state.status = STATUS_INVALID
            reason = f"LOW_VOLUME ({orb_volume:,} < {self.config.min_orb_volume:,})"
            logger.warning("%s | FILTER SKIP: %s", state.symbol, reason)
            await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
            return

        # ATR filter — also cache for ATR-based stop-loss
        atr_pct = await self._fetch_atr_pct(state.symbol, state.range_high)
        state.atr_pct = atr_pct
        if self.config.min_atr_pct > 0 and atr_pct < self.config.min_atr_pct:
            state.status = STATUS_INVALID
            reason = f"LOW_ATR ({atr_pct:.2f}% < {self.config.min_atr_pct:.1f}%)"
            logger.warning("%s | FILTER SKIP: %s", state.symbol, reason)
            await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
            return

        state.status = STATUS_SCANNING
        logger.info(
            "%s | ORB range SET — H=%.2f L=%.2f Mid=%.2f AvgVol=%.0f (bars=%d)",
            state.symbol, state.range_high, state.range_low,
            state.range_mid, state.avg_orb_volume, bars_received,
        )
        await self.db.save_orb_range(
            state.symbol, today,
            state.range_high, state.range_low, state.range_mid,
            bars_received, True,
        )

    async def _check_breakout(self, state: TickerState, bar: Bar, today: str):
        """
        Multi-factor breakout confirmation.
        All three filters must pass before an order is submitted:
          1. Close-based ORB breakout (no wick fakes).
          2. VWAP filter — long only above VWAP, short only below VWAP.
          3. Volume surge — breakout bar must print > volume_surge_mult × avg ORB volume.
          4. Spread filter — bid-ask spread checked live via quote API.
        """
        close = bar.close

        if close > state.range_high:
            signal = "LONG"
        elif close < state.range_low:
            signal = "SHORT"
        else:
            return

        # 1-entry-per-day guardrail
        if state.has_entered_today:
            state.status = STATUS_COMPLETED
            return

        # ── VWAP confirmation ──────────────────────────────────────────────────
        if self.config.vwap_filter and state.vwap_den > 0:
            vwap = state.vwap_num / state.vwap_den
            if signal == "LONG" and close <= vwap:
                reason = f"BELOW_VWAP (close={close:.2f} ≤ vwap={vwap:.2f}) — retrying"
                logger.info("%s | FILTER RETRY: %s", state.symbol, reason)
                await self.db.log_event("FILTER_RETRY", f"{state.symbol}: {reason}")
                return
            if signal == "SHORT" and close >= vwap:
                reason = f"ABOVE_VWAP (close={close:.2f} ≥ vwap={vwap:.2f}) — retrying"
                logger.info("%s | FILTER RETRY: %s", state.symbol, reason)
                await self.db.log_event("FILTER_RETRY", f"{state.symbol}: {reason}")
                return

        # ── Volume surge confirmation ──────────────────────────────────────────
        if self.config.volume_surge_mult > 0 and state.avg_orb_volume > 0:
            min_vol = state.avg_orb_volume * self.config.volume_surge_mult
            if float(bar.volume) < min_vol:
                reason = (f"LOW_VOL_SURGE (bar={bar.volume:,} < "
                          f"{self.config.volume_surge_mult}×avg={state.avg_orb_volume:,.0f}) — retrying")
                logger.info("%s | FILTER RETRY: %s", state.symbol, reason)
                await self.db.log_event("FILTER_RETRY", f"{state.symbol}: {reason}")
                return

        # ── Spread filter — transient; retry next bar ──────────────────────────
        spread_pct = await self._fetch_spread_pct(state.symbol)
        if self.config.max_spread_pct > 0 and spread_pct > self.config.max_spread_pct:
            reason = f"HIGH_SPREAD ({spread_pct:.4f}% > {self.config.max_spread_pct:.3f}%) — will retry"
            logger.warning("%s | FILTER RETRY: %s", state.symbol, reason)
            await self.db.log_event("FILTER_RETRY", f"{state.symbol}: {reason}")
            return

        # All filters passed — atomically commit to SUBMITTING before any order I/O
        state.status = STATUS_SUBMITTING

        logger.info(
            "%s | BREAKOUT %s — Close=%.2f H=%.2f L=%.2f VWAP=%.2f Vol=%d",
            state.symbol, signal, close, state.range_high, state.range_low,
            (state.vwap_num / state.vwap_den if state.vwap_den > 0 else 0),
            bar.volume,
        )
        await self._submit_bracket_order(state, bar, signal, today)

    # ── Analyst Filters ────────────────────────────────────────────────────────

    async def _fetch_atr_pct(self, symbol: str, ref_price: float) -> float:
        try:
            loop = asyncio.get_running_loop()
            req  = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=datetime.now(timezone.utc) - timedelta(days=30),
                end=datetime.now(timezone.utc)   - timedelta(days=1),
                feed=self.config.data_feed,
            )
            resp = await loop.run_in_executor(None, lambda: self.historical_client.get_stock_bars(req))
            bars = resp[symbol]
            if len(bars) < 2:
                return 0.0
            trs = [
                max(
                    float(bars[i].high) - float(bars[i].low),
                    abs(float(bars[i].high) - float(bars[i-1].close)),
                    abs(float(bars[i].low)  - float(bars[i-1].close)),
                )
                for i in range(1, len(bars))
            ]
            atr = sum(trs[-14:]) / min(14, len(trs))
            return (atr / ref_price) * 100
        except Exception as exc:
            logger.warning("%s | ATR fetch failed (filter skipped): %s", symbol, exc)
            return 0.0

    async def _fetch_spread_pct(self, symbol: str) -> float:
        try:
            loop = asyncio.get_running_loop()
            req  = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            resp = await loop.run_in_executor(
                None, lambda: self.historical_client.get_stock_latest_quote(req)
            )
            quote = resp[symbol]
            ask = float(quote.ask_price)
            bid = float(quote.bid_price)
            mid = (ask + bid) / 2.0
            if mid <= 0:
                return 0.0
            return ((ask - bid) / mid) * 100
        except Exception as exc:
            logger.warning("%s | Spread fetch failed (filter skipped): %s", symbol, exc)
            return 0.0

    # ── Order Execution ────────────────────────────────────────────────────────

    async def _submit_bracket_order(
        self, state: TickerState, bar: Bar, signal: str, today: str,
    ):
        close     = bar.close
        orb_range = state.range_high - state.range_low
        side      = OrderSide.BUY if signal == "LONG" else OrderSide.SELL

        offset = self.config.entry_limit_offset_pct
        if signal == "LONG":
            entry_stop  = round(state.range_high, 2)
            entry_limit = round(state.range_high * (1 + offset), 2)
        else:
            entry_stop  = round(state.range_low, 2)
            entry_limit = round(state.range_low  * (1 - offset), 2)

        if self.config.stop_loss_type == "atr" and state.atr_pct > 0:
            atr_dollar = (state.atr_pct / 100.0) * entry_stop
            sl_dist    = round(atr_dollar * self.config.atr_sl_mult, 2)
            sl = round(entry_stop - sl_dist, 2) if signal == "LONG" else round(entry_stop + sl_dist, 2)
        elif self.config.stop_loss_type == "midpoint":
            sl = round(state.range_mid, 2)
        else:
            sl = round(state.range_low if signal == "LONG" else state.range_high, 2)

        if signal == "LONG":
            tp = round(entry_stop + orb_range * self.config.risk_ratio, 2)
        else:
            tp = round(entry_stop - orb_range * self.config.risk_ratio, 2)

        risk_per_share = abs(entry_stop - sl)
        if risk_per_share <= 0:
            state.status = STATUS_COMPLETED
            return

        risk_dollar = self.starting_equity * self.config.risk_pct_equity / 100.0
        total_qty   = max(1, math.floor(risk_dollar / risk_per_share))
        if self.config.max_position_usd > 0:
            total_qty = min(total_qty, max(1, math.floor(self.config.max_position_usd / close)))
        # Hard leverage cap: position value ≤ max_leverage_pct% of equity
        if self.config.max_leverage_pct > 0:
            max_pos_qty = max(1, math.floor(
                self.starting_equity * self.config.max_leverage_pct / 100.0 / entry_stop
            ))
            total_qty = min(total_qty, max_pos_qty)

        entry_vwap = round(state.vwap_num / state.vwap_den, 4) if state.vwap_den > 0 else 0.0

        req = StopLimitOrderRequest(
            symbol=state.symbol, qty=total_qty, side=side,
            time_in_force=TimeInForce.DAY,
            stop_price=entry_stop, limit_price=entry_limit,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp),
            stop_loss=StopLossRequest(stop_price=sl),
        )

        try:
            loop  = asyncio.get_running_loop()
            order = await loop.run_in_executor(None, self.trading_client.submit_order, req)

            state.order_id            = str(order.id)
            state.order1_id           = str(order.id)
            state.entry_qty_requested = total_qty
            state.entry_qty_filled    = 0
            state.sl_price            = sl
            state.tp_price            = tp
            state.side                = signal
            state.status              = STATUS_IN_TRADE
            state.has_entered_today   = True

            trade_id = await self.db.insert_trade(
                date=today, symbol=state.symbol, side=signal,
                qty=total_qty, take_profit=tp, stop_loss=sl,
                order_id=state.order_id,
                tp1_price=tp,
                gap_pct=0.0, rvol=0.0,
                entry_vwap=entry_vwap,
            )
            state.trade_db_id = trade_id

            logger.info(
                "%s | ORDER SUBMITTED  stop=%.2f limit=%.2f  "
                "order=%s (x%d->TP=%.2f)  SL=%.2f  risk=$%.0f (%.1f%% equity)",
                state.symbol, entry_stop, entry_limit,
                order.id, total_qty, tp, sl,
                risk_dollar, self.config.risk_pct_equity,
            )
            await self.db.log_event(
                "ORDER_SUBMITTED",
                f"{state.symbol} {signal} qty={total_qty} TP={tp} SL={sl} order={order.id}",
            )
            await self._email_entry(state.symbol, signal, total_qty, entry_stop, sl, tp, risk_dollar)

        except Exception as exc:
            state.status = STATUS_COMPLETED
            logger.error("%s | Order submission FAILED: %s", state.symbol, exc)
            await self.db.log_event("ORDER_ERROR", f"{state.symbol}: {exc}")

    # ── Risk Management ────────────────────────────────────────────────────────

    async def _check_kill_switch(self):
        try:
            loop           = asyncio.get_running_loop()
            account        = await loop.run_in_executor(None, self.trading_client.get_account)
            current_equity = float(account.equity)
        except Exception as exc:
            logger.warning("Kill-switch check failed: %s", exc)
            return

        if self.starting_equity <= 0:
            return
        loss_pct = (self.starting_equity - current_equity) / self.starting_equity * 100
        if loss_pct >= self.config.max_daily_loss_pct:
            logger.critical(
                "KILL SWITCH — loss %.2f%% ≥ threshold %.2f%%",
                loss_pct, self.config.max_daily_loss_pct,
            )
            self.kill_switch_triggered = True
            await self.db.log_event("KILL_SWITCH", f"Loss {loss_pct:.2f}%")
            await self._flatten_all_positions(reason="KILL_SWITCH")
            mode = "PAPER" if self.config.paper else "LIVE"
            await self._send_email(
                f"[ORB {mode}] KILL SWITCH — Daily loss {loss_pct:.2f}%",
                f"<h2 style='color:#ef5350'>Kill Switch Triggered</h2>"
                f"<p>Daily loss <b>{loss_pct:.2f}%</b> exceeded threshold "
                f"<b>{self.config.max_daily_loss_pct:.2f}%</b>. All positions flattened.</p>"
                f"<p>{datetime.now(self.config.tz).strftime('%Y-%m-%d %H:%M EST')}</p>",
            )

    async def _flatten_all_positions(self, reason: str):
        logger.warning("Flattening all positions — reason: %s", reason)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, lambda: self.trading_client.close_all_positions(cancel_orders=True)
            )
        except Exception as exc:
            logger.error("Failed to flatten: %s", exc)

    # ── Email Notifications ────────────────────────────────────────────────────

    def _email_configured(self) -> bool:
        return bool(self.config.email_sender and self.config.email_password and self.config.email_recipient)

    def _send_email_sync(self, subject: str, html: str):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.config.email_sender
        msg["To"]      = self.config.email_recipient
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP("smtp.gmail.com", 587) as s:
            s.starttls()
            s.login(self.config.email_sender, self.config.email_password)
            s.sendmail(self.config.email_sender, self.config.email_recipient, msg.as_string())

    async def _send_email(self, subject: str, html: str):
        if not self._email_configured():
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._send_email_sync, subject, html)
            logger.debug("Email sent: %s", subject)
        except Exception as exc:
            logger.warning("Email failed: %s", exc)

    async def _email_entry(self, symbol: str, signal: str, qty: int, entry: float,
                           sl: float, tp: float, risk_dollar: float):
        mode  = "PAPER" if self.config.paper else "LIVE"
        color = "#26a69a" if signal == "LONG" else "#ef5350"
        arrow = "&#9650;" if signal == "LONG" else "&#9660;"
        html = f"""
        <div style="font-family:monospace;max-width:480px">
          <h2 style="color:{color}">{arrow} {signal} Entry — {symbol} [{mode}]</h2>
          <table style="border-collapse:collapse;width:100%">
            <tr><td><b>Symbol</b></td><td>{symbol}</td></tr>
            <tr><td><b>Side</b></td><td>{signal}</td></tr>
            <tr><td><b>Qty</b></td><td>{qty} shares</td></tr>
            <tr><td><b>Entry</b></td><td>${entry:.2f}</td></tr>
            <tr><td><b>Stop Loss</b></td><td>${sl:.2f}</td></tr>
            <tr><td><b>Take Profit</b></td><td>${tp:.2f}</td></tr>
            <tr><td><b>Risk $</b></td><td>${risk_dollar:.2f}</td></tr>
            <tr><td><b>Time</b></td><td>{datetime.now(self.config.tz).strftime('%H:%M:%S EST')}</td></tr>
          </table>
        </div>"""
        await self._send_email(f"[ORB {mode}] {arrow} {signal} {symbol} @ ${entry:.2f}", html)

    async def _email_exit(self, symbol: str, event: str, fill: float, pnl: float):
        mode   = "PAPER" if self.config.paper else "LIVE"
        labels = {
            "CLOSED_TP": ("Take Profit Hit", "#26a69a"),
            "CLOSED_SL": ("Stop Loss Hit",   "#ef5350"),
        }
        label, color = labels.get(event, (event, "#90a4ae"))
        pnl_color = "#26a69a" if pnl >= 0 else "#ef5350"
        html = f"""
        <div style="font-family:monospace;max-width:480px">
          <h2 style="color:{color}">{label} — {symbol} [{mode}]</h2>
          <table style="border-collapse:collapse;width:100%">
            <tr><td><b>Symbol</b></td><td>{symbol}</td></tr>
            <tr><td><b>Exit Price</b></td><td>${fill:.2f}</td></tr>
            <tr><td><b>PnL</b></td><td style="color:{pnl_color}">${pnl:+.2f}</td></tr>
            <tr><td><b>Time</b></td><td>{datetime.now(self.config.tz).strftime('%H:%M:%S EST')}</td></tr>
          </table>
        </div>"""
        await self._send_email(f"[ORB {mode}] {label} {symbol} ${pnl:+.2f}", html)

    async def _email_eod_summary(self, today: str):
        if not self._email_configured():
            return
        mode = "PAPER" if self.config.paper else "LIVE"
        try:
            import aiosqlite as _aio
            async with _aio.connect(self.config.db_path) as db:
                db.row_factory = _aio.Row
                cur = await db.execute(
                    "SELECT * FROM trades WHERE date=? ORDER BY created_at", (today,)
                )
                trades = [dict(r) for r in await cur.fetchall()]
        except Exception:
            trades = []

        total_pnl  = sum(float(t.get("realized_pnl") or 0) for t in trades)
        wins       = [t for t in trades if float(t.get("realized_pnl") or 0) > 0]
        losses     = [t for t in trades if float(t.get("realized_pnl") or 0) < 0]
        pnl_color  = "#26a69a" if total_pnl >= 0 else "#ef5350"

        rows = ""
        for t in trades:
            pnl  = float(t.get("realized_pnl") or 0)
            c    = "#26a69a" if pnl > 0 else ("#ef5350" if pnl < 0 else "#90a4ae")
            rows += (f"<tr><td>{t['symbol']}</td><td>{t['side']}</td>"
                     f"<td>${float(t.get('entry_price') or 0):.2f}</td>"
                     f"<td>{t.get('status','')}</td>"
                     f"<td style='color:{c}'>${pnl:+.2f}</td></tr>")

        html = f"""
        <div style="font-family:monospace;max-width:600px">
          <h2>End-of-Day Summary — {today} [{mode}]</h2>
          <p style="font-size:1.4em;color:{pnl_color}"><b>Total PnL: ${total_pnl:+.2f}</b></p>
          <p>{len(trades)} trades &nbsp;|&nbsp; {len(wins)} wins &nbsp;|&nbsp; {len(losses)} losses
             &nbsp;|&nbsp; Win rate: {(len(wins)/len(trades)*100 if trades else 0):.0f}%</p>
          <table style="border-collapse:collapse;width:100%;border:1px solid #444">
            <tr style="background:#222"><th>Symbol</th><th>Side</th><th>Entry</th><th>Status</th><th>PnL</th></tr>
            {rows if rows else '<tr><td colspan="5">No trades today</td></tr>'}
          </table>
        </div>"""
        await self._send_email(f"[ORB {mode}] EOD Summary {today} | PnL ${total_pnl:+.2f}", html)

        for state in self.states.values():
            async with state.lock:
                if state.status == STATUS_IN_TRADE:
                    state.status = STATUS_COMPLETED
                    if state.trade_db_id:
                        await self.db.update_trade(
                            state.trade_db_id, status="CLOSED_MANUAL",
                            closed_at=datetime.now(timezone.utc).isoformat(),
                        )
        await self.db.log_event("EOD_FLATTEN", "All positions flattened")

    # ── Background Tasks ───────────────────────────────────────────────────────

    async def _order_fill_monitor(self):
        """Polls every 20 s to sync bracket fills back to the DB."""
        while not self.kill_switch_triggered:
            await asyncio.sleep(20)
            loop = asyncio.get_running_loop()

            for state in self.states.values():
                async with state.lock:
                    if state.status != STATUS_IN_TRADE:
                        continue
                    order_id    = state.order_id
                    trade_db_id = state.trade_db_id
                    prev_filled = state.entry_qty_filled
                    qty_req     = state.entry_qty_requested

                try:
                    if not order_id:
                        continue
                    o = await loop.run_in_executor(
                        None, lambda oid=order_id: self.trading_client.get_order_by_id(oid)
                    )

                    # Track entry fill
                    filled = int(float(o.filled_qty or 0))
                    if filled > 0 and filled != prev_filled:
                        upd: dict = {"status": "OPEN", "qty": filled}
                        if o.filled_avg_price:
                            fill_px = round(float(o.filled_avg_price), 4)
                            upd["entry_price"] = fill_px
                            async with state.lock:
                                state.entry_price      = fill_px
                                state.entry_qty_filled = filled
                        if trade_db_id:
                            await self.db.update_trade(trade_db_id, **upd)
                        if 0 < filled < qty_req:
                            logger.warning("%s | PARTIAL FILL: %d/%d shares",
                                           state.symbol, filled, qty_req)

                    # Check bracket leg fills
                    for leg in (o.legs or []):
                        if str(getattr(leg, "status", "")).lower() != "filled":
                            continue
                        leg_type   = str(getattr(leg, "order_type", "")).lower()
                        fill_price = round(float(leg.filled_avg_price), 4)
                        entry      = state.entry_price or fill_price
                        pnl        = round(
                            (fill_price - entry) * qty_req if state.side == "LONG"
                            else (entry - fill_price) * qty_req,
                            2,
                        )

                        if "limit" in leg_type:
                            db_status = "CLOSED_TP"
                            log_event = "CLOSED_TP"
                            log_msg   = f"{state.symbol} TP fill={fill_price:.4f} pnl={pnl:.2f}"
                            logger.info("%s | TP HIT @ %.4f  pnl=$%.2f", state.symbol, fill_price, pnl)
                        else:
                            db_status = "CLOSED_SL"
                            log_event = "CLOSED_SL"
                            log_msg   = f"{state.symbol} SL fill={fill_price:.4f}"
                            logger.info("%s | SL HIT @ %.4f  pnl=$%.2f", state.symbol, fill_price, pnl)

                        async with state.lock:
                            state.status = STATUS_COMPLETED
                        if trade_db_id:
                            await self.db.update_trade(
                                trade_db_id,
                                exit_price=fill_price,
                                realized_pnl=pnl,
                                status=db_status,
                                closed_at=datetime.now(timezone.utc).isoformat(),
                            )
                        await self.db.log_event(log_event, log_msg)
                        await self._email_exit(state.symbol, db_status, fill_price, pnl)
                        break

                except Exception as exc:
                    logger.debug("Fill monitor error %s: %s", state.symbol, exc)

    async def _orphan_order_cleanup(self):
        """Every 5 min: cancel bracket if the position no longer exists."""
        while not self.kill_switch_triggered:
            await asyncio.sleep(300)
            loop = asyncio.get_running_loop()

            for symbol, state in self.states.items():
                async with state.lock:
                    if state.status != STATUS_IN_TRADE:
                        continue
                    trade_db_id = state.trade_db_id
                    order_ids   = [o for o in (state.order_id,) if o]

                try:
                    try:
                        await loop.run_in_executor(
                            None, lambda s=symbol: self.trading_client.get_open_position(s)
                        )
                        continue  # position still open — nothing to do
                    except Exception:
                        pass  # 404 → position gone

                    # Cancel all associated orders
                    cancelled = 0
                    for oid in order_ids:
                        try:
                            await loop.run_in_executor(
                                None, lambda i=oid: self.trading_client.cancel_order_by_id(i)
                            )
                            cancelled += 1
                        except Exception:
                            pass

                    logger.warning(
                        "%s | Orphan cleanup: position gone, cancelled %d order(s)",
                        symbol, cancelled,
                    )
                    await self.db.log_event(
                        "ORPHAN_CANCEL",
                        f"{symbol}: position closed externally, {cancelled} order(s) cancelled",
                    )
                    async with state.lock:
                        state.status = STATUS_COMPLETED
                        if trade_db_id:
                            await self.db.update_trade(
                                trade_db_id, status="CLOSED_MANUAL",
                                closed_at=datetime.now(timezone.utc).isoformat(),
                            )
                except Exception as exc:
                    logger.debug("Orphan cleanup error %s: %s", symbol, exc)

    async def _resync_from_rest(self):
        loop = asyncio.get_running_loop()
        try:
            positions    = await loop.run_in_executor(None, self.trading_client.get_all_positions)
            open_symbols = {p.symbol for p in positions}
        except Exception as exc:
            logger.error("Post-reconnect resync failed: %s", exc)
            return

        # Backfill bars missed during the disconnect gap
        await self._sync_historical_data()

        for symbol, state in self.states.items():
            async with state.lock:
                if state.status == STATUS_IN_TRADE:
                    trade_db_id = state.trade_db_id
                    if symbol not in open_symbols:
                        logger.warning("%s | Position closed during disconnect — COMPLETED", symbol)
                        state.status = STATUS_COMPLETED
                        if trade_db_id:
                            await self.db.update_trade(
                                trade_db_id, status="CLOSED_MANUAL",
                                closed_at=datetime.now(timezone.utc).isoformat(),
                            )
                        await self.db.log_event("RESYNC", f"{symbol}: closed during disconnect")

                elif state.status == STATUS_WAITING and state.range_high is None:
                    await self._seed_range_from_bars(symbol, state)

        logger.info("Resync complete. Open: %s", list(open_symbols))

    async def _sync_historical_data(self):
        """Fetch 1-min bars missed since last_bar_time (or 9:30 AM on startup) via REST.

        On startup (last_bar_time == 0): saves bars to DB only so that
        _seed_ranges_on_startup can reconstruct VWAP/avg_orb_volume from a
        complete price_bars record.
        On reconnection (last_bar_time > 0): saves and replays bars through
        on_bar to advance the state machine through any missed breakout signals.
        """
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(self.config.tz)
        mkt_open_est  = now_est.replace(hour=9,  minute=30, second=0, microsecond=0)
        mkt_close_est = now_est.replace(hour=16, minute=0,  second=0, microsecond=0)
        if not (mkt_open_est <= now_est <= mkt_close_est):
            return

        is_startup = self._last_bar_time == 0.0
        if is_startup:
            start_utc = mkt_open_est.astimezone(timezone.utc)
        else:
            start_utc = datetime.fromtimestamp(self._last_bar_time, tz=timezone.utc)
            if (now_utc - start_utc).total_seconds() < 90:
                return

        logger.info(
            "Syncing historical bars from %s UTC for %s (startup=%s)",
            start_utc.strftime("%H:%M:%S"), list(self.active_symbols), is_startup,
        )
        loop = asyncio.get_running_loop()
        try:
            req = StockBarsRequest(
                symbol_or_symbols=list(self.active_symbols),
                timeframe=TimeFrame.Minute,
                start=start_utc,
                end=now_utc,
                feed=self.config.data_feed,
            )
            bars_resp = await loop.run_in_executor(
                None, lambda: self.historical_client.get_stock_bars(req)
            )
            saved = 0
            for sym, bar_list in bars_resp.items():
                for bar in bar_list:
                    await self.db.save_bar(bar)
                    saved += 1
                    if not is_startup:
                        await self.on_bar(bar)
            if saved:
                logger.info("Synced %d bar(s) across %d symbol(s)", saved, len(bars_resp))
        except Exception as exc:
            logger.warning("Historical sync failed: %s", exc)

    async def _seed_range_from_bars(self, symbol: str, state: "TickerState"):
        """Load today's ORB bars from DB and set state to SCANNING if range is valid."""
        try:
            today = datetime.now(self.config.tz).strftime("%Y-%m-%d")
            orb_end_min = 30 + self.config.orb_minutes
            rows = await self.db.fetch_orb_bars(symbol, today, orb_end_min)
            if len(rows) < max(1, self.config.orb_minutes * 6 // 10):
                return
            highs  = [r["high"]   for r in rows]
            lows   = [r["low"]    for r in rows]
            vols   = [float(r["volume"]) for r in rows]
            closes = [r["close"]  for r in rows]
            state.range_high     = max(highs)
            state.range_low      = min(lows)
            state.range_mid      = (state.range_high + state.range_low) / 2
            state.avg_orb_volume = sum(vols) / len(vols)
            # Reconstruct VWAP from all bars in DB for today
            all_rows = await self.db.fetch_all_bars_today(symbol, today)
            for r in all_rows:
                typical = (r["high"] + r["low"] + r["close"]) / 3.0
                state.vwap_num += typical * float(r["volume"])
                state.vwap_den += float(r["volume"])
            state.status = STATUS_SCANNING
            logger.info(
                "%s | Range re-seeded from DB — H=%.2f L=%.2f Mid=%.2f (bars=%d)",
                symbol, state.range_high, state.range_low, state.range_mid, len(rows),
            )
            await self.db.save_orb_range(
                symbol, today,
                state.range_high, state.range_low, state.range_mid,
                len(rows), True,
            )
        except Exception as exc:
            logger.warning("%s | _seed_range_from_bars failed: %s", symbol, exc)

    async def _heartbeat_watchdog(self):
        while not self.kill_switch_triggered:
            await asyncio.sleep(90)
            tz  = self.config.tz
            now = datetime.now(tz)
            mkt_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
            mkt_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
            if not (mkt_open <= now <= mkt_close) or self._last_bar_time == 0.0:
                continue
            elapsed = now.timestamp() - self._last_bar_time
            if elapsed > 180:
                logger.warning("HEARTBEAT: no bar in %.0fs during market hours", elapsed)
                await self.db.log_event("HEARTBEAT_WARN", f"No bar in {elapsed:.0f}s")

    async def _risk_monitor(self):
        while not self.kill_switch_triggered:
            await asyncio.sleep(60)
            await self._check_kill_switch()

    async def _auto_flatten_scheduler(self):
        tz     = self.config.tz
        now    = datetime.now(tz)
        target = now.replace(
            hour=self.config.flatten_hour, minute=self.config.flatten_minute,
            second=0, microsecond=0,
        )
        if now >= target:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        logger.info("Auto-flatten in %.0fs (%s EST)", wait, target.strftime("%H:%M"))
        await asyncio.sleep(wait)
        if not self.kill_switch_triggered:
            logger.info("Auto-flatten triggered (3:55 PM EST)")
            await self._flatten_all_positions(reason="AUTO_FLATTEN_3:55PM")
        today = datetime.now(self.config.tz).strftime("%Y-%m-%d")
        await self._email_eod_summary(today)

    # ── Stream with Reconnection ───────────────────────────────────────────────

    async def _run_stream_with_reconnect(self):
        backoff = 5
        while not self.kill_switch_triggered:
            try:
                logger.info("Connecting to Alpaca data stream…")
                await self.stream._run_forever()
                logger.warning("Stream ended — reconnecting…")
                backoff = 5  # clean disconnect → reset backoff
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Stream error: %s", exc)
                # Do NOT reset backoff on error — let it grow so we stop hammering Alpaca

            if self.kill_switch_triggered:
                break
            logger.info("Reconnecting in %ds…", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

            async with self._stream_reconnect_lock:
                self.stream = StockDataStream(
                    api_key=self.config.api_key,
                    secret_key=self.config.api_secret,
                    feed=self.config.data_feed,
                )
                self.stream.subscribe_bars(self.on_bar, *self.active_symbols)

            await self._resync_from_rest()

    async def _seed_ranges_on_startup(self):
        """
        After a mid-day restart, seed ORB ranges from orb_ranges table (not price_bars)
        so a sparse price_bars record (caused by a crash during the ORB window) doesn't
        block recovery.  Also reconstructs VWAP and avg_orb_volume from price_bars.
        """
        now_est = datetime.now(self.config.tz)
        orb_end = now_est.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(minutes=self.config.orb_minutes)
        if now_est <= orb_end:
            return  # still inside the ORB window — live bars will build the range

        today = now_est.strftime("%Y-%m-%d")
        for symbol, state in self.states.items():
            async with state.lock:
                if state.status != STATUS_WAITING or state.range_high is not None:
                    continue

            # Read orb_ranges row outside the lock (async I/O)
            async with aiosqlite.connect(self.config.db_path) as db:
                db.row_factory = aiosqlite.Row
                row = await (await db.execute(
                    "SELECT * FROM orb_ranges WHERE date=? AND symbol=? AND valid=1",
                    (today, symbol),
                )).fetchone()
            if row is None:
                continue

            # Reconstruct VWAP and avg_orb_volume from price_bars
            all_bars  = await self.db.fetch_all_bars_today(symbol, today)
            orb_end_m = 30 + self.config.orb_minutes
            orb_bars  = await self.db.fetch_orb_bars(symbol, today, orb_end_m)

            async with state.lock:
                if state.status != STATUS_WAITING:
                    continue  # another coroutine already set the state
                state.range_high     = row["range_high"]
                state.range_low      = row["range_low"]
                state.range_mid      = row["range_mid"]
                state.avg_orb_volume = (
                    sum(float(r["volume"]) for r in orb_bars) / len(orb_bars)
                    if orb_bars else 0.0
                )
                for r in all_bars:
                    typical = (r["high"] + r["low"] + r["close"]) / 3.0
                    state.vwap_num += typical * float(r["volume"])
                    state.vwap_den += float(r["volume"])
                state.status = STATUS_SCANNING

            logger.info(
                "%s | Startup: range seeded from DB — H=%.2f L=%.2f AvgVol=%.0f VWAP=%.3f",
                symbol, row["range_high"], row["range_low"],
                state.avg_orb_volume,
                state.vwap_num / state.vwap_den if state.vwap_den > 0 else 0,
            )

    # ── Entry Point ────────────────────────────────────────────────────────────

    async def run(self):
        logger.info("ORB Bot starting — universe: %s", self.config.symbols)
        logger.info("Paper: %s | Feed: %s", self.config.paper, self.config.data_feed)

        await self.db.initialize()
        await self._fetch_starting_equity()
        await self.db.log_event("START", f"universe={self.config.symbols}")

        await self._resume_from_db()
        await self._sync_historical_data()    # fetch bars from 9:30 AM → DB
        await self._seed_ranges_on_startup()  # seed ranges + VWAP from complete DB

        self.stream.subscribe_bars(self.on_bar, *self.config.symbols)

        asyncio.create_task(self._risk_monitor())
        asyncio.create_task(self._auto_flatten_scheduler())
        asyncio.create_task(self._order_fill_monitor())
        asyncio.create_task(self._orphan_order_cleanup())
        asyncio.create_task(self._heartbeat_watchdog())

        await self._run_stream_with_reconnect()


# ── Graceful Shutdown ──────────────────────────────────────────────────────────

async def _shutdown(bot: ORBBot):
    logger.info("Shutdown — flattening positions…")
    await bot._flatten_all_positions(reason="SHUTDOWN")


# ── Main ───────────────────────────────────────────────────────────────────────

PID_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.pid")


def main():
    # Write PID so the dashboard can restart us cleanly
    with open(PID_FILE, "w") as _f:
        _f.write(str(os.getpid()))

    config = Config()
    bot    = ORBBot(config)

    import signal as _signal

    async def _run():
        loop = asyncio.get_running_loop()
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown(bot)))
            except NotImplementedError:
                pass  # Windows
        await bot.run()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Bot shut down.")
        try:
            os.remove(PID_FILE)
        except OSError:
            pass


if __name__ == "__main__":
    main()
