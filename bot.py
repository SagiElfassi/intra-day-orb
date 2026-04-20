#!/usr/bin/env python3
"""
ORB Dynamic Momentum Trading Bot
Production-ready automated trading system using Alpaca Markets API (alpaca-py).

Strategy Overview:
  - At 9:25 AM a PreMarketScanner ranks the configured universe by Gap % and RVOL,
    selecting the top N symbols with the most institutional momentum for the day.
  - A 1-minute Opening Range is built from 9:30–9:45 AM (configurable).
  - Breakout is only confirmed when three additional filters pass simultaneously:
      1. VWAP filter   — close must be above (LONG) or below (SHORT) intraday VWAP.
      2. Volume Surge  — breakout bar volume must exceed 1.5× the average ORB volume.
      3. Spread filter — bid-ask spread must be ≤ max_spread_pct at order time.
  - Position sizing is volatility-adjusted: risk_pct_equity % of account equity is
    divided by the per-share distance to stop-loss, so every trade risks the same
    dollar amount regardless of price or volatility.
  - Execution uses a two-leg scale-out bracket:
      Leg 1 (50% of shares): TP1 at 1.0R — locks in a guaranteed partial win.
      Leg 2 (50% of shares): TP2 at 3.0R — let winners run.
    When Leg 1 fills the bot cancels the original SL on Leg 2 and replaces it with
    a break-even stop, eliminating further downside on the second half.

Architecture:
  - StockDataStream (WebSocket) for real-time 1-min bar data.
  - Asyncio-native event loop with executor-wrapped blocking REST calls.
  - Per-symbol state machine:
      WAITING_RANGE → SCANNING → SUBMITTING → IN_TRADE
      → PARTIAL_EXIT → BREAK_EVEN → COMPLETED
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
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import aiosqlite
import json
from dotenv import load_dotenv

from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.models import Bar
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderClass, OrderSide, TimeInForce
from alpaca.trading.requests import (
    MarketOrderRequest,
    StopLossRequest,
    StopOrderRequest,
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
                ("gap_min_pct", float),     ("rvol_min", float),        ("risk_pct_equity", float),
                ("max_position_usd", float),("tp1_r", float),           ("tp2_r", float),
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

    # Pre-market scanner
    gap_min_pct: float  = field(default_factory=lambda: float(os.environ.get("GAP_MIN_PCT",  "1.5")))
    rvol_min: float     = field(default_factory=lambda: float(os.environ.get("RVOL_MIN",     "2.0")))
    scanner_top_n: int  = field(default_factory=lambda: int(os.environ.get("SCANNER_TOP_N", "5")))

    # Multi-factor confirmation filters
    vwap_filter: bool         = field(default_factory=lambda: os.environ.get("VWAP_FILTER", "true").lower() == "true")
    volume_surge_mult: float  = field(default_factory=lambda: float(os.environ.get("VOLUME_SURGE_MULT", "1.5")))
    min_orb_volume: int       = field(default_factory=lambda: int(os.environ.get("MIN_ORB_VOLUME", "1000000")))
    min_atr_pct: float        = field(default_factory=lambda: float(os.environ.get("MIN_ATR_PCT", "2.0")))
    max_spread_pct: float     = field(default_factory=lambda: float(os.environ.get("MAX_SPREAD_PCT", "0.05")))

    # Institutional risk management
    risk_pct_equity: float  = field(default_factory=lambda: float(os.environ.get("RISK_PCT_EQUITY", "1.0")))
    max_position_usd: float = field(default_factory=lambda: float(os.environ.get("MAX_POSITION_USD", "0")))
    tp1_r: float            = field(default_factory=lambda: float(os.environ.get("TP1_R", "1.0")))
    tp2_r: float            = field(default_factory=lambda: float(os.environ.get("TP2_R", "3.0")))

    # Legacy fixed size (kept for backtest compatibility; bot uses risk_pct_equity)
    position_size_usd: float = field(default_factory=lambda: float(os.environ.get("POSITION_SIZE_USD", "1000.0")))

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


# ── Per-Symbol State ───────────────────────────────────────────────────────────

STATUS_WAITING       = "WAITING_RANGE"
STATUS_SCANNING      = "SCANNING"
STATUS_SUBMITTING    = "SUBMITTING"      # order REST call in flight — blocks re-entry
STATUS_IN_TRADE      = "IN_TRADE"        # both bracket legs live
STATUS_PARTIAL_EXIT  = "PARTIAL_EXIT"    # TP1 hit, SL-to-BE move in progress
STATUS_BREAK_EVEN    = "BREAK_EVEN"      # TP1 hit, SL now at entry, TP2 leg still open
STATUS_COMPLETED     = "COMPLETED"
STATUS_INVALID       = "INVALID"


@dataclass
class TickerState:
    symbol: str
    status: str = STATUS_WAITING
    bars: List[Bar] = field(default_factory=list)   # ORB window bars
    range_high: Optional[float] = None
    range_low:  Optional[float] = None
    range_mid:  Optional[float] = None
    avg_orb_volume: float = 0.0                     # average volume per ORB bar

    # Multi-leg order IDs
    order_id:     Optional[str] = None              # alias for order1_id (backward-compat)
    order1_id:    Optional[str] = None              # TP1 bracket (half qty)
    order2_id:    Optional[str] = None              # TP2 bracket (remaining qty)
    sl2_order_id: Optional[str] = None              # break-even stop (placed after TP1 hits)

    trade_db_id: Optional[int] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    trade_date: Optional[str] = None

    # Position details
    side: Optional[str] = None                      # "LONG" | "SHORT"
    entry_price: Optional[float] = None
    sl_price:    Optional[float] = None
    tp1_price:   Optional[float] = None
    tp2_price:   Optional[float] = None
    half_qty: int = 0
    entry_qty_requested: int = 0
    entry_qty_filled:    int = 0

    # Running VWAP (accumulated from 9:30 AM)
    vwap_num: float = 0.0   # Σ(typical_price × volume)
    vwap_den: float = 0.0   # Σ(volume)

    # Partial-exit tracking
    realized_pnl_tp1: float = 0.0

    # Pre-market scanner results (recorded for audit trail)
    gap_pct: float = 0.0
    rvol:    float = 0.0


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

    async def load_active_trades(self, today: str) -> List[dict]:
        """Return today's trades that were still open at last shutdown."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades WHERE date=? AND status IN "
                "('PENDING','OPEN','PARTIAL_EXIT','BREAK_EVEN')",
                (today,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ── Pre-Market Scanner ─────────────────────────────────────────────────────────

class PreMarketScanner:
    """
    Runs at 9:25 AM EST to select the day's trading symbols.

    Scoring criteria (both must pass the configured minimums):
      Gap %  = (today's first premarket open − yesterday's close) / yesterday's close × 100
               Measures overnight momentum or news-driven gap.
      RVOL   = (today's premarket volume / 325 premarket minutes)
               ÷ (20-day avg daily volume / 390 regular-session minutes)
               > 1.0 means above-average premarket pace; 2.0 means 2× normal pace.

    Symbols are ranked by RVOL (highest first) among those that pass both thresholds.
    The top-N are returned as the active trading universe for the day.

    Note: uses the historical REST API, not the live stream.
          IEX feed covers premarket volume for US-listed stocks.
          SIP feed provides consolidated tape for more accurate volume.
    """

    _PREMARKET_MINUTES: int = 325   # 4:00 AM → 9:25 AM
    _SESSION_MINUTES:   int = 390   # 9:30 AM → 4:00 PM

    def __init__(self, client: StockHistoricalDataClient, feed: DataFeed):
        self.client = client
        self.feed   = feed

    async def scan(
        self,
        universe: List[str],
        gap_min_pct: float,
        rvol_min: float,
        top_n: int = 5,
    ) -> List[Tuple[str, float, float]]:
        """
        Returns List[(symbol, gap_pct, rvol)] for the top qualifying symbols.
        Returns an empty list if no symbol meets both thresholds.
        """
        loop    = asyncio.get_running_loop()
        today   = datetime.now(ZoneInfo("America/New_York")).date()
        results: List[Tuple[str, float, float]] = []

        for symbol in universe:
            try:
                gap_pct, rvol = await loop.run_in_executor(
                    None, lambda s=symbol: self._score_symbol(s, today)
                )
                logger.info(
                    "Scanner  %s  gap=%.2f%%  rvol=%.2fx", symbol, gap_pct, rvol
                )
                if abs(gap_pct) >= gap_min_pct and rvol >= rvol_min:
                    results.append((symbol, gap_pct, rvol))
            except Exception as exc:
                logger.debug("Scanner skip %s: %s", symbol, exc)

        results.sort(key=lambda x: x[2], reverse=True)
        return results[:top_n]

    def _score_symbol(self, symbol: str, today) -> Tuple[float, float]:
        """Blocking helper — runs in executor."""
        # ── Daily bars for previous close + 20-day average volume ──
        daily_req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=datetime(today.year, today.month, today.day, tzinfo=ZoneInfo("America/New_York"))
                  - timedelta(days=30),
            end=datetime(today.year, today.month, today.day, tzinfo=ZoneInfo("America/New_York")),
            feed=self.feed,
        )
        daily_resp = self.client.get_stock_bars(daily_req)
        daily_bars = daily_resp[symbol] if symbol in daily_resp else []
        if len(daily_bars) < 2:
            return 0.0, 0.0

        prev_close   = float(daily_bars[-1].close)
        avg_daily_vol = (
            sum(float(b.volume) for b in daily_bars[-20:]) / min(20, len(daily_bars))
        )
        if avg_daily_vol <= 0:
            return 0.0, 0.0

        # ── Today's premarket bars (4:00 AM → 9:25 AM) ──
        tz = ZoneInfo("America/New_York")
        pm_start = datetime(today.year, today.month, today.day, 4,  0, tzinfo=tz)
        pm_end   = datetime(today.year, today.month, today.day, 9, 25, tzinfo=tz)

        pm_req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=pm_start,
            end=pm_end,
            feed=self.feed,
        )
        pm_resp = self.client.get_stock_bars(pm_req)
        pm_bars = pm_resp[symbol] if symbol in pm_resp else []
        if not pm_bars:
            return 0.0, 0.0

        # Gap % vs previous close
        first_open = float(pm_bars[0].open)
        gap_pct    = (first_open - prev_close) / prev_close * 100.0

        # RVOL: annualise premarket volume to a full session equivalent
        pm_volume = sum(float(b.volume) for b in pm_bars)
        rvol = (pm_volume / self._PREMARKET_MINUTES) / (avg_daily_vol / self._SESSION_MINUTES)

        return round(gap_pct, 2), round(rvol, 2)


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
        self.scanner = PreMarketScanner(self.historical_client, config.data_feed)

        self.states: Dict[str, TickerState] = {
            sym: TickerState(symbol=sym) for sym in config.symbols
        }

        # Symbols selected by the pre-market scanner for today (starts as full universe)
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
                    target_status = (
                        STATUS_BREAK_EVEN if db_status == "BREAK_EVEN"
                        else STATUS_PARTIAL_EXIT if db_status == "PARTIAL_EXIT"
                        else STATUS_IN_TRADE
                    )
                    async with state.lock:
                        state.status              = target_status
                        state.order_id            = order_id
                        state.order1_id           = order_id
                        state.order2_id           = trade.get("order2_id")
                        state.trade_db_id         = trade_id
                        state.trade_date          = today
                        state.entry_qty_requested = int(float(trade["qty"]))
                        state.entry_qty_filled    = filled_qty
                        state.tp1_price           = trade.get("tp1_price")
                        state.tp2_price           = trade.get("tp2_price")
                        state.sl_price            = trade.get("stop_loss")
                        state.entry_price         = trade.get("entry_price")
                        state.half_qty            = int(float(trade["qty"])) // 2
                        state.side                = trade.get("side")
                        state.realized_pnl_tp1    = float(trade.get("partial_pnl") or 0)
                    logger.info("%s | RESUMED → %s (order=%s)", symbol, target_status, order_id)
                    await self.db.log_event("RESUMED", f"{symbol}: {target_status} order={order_id}")
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
                state.order_id = state.order1_id = state.order2_id = state.sl2_order_id = None
                state.trade_db_id         = None
                state.entry_price = state.sl_price = state.tp1_price = state.tp2_price = None
                state.side                = None
                state.half_qty            = 0
                state.entry_qty_requested = 0
                state.entry_qty_filled    = 0
                state.vwap_num            = 0.0
                state.vwap_den            = 0.0
                state.realized_pnl_tp1   = 0.0
                state.trade_date          = today

            # Block new signals for these terminal/transitional states
            if state.status in (
                STATUS_IN_TRADE, STATUS_SUBMITTING,
                STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN,
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

        # ATR filter
        atr_pct = await self._fetch_atr_pct(state.symbol, state.range_high)
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

        # Lock-in SUBMITTING immediately — prevents double-entry from rapid bars
        state.status = STATUS_SUBMITTING

        # ── VWAP confirmation ──────────────────────────────────────────────────
        if self.config.vwap_filter and state.vwap_den > 0:
            vwap = state.vwap_num / state.vwap_den
            if signal == "LONG" and close <= vwap:
                state.status = STATUS_COMPLETED
                reason = f"BELOW_VWAP (close={close:.2f} ≤ vwap={vwap:.2f})"
                logger.info("%s | FILTER SKIP: %s", state.symbol, reason)
                await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
                return
            if signal == "SHORT" and close >= vwap:
                state.status = STATUS_COMPLETED
                reason = f"ABOVE_VWAP (close={close:.2f} ≥ vwap={vwap:.2f})"
                logger.info("%s | FILTER SKIP: %s", state.symbol, reason)
                await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
                return

        # ── Volume surge confirmation ──────────────────────────────────────────
        if self.config.volume_surge_mult > 0 and state.avg_orb_volume > 0:
            min_vol = state.avg_orb_volume * self.config.volume_surge_mult
            if float(bar.volume) < min_vol:
                state.status = STATUS_COMPLETED
                reason = (f"LOW_VOL_SURGE (bar={bar.volume:,} < "
                           f"{self.config.volume_surge_mult}×avg={state.avg_orb_volume:,.0f})")
                logger.info("%s | FILTER SKIP: %s", state.symbol, reason)
                await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
                return

        # ── Spread filter ──────────────────────────────────────────────────────
        spread_pct = await self._fetch_spread_pct(state.symbol)
        if self.config.max_spread_pct > 0 and spread_pct > self.config.max_spread_pct:
            state.status = STATUS_COMPLETED
            reason = f"HIGH_SPREAD ({spread_pct:.4f}% > {self.config.max_spread_pct:.3f}%)"
            logger.warning("%s | FILTER SKIP: %s", state.symbol, reason)
            await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
            return

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
            ask   = float(quote.ask_price)
            bid   = float(quote.bid_price)
            if ask <= 0:
                return 0.0
            return ((ask - bid) / ask) * 100
        except Exception as exc:
            logger.warning("%s | Spread fetch failed (filter skipped): %s", symbol, exc)
            return 0.0

    # ── Order Execution ────────────────────────────────────────────────────────

    async def _submit_bracket_order(
        self, state: TickerState, bar: Bar, signal: str, today: str,
    ):
        """
        Two-leg scale-out bracket order.

        Sizing:
          risk_dollar = risk_pct_equity % of starting_equity
          total_qty   = floor(risk_dollar / risk_per_share)  ← volatility-adjusted
          half_qty    = total_qty // 2

        Leg 1  (half_qty,      TP1 = entry + 1.0R,  SL = range_mid/low)
        Leg 2  (remaining_qty, TP2 = entry + 3.0R,  SL = same level)

        When Leg 1's TP fills the bot moves Leg 2's SL to break-even (entry price).
        """
        close     = bar.close
        orb_range = state.range_high - state.range_low
        side      = OrderSide.BUY if signal == "LONG" else OrderSide.SELL

        if signal == "LONG":
            sl   = round(state.range_mid if self.config.stop_loss_type == "midpoint" else state.range_low,  2)
            tp1  = round(close + orb_range * self.config.tp1_r, 2)
            tp2  = round(close + orb_range * self.config.tp2_r, 2)
        else:
            sl   = round(state.range_mid if self.config.stop_loss_type == "midpoint" else state.range_high, 2)
            tp1  = round(close - orb_range * self.config.tp1_r, 2)
            tp2  = round(close - orb_range * self.config.tp2_r, 2)

        risk_per_share = abs(close - sl)
        if risk_per_share <= 0:
            state.status = STATUS_COMPLETED
            return

        risk_dollar    = self.starting_equity * self.config.risk_pct_equity / 100.0
        total_qty      = max(2, math.floor(risk_dollar / risk_per_share))
        if self.config.max_position_usd > 0:
            total_qty  = min(total_qty, max(2, math.floor(self.config.max_position_usd / close)))
        half_qty       = total_qty // 2
        remaining_qty  = total_qty - half_qty

        entry_vwap = round(state.vwap_num / state.vwap_den, 4) if state.vwap_den > 0 else 0.0

        req1 = MarketOrderRequest(
            symbol=state.symbol, qty=half_qty, side=side,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp1),
            stop_loss=StopLossRequest(stop_price=sl),
        )
        req2 = MarketOrderRequest(
            symbol=state.symbol, qty=remaining_qty, side=side,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp2),
            stop_loss=StopLossRequest(stop_price=sl),
        )

        try:
            loop   = asyncio.get_running_loop()
            order1 = await loop.run_in_executor(None, self.trading_client.submit_order, req1)
            order2 = await loop.run_in_executor(None, self.trading_client.submit_order, req2)

            state.order_id            = str(order1.id)
            state.order1_id           = str(order1.id)
            state.order2_id           = str(order2.id)
            state.sl2_order_id        = None
            state.half_qty            = half_qty
            state.entry_qty_requested = total_qty
            state.entry_qty_filled    = 0
            state.sl_price            = sl
            state.tp1_price           = tp1
            state.tp2_price           = tp2
            state.side                = signal
            state.status              = STATUS_IN_TRADE

            trade_id = await self.db.insert_trade(
                date=today, symbol=state.symbol, side=signal,
                qty=total_qty, take_profit=tp1, stop_loss=sl,
                order_id=state.order1_id,
                order2_id=state.order2_id,
                tp1_price=tp1, tp2_price=tp2,
                gap_pct=state.gap_pct, rvol=state.rvol,
                entry_vwap=entry_vwap,
            )
            state.trade_db_id = trade_id

            logger.info(
                "%s | ORDERS SUBMITTED  leg1=%s (×%d→TP1=%.2f)  "
                "leg2=%s (×%d→TP2=%.2f)  SL=%.2f  "
                "risk=$%.0f (%.1f%% equity)",
                state.symbol,
                order1.id, half_qty,      tp1,
                order2.id, remaining_qty, tp2,
                sl,
                risk_dollar, self.config.risk_pct_equity,
            )
            await self.db.log_event(
                "ORDER_SUBMITTED",
                f"{state.symbol} {signal} total_qty={total_qty} "
                f"TP1={tp1} TP2={tp2} SL={sl} "
                f"leg1={order1.id} leg2={order2.id}",
            )

        except Exception as exc:
            state.status = STATUS_COMPLETED
            logger.error("%s | Order submission FAILED: %s", state.symbol, exc)
            await self.db.log_event("ORDER_ERROR", f"{state.symbol}: {exc}")

    async def _move_sl_to_breakeven(self, state: TickerState):
        """
        Called after TP1 fills.
        1. Locates the original SL child leg of order2.
        2. Cancels it.
        3. Places a new stop order at the entry price (break-even).
        """
        if not state.order2_id or not state.entry_price:
            return
        loop = asyncio.get_running_loop()
        try:
            order2 = await loop.run_in_executor(
                None, lambda: self.trading_client.get_order_by_id(state.order2_id)
            )
            # Find and cancel the original SL leg
            for leg in (order2.legs or []):
                leg_type = str(getattr(leg, "order_type", "")).lower()
                if leg_type in ("stop", "stop_limit"):
                    await loop.run_in_executor(
                        None,
                        lambda oid=str(leg.id): self.trading_client.cancel_order_by_id(oid),
                    )
                    break

            # Submit break-even stop for the remaining shares
            remaining = state.entry_qty_requested - state.half_qty
            be_side   = OrderSide.SELL if state.side == "LONG" else OrderSide.BUY
            be_req    = StopOrderRequest(
                symbol=state.symbol, qty=remaining, side=be_side,
                time_in_force=TimeInForce.DAY,
                stop_price=round(state.entry_price, 2),
            )
            be_order = await loop.run_in_executor(None, self.trading_client.submit_order, be_req)

            async with state.lock:
                state.sl2_order_id = str(be_order.id)
                state.sl_price     = state.entry_price  # track updated SL level

            logger.info(
                "%s | SL moved to break-even %.2f  (be_order=%s)",
                state.symbol, state.entry_price, be_order.id,
            )
            await self.db.log_event(
                "BREAK_EVEN_SET",
                f"{state.symbol}: SL → entry {state.entry_price:.2f} (order {be_order.id})",
            )

        except Exception as exc:
            logger.error("%s | Failed to move SL to break-even: %s", state.symbol, exc)
            await self.db.log_event("BREAK_EVEN_ERROR", f"{state.symbol}: {exc}")

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

    async def _flatten_all_positions(self, reason: str):
        logger.warning("Flattening all positions — reason: %s", reason)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, lambda: self.trading_client.close_all_positions(cancel_orders=True)
            )
        except Exception as exc:
            logger.error("Failed to flatten: %s", exc)

        _live = (STATUS_IN_TRADE, STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN)
        for state in self.states.values():
            async with state.lock:
                if state.status in _live:
                    state.status = STATUS_COMPLETED
                    if state.trade_db_id:
                        await self.db.update_trade(
                            state.trade_db_id, status="CLOSED_MANUAL",
                            closed_at=datetime.now(timezone.utc).isoformat(),
                        )
        await self.db.log_event(reason, "All positions flattened")

    # ── Background Tasks ───────────────────────────────────────────────────────

    async def _order_fill_monitor(self):
        """
        Polls every 20 s to sync bracket leg fills back to the DB.

        STATE: IN_TRADE
          • Tracks entry partial fills.
          • If order1's TP1 limit leg fills → PARTIAL_EXIT, calls _move_sl_to_breakeven.
          • If any SL leg fills            → COMPLETED (full stop-out).

        STATE: PARTIAL_EXIT / BREAK_EVEN
          • If sl2_order_id fills (break-even stop) → COMPLETED (scratch second half).
          • If order2's TP2 limit leg fills          → COMPLETED (full winner).
        """
        _live = (STATUS_IN_TRADE, STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN)

        while not self.kill_switch_triggered:
            await asyncio.sleep(20)
            loop = asyncio.get_running_loop()

            for state in self.states.values():
                async with state.lock:
                    if state.status not in _live:
                        continue
                    order1_id    = state.order1_id
                    order2_id    = state.order2_id
                    sl2_id       = state.sl2_order_id
                    status       = state.status
                    trade_db_id  = state.trade_db_id
                    prev_filled  = state.entry_qty_filled
                    qty_req      = state.entry_qty_requested

                try:
                    # ── STATUS_IN_TRADE ──────────────────────────────────────
                    if status == STATUS_IN_TRADE and order1_id:
                        o1 = await loop.run_in_executor(
                            None, lambda oid=order1_id: self.trading_client.get_order_by_id(oid)
                        )

                        # Track entry partial fill
                        filled = int(float(o1.filled_qty or 0))
                        if filled > 0 and filled != prev_filled:
                            upd: dict = {"status": "OPEN", "qty": filled}
                            if o1.filled_avg_price:
                                fill_px = round(float(o1.filled_avg_price), 4)
                                upd["entry_price"] = fill_px
                                async with state.lock:
                                    state.entry_price      = fill_px
                                    state.entry_qty_filled = filled
                            if trade_db_id:
                                await self.db.update_trade(trade_db_id, **upd)
                            if 0 < filled < qty_req:
                                logger.warning(
                                    "%s | PARTIAL FILL: %d/%d shares (leg1)",
                                    state.symbol, filled, state.half_qty,
                                )

                        # Check TP1 / SL leg fills on order1
                        for leg in (o1.legs or []):
                            leg_status = str(getattr(leg, "status", "")).lower()
                            if leg_status != "filled":
                                continue
                            leg_type   = str(getattr(leg, "order_type", "")).lower()
                            fill_price = round(float(leg.filled_avg_price), 4)

                            if "limit" in leg_type:
                                # TP1 hit — partial exit
                                half   = state.half_qty
                                entry  = state.entry_price or fill_price
                                pnl1   = round(
                                    (fill_price - entry) * half if state.side == "LONG"
                                    else (entry - fill_price) * half,
                                    2,
                                )
                                async with state.lock:
                                    state.realized_pnl_tp1 = pnl1
                                    state.status = STATUS_PARTIAL_EXIT

                                if trade_db_id:
                                    await self.db.update_trade(
                                        trade_db_id, partial_pnl=pnl1, status="PARTIAL_EXIT"
                                    )
                                logger.info(
                                    "%s | TP1 HIT @ %.4f  partial_pnl=$%.2f",
                                    state.symbol, fill_price, pnl1,
                                )
                                await self.db.log_event(
                                    "TP1_HIT", f"{state.symbol} fill={fill_price:.4f} pnl={pnl1:.2f}"
                                )
                                await self._move_sl_to_breakeven(state)
                                async with state.lock:
                                    state.status = STATUS_BREAK_EVEN
                                if trade_db_id:
                                    await self.db.update_trade(trade_db_id, status="BREAK_EVEN")

                            else:
                                # SL hit on leg1 — full stop-out
                                remaining = state.entry_qty_requested - state.half_qty
                                entry     = state.entry_price or fill_price
                                total_pnl = round(
                                    (fill_price - entry) * state.entry_qty_requested
                                    if state.side == "LONG"
                                    else (entry - fill_price) * state.entry_qty_requested,
                                    2,
                                )
                                async with state.lock:
                                    state.status = STATUS_COMPLETED
                                if trade_db_id:
                                    await self.db.update_trade(
                                        trade_db_id,
                                        exit_price=fill_price,
                                        realized_pnl=total_pnl,
                                        status="CLOSED_SL",
                                        closed_at=datetime.now(timezone.utc).isoformat(),
                                    )
                                logger.info(
                                    "%s | SL HIT @ %.4f  total_pnl=$%.2f",
                                    state.symbol, fill_price, total_pnl,
                                )
                                await self.db.log_event(
                                    "CLOSED_SL", f"{state.symbol} exit={fill_price:.4f}"
                                )
                            break

                    # ── STATUS_PARTIAL_EXIT / BREAK_EVEN ────────────────────
                    elif status in (STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN):
                        # Check break-even stop
                        if sl2_id:
                            sl2 = await loop.run_in_executor(
                                None, lambda oid=sl2_id: self.trading_client.get_order_by_id(oid)
                            )
                            if str(sl2.status).lower() == "filled":
                                fill_price = round(float(sl2.filled_avg_price), 4)
                                remaining  = state.entry_qty_requested - state.half_qty
                                entry      = state.entry_price or fill_price
                                pnl_be     = round(
                                    (fill_price - entry) * remaining if state.side == "LONG"
                                    else (entry - fill_price) * remaining,
                                    2,
                                )
                                total_pnl = round(state.realized_pnl_tp1 + pnl_be, 2)
                                async with state.lock:
                                    state.status = STATUS_COMPLETED
                                if trade_db_id:
                                    await self.db.update_trade(
                                        trade_db_id,
                                        exit_price=fill_price,
                                        realized_pnl=total_pnl,
                                        status="CLOSED_BE",
                                        closed_at=datetime.now(timezone.utc).isoformat(),
                                    )
                                logger.info(
                                    "%s | BE STOP HIT @ %.4f  total_pnl=$%.2f",
                                    state.symbol, fill_price, total_pnl,
                                )
                                await self.db.log_event(
                                    "CLOSED_BE", f"{state.symbol} exit={fill_price:.4f} total_pnl={total_pnl:.2f}"
                                )
                                continue

                        # Check TP2 on order2
                        if order2_id:
                            o2 = await loop.run_in_executor(
                                None, lambda oid=order2_id: self.trading_client.get_order_by_id(oid)
                            )
                            for leg in (o2.legs or []):
                                if (str(getattr(leg, "status", "")).lower() == "filled"
                                        and "limit" in str(getattr(leg, "order_type", "")).lower()):
                                    fill_price = round(float(leg.filled_avg_price), 4)
                                    remaining  = state.entry_qty_requested - state.half_qty
                                    entry      = state.entry_price or fill_price
                                    pnl_tp2    = round(
                                        (fill_price - entry) * remaining if state.side == "LONG"
                                        else (entry - fill_price) * remaining,
                                        2,
                                    )
                                    total_pnl  = round(state.realized_pnl_tp1 + pnl_tp2, 2)
                                    async with state.lock:
                                        state.status = STATUS_COMPLETED
                                    if trade_db_id:
                                        await self.db.update_trade(
                                            trade_db_id,
                                            exit_price=fill_price,
                                            realized_pnl=total_pnl,
                                            status="CLOSED_TP2",
                                            closed_at=datetime.now(timezone.utc).isoformat(),
                                        )
                                    logger.info(
                                        "%s | TP2 HIT @ %.4f  total_pnl=$%.2f",
                                        state.symbol, fill_price, total_pnl,
                                    )
                                    await self.db.log_event(
                                        "CLOSED_TP2",
                                        f"{state.symbol} exit={fill_price:.4f} total_pnl={total_pnl:.2f}",
                                    )
                                    break

                except Exception as exc:
                    logger.debug("Fill monitor error %s: %s", state.symbol, exc)

    async def _orphan_order_cleanup(self):
        """Every 5 min: cancel both bracket legs if the position no longer exists."""
        _live = (STATUS_IN_TRADE, STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN)
        while not self.kill_switch_triggered:
            await asyncio.sleep(300)
            loop = asyncio.get_running_loop()

            for symbol, state in self.states.items():
                async with state.lock:
                    if state.status not in _live:
                        continue
                    trade_db_id = state.trade_db_id
                    order_ids   = [o for o in (state.order1_id, state.order2_id, state.sl2_order_id) if o]

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

        _live = (STATUS_IN_TRADE, STATUS_PARTIAL_EXIT, STATUS_BREAK_EVEN)
        for symbol, state in self.states.items():
            async with state.lock:
                if state.status not in _live:
                    continue
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

        logger.info("Resync complete. Open: %s", list(open_symbols))

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

    async def _premarket_scanner_task(self):
        """
        Waits until 9:25 AM EST, runs the PreMarketScanner, and updates
        self.active_symbols to the top qualifying symbols for the day.
        If no symbols qualify (no gap/RVOL threshold met), falls back to the
        full configured universe so the bot never sits idle.
        """
        tz  = self.config.tz
        now = datetime.now(tz)
        target = now.replace(hour=9, minute=25, second=0, microsecond=0)
        wait   = max(0.0, (target - now).total_seconds())

        if wait > 0:
            logger.info("PreMarket scanner fires in %.0fs", wait)
            await asyncio.sleep(wait)

        logger.info("Running PreMarket scanner over universe: %s", self.config.symbols)
        try:
            hits = await self.scanner.scan(
                universe=self.config.symbols,
                gap_min_pct=self.config.gap_min_pct,
                rvol_min=self.config.rvol_min,
                top_n=self.config.scanner_top_n,
            )

            if hits:
                self.active_symbols = {sym for sym, _, _ in hits}
                for sym, gap, rvol in hits:
                    if sym in self.states:
                        self.states[sym].gap_pct = gap
                        self.states[sym].rvol    = rvol
                logger.info("Scanner selected: %s", [(s, f"{g:.1f}%", f"{r:.1f}x") for s, g, r in hits])
                await self.db.log_event(
                    "SCANNER_COMPLETE",
                    "Selected: " + ", ".join(f"{s}(gap={g:.1f}%,rvol={r:.1f}x)" for s, g, r in hits),
                )
            else:
                logger.warning("Scanner: no symbols met gap≥%.1f%% + rvol≥%.1f× — using full universe",
                               self.config.gap_min_pct, self.config.rvol_min)
                self.active_symbols = set(self.config.symbols)
                await self.db.log_event("SCANNER_COMPLETE", "No qualifiers — using full universe")

        except Exception as exc:
            logger.error("PreMarket scanner failed: %s — using full universe", exc)
            self.active_symbols = set(self.config.symbols)

    # ── Stream with Reconnection ───────────────────────────────────────────────

    async def _run_stream_with_reconnect(self):
        backoff = 1
        while not self.kill_switch_triggered:
            try:
                logger.info("Connecting to Alpaca data stream…")
                await self.stream._run_forever()
                logger.warning("Stream ended — reconnecting…")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Stream error: %s", exc)

            if self.kill_switch_triggered:
                break
            logger.info("Reconnecting in %ds…", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

            async with self._stream_reconnect_lock:
                self.stream = StockDataStream(
                    api_key=self.config.api_key,
                    secret_key=self.config.api_secret,
                    feed=self.config.data_feed,
                )
                self.stream.subscribe_bars(self.on_bar, *self.active_symbols)

            await self._resync_from_rest()
            backoff = 1

    # ── Entry Point ────────────────────────────────────────────────────────────

    async def run(self):
        logger.info("ORB Bot starting — universe: %s", self.config.symbols)
        logger.info("Paper: %s | Feed: %s", self.config.paper, self.config.data_feed)

        await self.db.initialize()
        await self._fetch_starting_equity()
        await self.db.log_event("START", f"universe={self.config.symbols}")

        await self._resume_from_db()

        # Subscribe to all universe symbols initially (scanner refines at 9:25 AM)
        self.stream.subscribe_bars(self.on_bar, *self.config.symbols)

        asyncio.create_task(self._risk_monitor())
        asyncio.create_task(self._auto_flatten_scheduler())
        asyncio.create_task(self._order_fill_monitor())
        asyncio.create_task(self._orphan_order_cleanup())
        asyncio.create_task(self._heartbeat_watchdog())
        asyncio.create_task(self._premarket_scanner_task())

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
