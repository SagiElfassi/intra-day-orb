#!/usr/bin/env python3
"""
ORB (Opening Range Breakout) Trading Bot
Production-ready automated trading system using Alpaca Markets API (alpaca-py).

Architecture:
  - StockDataStream (WebSocket) for real-time 1-min bar data
  - Asyncio-native event loop with executor-wrapped blocking calls
  - Per-symbol state machine: WAITING_RANGE → SCANNING → SUBMITTING → IN_TRADE → COMPLETED
  - Bracket orders (entry + TP + SL) submitted atomically to Alpaca
  - SQLite for trade/event logging (read by dashboard.py)
  - Kill switch + auto-flatten safety protocols
  - WebSocket reconnection with exponential backoff + REST resync
  - Crash recovery: restores in-flight trades from DB on startup
  - Orphan order cleanup: cancels bracket legs when position is gone
  - Partial fill tracking: records actual filled qty, not just requested
"""

import asyncio
import logging
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
        key    = os.environ.get("ALPACA_API_KEY_PAPER") or os.environ.get("ALPACA_API_KEY", "")
        secret = os.environ.get("ALPACA_API_SECRET_PAPER") or os.environ.get("ALPACA_API_SECRET", "")
    else:
        key    = os.environ.get("ALPACA_API_KEY_LIVE") or os.environ.get("ALPACA_API_KEY", "")
        secret = os.environ.get("ALPACA_API_SECRET_LIVE") or os.environ.get("ALPACA_API_SECRET", "")
    return key, secret


@dataclass
class Config:
    # Alpaca credentials — resolved after paper mode is known
    paper: bool = field(default_factory=_resolve_paper_mode)
    api_key: str = ""
    api_secret: str = ""

    def __post_init__(self):
        if not self.api_key or not self.api_secret:
            self.api_key, self.api_secret = _resolve_credentials(self.paper)
        try:
            with open("live_config.json") as f:
                saved = json.load(f)
            for attr, cast in [
                ("min_orb_volume", int), ("min_atr_pct", float), ("max_spread_pct", float)
            ]:
                if attr in saved:
                    setattr(self, attr, cast(saved[attr]))
        except Exception:
            pass

    # Trading universe — comma-separated list in env var
    symbols: List[str] = field(
        default_factory=lambda: [
            s.strip().upper()
            for s in os.environ.get("SYMBOLS", "SPY,QQQ,AAPL").split(",")
        ]
    )

    # ORB parameters
    orb_minutes: int = field(
        default_factory=lambda: int(os.environ.get("ORB_MINUTES", "15"))
    )
    risk_ratio: float = field(
        default_factory=lambda: float(os.environ.get("RISK_RATIO", "2.0"))
    )
    position_size_usd: float = field(
        default_factory=lambda: float(os.environ.get("POSITION_SIZE_USD", "1000.0"))
    )
    # "midpoint" = SL at range midpoint | "hard" = SL at opposite range extreme
    stop_loss_type: str = field(
        default_factory=lambda: os.environ.get("STOP_LOSS_TYPE", "midpoint")
    )
    # Minimum bars needed to validate the ORB range (guards against data gaps)
    min_bars_required: int = field(
        default_factory=lambda: int(os.environ.get("MIN_BARS_REQUIRED", "13"))
    )

    # Analyst filters
    min_orb_volume: int = field(
        default_factory=lambda: int(os.environ.get("MIN_ORB_VOLUME", "1000000"))
    )
    min_atr_pct: float = field(
        default_factory=lambda: float(os.environ.get("MIN_ATR_PCT", "2.0"))
    )
    max_spread_pct: float = field(
        default_factory=lambda: float(os.environ.get("MAX_SPREAD_PCT", "0.05"))
    )

    # Risk management
    max_daily_loss_pct: float = field(
        default_factory=lambda: float(os.environ.get("MAX_DAILY_LOSS_PCT", "2.0"))
    )
    flatten_hour: int = 15    # 3 PM
    flatten_minute: int = 55  # :55 → 3:55 PM EST

    # Data feed: IEX (free/paper) or SIP (paid/live)
    data_feed: DataFeed = field(
        default_factory=lambda: (
            DataFeed.SIP
            if os.environ.get("ALPACA_DATA_FEED", "iex").lower() == "sip"
            else DataFeed.IEX
        )
    )

    # SQLite path — shared with dashboard
    db_path: str = field(
        default_factory=lambda: os.environ.get("DB_PATH", "orb_trades.db")
    )

    tz: ZoneInfo = field(default_factory=lambda: ZoneInfo("America/New_York"))


# ── Per-Symbol State ───────────────────────────────────────────────────────────

STATUS_WAITING    = "WAITING_RANGE"  # Collecting ORB bars
STATUS_SCANNING   = "SCANNING"       # Range set; watching for breakout
STATUS_SUBMITTING = "SUBMITTING"     # Order REST call in flight — blocks re-entry
STATUS_IN_TRADE   = "IN_TRADE"       # Bracket order live
STATUS_COMPLETED  = "COMPLETED"      # Done for the day
STATUS_INVALID    = "INVALID"        # Insufficient bars; skip today


@dataclass
class TickerState:
    symbol: str
    status: str = STATUS_WAITING
    bars: List[Bar] = field(default_factory=list)      # ORB window bars
    range_high: Optional[float] = None
    range_low: Optional[float] = None
    range_mid: Optional[float] = None
    order_id: Optional[str] = None
    trade_db_id: Optional[int] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    trade_date: Optional[str] = None                   # "YYYY-MM-DD" for today's session
    entry_qty_requested: int = 0                       # qty we sent to Alpaca
    entry_qty_filled: int = 0                          # actual filled qty (may differ on partial fill)


# ── Database Manager ───────────────────────────────────────────────────────────

class DatabaseManager:
    """Async SQLite interface. All write paths are serialized through this class."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    date        TEXT    NOT NULL,
                    symbol      TEXT    NOT NULL,
                    side        TEXT    NOT NULL,
                    qty         REAL    NOT NULL,
                    entry_price REAL,
                    take_profit REAL    NOT NULL,
                    stop_loss   REAL    NOT NULL,
                    status      TEXT    NOT NULL,
                    exit_price  REAL,
                    realized_pnl REAL,
                    order_id    TEXT,
                    created_at  TEXT    NOT NULL,
                    closed_at   TEXT
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
            await db.commit()
        logger.info("Database initialized at %s", self.db_path)

    async def log_event(self, event_type: str, message: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO bot_events (timestamp, event_type, message) VALUES (?,?,?)",
                (datetime.now(timezone.utc).isoformat(), event_type, message),
            )
            await db.commit()

    async def save_orb_range(
        self,
        symbol: str,
        date: str,
        high: float,
        low: float,
        mid: float,
        bars_received: int,
        valid: bool,
    ):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO orb_ranges
                   (date, symbol, range_high, range_low, range_mid,
                    bars_received, valid, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (date, symbol, high, low, mid, bars_received, int(valid),
                 datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()

    async def insert_trade(
        self,
        date: str,
        symbol: str,
        side: str,
        qty: float,
        take_profit: float,
        stop_loss: float,
        order_id: str,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO trades
                   (date, symbol, side, qty, take_profit, stop_loss,
                    status, order_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (date, symbol, side, qty, take_profit, stop_loss,
                 "PENDING", order_id, datetime.now(timezone.utc).isoformat()),
            )
            await db.commit()
            return cursor.lastrowid

    async def update_trade(self, trade_id: int, **kwargs):
        if not kwargs:
            return
        sets = ", ".join(f"{k}=?" for k in kwargs)
        values = list(kwargs.values()) + [trade_id]
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                f"UPDATE trades SET {sets} WHERE id=?", values
            )
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
                "SELECT * FROM trades WHERE date=? AND status IN ('PENDING','OPEN')",
                (today,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ── Core Bot ───────────────────────────────────────────────────────────────────

class ORBBot:
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseManager(config.db_path)

        self.trading_client = TradingClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=config.paper,
        )

        self.stream = StockDataStream(
            api_key=config.api_key,
            secret_key=config.api_secret,
            feed=config.data_feed,
        )

        self.historical_client = StockHistoricalDataClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
        )

        self.states: Dict[str, TickerState] = {
            sym: TickerState(symbol=sym) for sym in config.symbols
        }

        self.starting_equity: float = 0.0
        self.kill_switch_triggered: bool = False
        self._last_bar_time: float = 0.0          # epoch seconds; updated on every bar
        self._stream_reconnect_lock = asyncio.Lock()  # prevents concurrent reconnects

    # ── Initialisation ─────────────────────────────────────────────────────────

    async def _fetch_starting_equity(self):
        loop = asyncio.get_running_loop()
        account = await loop.run_in_executor(None, self.trading_client.get_account)
        self.starting_equity = float(account.equity)
        logger.info("Starting equity: $%.2f", self.starting_equity)

    # ── Crash Recovery ─────────────────────────────────────────────────────────

    async def _resume_from_db(self):
        """
        Called once at startup. Queries the DB for trades that were PENDING or OPEN
        at last shutdown, then verifies their status against Alpaca's REST API.
        Restores IN_TRADE state for positions that are still open; marks the rest closed.
        """
        today = datetime.now(self.config.tz).strftime("%Y-%m-%d")
        active = await self.db.load_active_trades(today)
        if not active:
            return

        logger.info("Resume: found %d active trade(s) in DB for %s", len(active), today)
        loop = asyncio.get_running_loop()

        for trade in active:
            symbol   = trade["symbol"]
            order_id = trade["order_id"]
            trade_id = trade["id"]

            if symbol not in self.states:
                continue

            state = self.states[symbol]

            try:
                order = await loop.run_in_executor(
                    None,
                    lambda oid=order_id: self.trading_client.get_order_by_id(oid),
                )
                order_status = str(order.status).lower()

                if order_status in ("filled", "partially_filled", "accepted", "pending_new", "new"):
                    filled_qty = int(float(order.filled_qty or trade["qty"]))
                    async with state.lock:
                        state.status              = STATUS_IN_TRADE
                        state.order_id            = order_id
                        state.trade_db_id         = trade_id
                        state.trade_date          = today
                        state.entry_qty_requested = int(float(trade["qty"]))
                        state.entry_qty_filled    = filled_qty
                    logger.info(
                        "%s | RESUMED → IN_TRADE  order=%s  filled_qty=%d",
                        symbol, order_id, filled_qty,
                    )
                    await self.db.log_event(
                        "RESUMED",
                        f"{symbol}: restored IN_TRADE from DB (order {order_id})",
                    )

                elif order_status in ("canceled", "expired", "done_for_day", "stopped", "rejected"):
                    await self.db.update_trade(
                        trade_id,
                        status="CLOSED_CANCELLED",
                        closed_at=datetime.now(timezone.utc).isoformat(),
                    )
                    async with state.lock:
                        state.trade_date = today
                    logger.info(
                        "%s | Order %s was %s — marked closed in DB",
                        symbol, order_id, order_status,
                    )

                else:
                    # Unknown status — default to restoring IN_TRADE conservatively
                    logger.warning(
                        "%s | Unknown order status '%s' on resume — assuming IN_TRADE",
                        symbol, order_status,
                    )
                    async with state.lock:
                        state.status      = STATUS_IN_TRADE
                        state.order_id    = order_id
                        state.trade_db_id = trade_id
                        state.trade_date  = today
                        state.entry_qty_requested = int(float(trade["qty"]))

            except Exception as exc:
                # REST call failed — assume position is still open to be conservative
                logger.error(
                    "%s | Failed to verify order %s on resume: %s — assuming IN_TRADE",
                    symbol, order_id, exc,
                )
                async with state.lock:
                    state.status      = STATUS_IN_TRADE
                    state.order_id    = order_id
                    state.trade_db_id = trade_id
                    state.trade_date  = today
                    state.entry_qty_requested = int(float(trade["qty"]))

    # ── WebSocket Bar Handler ──────────────────────────────────────────────────

    async def on_bar(self, bar: Bar):
        """Called by StockDataStream for every incoming 1-minute bar."""
        if self.kill_switch_triggered:
            return

        symbol = bar.symbol
        if symbol not in self.states:
            return

        # Update heartbeat timestamp on every received bar
        self._last_bar_time = datetime.now(timezone.utc).timestamp()

        state = self.states[symbol]

        await self.db.save_bar(bar)

        bar_time_est = bar.timestamp.astimezone(self.config.tz)
        today = bar_time_est.strftime("%Y-%m-%d")

        async with state.lock:
            # Reset state at the start of a new trading day
            if state.trade_date != today:
                state.status              = STATUS_WAITING
                state.bars                = []
                state.range_high          = state.range_low = state.range_mid = None
                state.order_id            = state.trade_db_id = None
                state.trade_date          = today
                state.entry_qty_requested = 0
                state.entry_qty_filled    = 0

            # STATUS_SUBMITTING blocks re-entry while an order REST call is in flight
            if state.status in (
                STATUS_IN_TRADE, STATUS_SUBMITTING, STATUS_COMPLETED, STATUS_INVALID
            ):
                return

            orb_start = bar_time_est.replace(hour=9, minute=30, second=0, microsecond=0)
            orb_end   = orb_start + timedelta(minutes=self.config.orb_minutes)

            if orb_start <= bar_time_est < orb_end:
                await self._process_range_bar(state, bar, today, bar_time_est)
            elif bar_time_est >= orb_end and state.status == STATUS_SCANNING:
                await self._check_breakout(state, bar, today)

    async def _process_range_bar(
        self,
        state: TickerState,
        bar: Bar,
        today: str,
        bar_time_est: datetime,
    ):
        state.bars.append(bar)
        logger.debug(
            "%s | ORB bar %d/%d  H=%.2f L=%.2f C=%.2f",
            state.symbol, len(state.bars), self.config.orb_minutes,
            bar.high, bar.low, bar.close,
        )
        if len(state.bars) >= self.config.orb_minutes:
            await self._finalize_range(state, today)

    async def _finalize_range(self, state: TickerState, today: str):
        bars_received = len(state.bars)
        valid = bars_received >= self.config.min_bars_required

        if not valid:
            state.status = STATUS_INVALID
            logger.warning(
                "%s | INVALID range — only %d/%d bars received",
                state.symbol, bars_received, self.config.orb_minutes,
            )
            await self.db.save_orb_range(
                state.symbol, today, 0, 0, 0, bars_received, valid=False
            )
            await self.db.log_event(
                "INVALID_RANGE",
                f"{state.symbol}: {bars_received}/{self.config.orb_minutes} bars — skipping",
            )
            return

        state.range_high = max(b.high for b in state.bars)
        state.range_low  = min(b.low  for b in state.bars)
        state.range_mid  = (state.range_high + state.range_low) / 2

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
            "%s | ORB range SET — High=%.2f  Low=%.2f  Mid=%.2f  (bars=%d)",
            state.symbol, state.range_high, state.range_low,
            state.range_mid, bars_received,
        )
        await self.db.save_orb_range(
            state.symbol, today,
            state.range_high, state.range_low, state.range_mid,
            bars_received, valid=True,
        )

    async def _check_breakout(self, state: TickerState, bar: Bar, today: str):
        """Detect close above ORB High (long) or below ORB Low (short)."""
        close = bar.close

        if close > state.range_high:
            signal = "LONG"
        elif close < state.range_low:
            signal = "SHORT"
        else:
            return

        # Set SUBMITTING *before* any await so rapid-fire bars can't double-enter.
        # The lock from on_bar is still held here, so this is atomic.
        state.status = STATUS_SUBMITTING

        # Spread filter — check live quote just before order submission
        spread_pct = await self._fetch_spread_pct(state.symbol)
        if self.config.max_spread_pct > 0 and spread_pct > self.config.max_spread_pct:
            state.status = STATUS_COMPLETED
            reason = f"HIGH_SPREAD ({spread_pct:.4f}% > {self.config.max_spread_pct:.3f}%)"
            logger.warning("%s | FILTER SKIP: %s", state.symbol, reason)
            await self.db.log_event("FILTER_SKIP", f"{state.symbol}: {reason}")
            return

        logger.info(
            "%s | BREAKOUT %s — Close=%.2f  High=%.2f  Low=%.2f",
            state.symbol, signal, close, state.range_high, state.range_low,
        )
        await self._submit_bracket_order(state, bar, signal, today)

    # ── Analyst Filters ────────────────────────────────────────────────────────

    async def _fetch_atr_pct(self, symbol: str, ref_price: float) -> float:
        """14-day ATR as % of ref_price. Returns 0.0 on failure (permissive)."""
        try:
            loop = asyncio.get_running_loop()
            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=datetime.now(timezone.utc) - timedelta(days=30),
                end=datetime.now(timezone.utc)   - timedelta(days=1),
                feed=self.config.data_feed,
            )
            resp = await loop.run_in_executor(
                None, lambda: self.historical_client.get_stock_bars(req)
            )
            bars = resp[symbol]
            if len(bars) < 2:
                return 0.0
            trs = [
                max(
                    float(bars[i].high) - float(bars[i].low),
                    abs(float(bars[i].high) - float(bars[i - 1].close)),
                    abs(float(bars[i].low)  - float(bars[i - 1].close)),
                )
                for i in range(1, len(bars))
            ]
            atr = sum(trs[-14:]) / min(14, len(trs))
            return (atr / ref_price) * 100
        except Exception as exc:
            logger.warning("%s | ATR fetch failed (filter skipped): %s", symbol, exc)
            return 0.0

    async def _fetch_spread_pct(self, symbol: str) -> float:
        """Bid-ask spread as % of ask price. Returns 0.0 on failure (permissive)."""
        try:
            loop = asyncio.get_running_loop()
            req = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            resp = await loop.run_in_executor(
                None, lambda: self.historical_client.get_stock_latest_quote(req)
            )
            quote = resp[symbol]
            ask = float(quote.ask_price)
            bid = float(quote.bid_price)
            if ask <= 0:
                return 0.0
            return ((ask - bid) / ask) * 100
        except Exception as exc:
            logger.warning("%s | Spread fetch failed (filter skipped): %s", symbol, exc)
            return 0.0

    # ── Order Execution ────────────────────────────────────────────────────────

    async def _submit_bracket_order(
        self,
        state: TickerState,
        bar: Bar,
        signal: str,
        today: str,
    ):
        """
        Build and submit a bracket order atomically to Alpaca.
        state.status is already STATUS_SUBMITTING when this is called.
        On failure, state moves to STATUS_COMPLETED (skip the day — don't retry).
        """
        close     = bar.close
        orb_range = state.range_high - state.range_low
        qty       = max(1, math.floor(self.config.position_size_usd / close))

        if signal == "LONG":
            side     = OrderSide.BUY
            tp_price = round(close + orb_range * self.config.risk_ratio, 2)
            sl_price = round(
                state.range_mid if self.config.stop_loss_type == "midpoint"
                else state.range_low,
                2,
            )
        else:
            side     = OrderSide.SELL
            tp_price = round(close - orb_range * self.config.risk_ratio, 2)
            sl_price = round(
                state.range_mid if self.config.stop_loss_type == "midpoint"
                else state.range_high,
                2,
            )

        order_request = MarketOrderRequest(
            symbol=state.symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp_price),
            stop_loss=StopLossRequest(stop_price=sl_price),
        )

        try:
            loop  = asyncio.get_running_loop()
            order = await loop.run_in_executor(
                None, self.trading_client.submit_order, order_request
            )

            state.order_id            = str(order.id)
            state.status              = STATUS_IN_TRADE
            state.entry_qty_requested = qty
            state.entry_qty_filled    = 0  # updated later by fill monitor

            trade_id = await self.db.insert_trade(
                date=today,
                symbol=state.symbol,
                side=signal,
                qty=qty,
                take_profit=tp_price,
                stop_loss=sl_price,
                order_id=state.order_id,
            )
            state.trade_db_id = trade_id

            logger.info(
                "%s | ORDER SUBMITTED  id=%s  side=%s  qty=%d  TP=%.2f  SL=%.2f",
                state.symbol, order.id, signal, qty, tp_price, sl_price,
            )
            await self.db.log_event(
                "ORDER_SUBMITTED",
                f"{state.symbol} {signal} qty={qty} TP={tp_price} SL={sl_price} "
                f"order_id={order.id}",
            )

        except Exception as exc:
            # Don't leave state stuck in SUBMITTING — mark day as done
            state.status = STATUS_COMPLETED
            logger.error("%s | Order submission FAILED: %s", state.symbol, exc)
            await self.db.log_event("ORDER_ERROR", f"{state.symbol}: {exc}")

    # ── Risk Management ────────────────────────────────────────────────────────

    async def _check_kill_switch(self):
        try:
            loop = asyncio.get_running_loop()
            account = await loop.run_in_executor(
                None, self.trading_client.get_account
            )
            current_equity = float(account.equity)
        except Exception as exc:
            logger.warning("Kill-switch check failed: %s", exc)
            return

        if self.starting_equity <= 0:
            return

        loss_pct = (self.starting_equity - current_equity) / self.starting_equity * 100

        if loss_pct >= self.config.max_daily_loss_pct:
            logger.critical(
                "KILL SWITCH — daily loss %.2f%% exceeds threshold %.2f%%",
                loss_pct, self.config.max_daily_loss_pct,
            )
            self.kill_switch_triggered = True
            await self.db.log_event(
                "KILL_SWITCH",
                f"Loss {loss_pct:.2f}% >= threshold {self.config.max_daily_loss_pct}%",
            )
            await self._flatten_all_positions(reason="KILL_SWITCH")

    async def _flatten_all_positions(self, reason: str):
        logger.warning("Flattening all positions — reason: %s", reason)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self.trading_client.close_all_positions(cancel_orders=True),
            )
            logger.info("All positions closed.")
        except Exception as exc:
            logger.error("Failed to flatten positions: %s", exc)

        for state in self.states.values():
            async with state.lock:
                if state.status == STATUS_IN_TRADE:
                    state.status = STATUS_COMPLETED
                    if state.trade_db_id:
                        await self.db.update_trade(
                            state.trade_db_id,
                            status="CLOSED_MANUAL",
                            closed_at=datetime.now(timezone.utc).isoformat(),
                        )

        await self.db.log_event(reason, "All positions flattened")

    # ── Background Tasks ───────────────────────────────────────────────────────

    async def _order_fill_monitor(self):
        """
        Poll every 20s to sync bracket-order fill prices back to the DB.

        Handles partial fills: if Alpaca only filled a portion of the requested qty,
        we record the actual filled qty. Alpaca's bracket OCO legs are automatically
        sized to the filled position, so no manual leg adjustment is needed.
        """
        while not self.kill_switch_triggered:
            await asyncio.sleep(20)
            loop = asyncio.get_running_loop()

            for state in self.states.values():
                async with state.lock:
                    if state.status != STATUS_IN_TRADE or not state.order_id:
                        continue
                    order_id     = state.order_id
                    trade_db_id  = state.trade_db_id
                    symbol       = state.symbol
                    prev_filled  = state.entry_qty_filled
                    qty_requested = state.entry_qty_requested

                try:
                    order = await loop.run_in_executor(
                        None,
                        lambda oid=order_id: self.trading_client.get_order_by_id(oid),
                    )

                    # ── Partial / full fill tracking ──────────────────────────
                    filled_qty = int(float(order.filled_qty or 0))
                    if filled_qty > 0 and filled_qty != prev_filled:
                        update_kwargs: dict = {"status": "OPEN", "qty": filled_qty}
                        if order.filled_avg_price:
                            update_kwargs["entry_price"] = round(
                                float(order.filled_avg_price), 4
                            )
                        if trade_db_id:
                            await self.db.update_trade(trade_db_id, **update_kwargs)

                        async with state.lock:
                            state.entry_qty_filled = filled_qty

                        if 0 < filled_qty < qty_requested:
                            logger.warning(
                                "%s | PARTIAL FILL: %d/%d shares filled — "
                                "bracket legs auto-adjusted by Alpaca",
                                symbol, filled_qty, qty_requested,
                            )
                            await self.db.log_event(
                                "PARTIAL_FILL",
                                f"{symbol}: filled {filled_qty}/{qty_requested} shares",
                            )

                    # ── Bracket leg fill check ────────────────────────────────
                    legs = getattr(order, "legs", None) or []
                    for leg in legs:
                        leg_status = str(getattr(leg, "status", "")).lower()
                        if leg_status == "filled":
                            exit_price = round(float(leg.filled_avg_price), 4)
                            order_type = str(getattr(leg, "order_type", "")).lower()
                            reason     = "CLOSED_TP" if "limit" in order_type else "CLOSED_SL"

                            if trade_db_id:
                                await self.db.update_trade(
                                    trade_db_id,
                                    exit_price=exit_price,
                                    status=reason,
                                    closed_at=datetime.now(timezone.utc).isoformat(),
                                )

                            async with state.lock:
                                state.status = STATUS_COMPLETED

                            logger.info(
                                "%s | Trade closed — reason=%s  exit=%.4f",
                                symbol, reason, exit_price,
                            )
                            await self.db.log_event(
                                reason, f"{symbol} exit={exit_price}"
                            )
                            break

                except Exception as exc:
                    logger.debug("Fill monitor error %s: %s", symbol, exc)

    async def _orphan_order_cleanup(self):
        """
        Every 5 minutes: for each symbol we believe is IN_TRADE, verify a position
        still exists on Alpaca. If the position is gone (manual close, external trigger,
        or a prior disconnect), cancel any remaining open orders for that symbol and
        mark the trade closed in the DB.
        """
        while not self.kill_switch_triggered:
            await asyncio.sleep(300)
            loop = asyncio.get_running_loop()

            for symbol, state in self.states.items():
                async with state.lock:
                    if state.status != STATUS_IN_TRADE:
                        continue
                    trade_db_id = state.trade_db_id

                try:
                    # Check whether the position still exists
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda s=symbol: self.trading_client.get_open_position(s),
                        )
                        continue  # position is alive — nothing to clean up
                    except Exception:
                        pass  # 404 or other error → position is gone

                    # Position gone — cancel any lingering bracket legs
                    all_orders = await loop.run_in_executor(
                        None, self.trading_client.get_orders
                    )
                    cancelled = 0
                    for order in all_orders:
                        if str(order.symbol) != symbol:
                            continue
                        try:
                            await loop.run_in_executor(
                                None,
                                lambda oid=str(order.id): self.trading_client.cancel_order_by_id(oid),
                            )
                            cancelled += 1
                        except Exception:
                            pass

                    logger.warning(
                        "%s | Orphan cleanup: position gone, cancelled %d open order(s)",
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
                                trade_db_id,
                                status="CLOSED_MANUAL",
                                closed_at=datetime.now(timezone.utc).isoformat(),
                            )

                except Exception as exc:
                    logger.debug("Orphan cleanup error for %s: %s", symbol, exc)

    async def _resync_from_rest(self):
        """
        Called after every stream reconnection. Cross-checks in-memory IN_TRADE
        states against live Alpaca positions. If a position closed while we were
        disconnected, marks it completed and updates the DB.
        """
        loop = asyncio.get_running_loop()
        try:
            positions    = await loop.run_in_executor(
                None, self.trading_client.get_all_positions
            )
            open_symbols = {p.symbol for p in positions}
        except Exception as exc:
            logger.error("Post-reconnect resync failed: %s", exc)
            return

        for symbol, state in self.states.items():
            async with state.lock:
                if state.status != STATUS_IN_TRADE:
                    continue
                trade_db_id = state.trade_db_id

                if symbol not in open_symbols:
                    logger.warning(
                        "%s | Position closed during disconnect — marking COMPLETED",
                        symbol,
                    )
                    state.status = STATUS_COMPLETED
                    if trade_db_id:
                        await self.db.update_trade(
                            trade_db_id,
                            status="CLOSED_MANUAL",
                            closed_at=datetime.now(timezone.utc).isoformat(),
                        )
                    await self.db.log_event(
                        "RESYNC",
                        f"{symbol}: position closed during stream disconnect",
                    )

        logger.info("Post-reconnect resync complete. Open positions: %s", list(open_symbols))

    async def _heartbeat_watchdog(self):
        """
        Logs a warning if no bar is received for > 3 minutes during market hours.
        Does not force-reconnect (that is handled by _run_stream_with_reconnect);
        this purely provides an observable signal in logs and the DB.
        """
        while not self.kill_switch_triggered:
            await asyncio.sleep(90)
            tz  = self.config.tz
            now = datetime.now(tz)
            mkt_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
            mkt_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)

            if not (mkt_open <= now <= mkt_close):
                continue
            if self._last_bar_time == 0.0:
                continue

            elapsed = now.timestamp() - self._last_bar_time
            if elapsed > 180:
                logger.warning(
                    "HEARTBEAT: no bar received in %.0fs during market hours",
                    elapsed,
                )
                await self.db.log_event(
                    "HEARTBEAT_WARN",
                    f"No bar in {elapsed:.0f}s during market hours",
                )

    async def _risk_monitor(self):
        while not self.kill_switch_triggered:
            await asyncio.sleep(60)
            await self._check_kill_switch()

    async def _auto_flatten_scheduler(self):
        tz  = self.config.tz
        now = datetime.now(tz)
        target = now.replace(
            hour=self.config.flatten_hour,
            minute=self.config.flatten_minute,
            second=0,
            microsecond=0,
        )
        if now >= target:
            target += timedelta(days=1)

        wait = (target - now).total_seconds()
        logger.info(
            "Auto-flatten scheduled in %.0f seconds (%s EST)",
            wait, target.strftime("%H:%M"),
        )
        await asyncio.sleep(wait)

        if not self.kill_switch_triggered:
            logger.info("Auto-flatten triggered (3:55 PM EST)")
            await self._flatten_all_positions(reason="AUTO_FLATTEN_3:55PM")

    # ── Stream with Reconnection ───────────────────────────────────────────────

    async def _run_stream_with_reconnect(self):
        """
        Run the WebSocket stream inside an exponential-backoff retry loop.
        On each reconnection: resync in-memory state from the REST API before
        resuming bar processing.
        """
        backoff     = 1
        max_backoff = 60

        while not self.kill_switch_triggered:
            try:
                logger.info("Connecting to Alpaca data stream…")
                await self.stream._run_forever()
                # _run_forever() returned without error — treat as silent disconnect
                logger.warning("Stream ended without exception — reconnecting…")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Stream error: %s", exc)

            if self.kill_switch_triggered:
                break

            logger.info("Reconnecting in %ds…", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

            # Recreate the stream — the old connection is dead
            async with self._stream_reconnect_lock:
                self.stream = StockDataStream(
                    api_key=self.config.api_key,
                    secret_key=self.config.api_secret,
                    feed=self.config.data_feed,
                )
                self.stream.subscribe_bars(self.on_bar, *self.config.symbols)

            # Resync before we start receiving bars again
            await self._resync_from_rest()
            backoff = 1  # reset after a successful reconnect attempt

    # ── Entry Point ────────────────────────────────────────────────────────────

    async def run(self):
        logger.info("ORB Bot starting — symbols: %s", self.config.symbols)
        logger.info("Paper mode: %s | Feed: %s", self.config.paper, self.config.data_feed)

        await self.db.initialize()
        await self._fetch_starting_equity()
        await self.db.log_event("START", f"symbols={self.config.symbols}")

        # Restore any trades that were open when the bot last stopped
        await self._resume_from_db()

        self.stream.subscribe_bars(self.on_bar, *self.config.symbols)

        asyncio.create_task(self._risk_monitor())
        asyncio.create_task(self._auto_flatten_scheduler())
        asyncio.create_task(self._order_fill_monitor())
        asyncio.create_task(self._orphan_order_cleanup())
        asyncio.create_task(self._heartbeat_watchdog())

        await self._run_stream_with_reconnect()


# ── Graceful Shutdown ──────────────────────────────────────────────────────────

async def _shutdown(bot: ORBBot):
    logger.info("Shutdown signal received — flattening positions…")
    await bot._flatten_all_positions(reason="SHUTDOWN")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    config = Config()
    bot    = ORBBot(config)

    import signal as _signal

    async def _run():
        loop = asyncio.get_running_loop()
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(_shutdown(bot))
                )
            except NotImplementedError:
                pass  # Windows doesn't support add_signal_handler
        await bot.run()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Bot shut down.")


if __name__ == "__main__":
    main()
