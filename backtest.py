"""
ORB Backtesting Engine
Fetches 1-minute historical bars from Alpaca and simulates ORB trades
using the exact same logic as bot.py.
"""

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

EST = ZoneInfo("America/New_York")


@dataclass
class BacktestTrade:
    date: str
    symbol: str
    side: str           # LONG | SHORT
    entry_time: str
    entry_price: float
    take_profit: float
    stop_loss: float
    range_high: float
    range_low: float
    exit_time: str
    exit_price: float
    exit_reason: str    # TP | SL | EOD
    qty: int
    pnl: float
    r_multiple: float


@dataclass
class BacktestStats:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    avg_r_multiple: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    days_with_signal: int = 0
    days_skipped_no_data: int = 0


@dataclass
class SkippedDay:
    date: str
    reason: str   # NO_BARS | INSUFFICIENT_BARS | ZERO_RANGE | NO_SIGNAL
    detail: str   # human-readable explanation


@dataclass
class BacktestResult:
    symbol: str
    start_date: str
    end_date: str
    orb_minutes: int
    risk_ratio: float
    stop_loss_type: str
    position_size_usd: float
    trades: List[BacktestTrade] = field(default_factory=list)
    skipped_days: List[SkippedDay] = field(default_factory=list)
    stats: BacktestStats = field(default_factory=BacktestStats)
    equity_curve: List[float] = field(default_factory=list)
    error: Optional[str] = None


class BacktestEngine:
    def __init__(self, api_key: str, api_secret: str):
        self.client = StockHistoricalDataClient(api_key, api_secret)

    # ── Data Fetch ─────────────────────────────────────────────────────────────

    def fetch_bars(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        """Download 1-minute bars for the given date range."""
        start_dt = datetime(start.year, start.month, start.day, 4, 0, tzinfo=EST)
        end_dt   = datetime(end.year,   end.month,   end.day,   23, 59, tzinfo=EST)

        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=start_dt,
            end=end_dt,
            feed=DataFeed.IEX,
        )
        raw = self.client.get_stock_bars(request)
        df  = raw.df

        if df.empty:
            return pd.DataFrame()

        df = df.reset_index()

        # alpaca-py may return MultiIndex (symbol, timestamp) or just timestamp
        if "symbol" in df.columns:
            df = df[df["symbol"] == symbol].copy()

        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert(EST)
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df

    # ── Main Backtest ──────────────────────────────────────────────────────────

    def run_backtest(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        orb_minutes: int = 15,
        risk_ratio: float = 2.0,
        stop_loss_type: str = "midpoint",
        min_bars_required: int = 13,
        position_size_usd: float = 1000.0,
    ) -> BacktestResult:

        result = BacktestResult(
            symbol=symbol,
            start_date=str(start_date),
            end_date=str(end_date),
            orb_minutes=orb_minutes,
            risk_ratio=risk_ratio,
            stop_loss_type=stop_loss_type,
            position_size_usd=position_size_usd,
        )

        try:
            df = self.fetch_bars(symbol, start_date, end_date)
        except Exception as exc:
            result.error = str(exc)
            return result

        if df.empty:
            result.error = "No data returned — check symbol and date range."
            return result

        df["_date"] = df["timestamp"].dt.date

        for trade_date, day_df in df.groupby("_date"):
            day_df = day_df.sort_values("timestamp").reset_index(drop=True)
            date_str = str(trade_date)

            mkt_open   = datetime(trade_date.year, trade_date.month, trade_date.day, 9, 30, tzinfo=EST)
            orb_end    = mkt_open + timedelta(minutes=orb_minutes)
            flatten_at = mkt_open.replace(hour=15, minute=55)

            orb_bars  = day_df[(day_df["timestamp"] >= mkt_open) & (day_df["timestamp"] < orb_end)]
            scan_bars = day_df[(day_df["timestamp"] >= orb_end)   & (day_df["timestamp"] < flatten_at)]

            if len(orb_bars) == 0:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="NO_BARS",
                    detail="No bars received at market open — likely a holiday or early close.",
                ))
                continue

            if len(orb_bars) < min_bars_required:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="INSUFFICIENT_BARS",
                    detail=f"Only {len(orb_bars)}/{orb_minutes} bars in the ORB window "
                           f"(need ≥ {min_bars_required}). Data gap at open.",
                ))
                continue

            rng_high = orb_bars["high"].max()
            rng_low  = orb_bars["low"].min()
            rng_mid  = (rng_high + rng_low) / 2
            rng_span = rng_high - rng_low

            if rng_span <= 0:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="ZERO_RANGE",
                    detail=f"ORB High == Low == {rng_high:.2f}. Range is flat — no trade possible.",
                ))
                continue

            trade_taken = False
            for i, (idx, bar) in enumerate(scan_bars.iterrows()):
                close = bar["close"]

                if close > rng_high:
                    side = "LONG"
                elif close < rng_low:
                    side = "SHORT"
                else:
                    continue  # no breakout yet

                entry_price = close
                qty = max(1, math.floor(position_size_usd / entry_price))

                if side == "LONG":
                    tp = round(entry_price + rng_span * risk_ratio, 2)
                    sl = round(rng_mid if stop_loss_type == "midpoint" else rng_low,  2)
                else:
                    tp = round(entry_price - rng_span * risk_ratio, 2)
                    sl = round(rng_mid if stop_loss_type == "midpoint" else rng_high, 2)

                risk_per_share = abs(entry_price - sl)
                if risk_per_share <= 0:
                    break

                # --- Simulate bar-by-bar until TP/SL/EOD ---
                after_entry = scan_bars.iloc[i + 1:]
                exit_price  = entry_price
                exit_reason = "FLAT"   # 3:55 PM flatten — TP/SL not reached
                exit_time   = bar["timestamp"]

                for _, ex in after_entry.iterrows():
                    if side == "LONG":
                        if ex["high"] >= tp:
                            exit_price  = tp
                            exit_reason = "TP"
                            exit_time   = ex["timestamp"]
                            break
                        if ex["low"] <= sl:
                            exit_price  = sl
                            exit_reason = "SL"
                            exit_time   = ex["timestamp"]
                            break
                    else:
                        if ex["low"] <= tp:
                            exit_price  = tp
                            exit_reason = "TP"
                            exit_time   = ex["timestamp"]
                            break
                        if ex["high"] >= sl:
                            exit_price  = sl
                            exit_reason = "SL"
                            exit_time   = ex["timestamp"]
                            break
                else:
                    if not after_entry.empty:
                        exit_price = after_entry.iloc[-1]["close"]
                        exit_time  = after_entry.iloc[-1]["timestamp"]

                raw_pnl = (
                    (exit_price - entry_price) if side == "LONG"
                    else (entry_price - exit_price)
                )
                pnl        = round(raw_pnl * qty, 2)
                r_multiple = round(raw_pnl / risk_per_share, 2)

                result.trades.append(BacktestTrade(
                    date        = str(trade_date),
                    symbol      = symbol,
                    side        = side,
                    entry_time  = str(bar["timestamp"]),
                    entry_price = entry_price,
                    take_profit = tp,
                    stop_loss   = sl,
                    range_high  = round(rng_high, 2),
                    range_low   = round(rng_low,  2),
                    exit_time   = str(exit_time),
                    exit_price  = exit_price,
                    exit_reason = exit_reason,
                    qty         = qty,
                    pnl         = pnl,
                    r_multiple  = r_multiple,
                ))
                trade_taken = True
                break  # one trade per day

            if not trade_taken:
                close_min = scan_bars["close"].min() if not scan_bars.empty else rng_low
                close_max = scan_bars["close"].max() if not scan_bars.empty else rng_high
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="NO_SIGNAL",
                    detail=f"Price stayed inside ORB [{rng_low:.2f} – {rng_high:.2f}] "
                           f"all day. Session range: {close_min:.2f} – {close_max:.2f}.",
                ))

        result.stats = _compute_stats(result.trades, len(result.skipped_days))
        result.equity_curve = _equity_curve(result.trades)
        return result


# ── Statistics ─────────────────────────────────────────────────────────────────

def _compute_stats(trades: List[BacktestTrade], days_skipped: int = 0) -> BacktestStats:
    if not trades:
        return BacktestStats(days_skipped_no_data=days_skipped)

    pnls   = [t.pnl for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))

    # Max drawdown (dollar)
    equity = peak = max_dd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    return BacktestStats(
        total_trades          = len(trades),
        winning_trades        = len(wins),
        losing_trades         = len(losses),
        win_rate              = round(len(wins) / len(trades) * 100, 1),
        total_pnl             = round(sum(pnls), 2),
        avg_win               = round(sum(wins)   / len(wins),   2) if wins   else 0.0,
        avg_loss              = round(sum(losses)  / len(losses), 2) if losses else 0.0,
        profit_factor         = round(gross_profit / gross_loss,  2) if gross_loss else float("inf"),
        max_drawdown          = round(max_dd, 2),
        avg_r_multiple        = round(sum(t.r_multiple for t in trades) / len(trades), 2),
        best_trade            = round(max(pnls), 2),
        worst_trade           = round(min(pnls), 2),
        days_with_signal      = len(trades),
        days_skipped_no_data  = days_skipped,
    )


def _equity_curve(trades: List[BacktestTrade]) -> List[float]:
    curve, equity = [], 0.0
    for t in trades:
        equity += t.pnl
        curve.append(round(equity, 2))
    return curve
