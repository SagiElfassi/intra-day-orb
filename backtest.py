"""
ORB Backtesting Engine
Fetches 1-minute historical bars from Alpaca and simulates ORB trades
using the exact same logic as bot.py.
"""

import math
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

EST = ZoneInfo("America/New_York")

# Regulatory pass-through fees (Alpaca is commission-free but passes these through)
_SEC_FEE_PER_DOLLAR  = 20.60 / 1_000_000   # $20.60 per $1M principal sold
_FINRA_TAF_PER_SHARE = 0.000195             # $0.000195/share sold
_FINRA_TAF_CAP       = 9.79                 # capped at $9.79 per transaction


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
    fees: float         # SEC + FINRA TAF regulatory fees
    pnl: float          # net of fees
    r_multiple: float   # price-based (pre-fee) for trade quality assessment


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
    expectancy: float = 0.0       # (win% * avg_win) - (loss% * |avg_loss|)
    recovery_factor: float = 0.0  # total_pnl / max_drawdown
    sharpe_ratio: float = 0.0     # mean(pnl) / std(pnl), trade-based
    total_fees: float = 0.0       # total regulatory fees paid


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
    def __init__(self, api_key: str, api_secret: str, cache_dir: str = "bar_cache"):
        self.client    = StockHistoricalDataClient(api_key, api_secret)
        self.cache_dir = Path(cache_dir)

    # ── Data Fetch ─────────────────────────────────────────────────────────────

    def fetch_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        feed: DataFeed = DataFeed.IEX,
    ) -> pd.DataFrame:
        """
        Download 1-minute bars for the given date range.
        Results are cached to {cache_dir}/{symbol}_{start}_{end}_{feed}.parquet
        so repeated calls with the same parameters skip the Alpaca API entirely.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = self.cache_dir / f"{symbol}_{start}_{end}_{feed.value}.parquet"

        if cache_file.exists():
            try:
                return pd.read_parquet(cache_file)
            except Exception:
                # Corrupt cache — fall through to re-download
                cache_file.unlink(missing_ok=True)

        start_dt = datetime(start.year, start.month, start.day, 4, 0, tzinfo=EST)
        end_dt   = datetime(end.year,   end.month,   end.day,   23, 59, tzinfo=EST)

        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=start_dt,
            end=end_dt,
            feed=feed,
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

        try:
            df.to_parquet(cache_file, index=False)
        except Exception:
            pass  # caching is best-effort; never block a backtest run

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
        slippage_bps: float = 0.0,
        feed: DataFeed = DataFeed.IEX,
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
            df = self.fetch_bars(symbol, start_date, end_date, feed=feed)
        except Exception as exc:
            result.error = str(exc)
            return result

        if df.empty:
            result.error = "No data returned — check symbol and date range."
            return result

        df["_date"] = df["timestamp"].dt.date

        for trade_date, day_df in df.groupby("_date"):
            day_df   = day_df.sort_values("timestamp").reset_index(drop=True)
            date_str = str(trade_date)

            mkt_open = datetime(
                trade_date.year, trade_date.month, trade_date.day, 9, 30, tzinfo=EST
            )
            orb_end  = mkt_open + timedelta(minutes=orb_minutes)

            # ── Dynamic flatten time ───────────────────────────────────────────
            # Derive actual close from the last bar of the day so half-sessions
            # (e.g. Black Friday, Christmas Eve) are handled correctly.
            # Last 1-min bar at HH:MM → market closed at HH:MM+1 → flatten 5 min prior.
            # On a full trading day: last bar = 15:59 → flatten = 15:55 (same as hardcoded).
            default_flatten = mkt_open.replace(hour=15, minute=55)
            last_bar_ts     = day_df["timestamp"].iloc[-1]
            inferred_flatten = last_bar_ts - timedelta(minutes=4)
            flatten_at = min(inferred_flatten, default_flatten)

            orb_bars  = day_df[
                (day_df["timestamp"] >= mkt_open) & (day_df["timestamp"] < orb_end)
            ]
            scan_bars = day_df[
                (day_df["timestamp"] >= orb_end) & (day_df["timestamp"] < flatten_at)
            ]

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

            if scan_bars.empty:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="NO_SIGNAL",
                    detail="No bars available after ORB window (early close or data gap).",
                ))
                continue

            # ── Entry: vectorized breakout detection ───────────────────────────
            # Use numpy argmax on boolean arrays — O(n) with no Python loop overhead.
            # argmax on a bool array returns the position of the first True value.
            close_vals = scan_bars["close"].values
            long_mask  = close_vals > rng_high
            short_mask = close_vals < rng_low

            has_long  = long_mask.any()
            has_short = short_mask.any()

            if not has_long and not has_short:
                close_min = close_vals.min()
                close_max = close_vals.max()
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="NO_SIGNAL",
                    detail=f"Price stayed inside ORB [{rng_low:.2f} – {rng_high:.2f}] "
                           f"all day. Session range: {close_min:.2f} – {close_max:.2f}.",
                ))
                continue

            long_iloc  = int(long_mask.argmax())  if has_long  else len(scan_bars)
            short_iloc = int(short_mask.argmax()) if has_short else len(scan_bars)

            if long_iloc <= short_iloc:
                side, entry_iloc = "LONG", long_iloc
            else:
                side, entry_iloc = "SHORT", short_iloc

            entry_bar = scan_bars.iloc[entry_iloc]
            close     = float(entry_bar["close"])

            slip = round(close * slippage_bps / 10_000, 2)
            entry_price = round(close + slip, 2) if side == "LONG" else round(close - slip, 2)

            qty = max(1, math.floor(position_size_usd / entry_price))

            if side == "LONG":
                tp = round(entry_price + rng_span * risk_ratio, 2)
                sl = round(rng_mid if stop_loss_type == "midpoint" else rng_low,  2)
            else:
                tp = round(entry_price - rng_span * risk_ratio, 2)
                sl = round(rng_mid if stop_loss_type == "midpoint" else rng_high, 2)

            risk_per_share = abs(entry_price - sl)
            if risk_per_share <= 0:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="ZERO_RANGE",
                    detail=f"Degenerate SL: entry={entry_price} sl={sl} — skipping.",
                ))
                continue

            # ── Exit: vectorized TP/SL search ──────────────────────────────────
            # after_entry starts at entry_iloc+1 to avoid look-ahead bias.
            after_entry = scan_bars.iloc[entry_iloc + 1:]

            if after_entry.empty:
                # Entered on the last scan bar — exit at EOD close
                eod_close = float(entry_bar["close"])
                exit_price  = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                exit_reason = "EOD"
                exit_time   = entry_bar["timestamp"]
            else:
                if side == "LONG":
                    tp_hit = after_entry["high"].values >= tp
                    sl_hit = after_entry["low"].values  <= sl
                else:
                    tp_hit = after_entry["low"].values  <= tp
                    sl_hit = after_entry["high"].values >= sl

                tp_iloc_a = int(tp_hit.argmax()) if tp_hit.any() else len(after_entry)
                sl_iloc_a = int(sl_hit.argmax()) if sl_hit.any() else len(after_entry)

                if tp_hit.any() and tp_iloc_a <= sl_iloc_a:
                    # TP hit first (or same bar as SL — TP takes priority)
                    exit_reason = "TP"
                    exit_row    = after_entry.iloc[tp_iloc_a]
                    exit_price  = round(tp - slip, 2) if side == "LONG" else round(tp + slip, 2)
                elif sl_hit.any():
                    exit_reason = "SL"
                    exit_row    = after_entry.iloc[sl_iloc_a]
                    exit_price  = round(sl - slip, 2) if side == "LONG" else round(sl + slip, 2)
                else:
                    exit_reason = "EOD"
                    exit_row    = after_entry.iloc[-1]
                    eod_close   = float(exit_row["close"])
                    exit_price  = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)

                exit_time = exit_row["timestamp"]

            # ── Regulatory fees (sell side only) ───────────────────────────────
            # LONG: sell happens at exit. SHORT: sell-to-open happens at entry.
            sell_principal = (exit_price if side == "LONG" else entry_price) * qty
            sec_fee        = sell_principal * _SEC_FEE_PER_DOLLAR
            finra_fee      = min(qty * _FINRA_TAF_PER_SHARE, _FINRA_TAF_CAP)
            fees           = round(sec_fee + finra_fee, 4)

            raw_pnl    = (exit_price - entry_price) if side == "LONG" else (entry_price - exit_price)
            pnl        = round(raw_pnl * qty - fees, 2)
            r_multiple = round(raw_pnl / risk_per_share, 2)  # price-based, pre-fee

            result.trades.append(BacktestTrade(
                date        = date_str,
                symbol      = symbol,
                side        = side,
                entry_time  = str(entry_bar["timestamp"]),
                entry_price = entry_price,
                take_profit = tp,
                stop_loss   = sl,
                range_high  = round(rng_high, 2),
                range_low   = round(rng_low,  2),
                exit_time   = str(exit_time),
                exit_price  = exit_price,
                exit_reason = exit_reason,
                qty         = qty,
                fees        = fees,
                pnl         = pnl,
                r_multiple  = r_multiple,
            ))

        result.stats        = _compute_stats(result.trades, len(result.skipped_days))
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

    equity = peak = max_dd = 0.0
    for p in pnls:
        equity += p
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    win_pct      = len(wins)   / len(trades)
    loss_pct     = len(losses) / len(trades)
    avg_win_val  = sum(wins)   / len(wins)   if wins   else 0.0
    avg_loss_val = sum(losses) / len(losses) if losses else 0.0

    expectancy      = round(win_pct * avg_win_val + loss_pct * avg_loss_val, 2)
    recovery_factor = round(sum(pnls) / max_dd, 2) if max_dd > 0 else float("inf")
    sharpe          = round(
        (sum(pnls) / len(pnls)) / statistics.stdev(pnls), 2
    ) if len(pnls) >= 2 and statistics.stdev(pnls) > 0 else 0.0

    return BacktestStats(
        total_trades         = len(trades),
        winning_trades       = len(wins),
        losing_trades        = len(losses),
        win_rate             = round(win_pct * 100, 1),
        total_pnl            = round(sum(pnls), 2),
        avg_win              = round(avg_win_val,  2),
        avg_loss             = round(avg_loss_val, 2),
        profit_factor        = round(gross_profit / gross_loss, 2) if gross_loss else float("inf"),
        max_drawdown         = round(max_dd, 2),
        avg_r_multiple       = round(sum(t.r_multiple for t in trades) / len(trades), 2),
        best_trade           = round(max(pnls), 2),
        worst_trade          = round(min(pnls), 2),
        days_with_signal     = len(trades),
        days_skipped_no_data = days_skipped,
        expectancy           = expectancy,
        recovery_factor      = recovery_factor,
        sharpe_ratio         = sharpe,
        total_fees           = round(sum(t.fees for t in trades), 4),
    )


def _equity_curve(trades: List[BacktestTrade]) -> List[float]:
    curve, equity = [], 0.0
    for t in trades:
        equity += t.pnl
        curve.append(round(equity, 2))
    return curve
