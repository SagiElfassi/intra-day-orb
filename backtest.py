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

import numpy as np
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
    take_profit: float  # = tp2 in scale-out mode, = tp in single-leg
    stop_loss: float
    range_high: float
    range_low: float
    exit_time: str      # time of the final (last) exit
    exit_price: float   # weighted avg of both legs in scale-out mode
    exit_reason: str    # TP / TP2 / BE / SL / EOD
    qty: int            # total quantity across all legs
    fees: float         # SEC + FINRA TAF regulatory fees (all legs combined)
    pnl: float          # net of fees (all legs combined)
    r_multiple: float   # net R = pnl / (risk_per_share × total_qty)
    # Two-leg extras — 0.0 / "" in single-leg mode
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    leg1_exit_reason: str = ""   # TP1 | SL | EOD
    leg1_pnl: float = 0.0


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
    reason: str   # NO_BARS | INSUFFICIENT_BARS | ZERO_RANGE | LOW_ORB_VOLUME | LOW_ATR | NO_SIGNAL
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
    tp1_r: float = 0.0
    tp2_r: float = 0.0
    risk_pct_equity: float = 0.0
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
        # Dynamic Momentum params
        tp1_r: float = 0.0,
        tp2_r: float = 0.0,
        risk_pct_equity: float = 0.0,
        starting_equity: float = 100_000.0,
        # Momentum filters (match bot.py _check_breakout logic)
        vwap_filter: bool = False,
        volume_surge_mult: float = 0.0,
        # Analyst filters (match bot.py _finalize_range logic)
        min_orb_volume: int = 0,
        min_atr_pct: float = 0.0,
        # Position cap (match bot.py max_position_usd)
        max_position_usd: float = 0.0,
        # ATR stop-loss multiplier (match bot.py atr_sl_mult)
        atr_sl_mult: float = 1.5,
        # Time-of-day filter: no new entries after this time (match bot.py)
        trading_window_end_hour: int = 10,
        trading_window_end_minute: int = 30,
    ) -> BacktestResult:

        result = BacktestResult(
            symbol=symbol,
            start_date=str(start_date),
            end_date=str(end_date),
            orb_minutes=orb_minutes,
            risk_ratio=risk_ratio,
            stop_loss_type=stop_loss_type,
            position_size_usd=position_size_usd,
            tp1_r=tp1_r,
            tp2_r=tp2_r,
            risk_pct_equity=risk_pct_equity,
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

        # ── Pre-compute 14-day rolling ATR per trade date ─────────────────────────
        # Used for: (1) LOW_ATR filter, (2) ATR stop-loss calculation.
        # Always computed so ATR SL works independently of min_atr_pct.
        daily = (
            df[df["timestamp"].dt.hour >= 9]
            .groupby("_date")
            .agg(daily_high=("high", "max"), daily_low=("low", "min"),
                 daily_close=("close", "last"))
            .reset_index()
            .sort_values("_date")
        )
        daily["prev_close"] = daily["daily_close"].shift(1)
        daily["tr"] = daily.apply(
            lambda r: max(
                r["daily_high"] - r["daily_low"],
                abs(r["daily_high"] - r["prev_close"]) if pd.notna(r["prev_close"]) else 0,
                abs(r["daily_low"]  - r["prev_close"]) if pd.notna(r["prev_close"]) else 0,
            ),
            axis=1,
        )
        daily["atr14"] = daily["tr"].rolling(14, min_periods=1).mean()
        daily["atr_pct"] = daily["atr14"] / daily["daily_close"] * 100
        _atr_by_date        = dict(zip(daily["_date"], daily["atr_pct"]))   # % (for filter)
        _atr_dollar_by_date = dict(zip(daily["_date"], daily["atr14"]))     # $ (for SL calc)

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

            # ── Running VWAP from 9:30 AM (matches bot.py on_bar accumulation) ─
            # Computed on day_df before slicing so scan_bars inherits the column.
            _mkt_mask = day_df["timestamp"] >= mkt_open
            _typ_px   = (day_df["high"] + day_df["low"] + day_df["close"]) / 3
            _cum_tpv  = (_typ_px * day_df["volume"]).where(_mkt_mask, 0.0).cumsum()
            _cum_vol  = day_df["volume"].where(_mkt_mask, 0.0).cumsum()
            day_df    = day_df.copy()
            day_df["_vwap"] = (_cum_tpv / _cum_vol.replace(0, np.nan)).ffill()

            # Time-of-day filter: entries only before trading_window_end (matches bot.py)
            window_end = mkt_open.replace(
                hour=trading_window_end_hour,
                minute=trading_window_end_minute,
                second=0, microsecond=0,
            )

            orb_bars     = day_df[
                (day_df["timestamp"] >= mkt_open) & (day_df["timestamp"] < orb_end)
            ]
            # all_post_orb: used for exit simulation (no time-of-day cap)
            all_post_orb = day_df[
                (day_df["timestamp"] >= orb_end) & (day_df["timestamp"] < flatten_at)
            ]
            # scan_bars: entry-eligible only (capped at trading_window_end)
            scan_bars    = all_post_orb[all_post_orb["timestamp"] < window_end]

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

            # ── Analyst filters (matches bot.py _finalize_range) ──────────────
            if min_orb_volume > 0:
                orb_vol = int(orb_bars["volume"].sum())
                if orb_vol < min_orb_volume:
                    result.skipped_days.append(SkippedDay(
                        date=date_str, reason="LOW_ORB_VOLUME",
                        detail=f"ORB volume {orb_vol:,} < min {min_orb_volume:,}.",
                    ))
                    continue

            if min_atr_pct > 0:
                atr_pct = _atr_by_date.get(trade_date, 0.0)
                if atr_pct < min_atr_pct:
                    result.skipped_days.append(SkippedDay(
                        date=date_str, reason="LOW_ATR",
                        detail=f"14-day ATR {atr_pct:.2f}% < min {min_atr_pct:.1f}%.",
                    ))
                    continue

            if all_post_orb.empty or scan_bars.empty:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="NO_SIGNAL",
                    detail="No bars available after ORB window (early close or data gap).",
                ))
                continue

            # ── Entry: vectorized breakout detection ───────────────────────────
            # argmax on a bool array returns the index of the first True.
            close_vals  = scan_bars["close"].values
            vwap_vals   = scan_bars["_vwap"].values
            volume_vals = scan_bars["volume"].values

            # Average per-bar volume during the ORB window (for surge gate)
            avg_orb_vol = orb_bars["volume"].mean() if not orb_bars.empty else 0.0

            # Build filter masks — both default to all-True when the feature is off
            if vwap_filter:
                vwap_long_ok  = close_vals > vwap_vals
                vwap_short_ok = close_vals < vwap_vals
            else:
                vwap_long_ok = vwap_short_ok = np.ones(len(scan_bars), dtype=bool)

            if volume_surge_mult > 0 and avg_orb_vol > 0:
                surge_ok = volume_vals >= avg_orb_vol * volume_surge_mult
            else:
                surge_ok = np.ones(len(scan_bars), dtype=bool)

            long_mask  = (close_vals > rng_high) & vwap_long_ok  & surge_ok
            short_mask = (close_vals < rng_low)  & vwap_short_ok & surge_ok

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

            entry_bar  = scan_bars.iloc[entry_iloc]
            # Entry fills at the ORB boundary (stop-limit order), not the breakout bar close
            entry_stop  = rng_high if side == "LONG" else rng_low
            slip        = round(entry_stop * slippage_bps / 10_000, 2)
            entry_price = round(entry_stop + slip, 2) if side == "LONG" else round(entry_stop - slip, 2)

            if stop_loss_type == "atr":
                atr_dollar = _atr_dollar_by_date.get(trade_date, 0.0)
                if atr_dollar > 0:
                    sl_dist = round(atr_dollar * atr_sl_mult, 2)
                else:
                    sl_dist = round((rng_high - rng_low) / 2, 2)  # fallback to midpoint distance
                sl = round(entry_stop - sl_dist, 2) if side == "LONG" else round(entry_stop + sl_dist, 2)
            elif stop_loss_type == "midpoint":
                sl = round(rng_mid, 2)
            else:  # hard
                sl = round(rng_low if side == "LONG" else rng_high, 2)

            risk_per_share = abs(entry_price - sl)
            if risk_per_share <= 0:
                result.skipped_days.append(SkippedDay(
                    date=date_str, reason="ZERO_RANGE",
                    detail=f"Degenerate SL: entry={entry_price} sl={sl} — skipping.",
                ))
                continue

            # ── Two-leg scale-out mode when tp1_r and tp2_r are set ────────────
            scale_out = tp1_r > 0 and tp2_r > tp1_r

            # Sizing: volatility-adjusted (risk % of equity) or fixed USD
            if risk_pct_equity > 0:
                risk_dollar = starting_equity * risk_pct_equity / 100.0
                total_qty   = max(2, math.floor(risk_dollar / risk_per_share))
            else:
                total_qty   = max(1, math.floor(position_size_usd / entry_price))
            if max_position_usd > 0:
                total_qty   = min(total_qty, max(2, math.floor(max_position_usd / entry_price)))

            half_qty = total_qty // 2
            rem_qty  = total_qty - half_qty

            # Target prices — bot.py uses ORB range (not risk_per_share) as the R unit
            orb_range_r = rng_high - rng_low
            if scale_out:
                if side == "LONG":
                    tp1_px = round(entry_stop + orb_range_r * tp1_r, 2)
                    tp2_px = round(entry_stop + orb_range_r * tp2_r, 2)
                else:
                    tp1_px = round(entry_stop - orb_range_r * tp1_r, 2)
                    tp2_px = round(entry_stop - orb_range_r * tp2_r, 2)
            else:
                if side == "LONG":
                    tp1_px = tp2_px = round(entry_stop + orb_range_r * risk_ratio, 2)
                else:
                    tp1_px = tp2_px = round(entry_stop - orb_range_r * risk_ratio, 2)

            # Fee helper (regulatory pass-through, sell side only)
            def _fee(q: int, sell_px: float) -> float:
                return round(
                    sell_px * q * _SEC_FEE_PER_DOLLAR
                    + min(q * _FINRA_TAF_PER_SHARE, _FINRA_TAF_CAP),
                    4,
                )

            # ── Exit: all post-ORB bars after entry timestamp (no time-of-day cap) ──
            entry_ts    = entry_bar["timestamp"]
            after_entry = all_post_orb[all_post_orb["timestamp"] > entry_ts]

            if not scale_out:
                # ── Single-leg (classic ORB) ──────────────────────────────────
                if after_entry.empty:
                    eod_close   = float(entry_bar["close"])
                    exit_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                    exit_reason = "EOD"
                    exit_ts     = entry_bar["timestamp"]
                else:
                    if side == "LONG":
                        tp_hit = after_entry["high"].values >= tp1_px
                        sl_hit = after_entry["low"].values  <= sl
                    else:
                        tp_hit = after_entry["low"].values  <= tp1_px
                        sl_hit = after_entry["high"].values >= sl

                    tp_i = int(tp_hit.argmax()) if tp_hit.any() else len(after_entry)
                    sl_i = int(sl_hit.argmax()) if sl_hit.any() else len(after_entry)

                    if tp_hit.any() and tp_i <= sl_i:
                        exit_reason = "TP"
                        exit_row    = after_entry.iloc[tp_i]
                        exit_px     = round(tp1_px - slip, 2) if side == "LONG" else round(tp1_px + slip, 2)
                    elif sl_hit.any():
                        exit_reason = "SL"
                        exit_row    = after_entry.iloc[sl_i]
                        exit_px     = round(sl - slip, 2) if side == "LONG" else round(sl + slip, 2)
                    else:
                        exit_reason = "EOD"
                        exit_row    = after_entry.iloc[-1]
                        eod_close   = float(exit_row["close"])
                        exit_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                    exit_ts = exit_row["timestamp"]

                sell_px  = exit_px if side == "LONG" else entry_price
                fees     = _fee(total_qty, sell_px)
                raw_pnl  = (exit_px - entry_price) if side == "LONG" else (entry_price - exit_px)
                pnl      = round(raw_pnl * total_qty - fees, 2)
                r_mult   = round(raw_pnl / risk_per_share, 2)
                tp1_out  = tp1_px
                tp2_out  = tp1_px
                l1_reason = exit_reason
                l1_pnl   = pnl

            else:
                # ── Two-leg scale-out ─────────────────────────────────────────
                # Phase 1: find TP1 or SL
                if after_entry.empty:
                    eod_close = float(entry_bar["close"])
                    l1_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                    l1_reason = "EOD"
                    l1_idx    = None
                    l1_ts     = entry_bar["timestamp"]
                else:
                    if side == "LONG":
                        tp1_hit = after_entry["high"].values >= tp1_px
                        sl_hit  = after_entry["low"].values  <= sl
                    else:
                        tp1_hit = after_entry["low"].values  <= tp1_px
                        sl_hit  = after_entry["high"].values >= sl

                    tp1_i = int(tp1_hit.argmax()) if tp1_hit.any() else len(after_entry)
                    sl_i  = int(sl_hit.argmax())  if sl_hit.any()  else len(after_entry)

                    if tp1_hit.any() and tp1_i <= sl_i:
                        l1_reason = "TP1"
                        l1_row    = after_entry.iloc[tp1_i]
                        l1_px     = round(tp1_px - slip, 2) if side == "LONG" else round(tp1_px + slip, 2)
                        l1_idx    = tp1_i
                        l1_ts     = l1_row["timestamp"]
                    elif sl_hit.any():
                        l1_reason = "SL"
                        l1_row    = after_entry.iloc[sl_i]
                        l1_px     = round(sl - slip, 2) if side == "LONG" else round(sl + slip, 2)
                        l1_idx    = sl_i
                        l1_ts     = l1_row["timestamp"]
                    else:
                        l1_reason = "EOD"
                        l1_row    = after_entry.iloc[-1]
                        eod_close = float(l1_row["close"])
                        l1_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                        l1_idx    = None
                        l1_ts     = l1_row["timestamp"]

                fees1  = _fee(half_qty, l1_px if side == "LONG" else entry_price)
                raw1   = (l1_px - entry_price) if side == "LONG" else (entry_price - l1_px)
                l1_pnl = round(raw1 * half_qty - fees1, 2)

                # Phase 2: if TP1 hit → scan with break-even SL; else both legs closed at l1
                if l1_reason in ("SL", "EOD"):
                    raw2      = raw1
                    l2_px     = l1_px
                    l2_reason = l1_reason
                    final_ts  = l1_ts
                    fees2     = _fee(rem_qty, l2_px if side == "LONG" else entry_price)
                else:
                    be_sl     = entry_price  # SL moves to break-even after TP1
                    after_tp1 = after_entry.iloc[l1_idx + 1:] if l1_idx is not None else pd.DataFrame()

                    if after_tp1.empty:
                        eod_close = float(l1_row["close"])
                        l2_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                        l2_reason = "EOD"
                        final_ts  = l1_ts
                    else:
                        if side == "LONG":
                            tp2_hit = after_tp1["high"].values >= tp2_px
                            be_hit  = after_tp1["low"].values  <= be_sl
                        else:
                            tp2_hit = after_tp1["low"].values  <= tp2_px
                            be_hit  = after_tp1["high"].values >= be_sl

                        tp2_i = int(tp2_hit.argmax()) if tp2_hit.any() else len(after_tp1)
                        be_i  = int(be_hit.argmax())  if be_hit.any()  else len(after_tp1)

                        if tp2_hit.any() and tp2_i <= be_i:
                            l2_reason = "TP2"
                            l2_row    = after_tp1.iloc[tp2_i]
                            l2_px     = round(tp2_px - slip, 2) if side == "LONG" else round(tp2_px + slip, 2)
                            final_ts  = l2_row["timestamp"]
                        elif be_hit.any():
                            l2_reason = "BE"
                            l2_row    = after_tp1.iloc[be_i]
                            l2_px     = round(be_sl - slip, 2) if side == "LONG" else round(be_sl + slip, 2)
                            final_ts  = l2_row["timestamp"]
                        else:
                            l2_reason = "EOD"
                            l2_row    = after_tp1.iloc[-1]
                            eod_close = float(l2_row["close"])
                            l2_px     = round(eod_close - slip, 2) if side == "LONG" else round(eod_close + slip, 2)
                            final_ts  = l2_row["timestamp"]

                    fees2 = _fee(rem_qty, l2_px if side == "LONG" else entry_price)
                    raw2  = (l2_px - entry_price) if side == "LONG" else (entry_price - l2_px)

                l2_pnl   = round(raw2 * rem_qty - fees2, 2)
                fees     = round(fees1 + fees2, 4)
                pnl      = round(l1_pnl + l2_pnl, 2)
                exit_px  = round((l1_px * half_qty + l2_px * rem_qty) / total_qty, 4)
                exit_ts  = final_ts
                exit_reason = l2_reason
                r_mult   = round(pnl / (risk_per_share * total_qty), 2) if risk_per_share * total_qty > 0 else 0.0
                tp1_out  = tp1_px
                tp2_out  = tp2_px

            result.trades.append(BacktestTrade(
                date             = date_str,
                symbol           = symbol,
                side             = side,
                entry_time       = str(entry_bar["timestamp"]),
                entry_price      = entry_price,
                take_profit      = tp2_out,
                stop_loss        = sl,
                range_high       = round(rng_high, 2),
                range_low        = round(rng_low,  2),
                exit_time        = str(exit_ts),
                exit_price       = round(exit_px, 2),
                exit_reason      = exit_reason,
                qty              = total_qty,
                fees             = fees,
                pnl              = pnl,
                r_multiple       = r_mult,
                tp1_price        = tp1_out,
                tp2_price        = tp2_out,
                leg1_exit_reason = l1_reason if scale_out else "",
                leg1_pnl         = l1_pnl    if scale_out else 0.0,
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
