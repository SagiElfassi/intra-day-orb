"""
ORB Dashboard — Streamlit + Plotly
Two tabs:
  • Live  — real-time chart with ORB box, TP/SL bands, live trade card, R:R
  • Backtest — parameter sweep over historical data with equity curve + trade log
"""

import json
import os
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots
from zoneinfo import ZoneInfo

load_dotenv(dotenv_path=".env")

DB_PATH      = os.environ.get("DB_PATH", "orb_trades.db")
CONFIG_PATH  = "live_config.json"
EST          = ZoneInfo("America/New_York")
REFRESH_SEC  = 10

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ORB Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.trade-card {
    background:#1a1f2e; border:1px solid #2d3348; border-radius:10px;
    padding:18px 22px; margin-bottom:12px;
}
.trade-live { border-left:4px solid #00e676; }
.trade-closed { border-left:4px solid #546e7a; }
.label { font-size:0.75rem; color:#90a4ae; letter-spacing:.06em; text-transform:uppercase; }
.val-green { font-size:1.4rem; font-weight:700; color:#00e676; }
.val-red   { font-size:1.4rem; font-weight:700; color:#ef5350; }
.val-white { font-size:1.4rem; font-weight:700; color:#eceff1; }
.badge-long  { background:#1b5e20; color:#a5d6a7; padding:2px 10px; border-radius:4px; font-size:.8rem; font-weight:600; }
.badge-short { background:#b71c1c; color:#ef9a9a; padding:2px 10px; border-radius:4px; font-size:.8rem; font-weight:600; }
.badge-tp    { background:#0d47a1; color:#90caf9; padding:2px 10px; border-radius:4px; font-size:.8rem; }
.badge-sl    { background:#4a148c; color:#ce93d8; padding:2px 10px; border-radius:4px; font-size:.8rem; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────────

def db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def load_config() -> dict:
    defaults = dict(
        symbols=["META", "TSLA", "SPY"],
        orb_minutes=15,
        risk_ratio=2.0,
        position_size_usd=2000.0,
        stop_loss_type="midpoint",
        alpaca_paper=True,
        min_orb_volume=1_000_000,
        min_atr_pct=2.0,
        max_spread_pct=0.05,
        # Dynamic Momentum scanner
        gap_min_pct=1.5,
        rvol_min=2.0,
        scanner_top_n=5,
        # Intraday filters
        vwap_filter=True,
        volume_surge_mult=1.5,
        # Volatility-adjusted sizing
        risk_pct_equity=1.0,
        # Scale-out targets
        tp1_r=1.0,
        tp2_r=3.0,
    )
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                saved = json.load(f)
            defaults.update(saved)
        except Exception:
            pass
    return defaults


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


def get_alpaca_credentials(paper: bool) -> tuple:
    if paper:
        key    = os.environ.get("ALPACA_API_KEY_PAPER") or os.environ.get("ALPACA_API_KEY", "")
        secret = os.environ.get("ALPACA_API_SECRET_PAPER") or os.environ.get("ALPACA_API_SECRET", "")
    else:
        key    = os.environ.get("ALPACA_API_KEY_LIVE") or os.environ.get("ALPACA_API_KEY", "")
        secret = os.environ.get("ALPACA_API_SECRET_LIVE") or os.environ.get("ALPACA_API_SECRET", "")
    return key, secret


@st.cache_data(ttl=3600)
def fetch_spy_benchmark(
    start_date, end_date, position_size_usd: float, api_key: str, api_secret: str
) -> pd.DataFrame | None:
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame
        from datetime import datetime, timezone
        client = StockHistoricalDataClient(api_key=api_key, secret_key=api_secret)
        req = StockBarsRequest(
            symbol_or_symbols="SPY",
            timeframe=TimeFrame.Day,
            start=datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc),
            end=datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc),
        )
        bars = client.get_stock_bars(req)["SPY"]
        if not bars:
            return None
        dates  = [str(b.timestamp.date()) for b in bars]
        closes = [float(b.close) for b in bars]
        entry  = closes[0]
        qty    = max(1, int(position_size_usd // entry))
        pnls   = [(c - entry) * qty for c in closes]
        return pd.DataFrame({"date": dates, "benchmark_pnl": pnls})
    except Exception:
        return None


@st.cache_data(ttl=30)
def fetch_account_balance(paper: bool) -> dict | None:
    try:
        from alpaca.trading.client import TradingClient
        if paper:
            key    = os.environ.get("ALPACA_API_KEY_PAPER", "")
            secret = os.environ.get("ALPACA_API_SECRET_PAPER", "")
        else:
            key    = os.environ.get("ALPACA_API_KEY_LIVE", "")
            secret = os.environ.get("ALPACA_API_SECRET_LIVE", "")
        if not key:
            return None
        client  = TradingClient(api_key=key, secret_key=secret, paper=paper)
        account = client.get_account()
        return {
            "equity":        float(account.equity),
            "buying_power":  float(account.buying_power),
            "cash":          float(account.cash),
        }
    except Exception:
        return None


@st.cache_data(ttl=REFRESH_SEC)
def q_trades(selected_date: str) -> pd.DataFrame:
    try:
        with db_conn() as c:
            return pd.read_sql_query(
                "SELECT * FROM trades WHERE date=? ORDER BY created_at", c,
                params=(selected_date,)
            )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC)
def q_ranges(selected_date: str) -> pd.DataFrame:
    try:
        with db_conn() as c:
            return pd.read_sql_query(
                "SELECT * FROM orb_ranges WHERE date=? AND valid=1", c,
                params=(selected_date,)
            )
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC)
def q_bars(symbol: str, selected_date: str) -> pd.DataFrame:
    try:
        with db_conn() as c:
            df = pd.read_sql_query(
                """SELECT * FROM price_bars
                   WHERE symbol=? AND DATE(timestamp)=?
                   ORDER BY timestamp""",
                c, params=(symbol, selected_date)
            )
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(EST)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC)
def q_events(selected_date: str) -> pd.DataFrame:
    try:
        with db_conn() as c:
            return pd.read_sql_query(
                """SELECT * FROM bot_events WHERE DATE(timestamp)=?
                   ORDER BY timestamp DESC LIMIT 60""",
                c, params=(selected_date,)
            )
    except Exception:
        return pd.DataFrame()


def latest_price(symbol: str, selected_date: str) -> float | None:
    bars = q_bars(symbol, selected_date)
    if bars.empty:
        return None
    return float(bars.iloc[-1]["close"])


# ── Helpers ────────────────────────────────────────────────────────────────────

def trading_hours(bars: pd.DataFrame) -> pd.DataFrame:
    """Keep only bars between 9:30 AM and 4:00 PM EST."""
    if bars.empty:
        return bars
    t = bars["timestamp"]
    mask = (
        ((t.dt.hour == 9)  & (t.dt.minute >= 30)) |
        ((t.dt.hour >= 10) & (t.dt.hour < 16))
    )
    return bars[mask].copy()


def apply_rangebreaks(fig: go.Figure, rows: int = 1):
    """Remove pre/post-market gaps and weekends from every x-axis."""
    breaks = [
        dict(bounds=[16, 9.5], pattern="hour"),   # outside 9:30–16:00
        dict(bounds=["sat", "mon"]),               # weekends
    ]
    for i in range(1, rows + 1):
        axis = "xaxis" if i == 1 else f"xaxis{i}"
        fig.update_layout(**{axis: dict(rangebreaks=breaks)})


# ── Chart builder ──────────────────────────────────────────────────────────────

def build_live_chart(
    symbol: str,
    bars: pd.DataFrame,
    orb: pd.Series | None,
    trade: pd.Series | None,
    orb_minutes: int,
) -> go.Figure:

    bars = trading_hours(bars)

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.78, 0.22], vertical_spacing=0.02,
        subplot_titles=(f"{symbol}  •  1-min", "Volume"),
    )

    if bars.empty:
        fig.add_annotation(text="Waiting for bar data…",
                           x=0.5, y=0.5, xref="paper", yref="paper",
                           showarrow=False, font=dict(size=16, color="#90a4ae"))
        _dark_layout(fig)
        return fig

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=bars["timestamp"], open=bars["open"], high=bars["high"],
        low=bars["low"],     close=bars["close"], name=symbol,
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
    ), row=1, col=1)

    # Volume
    v_colors = ["#26a69a" if c >= o else "#ef5350"
                for c, o in zip(bars["close"], bars["open"])]
    fig.add_trace(go.Bar(
        x=bars["timestamp"], y=bars["volume"],
        marker_color=v_colors, showlegend=False, name="Vol",
    ), row=2, col=1)

    x0 = bars["timestamp"].iloc[0]
    x1 = bars["timestamp"].iloc[-1]

    # ── ORB range box ──
    if orb is not None:
        fig.add_shape(type="rect", x0=x0, x1=x1,
                      y0=orb["range_low"], y1=orb["range_high"],
                      line=dict(color="rgba(255,214,0,.6)", width=1),
                      fillcolor="rgba(255,214,0,.06)", layer="below",
                      row=1, col=1)
        for level, label, color in [
            (orb["range_high"], f"ORB High  {orb['range_high']:.2f}", "#FFD700"),
            (orb["range_low"],  f"ORB Low   {orb['range_low']:.2f}",  "#FFA000"),
            (orb["range_mid"],  f"Mid       {orb['range_mid']:.2f}",  "#FFCC44"),
        ]:
            fig.add_hline(y=level, line_dash="dot", line_color=color,
                          annotation_text=label, annotation_position="right",
                          row=1, col=1)

    # ── Active trade TP / SL bands ──
    if trade is not None:
        entry = trade.get("entry_price")
        tp    = trade.get("take_profit")
        sl    = trade.get("stop_loss")
        side  = trade.get("side", "")

        # Entry line
        if pd.notna(entry) and entry:
            fig.add_hline(y=entry, line_dash="solid",
                          line_color="#82b1ff", line_width=1.5,
                          annotation_text=f"Entry  {entry:.2f}",
                          annotation_position="right", row=1, col=1)

        # TP band
        if pd.notna(tp) and tp:
            fig.add_hline(y=tp, line_dash="dash", line_color="#00e676", line_width=2,
                          annotation_text=f"TP  {tp:.2f}", annotation_position="right",
                          row=1, col=1)
            if pd.notna(entry) and entry:
                fig.add_shape(type="rect", x0=x0, x1=x1,
                              y0=min(entry, tp), y1=max(entry, tp),
                              fillcolor="rgba(0,230,118,.07)",
                              line=dict(width=0), layer="below", row=1, col=1)

        # SL band
        if pd.notna(sl) and sl:
            fig.add_hline(y=sl, line_dash="dash", line_color="#ef5350", line_width=2,
                          annotation_text=f"SL  {sl:.2f}", annotation_position="right",
                          row=1, col=1)
            if pd.notna(entry) and entry:
                fig.add_shape(type="rect", x0=x0, x1=x1,
                              y0=min(entry, sl), y1=max(entry, sl),
                              fillcolor="rgba(239,83,80,.07)",
                              line=dict(width=0), layer="below", row=1, col=1)

        # Entry marker
        if pd.notna(entry) and entry and pd.notna(trade.get("created_at")):
            try:
                et = pd.to_datetime(trade["created_at"], utc=True).tz_convert(EST)
                fig.add_trace(go.Scatter(
                    x=[et], y=[entry], mode="markers",
                    marker=dict(
                        symbol="triangle-up" if side == "LONG" else "triangle-down",
                        size=14,
                        color="#00e676" if side == "LONG" else "#ef5350",
                    ),
                    name=f"Entry ({side})", showlegend=True,
                ), row=1, col=1)
            except Exception:
                pass

    fig.update_layout(
        xaxis_rangeslider_visible=False, height=520,
        margin=dict(l=0, r=90, t=30, b=0),
        legend=dict(orientation="h", y=1.04, x=0),
    )
    _dark_layout(fig)
    apply_rangebreaks(fig, rows=2)
    return fig


def _dark_layout(fig: go.Figure):
    fig.update_layout(
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117", font_color="#eceff1",
    )
    fig.update_xaxes(gridcolor="#1e2130", showgrid=True)
    fig.update_yaxes(gridcolor="#1e2130", showgrid=True)


# ── Trade card ─────────────────────────────────────────────────────────────────

def render_trade_card(trade: pd.Series, current_price: float | None):
    side   = trade.get("side", "")
    entry  = trade.get("entry_price")
    tp     = trade.get("take_profit")
    sl     = trade.get("stop_loss")
    qty    = trade.get("qty", 0)
    status = trade.get("status", "")
    is_live = status in ("PENDING", "OPEN")

    card_cls = "trade-card trade-live" if is_live else "trade-card trade-closed"
    badge    = f'<span class="badge-{"long" if side=="LONG" else "short"}">{side}</span>'

    # Unrealized P&L
    upnl_html = ""
    rr_html   = ""
    if is_live and current_price and pd.notna(entry) and entry:
        direction = 1 if side == "LONG" else -1
        upnl = (current_price - entry) * direction * qty
        cls  = "val-green" if upnl >= 0 else "val-red"
        sign = "+" if upnl >= 0 else ""
        upnl_html = f"""
        <div style="margin-top:12px; display:flex; gap:32px">
          <div>
            <div class="label">Current Price</div>
            <div class="val-white">{current_price:.2f}</div>
          </div>
          <div>
            <div class="label">Unrealized P&L</div>
            <div class="{cls}">{sign}${upnl:.2f}</div>
          </div>
        </div>"""

    # R:R ratio display
    if pd.notna(entry) and entry and pd.notna(tp) and pd.notna(sl):
        reward = abs(tp - entry)
        risk   = abs(entry - sl)
        if risk > 0:
            rr = reward / risk
            tp_dist  = abs(tp - entry)
            sl_dist  = abs(entry - sl)
            tp_r     = round(tp_dist / risk, 1)
            sl_r     = round(sl_dist / risk, 1)
            rr_html = f"""
            <div style="margin-top:12px; display:flex; gap:32px; align-items:center">
              <div>
                <div class="label">R:R Ratio</div>
                <div class="val-white">1 : {rr:.1f}</div>
              </div>
              <div>
                <span class="badge-tp">TP +{tp_dist:.2f} ({tp_r}R)</span>
                &nbsp;
                <span class="badge-sl">SL −{sl_dist:.2f} ({sl_r}R)</span>
              </div>
            </div>"""

    realized = trade.get("realized_pnl")
    pnl_row  = ""
    if pd.notna(realized) and realized is not None:
        cls  = "val-green" if realized >= 0 else "val-red"
        sign = "+" if realized >= 0 else ""
        pnl_row = f"""<div style="margin-top:8px">
            <span class="label">Realized P&L</span>&nbsp;
            <span class="{cls}">{sign}${realized:.2f}</span>
            &nbsp;<span class="label">· Exit reason:</span>&nbsp;
            <span class="val-white">{trade.get("status","")}</span>
        </div>"""

    html = f"""
    <div class="{card_cls}">
      <div style="display:flex; justify-content:space-between; align-items:center">
        <span style="font-size:1.1rem; font-weight:600">{trade.get("symbol","")} &nbsp;{badge}</span>
        <span class="label">{'🟢 LIVE' if is_live else '⚫ CLOSED'} · qty {int(qty)}</span>
      </div>
      <div style="margin-top:12px; display:flex; gap:32px; flex-wrap:wrap">
        <div><div class="label">Entry</div>
             <div class="val-white">{f"{entry:.2f}" if pd.notna(entry) and entry else "pending"}</div></div>
        <div><div class="label">Take Profit</div>
             <div class="val-green">{f"{tp:.2f}" if pd.notna(tp) else "—"}</div></div>
        <div><div class="label">Stop Loss</div>
             <div class="val-red">{f"{sl:.2f}" if pd.notna(sl) else "—"}</div></div>
      </div>
      {upnl_html}
      {rr_html}
      {pnl_row}
    </div>"""
    st.markdown(html, unsafe_allow_html=True)


# ── Backtest charts ────────────────────────────────────────────────────────────

def build_equity_curve(
    trades_df: pd.DataFrame,
    benchmark_df: pd.DataFrame | None = None,
) -> go.Figure:
    fig = go.Figure()
    if trades_df.empty:
        fig.add_annotation(text="No trades", x=0.5, y=0.5,
                           xref="paper", yref="paper", showarrow=False)
        _dark_layout(fig)
        fig.update_layout(height=300)
        return fig

    x = list(range(1, len(trades_df) + 1))
    y = trades_df["pnl"].cumsum().tolist()

    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines+markers",
        line=dict(color="#82b1ff", width=2),
        fill="tozeroy",
        fillcolor="rgba(130,177,255,0.1)",
        name="ORB Strategy",
        hovertemplate="Trade %{x}<br>ORB: $%{y:.2f}<extra></extra>",
    ))

    if benchmark_df is not None and not benchmark_df.empty:
        bench_by_date = dict(zip(benchmark_df["date"], benchmark_df["benchmark_pnl"]))
        sorted_dates  = sorted(bench_by_date.keys())
        bench_y = []
        for td in trades_df["date"].tolist():
            td_str = str(td)
            candidates = [d for d in sorted_dates if d <= td_str]
            bench_y.append(bench_by_date[candidates[-1]] if candidates else None)
        fig.add_trace(go.Scatter(
            x=x, y=bench_y,
            mode="lines",
            line=dict(color="#FFD700", width=1.5, dash="dot"),
            name="SPY Buy & Hold",
            hovertemplate="Trade %{x}<br>SPY B&H: $%{y:.2f}<extra></extra>",
        ))

    fig.update_layout(
        title="Equity Curve vs SPY Buy & Hold", height=300,
        xaxis_title="Trade #", yaxis_title="Cumulative P&L ($)",
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(orientation="h", y=1.15, x=0),
    )
    _dark_layout(fig)
    return fig


def build_r_distribution(trades_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if trades_df.empty:
        _dark_layout(fig)
        fig.update_layout(height=260)
        return fig

    r_vals = trades_df["r_multiple"].tolist()
    colors = ["#00e676" if r >= 0 else "#ef5350" for r in r_vals]
    fig.add_trace(go.Bar(
        x=list(range(1, len(r_vals) + 1)), y=r_vals,
        marker_color=colors, name="R-Multiple",
        hovertemplate="Trade %{x}<br>R: %{y:.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="#546e7a", line_width=1)
    fig.update_layout(
        title="R-Multiple per Trade", height=260,
        xaxis_title="Trade #", yaxis_title="R",
        margin=dict(l=0, r=0, t=40, b=0),
    )
    _dark_layout(fig)
    return fig


def build_bt_day_chart(trade_row: pd.Series, day_bars: pd.DataFrame) -> go.Figure:
    """Backtest day chart: candlestick + ORB box + VWAP + TP1/TP2/SL levels + exit markers."""
    fig = go.Figure()
    if day_bars.empty:
        _dark_layout(fig)
        return fig

    day_bars = trading_hours(day_bars).copy()

    # ── VWAP (running from first bar, same formula as bot.py / backtest.py) ──
    typ_px  = (day_bars["high"] + day_bars["low"] + day_bars["close"]) / 3
    cum_vol = day_bars["volume"].cumsum()
    day_bars["_vwap"] = (typ_px * day_bars["volume"]).cumsum() / cum_vol.replace(0, float("nan"))

    fig.add_trace(go.Candlestick(
        x=day_bars["timestamp"],
        open=day_bars["open"], high=day_bars["high"],
        low=day_bars["low"],   close=day_bars["close"],
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        name="Price",
    ))

    # VWAP line
    fig.add_trace(go.Scatter(
        x=day_bars["timestamp"], y=day_bars["_vwap"],
        mode="lines", line=dict(color="#FF6F00", width=1.5, dash="dot"),
        name="VWAP",
        hovertemplate="%{x|%H:%M}<br>VWAP: $%{y:.2f}<extra></extra>",
    ))

    x0, x1 = day_bars["timestamp"].iloc[0], day_bars["timestamp"].iloc[-1]

    # ORB box
    rh, rl = trade_row["range_high"], trade_row["range_low"]
    fig.add_shape(type="rect", x0=x0, x1=x1, y0=rl, y1=rh,
                  fillcolor="rgba(255,214,0,.07)",
                  line=dict(color="rgba(255,214,0,.5)", width=1))

    # 3:55 PM flatten line
    entry_ts   = pd.to_datetime(trade_row["entry_time"])
    flatten_ts = entry_ts.normalize().replace(hour=15, minute=55)
    fig.add_vline(x=flatten_ts.timestamp() * 1000,
                  line_dash="dot", line_color="#546e7a", line_width=1,
                  annotation_text="3:55 Flatten", annotation_position="top right",
                  annotation_font_color="#546e7a")

    # ── TP / SL / Entry levels ────────────────────────────────────────────────
    tp1_px  = trade_row.get("tp1_price", 0.0) or 0.0
    tp2_px  = trade_row.get("tp2_price", 0.0) or 0.0
    entry   = trade_row["entry_price"]
    sl      = trade_row["stop_loss"]
    scale_out_trade = tp1_px > 0 and tp2_px > 0 and abs(tp1_px - tp2_px) > 0.001

    # Entry + SL (always present)
    fig.add_hline(y=entry, line_dash="solid", line_color="#82b1ff", line_width=1.5,
                  annotation_text=f"Entry  {entry:.2f}", annotation_position="right",
                  annotation_font_color="#82b1ff")
    fig.add_hline(y=sl, line_dash="dash", line_color="#ef5350", line_width=1.5,
                  annotation_text=f"SL  {sl:.2f}", annotation_position="right",
                  annotation_font_color="#ef5350")

    if scale_out_trade:
        fig.add_hline(y=tp1_px, line_dash="dash", line_color="#69f0ae", line_width=1.5,
                      annotation_text=f"TP1  {tp1_px:.2f}", annotation_position="right",
                      annotation_font_color="#69f0ae")
        fig.add_hline(y=tp2_px, line_dash="dash", line_color="#00e676", line_width=2,
                      annotation_text=f"TP2  {tp2_px:.2f}", annotation_position="right",
                      annotation_font_color="#00e676")
        # Break-even level (= entry, shown as a note after TP1)
        fig.add_annotation(x=1.01, xref="paper", y=entry,
                           text="B/E after TP1", showarrow=False,
                           xanchor="left", yanchor="bottom",
                           font=dict(size=9, color="#82b1ff"))
    else:
        tp_px = tp2_px if tp2_px > 0 else trade_row.get("take_profit", 0.0)
        if tp_px:
            fig.add_hline(y=tp_px, line_dash="dash", line_color="#00e676", line_width=1.5,
                          annotation_text=f"TP  {tp_px:.2f}", annotation_position="right",
                          annotation_font_color="#00e676")

    # ── Exit markers ─────────────────────────────────────────────────────────
    _exit_colors = {
        "TP": "#00e676", "TP1": "#69f0ae", "TP2": "#00e676",
        "SL": "#ef5350", "BE": "#82b1ff",  "EOD": "#FF6F00",
    }
    exit_reason = trade_row.get("exit_reason", "")
    leg1_reason = trade_row.get("leg1_exit", "")

    # Leg 1 exit marker (only in scale-out mode when TP1 was hit)
    if scale_out_trade and leg1_reason == "TP1":
        try:
            fig.add_trace(go.Scatter(
                x=[entry_ts], y=[tp1_px],
                mode="markers",
                marker=dict(symbol="circle", size=10,
                            color="#69f0ae", line=dict(color="#fff", width=1)),
                name="TP1 hit",
                hovertemplate="TP1 hit<br>$%{y:.2f}<extra></extra>",
            ))
        except Exception:
            pass

    # Final exit marker
    if pd.notna(trade_row.get("exit_time")) and pd.notna(trade_row.get("exit_price")):
        try:
            final_ts    = pd.to_datetime(trade_row["exit_time"])
            final_color = _exit_colors.get(exit_reason, "#FF6F00")
            fig.add_trace(go.Scatter(
                x=[final_ts], y=[float(trade_row["exit_price"])],
                mode="markers",
                marker=dict(symbol="x", size=12, color=final_color, line_width=2),
                name=f"Exit ({exit_reason})",
                hovertemplate=f"Exit: {exit_reason}<br>$%{{y:.2f}}<extra></extra>",
            ))
        except Exception:
            pass

    fig.update_layout(
        xaxis_rangeslider_visible=False, height=420,
        margin=dict(l=0, r=120, t=10, b=0),
        legend=dict(orientation="h", y=1.06, x=0, font=dict(size=11)),
    )
    _dark_layout(fig)
    apply_rangebreaks(fig, rows=1)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

cfg = load_config()

with st.sidebar:
    st.title("⚡ ORB Bot")
    st.divider()

    st.subheader("Account")
    paper_mode = cfg.get("alpaca_paper", True)
    account_choice = st.radio(
        "Mode", ["📄 Paper", "💰 Live"],
        index=0 if paper_mode else 1,
        horizontal=True,
        label_visibility="collapsed",
    )
    alpaca_paper = account_choice.startswith("📄")
    if alpaca_paper:
        st.success("Paper trading")
    else:
        st.warning("⚠️ Live trading")

    acct = fetch_account_balance(alpaca_paper)
    if acct:
        st.metric("Equity",       f"${acct['equity']:,.2f}")
        st.metric("Buying Power", f"${acct['buying_power']:,.2f}")
        st.metric("Cash",         f"${acct['cash']:,.2f}")
    else:
        st.caption("Balance unavailable")
    st.divider()

    st.subheader("Symbols")
    sym_input = st.text_input(
        "Comma-separated", value=", ".join(cfg["symbols"]),
        placeholder="SPY, QQQ, AAPL"
    )
    symbols = [s.strip().upper() for s in sym_input.split(",") if s.strip()]

    st.subheader("Strategy")
    orb_minutes = st.radio(
        "ORB Timeframe", [5, 15, 30], index=[5, 15, 30].index(cfg["orb_minutes"]),
        format_func=lambda x: f"{x} min",
        horizontal=True,
    )
    risk_ratio = st.slider("Risk:Reward Ratio", 1.0, 5.0, float(cfg["risk_ratio"]), 0.5)
    pos_size   = st.number_input("Position Size (USD)", 100, 100000,
                                  int(cfg["position_size_usd"]), 100)
    sl_type    = st.radio("Stop-Loss Type", ["midpoint", "hard"], horizontal=True,
                          index=0 if cfg["stop_loss_type"] == "midpoint" else 1)

    st.subheader("Analyst Filters")
    min_vol_m = st.number_input(
        "Min ORB Volume (M shares)", 0.1, 20.0,
        round(cfg.get("min_orb_volume", 1_000_000) / 1e6, 1), 0.1,
        format="%.1f",
    )
    min_orb_volume = int(min_vol_m * 1_000_000)
    min_atr_pct    = st.slider("Min Daily ATR %", 0.0, 10.0, float(cfg.get("min_atr_pct", 2.0)), 0.5)
    max_spread_pct = st.number_input(
        "Max Bid-Ask Spread %", 0.01, 1.0,
        float(cfg.get("max_spread_pct", 0.05)), 0.01,
        format="%.3f",
    )

    st.subheader("Pre-Market Scanner")
    gap_min_pct = st.number_input(
        "Min Gap %", 0.0, 20.0, float(cfg.get("gap_min_pct", 1.5)), 0.1,
        format="%.1f", help="Minimum overnight gap vs prior close",
    )
    rvol_min = st.number_input(
        "Min RVOL", 0.5, 20.0, float(cfg.get("rvol_min", 2.0)), 0.1,
        format="%.1f", help="Min relative volume vs 20-day average",
    )
    scanner_top_n = st.number_input(
        "Top N symbols", 1, 20, int(cfg.get("scanner_top_n", 5)), 1,
        help="How many top-scoring symbols to trade each day",
    )

    st.subheader("Momentum Filters")
    vwap_filter = st.toggle(
        "VWAP Filter", value=bool(cfg.get("vwap_filter", True)),
        help="Long must break out above VWAP; short below",
    )
    volume_surge_mult = st.number_input(
        "Volume Surge Multiplier", 1.0, 10.0,
        float(cfg.get("volume_surge_mult", 1.5)), 0.1, format="%.1f",
        help="Breakout bar volume ≥ N × avg ORB volume",
    )

    st.subheader("Scale-Out & Sizing")
    risk_pct_equity = st.number_input(
        "Risk % of Equity per Trade", 0.1, 5.0,
        float(cfg.get("risk_pct_equity", 1.0)), 0.1, format="%.1f",
        help="Max loss if SL is hit = equity × this %",
    )
    tp1_r = st.number_input(
        "TP1 (R-multiple)", 0.5, 5.0, float(cfg.get("tp1_r", 1.0)), 0.25,
        format="%.2f", help="First exit target — 50% of position",
    )
    tp2_r = st.number_input(
        "TP2 (R-multiple)", 1.0, 10.0, float(cfg.get("tp2_r", 3.0)), 0.25,
        format="%.2f", help="Second exit target — remaining 50%",
    )

    if st.button("💾 Save Config", use_container_width=True):
        new_cfg = dict(
            symbols=symbols, orb_minutes=orb_minutes,
            risk_ratio=risk_ratio, position_size_usd=float(pos_size),
            stop_loss_type=sl_type, alpaca_paper=alpaca_paper,
            min_orb_volume=min_orb_volume, min_atr_pct=min_atr_pct,
            max_spread_pct=max_spread_pct,
            gap_min_pct=gap_min_pct, rvol_min=rvol_min,
            scanner_top_n=int(scanner_top_n),
            vwap_filter=vwap_filter,
            volume_surge_mult=volume_surge_mult,
            risk_pct_equity=risk_pct_equity,
            tp1_r=tp1_r, tp2_r=tp2_r,
        )
        save_config(new_cfg)
        cfg = new_cfg
        st.success("Saved — restart bot to apply")

    st.divider()
    selected_date = st.date_input("Date", value=date.today())
    selected_date_str = str(selected_date)

    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("Click Refresh to reload data")


# ══════════════════════════════════════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_live, tab_bt, tab_scanner, tab_rules = st.tabs([
    "📡  Live Trading", "🔬  Backtest", "🔭  Scanner", "📐  Entry Conditions"
])


# ── TAB 1 : LIVE ──────────────────────────────────────────────────────────────

with tab_live:
    trades_df = q_trades(selected_date_str)
    ranges_df = q_ranges(selected_date_str)
    events_df = q_events(selected_date_str)

    # Summary row
    total = len(trades_df)
    active = int((trades_df["status"].isin(["PENDING","OPEN"])).sum()) if not trades_df.empty else 0
    closed = trades_df[trades_df["status"].str.startswith("CLOSED", na=False)] if not trades_df.empty else pd.DataFrame()
    rpnl   = closed["realized_pnl"].sum() if not closed.empty else 0.0
    wr     = ((closed["realized_pnl"] > 0).sum() / len(closed) * 100) if not closed.empty else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Signals",      total)
    c2.metric("Active",       active)
    c3.metric("Realized PnL", f"${rpnl:+.2f}")
    c4.metric("Win Rate",     f"{wr:.0f}%")
    c5.metric("ORB Ranges",   len(ranges_df) if not ranges_df.empty else 0)

    st.divider()

    # Per-symbol
    for sym in (symbols if symbols else ["SPY"]):
        st.subheader(f"📊 {sym}")
        col_chart, col_card = st.columns([3, 1])

        bars    = q_bars(sym, selected_date_str)
        orb_row = (ranges_df[ranges_df["symbol"] == sym].iloc[0]
                   if not ranges_df.empty and sym in ranges_df["symbol"].values else None)
        sym_trades = (trades_df[trades_df["symbol"] == sym]
                      if not trades_df.empty and sym in trades_df["symbol"].values
                      else pd.DataFrame())

        live_trade = None
        if not sym_trades.empty:
            live_rows = sym_trades[sym_trades["status"].isin(["PENDING", "OPEN"])]
            if not live_rows.empty:
                live_trade = live_rows.iloc[0]

        with col_chart:
            st.plotly_chart(
                build_live_chart(sym, bars, orb_row, live_trade, orb_minutes),
                use_container_width=True,
            )

        with col_card:
            if live_trade is not None:
                cur = latest_price(sym, selected_date_str)
                render_trade_card(live_trade, cur)
            elif not sym_trades.empty:
                st.markdown("**Closed trades**")
                for _, t in sym_trades.iterrows():
                    render_trade_card(t, None)
            else:
                orb_status = "✅ Range set — scanning" if orb_row is not None else "⏳ Waiting for 9:30 AM ORB window"
                st.info(orb_status)
                if orb_row is not None:
                    st.markdown(f"""
**ORB Range**
- High: `{orb_row['range_high']:.2f}`
- Low: `{orb_row['range_low']:.2f}`
- Mid: `{orb_row['range_mid']:.2f}`
- Span: `{orb_row['range_high'] - orb_row['range_low']:.2f}`
                    """)

        st.divider()

    # Event log
    with st.expander("🗒️ Bot Event Log", expanded=False):
        if events_df.empty:
            st.caption("No events logged.")
        else:
            st.dataframe(events_df[["timestamp", "event_type", "message"]],
                         use_container_width=True, hide_index=True)



# ── TAB 2 : BACKTEST ──────────────────────────────────────────────────────────

with tab_bt:
    st.subheader("Backtest Parameters")

    bc1, bc2, bc3, bc4 = st.columns(4)
    with bc1:
        bt_symbol  = st.text_input("Symbol", value=symbols[0] if symbols else "SPY").upper()
        bt_orb     = st.radio("ORB Timeframe ", [5, 15, 30],
                              index=[5, 15, 30].index(orb_minutes),
                              format_func=lambda x: f"{x} min", horizontal=True,
                              key="bt_orb")
        bt_sl_type = st.radio("Stop-Loss", ["midpoint", "hard"],
                              index=0 if sl_type == "midpoint" else 1,
                              horizontal=True, key="bt_sl")
    with bc2:
        bt_start = st.date_input("Start Date", value=date.today() - timedelta(days=90), key="bt_start")
        bt_end   = st.date_input("End Date",   value=date.today() - timedelta(days=1),  key="bt_end")
        bt_slippage = st.number_input("Slippage (bps)", 0.0, 50.0, 0.0, 0.5, format="%.1f", key="bt_slip")
    with bc3:
        bt_scale_out = st.toggle("Two-Leg Scale-Out", value=True, key="bt_scale_out",
                                  help="Simulate TP1+TP2 with break-even SL (Dynamic Momentum mode)")
        bt_tp1_r = st.number_input("TP1 (R)", 0.5, 5.0, float(cfg.get("tp1_r", 1.0)), 0.25,
                                    format="%.2f", key="bt_tp1",
                                    help="First exit target — 50% of position")
        bt_tp2_r = st.number_input("TP2 (R)", 1.0, 10.0, float(cfg.get("tp2_r", 3.0)), 0.25,
                                    format="%.2f", key="bt_tp2",
                                    help="Second exit target — remaining 50%")
        if not bt_scale_out:
            bt_rr = st.slider("Risk:Reward (single-leg)", 1.0, 5.0, risk_ratio, 0.5, key="bt_rr")
        else:
            bt_rr = risk_ratio  # unused in scale-out mode
    with bc4:
        bt_risk_mode = st.radio("Sizing Mode", ["% of Equity", "Fixed USD"],
                                 index=0, horizontal=False, key="bt_risk_mode")
        if bt_risk_mode == "% of Equity":
            bt_risk_pct = st.number_input("Risk % per Trade", 0.1, 5.0,
                                           float(cfg.get("risk_pct_equity", 1.0)), 0.1,
                                           format="%.1f", key="bt_risk_pct")
            bt_equity   = st.number_input("Starting Equity ($)", 10_000, 10_000_000,
                                           100_000, 10_000, key="bt_equity")
            bt_pos      = int(pos_size)  # not used in risk-pct mode
        else:
            bt_risk_pct = 0.0
            bt_equity   = 100_000.0
            bt_pos      = st.number_input("Position Size (USD)", 100, 100_000,
                                           int(pos_size), 100, key="bt_pos")

    run_btn = st.button("▶  Run Backtest", type="primary", use_container_width=False)

    if run_btn:
        if bt_start >= bt_end:
            st.error("End date must be after start date.")
        else:
            import os
            from backtest import BacktestEngine

            api_key, api_secret = get_alpaca_credentials(alpaca_paper)

            if not api_key:
                st.error("ALPACA_API_KEY not set. Add it to .env or environment.")
            else:
                mode_label = f"Two-Leg Scale-Out (TP1={bt_tp1_r}R / TP2={bt_tp2_r}R)" if bt_scale_out else f"Classic (RR={bt_rr})"
                with st.spinner(f"Backtesting {bt_symbol}  {bt_start} → {bt_end}  [{mode_label}]…"):
                    engine = BacktestEngine(api_key, api_secret)
                    result = engine.run_backtest(
                        symbol            = bt_symbol,
                        start_date        = bt_start,
                        end_date          = bt_end,
                        orb_minutes       = bt_orb,
                        risk_ratio        = bt_rr,
                        stop_loss_type    = bt_sl_type,
                        position_size_usd = float(bt_pos),
                        slippage_bps      = bt_slippage,
                        tp1_r             = bt_tp1_r if bt_scale_out else 0.0,
                        tp2_r             = bt_tp2_r if bt_scale_out else 0.0,
                        risk_pct_equity   = bt_risk_pct,
                        starting_equity   = float(bt_equity),
                        vwap_filter       = vwap_filter if bt_scale_out else False,
                        volume_surge_mult = volume_surge_mult if bt_scale_out else 0.0,
                    )

                if result.error:
                    st.error(f"Backtest error: {result.error}")
                elif not result.trades:
                    st.warning("No trades generated. Try a wider date range or different symbol.")
                else:
                    st.session_state["bt_result"] = result
                    st.session_state["bt_benchmark"] = fetch_spy_benchmark(
                        bt_start, bt_end, float(bt_pos), api_key, api_secret
                    )

    # ── Results ──
    if "bt_result" in st.session_state:
        result = st.session_state["bt_result"]
        s      = result.stats
        trades_list = result.trades

        st.divider()
        st.subheader(
            f"Results — {result.symbol}  |  {result.orb_minutes}-min ORB  "
            f"|  {result.start_date} → {result.end_date}"
        )

        # Stats rows
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Total Trades",   s.total_trades)
        s2.metric("Win Rate",       f"{s.win_rate}%")
        s3.metric("Total PnL",      f"${s.total_pnl:+.2f}")
        s4.metric("Profit Factor",  f"{s.profit_factor:.2f}" if s.profit_factor != float("inf") else "∞")
        s5.metric("Max Drawdown",   f"-${s.max_drawdown:.2f}")
        s6.metric("Avg R",          f"{s.avg_r_multiple:.2f}R")

        sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
        sc1.metric("Avg Win",        f"${s.avg_win:.2f}")
        sc2.metric("Avg Loss",       f"${s.avg_loss:.2f}")
        sc3.metric("Expectancy",     f"${s.expectancy:.2f}")
        sc4.metric("Sharpe",         f"{s.sharpe_ratio:.2f}")
        sc5.metric("Recovery Factor",f"{s.recovery_factor:.2f}" if s.recovery_factor != float("inf") else "∞")
        sc6.metric("Total Fees",     f"${s.total_fees:.2f}")

        # Charts
        _is_scale_out = result.tp1_r > 0 and result.tp2_r > result.tp1_r
        trades_df_bt = pd.DataFrame([
            dict(date=t.date, symbol=t.symbol, side=t.side,
                 entry_price=t.entry_price, take_profit=t.take_profit,
                 stop_loss=t.stop_loss, range_high=t.range_high,
                 range_low=t.range_low, exit_price=t.exit_price,
                 exit_reason=t.exit_reason, qty=t.qty,
                 pnl=t.pnl, r_multiple=t.r_multiple,
                 entry_time=t.entry_time, exit_time=t.exit_time,
                 tp1_price=t.tp1_price, tp2_price=t.tp2_price,
                 leg1_exit=t.leg1_exit_reason, leg1_pnl=t.leg1_pnl,
                 fees=t.fees)
            for t in trades_list
        ])

        benchmark_df = st.session_state.get("bt_benchmark")

        ch1, ch2 = st.columns([2, 1])
        with ch1:
            st.plotly_chart(build_equity_curve(trades_df_bt, benchmark_df), use_container_width=True)
        with ch2:
            st.plotly_chart(build_r_distribution(trades_df_bt), use_container_width=True)

        # Trade log
        st.subheader("Trade Log")

        _exit_icons = {
            "TP":  "✅ TP",  "TP2": "✅ TP2", "TP1": "✅ TP1",
            "SL":  "❌ SL",  "BE":  "↩️ BE",
            "EOD": "🕐 EOD",
        }
        _l1_icons = {
            "TP1": "✅ TP1", "SL": "❌ SL", "EOD": "🕐 EOD", "": "",
        }

        display = trades_df_bt.copy()
        display["#"]      = range(1, len(display) + 1)
        display["Side"]   = display["side"].map({"LONG": "▲ LONG", "SHORT": "▼ SHORT"})
        display["Final"]  = display["exit_reason"].map(lambda x: _exit_icons.get(x, x))
        display["PnL"]    = display["pnl"].map(lambda x: f"+${x:.2f}" if x >= 0 else f"-${abs(x):.2f}")
        display["R"]      = display["r_multiple"].map(lambda x: f"+{x:.2f}R" if x >= 0 else f"{x:.2f}R")
        display["Fees"]   = display["fees"].map(lambda x: f"${x:.3f}")

        if _is_scale_out:
            display["Leg1"]     = display["leg1_exit"].map(lambda x: _l1_icons.get(x, x))
            display["Leg1 PnL"] = display["leg1_pnl"].map(lambda x: f"+${x:.2f}" if x >= 0 else f"-${abs(x):.2f}")
            show_cols = [
                "#", "date", "Side", "entry_price", "tp1_price", "tp2_price", "stop_loss",
                "exit_price", "Leg1", "Leg1 PnL", "Final", "qty", "PnL", "R", "Fees",
            ]
            col_cfg = {
                "#":           st.column_config.NumberColumn(width="small"),
                "date":        st.column_config.TextColumn("Date", width="small"),
                "Side":        st.column_config.TextColumn(width="small"),
                "entry_price": st.column_config.NumberColumn("Entry", format="$%.2f", width="small"),
                "tp1_price":   st.column_config.NumberColumn("TP1", format="$%.2f", width="small"),
                "tp2_price":   st.column_config.NumberColumn("TP2", format="$%.2f", width="small"),
                "stop_loss":   st.column_config.NumberColumn("SL", format="$%.2f", width="small"),
                "exit_price":  st.column_config.NumberColumn("Avg Exit", format="$%.2f", width="small"),
                "Leg1":        st.column_config.TextColumn(width="small"),
                "Leg1 PnL":    st.column_config.TextColumn(width="small"),
                "Final":       st.column_config.TextColumn(width="small"),
                "qty":         st.column_config.NumberColumn("Qty", width="small"),
                "PnL":         st.column_config.TextColumn(width="small"),
                "R":           st.column_config.TextColumn(width="small"),
                "Fees":        st.column_config.TextColumn(width="small"),
            }
        else:
            show_cols = [
                "#", "date", "Side", "entry_price", "take_profit", "stop_loss",
                "exit_price", "Final", "qty", "PnL", "R", "Fees",
            ]
            col_cfg = {
                "#":           st.column_config.NumberColumn(width="small"),
                "date":        st.column_config.TextColumn("Date", width="small"),
                "Side":        st.column_config.TextColumn(width="small"),
                "entry_price": st.column_config.NumberColumn("Entry", format="$%.2f", width="small"),
                "take_profit": st.column_config.NumberColumn("TP", format="$%.2f", width="small"),
                "stop_loss":   st.column_config.NumberColumn("SL", format="$%.2f", width="small"),
                "exit_price":  st.column_config.NumberColumn("Exit", format="$%.2f", width="small"),
                "Final":       st.column_config.TextColumn("Result", width="small"),
                "qty":         st.column_config.NumberColumn("Qty", width="small"),
                "PnL":         st.column_config.TextColumn(width="small"),
                "R":           st.column_config.TextColumn(width="small"),
                "Fees":        st.column_config.TextColumn(width="small"),
            }

        st.dataframe(
            display[show_cols],
            use_container_width=True,
            hide_index=True,
            column_config=col_cfg,
        )

        # Skipped days breakdown
        if result.skipped_days:
            reason_icons = {
                "NO_BARS":           "🚫",
                "INSUFFICIENT_BARS": "⚠️",
                "ZERO_RANGE":        "➖",
                "NO_SIGNAL":         "😴",
            }
            skipped_df = pd.DataFrame([
                {
                    "Date":   d.date,
                    "Reason": f"{reason_icons.get(d.reason, '')} {d.reason.replace('_', ' ').title()}",
                    "Detail": d.detail,
                }
                for d in result.skipped_days
            ])
            with st.expander(f"📋 Days with no trade — {len(result.skipped_days)} days", expanded=False):
                st.dataframe(skipped_df, use_container_width=True, hide_index=True,
                             column_config={
                                 "Date":   st.column_config.TextColumn(width="small"),
                                 "Reason": st.column_config.TextColumn(width="medium"),
                                 "Detail": st.column_config.TextColumn(width="large"),
                             })

        # Drill-down chart for selected trade
        st.subheader("Trade Detail")
        trade_idx = st.selectbox(
            "Select trade to inspect",
            options=list(range(len(trades_list))),
            format_func=lambda i: f"#{i+1}  {trades_list[i].date}  {trades_list[i].side}  {trades_list[i].exit_reason}  ${trades_list[i].pnl:+.2f}",
        )
        if trade_idx is not None:
            sel = trades_list[trade_idx]
            sel_date = sel.date

            @st.cache_data(ttl=3600)
            def _get_day_bars(sym, d, paper):
                from datetime import date as _date
                from backtest import BacktestEngine as _BTE
                import os as _os
                from dotenv import load_dotenv
                load_dotenv(".env")
                if paper:
                    _key = _os.environ.get("ALPACA_API_KEY_PAPER") or _os.environ.get("ALPACA_API_KEY", "")
                    _sec = _os.environ.get("ALPACA_API_SECRET_PAPER") or _os.environ.get("ALPACA_API_SECRET", "")
                else:
                    _key = _os.environ.get("ALPACA_API_KEY_LIVE") or _os.environ.get("ALPACA_API_KEY", "")
                    _sec = _os.environ.get("ALPACA_API_SECRET_LIVE") or _os.environ.get("ALPACA_API_SECRET", "")
                yr, mo, dy = map(int, d.split("-"))
                return _BTE(_key, _sec).fetch_bars(sym, _date(yr, mo, dy), _date(yr, mo, dy))

            day_bars = _get_day_bars(bt_symbol, sel_date, alpaca_paper)
            st.plotly_chart(
                build_bt_day_chart(trades_df_bt.iloc[trade_idx], day_bars),
                use_container_width=True,
            )


# ── TAB 3 : SCANNER ──────────────────────────────────────────────────────────

SCAN_UNIVERSE_DEFAULT = (
    "SPY, QQQ, IWM, AAPL, NVDA, META, TSLA, AMZN, MSFT, GOOGL, AMD, "
    "COIN, PLTR, NFLX, JPM, GS, MSTR, HOOD, SOFI, IONQ"
)

with tab_scanner:
    st.subheader("ORB Stock Scanner")
    st.caption("Ranks stocks by ORB performance over the selected lookback period using your current strategy settings.")

    scn_col1, scn_col2 = st.columns([3, 1])
    with scn_col1:
        universe_input = st.text_area(
            "Stock Universe (comma-separated)",
            value=SCAN_UNIVERSE_DEFAULT,
            height=80,
            key="scan_universe",
        )
        scan_symbols = [s.strip().upper() for s in universe_input.split(",") if s.strip()]
        st.caption(f"{len(scan_symbols)} symbols loaded")

    with scn_col2:
        scan_days = st.selectbox(
            "Lookback Period", [7, 14, 30, 60, 90], index=1,
            format_func=lambda x: f"{x} days", key="scan_days",
        )
        scan_orb  = st.radio(
            "ORB Timeframe", [5, 15, 30],
            index=[5, 15, 30].index(orb_minutes),
            format_func=lambda x: f"{x} min",
            horizontal=True, key="scan_orb_tf",
        )
        rank_by = st.selectbox(
            "Rank By",
            ["Total P&L", "Profit Factor", "Win Rate %", "Avg R", "# Trades"],
            key="scan_rank",
        )

    scan_btn = st.button("🔭  Run Scanner", type="primary", key="scan_run")

    if scan_btn:
        api_key, api_secret = get_alpaca_credentials(alpaca_paper)
        if not api_key:
            st.error("API keys not configured.")
        else:
            from backtest import BacktestEngine
            scan_end   = date.today() - timedelta(days=1)
            scan_start = scan_end - timedelta(days=scan_days)

            scan_rows = []
            prog = st.progress(0, text="Starting scan…")

            for i, sym in enumerate(scan_symbols):
                prog.progress((i + 1) / len(scan_symbols), text=f"Scanning {sym}  ({i+1}/{len(scan_symbols)})")
                try:
                    result = BacktestEngine(api_key, api_secret).run_backtest(
                        symbol=sym,
                        start_date=scan_start,
                        end_date=scan_end,
                        orb_minutes=scan_orb,
                        risk_ratio=risk_ratio,
                        stop_loss_type=sl_type,
                        position_size_usd=float(pos_size),
                    )
                    if result.error or not result.trades:
                        scan_rows.append({"Symbol": sym, "_no_trade": True, "_reason": result.error or "no signals"})
                        continue
                    s = result.stats
                    scan_rows.append({
                        "Symbol":        sym,
                        "Trades":        s.total_trades,
                        "Win Rate %":    s.win_rate,
                        "Total P&L":     round(s.total_pnl, 2),
                        "Profit Factor": round(s.profit_factor, 2) if s.profit_factor != float("inf") else 999.0,
                        "Avg R":         round(s.avg_r_multiple, 2),
                        "Max DD ($)":    round(s.max_drawdown, 2),
                        "Avg Win":       round(s.avg_win, 2),
                        "Avg Loss":      round(s.avg_loss, 2),
                        "Skipped Days":  len(result.skipped_days),
                        "_no_trade":     False,
                        "_reason":       "",
                    })
                except Exception as exc:
                    scan_rows.append({"Symbol": sym, "_no_trade": True, "_reason": str(exc)})

            prog.empty()
            st.session_state["scan_results"] = scan_rows
            st.session_state["scan_meta"] = {
                "days": scan_days, "orb": scan_orb,
                "start": str(scan_start), "end": str(scan_end),
            }

    if "scan_results" in st.session_state:
        scan_rows = st.session_state["scan_results"]
        meta      = st.session_state.get("scan_meta", {})

        traded     = [r for r in scan_rows if not r.get("_no_trade")]
        no_signals = [r for r in scan_rows if r.get("_no_trade")]

        if not traded:
            st.warning("No symbols generated trades in the selected period.")
        else:
            rank_col_map = {
                "Total P&L":    "Total P&L",
                "Profit Factor":"Profit Factor",
                "Win Rate %":   "Win Rate %",
                "Avg R":        "Avg R",
                "# Trades":     "Trades",
            }
            sort_col = rank_col_map.get(rank_by, "Total P&L")
            df_scan  = pd.DataFrame(traded).sort_values(sort_col, ascending=False).reset_index(drop=True)
            df_scan.index = range(1, len(df_scan) + 1)

            period_label = f"{meta.get('start','?')} → {meta.get('end','?')}  ·  {meta.get('orb','?')}-min ORB"
            st.markdown(f"**{len(df_scan)} symbols with trades** — {period_label}")

            # Summary metrics for top symbol
            top = df_scan.iloc[0]
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("🏆 Top Symbol",  top["Symbol"])
            m2.metric("Total P&L",      f"${top['Total P&L']:+,.2f}")
            m3.metric("Win Rate",        f"{top['Win Rate %']:.0f}%")
            m4.metric("Profit Factor",   f"{top['Profit Factor']:.2f}")
            m5.metric("Avg R",           f"{top['Avg R']:.2f}R")

            st.divider()

            display_cols = ["Symbol", "Trades", "Win Rate %", "Total P&L",
                            "Profit Factor", "Avg R", "Max DD ($)", "Skipped Days"]
            st.dataframe(
                df_scan[display_cols],
                use_container_width=True,
                column_config={
                    "Symbol":        st.column_config.TextColumn(width="small"),
                    "Trades":        st.column_config.NumberColumn(width="small"),
                    "Win Rate %":    st.column_config.NumberColumn(format="%.0f%%", width="small"),
                    "Total P&L":     st.column_config.NumberColumn(format="$%+.2f"),
                    "Profit Factor": st.column_config.NumberColumn(format="%.2f", width="small"),
                    "Avg R":         st.column_config.NumberColumn(format="%.2f", width="small"),
                    "Max DD ($)":    st.column_config.NumberColumn(format="$%.2f"),
                    "Skipped Days":  st.column_config.NumberColumn(width="small"),
                },
            )

            # Per-symbol equity curves for top 5
            st.divider()
            st.subheader("Top 5 Equity Curves")
            top5 = df_scan.head(5)["Symbol"].tolist()
            api_key_scan, api_secret_scan = get_alpaca_credentials(alpaca_paper)
            scan_end_d   = date.today() - timedelta(days=1)
            scan_start_d = scan_end_d - timedelta(days=scan_days)

            eq_cols = st.columns(min(len(top5), 5))
            for col, sym in zip(eq_cols, top5):
                with col:
                    st.caption(sym)
                    try:
                        from backtest import BacktestEngine as _BTE
                        @st.cache_data(ttl=3600)
                        def _scan_equity(s, sd, ed, orb, rr, sl, ps, key, sec):
                            r = _BTE(key, sec).run_backtest(s, sd, ed, orb, rr, sl, ps)
                            if r.trades:
                                return pd.DataFrame({"pnl": [t.pnl for t in r.trades]})
                            return pd.DataFrame()

                        eq_df = _scan_equity(
                            sym, scan_start_d, scan_end_d, scan_orb,
                            risk_ratio, sl_type, float(pos_size),
                            api_key_scan, api_secret_scan,
                        )
                        if not eq_df.empty:
                            fig_mini = go.Figure(go.Scatter(
                                y=eq_df["pnl"].cumsum(),
                                mode="lines", line=dict(color="#82b1ff", width=2),
                                fill="tozeroy", fillcolor="rgba(130,177,255,0.1)",
                            ))
                            fig_mini.update_layout(
                                height=140, margin=dict(l=0, r=0, t=0, b=0),
                                showlegend=False,
                                xaxis=dict(visible=False),
                                yaxis=dict(tickformat="$,.0f", tickfont=dict(size=9)),
                            )
                            _dark_layout(fig_mini)
                            st.plotly_chart(fig_mini, use_container_width=True)
                    except Exception:
                        st.caption("chart unavailable")

        if no_signals:
            with st.expander(f"⚠️ No signals — {len(no_signals)} symbols", expanded=False):
                st.dataframe(
                    pd.DataFrame([{"Symbol": r["Symbol"], "Reason": r["_reason"]} for r in no_signals]),
                    use_container_width=True, hide_index=True,
                )


# ── TAB 4 : ENTRY CONDITIONS ──────────────────────────────────────────────────

with tab_rules:

    # Pull live sidebar values so all formulas update in real time
    _gap_min     = cfg.get("gap_min_pct", 1.5)
    _rvol_min    = cfg.get("rvol_min", 2.0)
    _top_n       = int(cfg.get("scanner_top_n", 5))
    _vwap_on     = cfg.get("vwap_filter", True)
    _surge_mult  = cfg.get("volume_surge_mult", 1.5)
    _risk_pct    = cfg.get("risk_pct_equity", 1.0)
    _tp1_r       = cfg.get("tp1_r", 1.0)
    _tp2_r       = cfg.get("tp2_r", 3.0)

    rng_example  = 2.50
    entry_long   = 100.00
    entry_short  = 97.50
    orb_risk     = rng_example / 2  # midpoint SL distance

    tp1_l  = round(entry_long  + orb_risk * _tp1_r, 2)
    tp2_l  = round(entry_long  + orb_risk * _tp2_r, 2)
    sl_l   = round(entry_long  - orb_risk, 2)
    tp1_s  = round(entry_short - orb_risk * _tp1_r, 2)
    tp2_s  = round(entry_short - orb_risk * _tp2_r, 2)
    sl_s   = round(entry_short + orb_risk, 2)

    st.subheader("Dynamic Momentum ORB — Entry Conditions")
    st.caption(f"All formulas reflect your current sidebar settings: "
               f"**{orb_minutes}-min ORB · Gap ≥{_gap_min:.1f}% · RVOL ≥{_rvol_min:.1f}× · "
               f"Risk {_risk_pct:.1f}% equity · TP1={_tp1_r:.1f}R / TP2={_tp2_r:.1f}R**")

    # ── Strategy Overview ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Strategy Overview")

    st.info("""
**Dynamic Momentum ORB** combines a pre-market scanning pass with intraday momentum confirmation to produce a high-quality trade list every morning.

- **Pre-market scanner** runs at 4:00 AM–9:25 AM, ranking stocks by overnight gap and relative volume. Only the top symbols trade that day.
- **Intraday momentum filters** (VWAP alignment + volume surge) confirm genuine breakout momentum before a position is opened.
- **Volatility-adjusted sizing** risking a fixed % of account equity, so position size automatically scales with the stock's move potential.
- **Two-leg scale-out** locks in profits at TP1 (1R) and moves the stop to break-even, letting the second leg ride to TP2 (3R) risk-free.
- **Bracket orders** sent to Alpaca ensure the broker manages exits even if the bot crashes.
- **Auto-flatten at 3:55 PM** eliminates overnight exposure.
    """)

    ov1, ov2, ov3, ov4 = st.columns(4)
    with ov1:
        st.metric("TP1 Target", f"{_tp1_r:.1f}R", "50% position")
    with ov2:
        st.metric("TP2 Target", f"{_tp2_r:.1f}R", "50% position")
    with ov3:
        st.metric("Risk per trade", f"{_risk_pct:.1f}% equity", "volatility-adjusted")
    with ov4:
        st.metric("Top N symbols", str(_top_n), "from scanner")

    # ── Phase 0 : Pre-Market Scanner ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 0 · Pre-Market Scanner  *(4:00 AM → 9:25 AM)*")

    sc1, sc2 = st.columns([1, 1])
    with sc1:
        st.markdown(f"""
| Metric | Formula | Threshold |
|--------|---------|-----------|
| **Gap %** | `(first_pm_open − prev_close) / prev_close × 100` | ≥ **{_gap_min:.1f}%** |
| **RVOL** | `(pm_vol / 325 min) ÷ (avg_daily_vol / 390 min)` | ≥ **{_rvol_min:.1f}×** |
| **ATR %** | 14-day average true range as % of price | ≥ **{min_atr_pct:.1f}%** |
| **Top N** | Symbols ranked by Gap% × RVOL score | Keep **{_top_n}** |

Scanner fires at **9:25 AM EST**. The resulting symbol list becomes `active_symbols`
— only these tickers receive intraday signals for the rest of the day.
        """)
    with sc2:
        st.info(f"""
**Why scan pre-market?**

Most ORB breakouts with follow-through come from stocks that *already have momentum* before the bell:
- A large gap (≥ {_gap_min:.1f}%) signals a catalyst (earnings, news, upgrade).
- High RVOL (≥ {_rvol_min:.1f}×) shows institutional participation, not just retail noise.

Scanning the entire universe and picking the top **{_top_n}** avoids the "boiling the ocean" problem and keeps the bot focused on the strongest setups of the day.

*RVOL is calculated as a rate comparison — premarket volume per minute vs. the 20-day average volume per minute — so it fairly compares tickers with different intraday rhythms.*
        """)

    # ── Phase 1 : Build the Opening Range ─────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 1 · Build the Opening Range  *(9:30 AM)*")

    p1c1, p1c2 = st.columns([1, 1])
    with p1c1:
        st.markdown(f"""
| Condition | Value |
|-----------|-------|
| Market opens | **9:30 AM EST** |
| ORB window | **9:30 → 9:{30 + orb_minutes:02d} AM EST** ({orb_minutes} bars) |
| Range High | `max(high)` over ORB window |
| Range Low | `min(low)` over ORB window |
| Range Mid | `(High + Low) / 2` |
| Min bars required | **{min(orb_minutes, 13)} of {orb_minutes}** (skipped on data gap) |
| VWAP | Running `Σ(typical_price × vol) / Σ(vol)` from 9:30 AM |
        """)
    with p1c2:
        st.info(f"""
**VWAP accumulation**

VWAP (Volume Weighted Average Price) is computed live from the first bar of the session.
It acts as the market's "fair value" reference — breakouts *above* VWAP are statistically stronger for longs, and breakouts *below* are stronger for shorts.

The bot resets VWAP every morning and accumulates it bar-by-bar, so it reflects intraday price discovery correctly.
        """)

    # ── Phase 2 : Breakout Signal ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 2 · Detect the Breakout + Momentum Confirmation")

    st.markdown(f"""
The bot watches every 1-minute bar **after 9:{30 + orb_minutes:02d} AM EST**.
A signal requires **all three conditions** to be true simultaneously:

| Gate | Long | Short |
|------|------|-------|
| **Price breakout** | Bar close `>` ORB High | Bar close `<` ORB Low |
| **VWAP filter** {"✅ enabled" if _vwap_on else "⚠️ disabled"} | Close `>` VWAP | Close `<` VWAP |
| **Volume surge** | Bar volume `≥` {_surge_mult:.1f}× avg ORB volume | Bar volume `≥` {_surge_mult:.1f}× avg ORB volume |
| **Spread filter** | Bid-ask spread `≤` {max_spread_pct:.3f}% of ask | Bid-ask spread `≤` {max_spread_pct:.3f}% of ask |

> **One trade per symbol per day.** Once a signal fires, the symbol transitions `SCANNING → SUBMITTING → IN_TRADE`.
    """)

    fil1, fil2 = st.columns(2)
    with fil1:
        st.markdown(f"""
**📐 VWAP Filter** {"(ON)" if _vwap_on else "(OFF)"}

Ensures the stock's intraday momentum is aligned with the breakout direction.
A close above ORB High but *below* VWAP is a weak breakout — the "average" buyer is still underwater and will sell into any rally.

`Long signal requires:  close > VWAP`
`Short signal requires: close < VWAP`
        """)
    with fil2:
        st.markdown(f"""
**📊 Volume Surge Filter**

Genuine breakouts expand volume.
A close above the range on low volume is likely a false breakout that will snap back.

`Bar volume ≥ {_surge_mult:.1f} × avg_orb_volume`

`avg_orb_volume` = average per-bar volume across the {orb_minutes}-minute ORB window.
        """)

    # ── Phase 3 : Scale-Out Execution ────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 3 · Two-Leg Scale-Out Execution")

    ex1, ex2 = st.columns(2)
    with ex1:
        st.markdown("#### 🟢 Long — Scale-Out Levels")
        st.markdown(f"""
```
Entry      ≈ {entry_long:.2f}  (close above ORB High)
Risk       = Entry − SL = {entry_long - sl_l:.2f}  (midpoint stop)
SL         = {sl_l:.2f}

Leg 1 (50% qty)
  TP1      = Entry + {_tp1_r:.1f}R = {tp1_l:.2f}
  After TP1 fills → SL moves to break-even ({entry_long:.2f})

Leg 2 (50% qty)
  TP2      = Entry + {_tp2_r:.1f}R = {tp2_l:.2f}
  SL       = break-even after Leg 1 fills

Worst case  → both legs stopped out at SL {sl_l:.2f}
After TP1   → Leg 2 rides risk-free to {tp2_l:.2f}
```
        """)
    with ex2:
        st.markdown("#### 🔴 Short — Scale-Out Levels")
        st.markdown(f"""
```
Entry      ≈ {entry_short:.2f}  (close below ORB Low)
Risk       = SL − Entry = {sl_s - entry_short:.2f}  (midpoint stop)
SL         = {sl_s:.2f}

Leg 1 (50% qty)
  TP1      = Entry − {_tp1_r:.1f}R = {tp1_s:.2f}
  After TP1 fills → SL moves to break-even ({entry_short:.2f})

Leg 2 (50% qty)
  TP2      = Entry − {_tp2_r:.1f}R = {tp2_s:.2f}
  SL       = break-even after Leg 1 fills

Worst case  → both legs stopped out at SL {sl_s:.2f}
After TP1   → Leg 2 rides risk-free to {tp2_s:.2f}
```
        """)

    st.info("""
**Implementation detail:** Two separate bracket orders are submitted simultaneously — one for each leg with its own TP and the same initial SL.
After Leg 1's TP fills, the bot cancels Leg 2's original SL and replaces it with a stop at the entry price (break-even).
Alpaca's OCO mechanism auto-cancels the paired SL when TP fills, so no orphan orders are left.
    """)

    # ── Visual diagram ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Visual Setup Diagram — Long Entry")

    fig_diag = go.Figure()

    orb_high_d = 100.00
    orb_low_d  = 97.50
    orb_mid_d  = (orb_high_d + orb_low_d) / 2
    orb_span_d = orb_high_d - orb_low_d
    e_l        = orb_high_d + 0.20
    _risk      = e_l - orb_mid_d
    _tp1_px    = round(e_l + _risk * _tp1_r, 2)
    _tp2_px    = round(e_l + _risk * _tp2_r, 2)
    _sl_px     = round(orb_mid_d, 2)

    # ORB box
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=orb_low_d, y1=orb_high_d,
                       fillcolor="rgba(255,214,0,0.08)",
                       line=dict(color="rgba(255,214,0,0.5)", width=1))
    # Profit zone (entry → TP2)
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=e_l, y1=_tp2_px,
                       fillcolor="rgba(0,230,118,0.05)", line=dict(width=0))
    # Risk zone (SL → entry)
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=_sl_px, y1=e_l,
                       fillcolor="rgba(239,83,80,0.05)", line=dict(width=0))

    for y, label, color, dash in [
        (orb_high_d, f"ORB High  {orb_high_d:.2f}",    "#FFD700", "dot"),
        (orb_low_d,  f"ORB Low   {orb_low_d:.2f}",     "#FFA000", "dot"),
        (orb_mid_d,  f"ORB Mid   {orb_mid_d:.2f}",     "#FFCC44", "dot"),
        (_tp2_px,    f"TP2 ({_tp2_r:.1f}R)  {_tp2_px:.2f}",   "#00e676", "dash"),
        (_tp1_px,    f"TP1 ({_tp1_r:.1f}R)  {_tp1_px:.2f}",   "#69f0ae", "dash"),
        (e_l,        f"Entry / Break-even  {e_l:.2f}", "#82b1ff", "solid"),
        (_sl_px,     f"Initial SL  {_sl_px:.2f}",      "#ef5350", "dash"),
    ]:
        fig_diag.add_shape(type="line", x0=0, x1=1, y0=y, y1=y,
                           line=dict(color=color, dash=dash, width=1.5))
        fig_diag.add_annotation(x=1.01, y=y, text=label, showarrow=False,
                                xanchor="left", font=dict(size=11, color=color))

    # Entry arrow
    fig_diag.add_annotation(x=0.5, y=e_l, ax=0.5, ay=orb_high_d + 0.05,
                             text=f"▲ LONG entry ~{e_l:.2f}", showarrow=True,
                             arrowhead=2, arrowcolor="#82b1ff",
                             font=dict(color="#82b1ff", size=12))
    # TP1 label
    fig_diag.add_annotation(x=0.25, y=_tp1_px,
                             text=f"← 50% exits here ({_tp1_r:.1f}R) → SL moves to B/E",
                             showarrow=False, font=dict(color="#69f0ae", size=10))
    # TP2 label
    fig_diag.add_annotation(x=0.25, y=_tp2_px,
                             text=f"← remaining 50% exits here ({_tp2_r:.1f}R) risk-free",
                             showarrow=False, font=dict(color="#00e676", size=10))

    fig_diag.update_layout(
        height=500, margin=dict(l=10, r=200, t=20, b=20),
        xaxis=dict(visible=False, range=[0, 1]),
        yaxis=dict(gridcolor="#1e2130", tickformat="$.2f"),
        showlegend=False,
    )
    _dark_layout(fig_diag)
    st.plotly_chart(fig_diag, use_container_width=True)

    # ── Phase 4 : Position Sizing ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 4 · Volatility-Adjusted Position Sizing")

    sz1, sz2 = st.columns([1, 1])
    with sz1:
        st.markdown(f"""
```
risk_dollar   = account_equity × {_risk_pct:.1f}%
risk_per_share = entry_price − sl_price
total_qty      = floor(risk_dollar ÷ risk_per_share)
                 (minimum 2 shares)
half_qty       = total_qty ÷ 2          → Leg 1
remaining_qty  = total_qty − half_qty   → Leg 2
```

This means **every trade risks the same dollar amount** regardless of entry price.
A high-volatility stock with a wide SL gets fewer shares.
A tight-range stock gets more shares.
        """)
    with sz2:
        st.markdown(f"#### Sizing examples at {_risk_pct:.1f}% risk, $100,000 equity")
        _eq = 100_000
        _rd = _eq * _risk_pct / 100
        ex_rows = []
        for entry_px, sl_distance in [(50, 1.0), (100, 1.5), (200, 2.5), (500, 5.0)]:
            rps = sl_distance
            qty = max(2, int(_rd / rps))
            ex_rows.append({
                "Entry ($)": entry_px,
                "Risk/share ($)": round(rps, 2),
                "Total Qty": qty,
                "Max Loss ($)": round(qty * rps, 2),
            })
        st.dataframe(
            pd.DataFrame(ex_rows), use_container_width=True, hide_index=True,
            column_config={
                "Entry ($)":       st.column_config.NumberColumn(format="$%.0f"),
                "Risk/share ($)":  st.column_config.NumberColumn(format="$%.2f"),
                "Total Qty":       st.column_config.NumberColumn(),
                "Max Loss ($)":    st.column_config.NumberColumn(format="$%.2f"),
            },
        )

    # ── Phase 5 : Risk Management ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 5 · Risk Management & Safety")

    rm1, rm2, rm3 = st.columns(3)
    with rm1:
        st.markdown(f"""
**🛑 Kill Switch**

Checked every **60 seconds**.

If daily loss ≥ **{cfg.get('max_daily_loss_pct', 2.0):.1f}%** of starting equity:
- All positions flattened immediately
- All open orders cancelled
- Bot stops taking new signals for the rest of the day
        """)
    with rm2:
        st.markdown("""
**⏰ Auto-Flatten**

Every day at **3:55 PM EST** (5 min before close):
- All open positions closed at market
- All pending orders cancelled

Prevents overnight exposure regardless of whether TP/SL was hit.
        """)
    with rm3:
        st.markdown("""
**📦 Broker-Managed Exits**

Two bracket orders (one per leg) are submitted atomically to Alpaca.
Each has its own TP and the shared initial SL.

If the bot crashes after submission, Alpaca's OCO mechanism still manages all exits.
**You are never left with a naked position.**
        """)

    # ── Analyst Filters ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Analyst Filters")
    st.caption("Applied in addition to the momentum confirmation before any order is sent.")

    af1, af2, af3 = st.columns(3)
    with af1:
        st.markdown(f"""
**📊 Volume Filter**

ORB window must accumulate
≥ **{min_orb_volume:,} shares**
in the first **{orb_minutes} minutes**.

*Ensures you can exit without
moving the market.*
        """)
    with af2:
        st.markdown(f"""
**📈 ATR Filter**

14-day Average True Range
must be ≥ **{min_atr_pct:.1f}%** of price.

*Filters out low-volatility stocks
that can't reach even TP1.*
        """)
    with af3:
        st.markdown(f"""
**⚖️ Spread Filter**

Bid-ask spread must be
≤ **{max_spread_pct:.3f}%** of ask price.

*Eliminates hidden entry/exit
cost on thinly-traded names.*
        """)
