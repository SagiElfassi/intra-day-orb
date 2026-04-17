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
    """Mini chart for a single backtest trade showing the ORB box + entry/exit."""
    fig = go.Figure()
    if day_bars.empty:
        _dark_layout(fig)
        return fig

    day_bars = trading_hours(day_bars)

    fig.add_trace(go.Candlestick(
        x=day_bars["timestamp"],
        open=day_bars["open"], high=day_bars["high"],
        low=day_bars["low"],   close=day_bars["close"],
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
        name="Price",
    ))

    x0, x1 = day_bars["timestamp"].iloc[0], day_bars["timestamp"].iloc[-1]

    # ORB box
    rh, rl = trade_row["range_high"], trade_row["range_low"]
    fig.add_shape(type="rect", x0=x0, x1=x1, y0=rl, y1=rh,
                  fillcolor="rgba(255,214,0,.07)",
                  line=dict(color="rgba(255,214,0,.5)", width=1))

    # 3:55 PM flatten line
    entry_ts = pd.to_datetime(trade_row["entry_time"])
    flatten_ts = entry_ts.normalize().replace(hour=15, minute=55)
    fig.add_vline(x=flatten_ts.timestamp() * 1000,
                  line_dash="dot", line_color="#FF6F00", line_width=1.5,
                  annotation_text="3:55 Flatten", annotation_position="top right",
                  annotation_font_color="#FF6F00")

    # TP / SL / Entry
    exit_reason = trade_row.get("exit_reason", "")
    for level, label, color in [
        (trade_row["take_profit"], f"TP  {trade_row['take_profit']:.2f}", "#00e676"),
        (trade_row["stop_loss"],   f"SL  {trade_row['stop_loss']:.2f}",   "#ef5350"),
        (trade_row["entry_price"], f"Entry  {trade_row['entry_price']:.2f}", "#82b1ff"),
    ]:
        fig.add_hline(y=level, line_dash="dash", line_color=color,
                      annotation_text=label, annotation_position="right")

    # Exit marker
    if pd.notna(trade_row.get("exit_time")) and pd.notna(trade_row.get("exit_price")):
        try:
            exit_ts = pd.to_datetime(trade_row["exit_time"])
            exit_color = "#00e676" if exit_reason == "TP" else ("#ef5350" if exit_reason == "SL" else "#FF6F00")
            fig.add_trace(go.Scatter(
                x=[exit_ts], y=[trade_row["exit_price"]],
                mode="markers",
                marker=dict(symbol="x", size=12, color=exit_color, line_width=2),
                name=f"Exit ({exit_reason})",
            ))
        except Exception:
            pass

    fig.update_layout(
        xaxis_rangeslider_visible=False, height=380,
        margin=dict(l=0, r=90, t=10, b=0),
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

    if st.button("💾 Save Config", use_container_width=True):
        new_cfg = dict(
            symbols=symbols, orb_minutes=orb_minutes,
            risk_ratio=risk_ratio, position_size_usd=float(pos_size),
            stop_loss_type=sl_type, alpaca_paper=alpaca_paper,
            min_orb_volume=min_orb_volume, min_atr_pct=min_atr_pct,
            max_spread_pct=max_spread_pct,
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

    bc1, bc2, bc3 = st.columns(3)
    with bc1:
        bt_symbol    = st.text_input("Symbol", value=symbols[0] if symbols else "SPY").upper()
        bt_orb       = st.radio("ORB Timeframe ", [5, 15, 30],
                                index=[5, 15, 30].index(orb_minutes),
                                format_func=lambda x: f"{x} min", horizontal=True,
                                key="bt_orb")
    with bc2:
        bt_start = st.date_input("Start Date", value=date.today() - timedelta(days=90), key="bt_start")
        bt_end   = st.date_input("End Date",   value=date.today() - timedelta(days=1),  key="bt_end")
    with bc3:
        bt_rr      = st.slider("Risk:Reward", 1.0, 5.0, risk_ratio, 0.5, key="bt_rr")
        bt_sl_type = st.radio("Stop-Loss",  ["midpoint", "hard"],
                              index=0 if sl_type == "midpoint" else 1,
                              horizontal=True, key="bt_sl")
        bt_pos     = st.number_input("Position Size (USD)", 100, 100000,
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
                with st.spinner(f"Fetching {bt_symbol} bars {bt_start} → {bt_end}…"):
                    engine = BacktestEngine(api_key, api_secret)
                    result = engine.run_backtest(
                        symbol           = bt_symbol,
                        start_date       = bt_start,
                        end_date         = bt_end,
                        orb_minutes      = bt_orb,
                        risk_ratio       = bt_rr,
                        stop_loss_type   = bt_sl_type,
                        position_size_usd= float(bt_pos),
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

        # Stats row
        s1,s2,s3,s4,s5,s6 = st.columns(6)
        s1.metric("Total Trades",  s.total_trades)
        s2.metric("Win Rate",      f"{s.win_rate}%")
        s3.metric("Total PnL",     f"${s.total_pnl:+.2f}")
        s4.metric("Profit Factor", f"{s.profit_factor:.2f}" if s.profit_factor != float("inf") else "∞")
        s5.metric("Max Drawdown",  f"-${s.max_drawdown:.2f}")
        s6.metric("Avg R",         f"{s.avg_r_multiple:.2f}R")

        sc1, sc2 = st.columns(2)
        sc1.metric("Avg Win",    f"${s.avg_win:.2f}")
        sc2.metric("Avg Loss",   f"${s.avg_loss:.2f}")

        # Charts
        trades_df_bt = pd.DataFrame([
            dict(date=t.date, symbol=t.symbol, side=t.side,
                 entry_price=t.entry_price, take_profit=t.take_profit,
                 stop_loss=t.stop_loss, range_high=t.range_high,
                 range_low=t.range_low, exit_price=t.exit_price,
                 exit_reason=t.exit_reason, qty=t.qty,
                 pnl=t.pnl, r_multiple=t.r_multiple,
                 entry_time=t.entry_time, exit_time=t.exit_time)
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

        display = trades_df_bt[[
            "date","side","entry_price","take_profit","stop_loss",
            "exit_price","exit_reason","qty","pnl","r_multiple"
        ]].copy()

        # Human-readable columns
        display["#"]       = range(1, len(display) + 1)
        display["Result"]  = display["exit_reason"].map({"TP": "✅ TP", "SL": "❌ SL", "FLAT": "🕐 3:55 Flat"})
        display["Side"]    = display["side"].map({"LONG": "▲ LONG", "SHORT": "▼ SHORT"})
        display["PnL"]     = display["pnl"].map(lambda x: f"+${x:.2f}" if x >= 0 else f"-${abs(x):.2f}")
        display["R"]       = display["r_multiple"].map(lambda x: f"+{x:.2f}R" if x >= 0 else f"{x:.2f}R")

        st.dataframe(
            display.rename(columns={
                "date": "Date", "entry_price": "Entry",
                "take_profit": "TP", "stop_loss": "SL",
                "exit_price": "Exit", "qty": "Qty",
            })[[
                "#", "Date", "Side", "Entry", "TP", "SL",
                "Exit", "Result", "Qty", "PnL", "R"
            ]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "#":      st.column_config.NumberColumn(width="small"),
                "Date":   st.column_config.TextColumn(width="small"),
                "Side":   st.column_config.TextColumn(width="small"),
                "Entry":  st.column_config.NumberColumn(format="$%.2f", width="small"),
                "TP":     st.column_config.NumberColumn(format="$%.2f", width="small"),
                "SL":     st.column_config.NumberColumn(format="$%.2f", width="small"),
                "Exit":   st.column_config.NumberColumn(format="$%.2f", width="small"),
                "Result": st.column_config.TextColumn(width="small"),
                "Qty":    st.column_config.NumberColumn(width="small"),
                "PnL":    st.column_config.TextColumn(width="small"),
                "R":      st.column_config.TextColumn(width="small"),
            },
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

    # Use current sidebar values so the formulas are live
    rng_example   = 2.50   # illustrative ORB range for the diagram
    entry_long    = 100.00
    entry_short   = 97.50
    tp_long       = round(entry_long  + rng_example * risk_ratio, 2)
    sl_long_mid   = round(entry_long  - rng_example / 2, 2)
    sl_long_hard  = round(entry_long  - rng_example, 2)
    tp_short      = round(entry_short - rng_example * risk_ratio, 2)
    sl_short_mid  = round(entry_short + rng_example / 2, 2)
    sl_short_hard = round(entry_short + rng_example, 2)

    st.subheader("Opening Range Breakout — Entry Conditions")
    st.caption(f"All formulas reflect your current sidebar settings: "
               f"**{orb_minutes}-min ORB · {risk_ratio}R · {sl_type} stop · ${pos_size} per trade**")

    # ── Analyst Filters ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 🔍 Analyst Filters")
    st.caption("Applied before a trade is taken. Configured in the sidebar.")

    af1, af2, af3 = st.columns(3)
    with af1:
        st.markdown(f"""
**📊 Volume Filter**

ORB window must accumulate
≥ **{min_orb_volume:,} shares**
in the first **{orb_minutes} minutes**.

*Ensures you can exit a ${pos_size:,}
position without moving the market.*
        """)
    with af2:
        st.markdown(f"""
**📈 ATR Filter**

14-day Average True Range
must be ≥ **{min_atr_pct:.1f}%** of price.

*Filters out "sleepy" stocks
that can't reach the {risk_ratio}:1 TP target.*
        """)
    with af3:
        st.markdown(f"""
**⚖️ Spread Filter**

Bid-ask spread must be
≤ **{max_spread_pct:.3f}%** of ask price.

*Eliminates the hidden entry/exit
cost that kills small accounts.*
        """)

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 1 · Build the Opening Range")

    p1c1, p1c2 = st.columns([1, 1])
    with p1c1:
        st.markdown(f"""
| Condition | Value |
|-----------|-------|
| Market opens | **9:30 AM EST** |
| ORB window | **9:30 → 9:{30 + orb_minutes:02d} AM EST** ({orb_minutes} bars) |
| Range High | `max(high)` of all {orb_minutes} bars |
| Range Low | `min(low)` of all {orb_minutes} bars |
| Range Mid | `(High + Low) / 2` |
| Minimum bars required | **{min(orb_minutes, 13)} of {orb_minutes}** (skipped if data gap) |
        """)
    with p1c2:
        st.info(f"""
**Why validate bar count?**

The first minutes of trading are chaotic — your broker may deliver bars late or skip them entirely during high-volatility opens (earnings, macro events).

If fewer than **{min(orb_minutes, 13)} bars** arrive in the {orb_minutes}-minute window, the range is marked **INVALID** and no trade is taken that day. This protects against setting a false range from incomplete data.
        """)

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 2 · Detect the Breakout Signal")

    st.markdown(f"""
The bot watches every 1-minute bar **after 9:{30 + orb_minutes:02d} AM EST**.
A signal fires on the **close** of the bar — not the high/low — to avoid wicks.

| Signal | Condition | Direction |
|--------|-----------|-----------|
| 🟢 **LONG** | Bar close `>` ORB High | Buy |
| 🔴 **SHORT** | Bar close `<` ORB Low | Sell short |
| ⬜ No trade | Close inside range | Wait for next bar |

> **One trade per symbol per day.** Once a signal fires, the symbol is marked `IN_TRADE` and no further signals are checked.
    """)

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 3 · Bracket Order Calculation")

    bc1, bc2 = st.columns(2)

    with bc1:
        st.markdown("#### 🟢 Long Entry")
        sl_val_long = sl_long_mid if sl_type == "midpoint" else sl_long_hard
        st.markdown(f"""
```
ORB Range   = High − Low
            = {entry_long:.2f} − {entry_long - rng_example:.2f} = {rng_example:.2f}

Entry       = close price at breakout bar
            ≈ {entry_long:.2f}  (above ORB High)

Take Profit = Entry + (Range × Ratio)
            = {entry_long:.2f} + ({rng_example:.2f} × {risk_ratio})
            = {tp_long:.2f}

Stop Loss   = {"Range Midpoint" if sl_type == "midpoint" else "ORB Low (hard)"}
            = {sl_val_long:.2f}

Risk        = Entry − SL = {entry_long - sl_val_long:.2f}
Reward      = TP − Entry = {tp_long - entry_long:.2f}
R:R         = 1 : {(tp_long - entry_long) / (entry_long - sl_val_long):.1f}
```
        """)

    with bc2:
        st.markdown("#### 🔴 Short Entry")
        sl_val_short = sl_short_mid if sl_type == "midpoint" else sl_short_hard
        st.markdown(f"""
```
ORB Range   = High − Low
            = {entry_short + rng_example:.2f} − {entry_short:.2f} = {rng_example:.2f}

Entry       = close price at breakout bar
            ≈ {entry_short:.2f}  (below ORB Low)

Take Profit = Entry − (Range × Ratio)
            = {entry_short:.2f} − ({rng_example:.2f} × {risk_ratio})
            = {tp_short:.2f}

Stop Loss   = {"Range Midpoint" if sl_type == "midpoint" else "ORB High (hard)"}
            = {sl_val_short:.2f}

Risk        = SL − Entry = {sl_val_short - entry_short:.2f}
Reward      = Entry − TP = {entry_short - tp_short:.2f}
R:R         = 1 : {(entry_short - tp_short) / (sl_val_short - entry_short):.1f}
```
        """)

    # ── Visual diagram ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Visual Setup Diagram")

    fig_diag = go.Figure()

    orb_high  = 100.00
    orb_low   = 97.50
    orb_mid   = (orb_high + orb_low) / 2
    orb_span  = orb_high - orb_low
    e_long    = orb_high + 0.20
    e_short   = orb_low  - 0.20
    tp_l      = round(e_long  + orb_span * risk_ratio, 2)
    sl_l      = round(orb_mid if sl_type == "midpoint" else orb_low,  2)
    tp_s      = round(e_short - orb_span * risk_ratio, 2)
    sl_s      = round(orb_mid if sl_type == "midpoint" else orb_high, 2)

    x = [0, 1]

    # ORB range box
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=orb_low, y1=orb_high,
                       fillcolor="rgba(255,214,0,0.08)",
                       line=dict(color="rgba(255,214,0,0.5)", width=1))

    for y, label, color, dash in [
        (orb_high, f"ORB High  {orb_high:.2f}",  "#FFD700", "dot"),
        (orb_low,  f"ORB Low   {orb_low:.2f}",   "#FFA000", "dot"),
        (orb_mid,  f"ORB Mid   {orb_mid:.2f}",   "#FFCC44", "dot"),
        (tp_l,     f"TP (Long) {tp_l:.2f}",      "#00e676", "dash"),
        (sl_l,     f"SL (Long) {sl_l:.2f}",      "#ef5350", "dash"),
        (tp_s,     f"TP (Short) {tp_s:.2f}",     "#69f0ae", "dashdot"),
        (sl_s,     f"SL (Short) {sl_s:.2f}",     "#ff5252", "dashdot"),
    ]:
        fig_diag.add_shape(type="line", x0=0, x1=1, y0=y, y1=y,
                           line=dict(color=color, dash=dash, width=1.5))
        fig_diag.add_annotation(x=1.01, y=y, text=label, showarrow=False,
                                xanchor="left", font=dict(size=11, color=color))

    # Long entry arrow
    fig_diag.add_annotation(x=0.5, y=e_long, ax=0.5, ay=orb_high + 0.05,
                             text=f"▲ LONG entry ~{e_long:.2f}", showarrow=True,
                             arrowhead=2, arrowcolor="#00e676",
                             font=dict(color="#00e676", size=12))

    # Short entry arrow
    fig_diag.add_annotation(x=0.5, y=e_short, ax=0.5, ay=orb_low - 0.05,
                             text=f"▼ SHORT entry ~{e_short:.2f}", showarrow=True,
                             arrowhead=2, arrowcolor="#ef5350",
                             font=dict(color="#ef5350", size=12))

    # TP fill zones
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=e_long, y1=tp_l,
                       fillcolor="rgba(0,230,118,0.06)", line=dict(width=0))
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=sl_l, y1=e_long,
                       fillcolor="rgba(239,83,80,0.06)", line=dict(width=0))

    fig_diag.update_layout(
        height=480, margin=dict(l=10, r=160, t=20, b=20),
        xaxis=dict(visible=False, range=[0, 1]),
        yaxis=dict(gridcolor="#1e2130", tickformat="$.2f"),
        showlegend=False,
    )
    _dark_layout(fig_diag)
    st.plotly_chart(fig_diag, use_container_width=True)

    # ── Phase 4 : Risk Management ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 4 · Risk Management & Safety")

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
        st.markdown(f"""
**📦 Bracket Orders**

Entry + TP + SL are sent as a **single atomic instruction** to Alpaca.

If your bot crashes after submission, the broker still manages the exits. You are never left with a naked position.

Position size: **${pos_size:,}** per trade
Qty = `floor({pos_size} ÷ entry_price)`
        """)

    # ── Position Sizing ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Position Sizing Examples")

    examples = [50, 100, 200, 500]
    sizing_data = {
        "Entry Price ($)": examples,
        "Qty (shares)": [max(1, int(pos_size // p)) for p in examples],
        "Actual Exposure ($)": [max(1, int(pos_size // p)) * p for p in examples],
    }
    st.dataframe(
        pd.DataFrame(sizing_data),
        use_container_width=False,
        hide_index=True,
        column_config={
            "Entry Price ($)":    st.column_config.NumberColumn(format="$%.0f"),
            "Qty (shares)":       st.column_config.NumberColumn(),
            "Actual Exposure ($)":st.column_config.NumberColumn(format="$%.0f"),
        },
    )
