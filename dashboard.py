"""
ORB Dashboard — Streamlit + Plotly
Four tabs:
  • Live       — real-time chart with ORB box, TP/SL bands, live trade card, R:R
  • Backtest   — historical simulation using the exact same rules as the live bot
  • Scanner    — multi-symbol sweep ranked by performance metrics
  • Conditions — complete strategy rule-book with live formula values
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

import subprocess
import sys

DB_PATH      = os.environ.get("DB_PATH", "orb_trades.db")
CONFIG_PATH  = "live_config.json"
PID_FILE     = "bot.pid"
BOT_LOG      = "bot_output.log"
EST          = ZoneInfo("America/New_York")
REFRESH_SEC  = 10


def restart_bot() -> str:
    """Kill existing bot (via PID file) and start a fresh one. Returns status message."""
    project_dir = os.path.dirname(os.path.abspath(__file__))

    # Kill old process
    killed_pid = None
    pid_path = os.path.join(project_dir, PID_FILE)
    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
            subprocess.run(["taskkill", "/F", "/PID", str(old_pid)],
                           capture_output=True, timeout=5)
            killed_pid = old_pid
        except Exception:
            pass

    import time
    time.sleep(1)

    # Start new process — use pythonw.exe (no console window, fully detached from any terminal)
    log_path = os.path.join(project_dir, BOT_LOG)
    pythonw = sys.executable.replace("python.exe", "pythonw.exe")
    if not os.path.exists(pythonw):
        pythonw = sys.executable  # fallback
    try:
        proc = subprocess.Popen(
            [pythonw, os.path.join(project_dir, "bot.py")],
            stdout=open(log_path, "w"),
            stderr=subprocess.STDOUT,
            cwd=project_dir,
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        msg = f"Bot restarted (PID {proc.pid})"
        if killed_pid:
            msg += f" — killed old PID {killed_pid}"
        return msg
    except Exception as e:
        return f"Failed to restart bot: {e}"


def stop_bot() -> str:
    """Kill bot process via PID file without restarting."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    pid_path = os.path.join(project_dir, PID_FILE)
    if not os.path.exists(pid_path):
        return "Bot is not running (no PID file)."
    try:
        old_pid = int(open(pid_path).read().strip())
        result = subprocess.run(["taskkill", "/F", "/PID", str(old_pid)],
                                capture_output=True, timeout=5)
        if result.returncode == 0:
            try:
                os.remove(pid_path)
            except OSError:
                pass
            return f"Bot stopped (PID {old_pid})."
        return f"taskkill failed (PID {old_pid}): {result.stderr.decode()}"
    except Exception as e:
        return f"Failed to stop bot: {e}"


def start_bot() -> str:
    """Start bot without saving config (assumes live_config.json is current)."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    pid_path = os.path.join(project_dir, PID_FILE)
    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
            chk = subprocess.run(["tasklist", "/FI", f"PID eq {old_pid}"],
                                 capture_output=True, timeout=5)
            if str(old_pid) in chk.stdout.decode():
                return f"Bot is already running (PID {old_pid})."
        except Exception:
            pass
    log_path = os.path.join(project_dir, BOT_LOG)
    pythonw = sys.executable.replace("python.exe", "pythonw.exe")
    if not os.path.exists(pythonw):
        pythonw = sys.executable
    try:
        proc = subprocess.Popen(
            [pythonw, os.path.join(project_dir, "bot.py")],
            stdout=open(log_path, "w"),
            stderr=subprocess.STDOUT,
            cwd=project_dir,
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        return f"Bot started (PID {proc.pid})."
    except Exception as e:
        return f"Failed to start bot: {e}"


def bot_status() -> tuple[str, str]:
    """Return (label, color) indicating whether bot process is alive."""
    project_dir = os.path.dirname(os.path.abspath(__file__))
    pid_path = os.path.join(project_dir, PID_FILE)
    if not os.path.exists(pid_path):
        return "🔴 Stopped", "red"
    try:
        pid = int(open(pid_path).read().strip())
        chk = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                             capture_output=True, timeout=5)
        if str(pid) in chk.stdout.decode():
            return f"🟢 Running ({pid})", "green"
        return "🔴 Stopped", "red"
    except Exception:
        return "🔴 Stopped", "red"


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ORB Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
/* hide the default sidebar toggle arrow */
[data-testid="collapsedControl"] { display: none; }

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

/* settings bar */
.settings-bar {
    background:#12151e;
    border:1px solid #2d3348;
    border-radius:8px;
    padding:10px 16px 4px 16px;
    margin-bottom:12px;
}
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
        stop_loss_type="midpoint",
        alpaca_paper=True,
        min_orb_volume=1_000_000,
        max_position_usd=0,
        min_atr_pct=2.0,
        max_spread_pct=0.05,
        gap_min_pct=1.5,
        rvol_min=2.0,
        scanner_top_n=5,
        vwap_filter=True,
        volume_surge_mult=1.5,
        risk_pct_equity=1.0,
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
    start_date, end_date, starting_equity: float, api_key: str, api_secret: str
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
        risk_dollar = starting_equity * 0.01  # 1% of equity as proxy position
        qty    = max(1, int(risk_dollar // entry))
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


def trading_hours(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return bars
    t = bars["timestamp"]
    mask = (
        ((t.dt.hour == 9)  & (t.dt.minute >= 30)) |
        ((t.dt.hour >= 10) & (t.dt.hour < 16))
    )
    return bars[mask].copy()


def apply_rangebreaks(fig: go.Figure, rows: int = 1):
    breaks = [
        dict(bounds=[16, 9.5], pattern="hour"),
        dict(bounds=["sat", "mon"]),
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

    fig.add_trace(go.Candlestick(
        x=bars["timestamp"], open=bars["open"], high=bars["high"],
        low=bars["low"],     close=bars["close"], name=symbol,
        increasing_line_color="#26a69a", decreasing_line_color="#ef5350",
    ), row=1, col=1)

    v_colors = ["#26a69a" if c >= o else "#ef5350"
                for c, o in zip(bars["close"], bars["open"])]
    fig.add_trace(go.Bar(
        x=bars["timestamp"], y=bars["volume"],
        marker_color=v_colors, showlegend=False, name="Vol",
    ), row=2, col=1)

    x0 = bars["timestamp"].iloc[0]
    x1 = bars["timestamp"].iloc[-1]

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

    if trade is not None:
        entry = trade.get("entry_price")
        tp    = trade.get("take_profit")
        sl    = trade.get("stop_loss")
        side  = trade.get("side", "")

        if pd.notna(entry) and entry:
            fig.add_hline(y=entry, line_dash="solid",
                          line_color="#82b1ff", line_width=1.5,
                          annotation_text=f"Entry  {entry:.2f}",
                          annotation_position="right", row=1, col=1)

        if pd.notna(tp) and tp:
            fig.add_hline(y=tp, line_dash="dash", line_color="#00e676", line_width=2,
                          annotation_text=f"TP  {tp:.2f}", annotation_position="right",
                          row=1, col=1)
            if pd.notna(entry) and entry:
                fig.add_shape(type="rect", x0=x0, x1=x1,
                              y0=min(entry, tp), y1=max(entry, tp),
                              fillcolor="rgba(0,230,118,.07)",
                              line=dict(width=0), layer="below", row=1, col=1)

        if pd.notna(sl) and sl:
            fig.add_hline(y=sl, line_dash="dash", line_color="#ef5350", line_width=2,
                          annotation_text=f"SL  {sl:.2f}", annotation_position="right",
                          row=1, col=1)
            if pd.notna(entry) and entry:
                fig.add_shape(type="rect", x0=x0, x1=x1,
                              y0=min(entry, sl), y1=max(entry, sl),
                              fillcolor="rgba(239,83,80,.07)",
                              line=dict(width=0), layer="below", row=1, col=1)

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
    fig = go.Figure()
    if day_bars.empty:
        _dark_layout(fig)
        return fig

    day_bars = trading_hours(day_bars).copy()

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

    fig.add_trace(go.Scatter(
        x=day_bars["timestamp"], y=day_bars["_vwap"],
        mode="lines", line=dict(color="#FF6F00", width=1.5, dash="dot"),
        name="VWAP",
        hovertemplate="%{x|%H:%M}<br>VWAP: $%{y:.2f}<extra></extra>",
    ))

    x0, x1 = day_bars["timestamp"].iloc[0], day_bars["timestamp"].iloc[-1]

    rh, rl = trade_row["range_high"], trade_row["range_low"]
    fig.add_shape(type="rect", x0=x0, x1=x1, y0=rl, y1=rh,
                  fillcolor="rgba(255,214,0,.07)",
                  line=dict(color="rgba(255,214,0,.5)", width=1))

    entry_ts   = pd.to_datetime(trade_row["entry_time"])
    flatten_ts = entry_ts.normalize().replace(hour=15, minute=55)
    fig.add_vline(x=flatten_ts.timestamp() * 1000,
                  line_dash="dot", line_color="#546e7a", line_width=1,
                  annotation_text="3:55 Flatten", annotation_position="top right",
                  annotation_font_color="#546e7a")

    tp1_px  = trade_row.get("tp1_price", 0.0) or 0.0
    tp2_px  = trade_row.get("tp2_price", 0.0) or 0.0
    entry   = trade_row["entry_price"]
    sl      = trade_row["stop_loss"]
    scale_out_trade = tp1_px > 0 and tp2_px > 0 and abs(tp1_px - tp2_px) > 0.001

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

    _exit_colors = {
        "TP": "#00e676", "TP1": "#69f0ae", "TP2": "#00e676",
        "SL": "#ef5350", "BE": "#82b1ff",  "EOD": "#FF6F00",
    }
    exit_reason = trade_row.get("exit_reason", "")
    leg1_reason = trade_row.get("leg1_exit", "")

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


def _run_preflight_test(paper: bool, symbols: list):
    """Full pre-flight check: credentials, account, market data, order flow."""
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import LimitOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockLatestQuoteRequest

    api_key, api_secret = get_alpaca_credentials(paper)
    mode_label = "📄 PAPER" if paper else "💰 LIVE"
    results = []

    def ok(msg):  results.append(("✅", msg))
    def warn(msg): results.append(("⚠️", msg))
    def err(msg):  results.append(("❌", msg))

    # 1. Credentials present
    if not api_key or not api_secret:
        err(f"API keys not found for {mode_label} mode — check your .env file")
    else:
        ok(f"API keys found for {mode_label} (key: …{api_key[-6:]})")

    # 2. Account reachable + status
    try:
        tc = TradingClient(api_key=api_key, secret_key=api_secret, paper=paper)
        acct = tc.get_account()
        if str(acct.status) != "ACTIVE":
            warn(f"Account status: {acct.status} (expected ACTIVE)")
        else:
            ok(f"Account ACTIVE · equity ${float(acct.equity):,.2f} · "
               f"buying power ${float(acct.buying_power):,.2f}")
        if getattr(acct, "trading_blocked", False):
            err("Account has trading_blocked = True — orders will be rejected")
        else:
            ok("Trading not blocked")
        if getattr(acct, "pattern_day_trader", False):
            warn("Account flagged as Pattern Day Trader (PDT) — check margin rules")
    except Exception as e:
        err(f"Cannot reach trading API: {e}")

    # 3. Market data — latest quote for each symbol
    try:
        hc = StockHistoricalDataClient(api_key=api_key, secret_key=api_secret)
        syms_to_test = symbols[:3] if symbols else ["SPY"]
        quotes = hc.get_stock_latest_quote(StockLatestQuoteRequest(symbol_or_symbols=syms_to_test))
        for sym in syms_to_test:
            q = quotes.get(sym)
            if q and float(q.ask_price) > 0:
                spread_pct = (float(q.ask_price) - float(q.bid_price)) / float(q.ask_price) * 100
                ok(f"{sym} quote: bid ${float(q.bid_price):.2f} / ask ${float(q.ask_price):.2f} "
                   f"(spread {spread_pct:.3f}%)")
            else:
                warn(f"{sym}: no live quote — market may be closed or symbol invalid")
    except Exception as e:
        warn(f"Market data check failed: {e}")

    # 4. Order submission test — place a $0.01 limit buy far below market, cancel immediately
    test_sym = symbols[0] if symbols else "SPY"
    try:
        tc2 = TradingClient(api_key=api_key, secret_key=api_secret, paper=paper)
        test_order = tc2.submit_order(LimitOrderRequest(
            symbol=test_sym,
            qty=1,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=0.01,   # $0.01 — will never fill
        ))
        tc2.cancel_order_by_id(test_order.id)
        ok(f"Order flow OK — placed & cancelled test limit order on {test_sym} "
           f"(id …{str(test_order.id)[-8:]})")
    except Exception as e:
        err(f"Order submission failed on {test_sym}: {e}")

    # 5. Bot process running?
    project_dir = os.path.dirname(os.path.abspath(__file__))
    pid_path = os.path.join(project_dir, PID_FILE)
    if os.path.exists(pid_path):
        try:
            pid = int(open(pid_path).read().strip())
            # Check process is alive (tasklist on Windows)
            r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                               capture_output=True, text=True, timeout=5)
            if str(pid) in r.stdout:
                ok(f"Bot process running (PID {pid})")
            else:
                warn(f"PID file says {pid} but process not found — bot may have crashed")
        except Exception:
            warn("Could not verify bot process status")
    else:
        warn("bot.pid not found — bot is not running")

    # ── Display results ───────────────────────────────────────────────────────
    st.markdown(f"#### 🧪 Pre-flight Check — {mode_label}")
    all_ok = all(icon == "✅" for icon, _ in results)
    for icon, msg in results:
        st.markdown(f"{icon} {msg}")
    if all_ok:
        st.success("All checks passed — bot is ready to trade.")
    elif any(icon == "❌" for icon, _ in results):
        st.error("One or more critical checks failed — fix before trading.")
    else:
        st.warning("Ready with warnings — review above before market open.")


# ══════════════════════════════════════════════════════════════════════════════
#  SETTINGS BAR  (single source of truth — all tabs read from here)
# ══════════════════════════════════════════════════════════════════════════════

cfg = load_config()

st.markdown('<div class="settings-bar">', unsafe_allow_html=True)

# ── Row 0: account strip ──────────────────────────────────────────────────────
_acct_c1, _acct_c2, _acct_c3, _acct_c4, _acct_c5, _acct_c6, _acct_c7, _acct_c8, _acct_c9, _acct_c10, _acct_c11 = st.columns([1.2, 1.2, 1.2, 1.2, 1, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9])

with _acct_c1:
    account_choice = st.radio(
        "Mode", ["📄 Paper", "💰 Live"],
        index=0 if cfg.get("alpaca_paper", True) else 1,
        horizontal=True, label_visibility="collapsed",
        key="acct_mode",
    )
    alpaca_paper = account_choice.startswith("📄")
    st.caption("📄 Paper" if alpaca_paper else "⚠️ Live Trading")

acct = fetch_account_balance(alpaca_paper)
with _acct_c2:
    st.metric("Equity",       f"${acct['equity']:,.0f}"      if acct else "—")
with _acct_c3:
    st.metric("Buying Power", f"${acct['buying_power']:,.0f}" if acct else "—")
with _acct_c4:
    st.metric("Cash",         f"${acct['cash']:,.0f}"         if acct else "—")
with _acct_c5:
    selected_date = st.date_input("Date", value=date.today(), label_visibility="collapsed")
    st.caption(f"Date: {selected_date}")
with _acct_c6:
    if st.button("🔄 Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
with _acct_c7:
    _save_now = st.button("💾 Save & Restart", use_container_width=True, type="primary")
with _acct_c8:
    _run_test = st.button("🧪 Test Bot", use_container_width=True)

_bot_status_label, _bot_status_color = bot_status()
with _acct_c9:
    st.markdown(
        f'<div style="padding:6px 0 2px;font-size:0.82em;color:{_bot_status_color};font-weight:600">'
        f'{_bot_status_label}</div>',
        unsafe_allow_html=True,
    )
    st.caption("Bot status")
with _acct_c10:
    if st.button("▶ Start", use_container_width=True):
        _msg = start_bot()
        st.toast(_msg)
        import time; time.sleep(1)
        st.rerun()
with _acct_c11:
    if st.button("⏹ Stop", use_container_width=True):
        _msg = stop_bot()
        st.toast(_msg)
        import time; time.sleep(0.5)
        st.rerun()

selected_date_str = str(selected_date)

st.divider()

# ── Row 1: trading params ─────────────────────────────────────────────────────
_r1c1, _r1c2, _r1c3, _r1c4, _r1c5, _r1c6, _r1c7, _r1c8 = st.columns([2.5, 1, 1, 1.0, 0.9, 1.0, 1.0, 1.1])

with _r1c1:
    sym_input = st.text_input(
        "Symbols (comma-separated)",
        value=", ".join(cfg["symbols"]),
        placeholder="META, TSLA, SPY",
    )
    symbols = [s.strip().upper() for s in sym_input.split(",") if s.strip()]

with _r1c2:
    orb_minutes = st.radio(
        "ORB Window",
        [5, 15, 30],
        index=[5, 15, 30].index(cfg["orb_minutes"]),
        format_func=lambda x: f"{x} min",
        horizontal=False,
    )

with _r1c3:
    sl_type = st.radio(
        "Stop-Loss",
        ["midpoint", "hard"],
        index=0 if cfg["stop_loss_type"] == "midpoint" else 1,
        horizontal=False,
    )

with _r1c4:
    risk_pct_equity = st.number_input(
        "Risk % / Trade",
        0.1, 5.0,
        float(cfg["risk_pct_equity"]),
        0.1, format="%.1f",
        help="Account equity × this % = max loss if SL is hit",
    )

with _r1c5:
    _tp_mode_default = int(cfg.get("tp_legs", 2))
    tp_legs = st.radio(
        "TP Mode",
        [2, 1],
        index=0 if _tp_mode_default == 2 else 1,
        format_func=lambda x: "2 TP" if x == 2 else "1 TP",
        horizontal=False,
        help="2 TP = scale-out (50%+50%). 1 TP = full position exits at single target.",
    )
    two_tp = (tp_legs == 2)

with _r1c6:
    if two_tp:
        tp1_r = st.number_input(
            "TP1 (R)",
            0.5, 5.0,
            float(cfg["tp1_r"]),
            0.25, format="%.2f",
            help="First target — 50% of position exits here",
        )
    else:
        tp1_r = 0.0
        st.caption("TP1")
        st.caption("— (1 TP mode)")

with _r1c7:
    _tp2_label = "TP2 (R)" if two_tp else "TP (R)"
    _tp2_help  = ("Second target — remaining 50% (risk-free after TP1)"
                  if two_tp else "Single take-profit — full position exits here")
    tp2_r = st.number_input(
        _tp2_label,
        0.5, 10.0,
        float(cfg["tp2_r"]),
        0.25, format="%.2f",
        help=_tp2_help,
    )

with _r1c8:
    max_position_usd = st.number_input(
        "Max $ / Symbol",
        0, 500_000,
        int(cfg.get("max_position_usd", 0)),
        1_000,
        help="Hard cap on notional per trade. 0 = no cap (size comes from Risk % only).",
    )

# ── Row 2: filters ────────────────────────────────────────────────────────────
_r2c1, _r2c2, _r2c3, _r2c4, _r2c5, _r2c6, _r2c7, _r2c8 = st.columns(8)

with _r2c1:
    vwap_filter = st.toggle(
        "VWAP Filter",
        value=bool(cfg.get("vwap_filter", True)),
        help="Long breakout must be above VWAP; short below",
    )

with _r2c2:
    volume_surge_mult = st.number_input(
        "Vol Surge ×",
        1.0, 10.0,
        float(cfg.get("volume_surge_mult", 1.5)),
        0.1, format="%.1f",
        help="Breakout bar volume ≥ N × avg ORB-window volume",
    )

with _r2c3:
    max_spread_pct = st.number_input(
        "Max Spread %",
        0.01, 1.0,
        float(cfg.get("max_spread_pct", 0.05)),
        0.01, format="%.3f",
        help="Live only — bid-ask spread must be ≤ this % of ask",
    )

with _r2c4:
    min_vol_k = st.number_input(
        "Min ORB Vol (K)",
        0, 20_000,
        int(round(cfg.get("min_orb_volume", 1_000_000) / 1_000)),
        100,
        help="Total shares traded in ORB window must exceed this",
    )
    min_orb_volume = min_vol_k * 1_000

with _r2c5:
    min_atr_pct = st.number_input(
        "Min ATR %",
        0.0, 10.0,
        float(cfg.get("min_atr_pct", 2.0)),
        0.5, format="%.1f",
        help="14-day ATR as % of price — filters out low-vol stocks",
    )

with _r2c6:
    gap_min_pct = st.number_input(
        "Gap Min %",
        0.0, 20.0,
        float(cfg.get("gap_min_pct", 1.5)),
        0.1, format="%.1f",
        help="Pre-market scanner: min overnight gap vs prior close",
    )

with _r2c7:
    rvol_min = st.number_input(
        "Min RVOL ×",
        0.5, 20.0,
        float(cfg.get("rvol_min", 2.0)),
        0.1, format="%.1f",
        help="Pre-market scanner: min relative volume (pm vs 20-day avg)",
    )

with _r2c8:
    scanner_top_n = st.number_input(
        "Top N",
        1, 20,
        int(cfg.get("scanner_top_n", 5)),
        1,
        help="Pre-market scanner: keep top N symbols by score each day",
    )

st.markdown('</div>', unsafe_allow_html=True)

# Handle save
if _save_now:
    new_cfg = dict(
        symbols=symbols,
        orb_minutes=orb_minutes,
        stop_loss_type=sl_type,
        alpaca_paper=alpaca_paper,
        min_orb_volume=min_orb_volume,
        min_atr_pct=min_atr_pct,
        max_spread_pct=max_spread_pct,
        gap_min_pct=gap_min_pct,
        rvol_min=rvol_min,
        scanner_top_n=int(scanner_top_n),
        vwap_filter=vwap_filter,
        volume_surge_mult=volume_surge_mult,
        risk_pct_equity=risk_pct_equity,
        tp1_r=tp1_r,
        tp2_r=tp2_r,
        tp_legs=int(tp_legs),
        max_position_usd=int(max_position_usd),
    )
    save_config(new_cfg)
    cfg = new_cfg
    _restart_msg = restart_bot()
    st.success(f"Config saved & bot restarted — {_restart_msg}")

if _run_test:
    _run_preflight_test(alpaca_paper, symbols)

# ══════════════════════════════════════════════════════════════════════════════
#  TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_live, tab_bt, tab_scanner, tab_rules = st.tabs([
    "📡  Live Trading", "🔬  Backtest", "🔭  Scanner", "📐  Strategy Rules"
])


# ── TAB 1 : LIVE ──────────────────────────────────────────────────────────────

with tab_live:
    trades_df = q_trades(selected_date_str)
    ranges_df = q_ranges(selected_date_str)
    events_df = q_events(selected_date_str)

    total  = len(trades_df)
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

    # ── Capital allocation summary ────────────────────────────────────────────
    _eq_live   = acct["equity"] if acct else 100_000.0
    _risk_each = _eq_live * risk_pct_equity / 100
    _sym_list  = symbols if symbols else ["SPY"]

    with st.expander("💰 Capital Allocation per Symbol", expanded=True):
        st.caption(
            f"Equity **${_eq_live:,.0f}** · Risk **{risk_pct_equity:.1f}% = ${_risk_each:,.0f}** per trade · "
            f"{'Max $' + f'{max_position_usd:,} notional cap' if max_position_usd > 0 else 'No notional cap (size set by risk % only)'}"
        )
        alloc_cols = st.columns(len(_sym_list))
        for _col, _sym in zip(alloc_cols, _sym_list):
            with _col:
                # Try to get the actual trade entry if one exists
                _trade_row = None
                if not trades_df.empty and _sym in trades_df["symbol"].values:
                    _tr = trades_df[trades_df["symbol"] == _sym]
                    if not _tr.empty:
                        _trade_row = _tr.iloc[-1]

                if _trade_row is not None and pd.notna(_trade_row.get("entry_price")) and pd.notna(_trade_row.get("stop_loss")):
                    _ep  = float(_trade_row["entry_price"])
                    _sl  = float(_trade_row["stop_loss"])
                    _q   = int(_trade_row.get("qty", 0))
                    _rps = abs(_ep - _sl)
                    _not = round(_ep * _q, 0)
                    _act_risk = round(_rps * _q, 0)
                    _status = _trade_row.get("status", "")
                    _icon = "🟢" if _status in ("PENDING", "OPEN") else "⚫"
                    st.markdown(f"**{_icon} {_sym}** *(actual)*")
                    st.markdown(f"- Entry `${_ep:.2f}` · SL `${_sl:.2f}`")
                    st.markdown(f"- Qty **{_q}** · Notional **${_not:,.0f}**")
                    st.markdown(f"- Risk `${_act_risk:,.0f}` (`${_rps:.2f}`/sh)")
                else:
                    # Estimate from ORB range if available, else use risk $ directly
                    _orb = (ranges_df[ranges_df["symbol"] == _sym].iloc[0]
                            if not ranges_df.empty and _sym in ranges_df["symbol"].values else None)
                    if _orb is not None:
                        _rh2 = float(_orb["range_high"])
                        _rm2 = float(_orb["range_mid"])
                        _rps2 = max(round(_rh2 - _rm2, 2) if sl_type == "midpoint" else round(_rh2 - float(_orb["range_low"]), 2), 0.01)
                        _q2   = max(2, int(_risk_each / _rps2))
                        if max_position_usd > 0:
                            _q2 = min(_q2, max(2, int(max_position_usd / _rh2)))
                        _not2 = round(_q2 * _rh2, 0)
                        st.markdown(f"**⏳ {_sym}** *(est. from ORB)*")
                        st.markdown(f"- Risk/sh `${_rps2:.2f}` · Qty ≈ **{_q2}**")
                        st.markdown(f"- Notional ≈ **${_not2:,.0f}**")
                    else:
                        st.markdown(f"**⏳ {_sym}** *(no range yet)*")
                        st.markdown(f"- Max risk `${_risk_each:,.0f}`")
                        if max_position_usd > 0:
                            st.markdown(f"- Notional cap `${max_position_usd:,}`")

    st.divider()

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
                    _rh  = float(orb_row['range_high'])
                    _rl  = float(orb_row['range_low'])
                    _rm  = float(orb_row['range_mid'])
                    _span = _rh - _rl

                    st.markdown(f"""
**ORB Range**
- High: `{_rh:.2f}`  · Low: `{_rl:.2f}`
- Mid: `{_rm:.2f}`   · Span: `{_span:.2f}`
                    """)

                    # ── Estimated position size ──────────────────────────────
                    _equity      = acct["equity"] if acct else 100_000.0
                    _risk_dollar = _equity * risk_pct_equity / 100
                    _rps         = max(round(_rh - _rm, 2) if sl_type == "midpoint" else round(_span, 2), 0.01)
                    _sl_est      = round(_rm if sl_type == "midpoint" else _rl, 2)

                    _qty_risk = max(2, int(_risk_dollar / _rps))
                    if max_position_usd > 0:
                        _qty_cap = max(2, int(max_position_usd / _rh))
                        _qty     = min(_qty_risk, _qty_cap)
                        _capped  = _qty < _qty_risk
                    else:
                        _qty    = _qty_risk
                        _capped = False

                    _half     = _qty // 2
                    _rem      = _qty - _half
                    _notional = round(_qty * _rh, 0)
                    _cap_note = f"  *(capped from {_qty_risk} by ${max_position_usd:,} limit)*" if _capped else ""

                    if two_tp:
                        _tp1_est = round(_rh + _rps * tp1_r, 2)
                        _tp2_est = round(_rh + _rps * tp2_r, 2)
                        tp_lines = (
                            f"- TP1: `{_tp1_est:.2f}` (+{tp1_r:.1f}R) · {_half} shares\n"
                            f"- TP2: `{_tp2_est:.2f}` (+{tp2_r:.1f}R) · {_rem} shares"
                        )
                    else:
                        _tp_est  = round(_rh + _rps * tp2_r, 2)
                        tp_lines = f"- TP: `{_tp_est:.2f}` (+{tp2_r:.1f}R) · {_qty} shares"

                    st.markdown(f"""
**Est. Position (long signal)**
- Equity `${_equity:,.0f}` · Risk `${_risk_dollar:,.0f}` ({risk_pct_equity:.1f}%)
- Risk/share `${_rps:.2f}` · SL `{_sl_est:.2f}`
- **{_qty} shares · ≈ ${_notional:,.0f} notional**{_cap_note}
{tp_lines}
                    """)

        st.divider()

    with st.expander("🗒️ Bot Event Log", expanded=False):
        if events_df.empty:
            st.caption("No events logged.")
        else:
            st.dataframe(events_df[["timestamp", "event_type", "message"]],
                         use_container_width=True, hide_index=True)


# ── TAB 2 : BACKTEST ──────────────────────────────────────────────────────────
# Uses all filter/strategy settings from the top bar.
# Backtest-only params: symbol, date range, slippage, starting equity.

with tab_bt:
    st.caption(
        f"All strategy rules are applied: ORB={orb_minutes} min · SL={sl_type} · "
        f"VWAP={'on' if vwap_filter else 'off'} · Surge={volume_surge_mult:.1f}× · "
        f"Min ORB Vol={min_orb_volume//1000:,}K · Min ATR={min_atr_pct:.1f}% · "
        f"{'TP1=' + str(round(tp1_r,2)) + 'R / TP2=' if two_tp else 'TP='}{tp2_r:.2f}R · Risk {risk_pct_equity:.1f}% equity"
    )

    bc1, bc2, bc3 = st.columns(3)
    with bc1:
        bt_symbol = st.text_input("Symbol", value=symbols[0] if symbols else "SPY").upper()
        bt_slippage = st.number_input("Slippage (bps)", 0.0, 50.0, 0.0, 0.5, format="%.1f", key="bt_slip")
    with bc2:
        bt_start = st.date_input("Start Date", value=date.today() - timedelta(days=90), key="bt_start")
        bt_end   = st.date_input("End Date",   value=date.today() - timedelta(days=1),  key="bt_end")
    with bc3:
        bt_equity = st.number_input("Starting Equity ($)", 10_000, 10_000_000, 100_000, 10_000, key="bt_equity")
        st.caption("Filters and targets come from the settings bar above.")

    run_btn = st.button("▶  Run Backtest", type="primary")

    if run_btn:
        if bt_start >= bt_end:
            st.error("End date must be after start date.")
        else:
            from backtest import BacktestEngine
            api_key, api_secret = get_alpaca_credentials(alpaca_paper)
            if not api_key:
                st.error("ALPACA_API_KEY not set. Add it to .env or environment.")
            else:
                _mode_label = f"2 TP  TP1={tp1_r:.2f}R / TP2={tp2_r:.2f}R" if two_tp else f"1 TP  TP={tp2_r:.2f}R"
                with st.spinner(f"Backtesting {bt_symbol}  {bt_start} → {bt_end}  [{_mode_label}]…"):
                    engine = BacktestEngine(api_key, api_secret)
                    result = engine.run_backtest(
                        symbol            = bt_symbol,
                        start_date        = bt_start,
                        end_date          = bt_end,
                        orb_minutes       = orb_minutes,
                        stop_loss_type    = sl_type,
                        slippage_bps      = bt_slippage,
                        risk_ratio        = tp2_r,       # used only in 1-TP mode
                        tp1_r             = tp1_r,       # 0.0 when 1-TP mode
                        tp2_r             = tp2_r,
                        risk_pct_equity   = risk_pct_equity,
                        starting_equity   = float(bt_equity),
                        vwap_filter       = vwap_filter,
                        volume_surge_mult = volume_surge_mult,
                        min_orb_volume    = min_orb_volume,
                        min_atr_pct       = min_atr_pct,
                        max_position_usd  = float(max_position_usd),
                    )

                if result.error:
                    st.error(f"Backtest error: {result.error}")
                elif not result.trades:
                    st.warning("No trades generated. Try a wider date range or different symbol.")
                else:
                    st.session_state["bt_result"] = result
                    st.session_state["bt_benchmark"] = fetch_spy_benchmark(
                        bt_start, bt_end, float(bt_equity), api_key, api_secret
                    )

    if "bt_result" in st.session_state:
        result = st.session_state["bt_result"]
        s      = result.stats
        trades_list = result.trades

        st.divider()
        st.subheader(
            f"Results — {result.symbol}  |  {result.orb_minutes}-min ORB  "
            f"|  {result.start_date} → {result.end_date}"
        )

        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Total Trades",   s.total_trades)
        s2.metric("Win Rate",       f"{s.win_rate}%")
        s3.metric("Total PnL",      f"${s.total_pnl:+.2f}")
        s4.metric("Profit Factor",  f"{s.profit_factor:.2f}" if s.profit_factor != float("inf") else "∞")
        s5.metric("Max Drawdown",   f"-${s.max_drawdown:.2f}")
        s6.metric("Avg R",          f"{s.avg_r_multiple:.2f}R")

        sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
        sc1.metric("Avg Win",         f"${s.avg_win:.2f}")
        sc2.metric("Avg Loss",        f"${s.avg_loss:.2f}")
        sc3.metric("Expectancy",      f"${s.expectancy:.2f}")
        sc4.metric("Sharpe",          f"{s.sharpe_ratio:.2f}")
        sc5.metric("Recovery Factor", f"{s.recovery_factor:.2f}" if s.recovery_factor != float("inf") else "∞")
        sc6.metric("Total Fees",      f"${s.total_fees:.2f}")

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

        st.subheader("Trade Log")

        _exit_icons = {
            "TP":  "✅ TP",  "TP2": "✅ TP2", "TP1": "✅ TP1",
            "SL":  "❌ SL",  "BE":  "↩️ BE",
            "EOD": "🕐 EOD",
        }
        _l1_icons = {"TP1": "✅ TP1", "SL": "❌ SL", "EOD": "🕐 EOD", "": ""}

        display = trades_df_bt.copy()
        display["#"]     = range(1, len(display) + 1)
        display["Side"]  = display["side"].map({"LONG": "▲ LONG", "SHORT": "▼ SHORT"})
        display["Final"] = display["exit_reason"].map(lambda x: _exit_icons.get(x, x))
        display["PnL"]   = display["pnl"].map(lambda x: f"+${x:.2f}" if x >= 0 else f"-${abs(x):.2f}")
        display["R"]     = display["r_multiple"].map(lambda x: f"+{x:.2f}R" if x >= 0 else f"{x:.2f}R")
        display["Fees"]  = display["fees"].map(lambda x: f"${x:.3f}")
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
        st.dataframe(display[show_cols], use_container_width=True, hide_index=True, column_config=col_cfg)

        if result.skipped_days:
            reason_icons = {
                "NO_BARS":           "🚫",
                "INSUFFICIENT_BARS": "⚠️",
                "ZERO_RANGE":        "➖",
                "LOW_ORB_VOLUME":    "📉",
                "LOW_ATR":           "😴",
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

        st.subheader("Trade Detail")
        trade_idx = st.selectbox(
            "Select trade to inspect",
            options=list(range(len(trades_list))),
            format_func=lambda i: f"#{i+1}  {trades_list[i].date}  {trades_list[i].side}  {trades_list[i].exit_reason}  ${trades_list[i].pnl:+.2f}",
        )
        if trade_idx is not None:
            sel      = trades_list[trade_idx]
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
# Uses all filter/strategy settings from the top bar.
# Scanner-only params: universe, lookback period, rank metric.

SCAN_UNIVERSE_DEFAULT = (
    "SPY, QQQ, IWM, AAPL, NVDA, META, TSLA, AMZN, MSFT, GOOGL, AMD, "
    "COIN, PLTR, NFLX, JPM, GS, MSTR, HOOD, SOFI, IONQ"
)

with tab_scanner:
    st.caption(
        f"All strategy rules are applied: ORB={orb_minutes} min · SL={sl_type} · "
        f"VWAP={'on' if vwap_filter else 'off'} · Surge={volume_surge_mult:.1f}× · "
        f"Min ORB Vol={min_orb_volume//1000:,}K · Min ATR={min_atr_pct:.1f}% · "
        f"TP1={tp1_r:.2f}R / TP2={tp2_r:.2f}R · Risk {risk_pct_equity:.1f}% equity"
    )

    scn_col1, scn_col2, scn_col3 = st.columns([3, 1, 1])
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

    with scn_col3:
        rank_by = st.selectbox(
            "Rank By",
            ["Total P&L", "Profit Factor", "Win Rate %", "Avg R", "Expectancy", "Sharpe", "# Trades"],
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
                prog.progress((i + 1) / len(scan_symbols),
                              text=f"Scanning {sym}  ({i+1}/{len(scan_symbols)})")
                try:
                    result = BacktestEngine(api_key, api_secret).run_backtest(
                        symbol            = sym,
                        start_date        = scan_start,
                        end_date          = scan_end,
                        orb_minutes       = orb_minutes,
                        stop_loss_type    = sl_type,
                        risk_ratio        = tp2_r,
                        tp1_r             = tp1_r,
                        tp2_r             = tp2_r,
                        risk_pct_equity   = risk_pct_equity,
                        starting_equity   = 100_000.0,
                        vwap_filter       = vwap_filter,
                        volume_surge_mult = volume_surge_mult,
                        min_orb_volume    = min_orb_volume,
                        min_atr_pct       = min_atr_pct,
                        max_position_usd  = float(max_position_usd),
                    )
                    if result.error or not result.trades:
                        scan_rows.append({"Symbol": sym, "_no_trade": True,
                                          "_reason": result.error or "no signals"})
                        continue
                    s = result.stats
                    scan_rows.append({
                        "Symbol":        sym,
                        "Trades":        s.total_trades,
                        "Win Rate %":    s.win_rate,
                        "Total P&L":     round(s.total_pnl, 2),
                        "Profit Factor": round(s.profit_factor, 2) if s.profit_factor != float("inf") else 999.0,
                        "Avg R":         round(s.avg_r_multiple, 2),
                        "Expectancy":    round(s.expectancy, 2),
                        "Sharpe":        round(s.sharpe_ratio, 2),
                        "Max DD ($)":    round(s.max_drawdown, 2),
                        "Avg Win":       round(s.avg_win, 2),
                        "Avg Loss":      round(s.avg_loss, 2),
                        "Total Fees":    round(s.total_fees, 2),
                        "Skipped Days":  len(result.skipped_days),
                        "_no_trade":     False,
                        "_reason":       "",
                    })
                except Exception as exc:
                    scan_rows.append({"Symbol": sym, "_no_trade": True, "_reason": str(exc)})

            prog.empty()
            st.session_state["scan_results"] = scan_rows
            st.session_state["scan_meta"] = {
                "days": scan_days, "orb": orb_minutes,
                "start": str(scan_start), "end": str(scan_end),
                "tp1_r": tp1_r, "tp2_r": tp2_r,
                "risk_pct": risk_pct_equity,
                "vwap": vwap_filter, "surge": volume_surge_mult,
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
                "Expectancy":   "Expectancy",
                "Sharpe":       "Sharpe",
                "# Trades":     "Trades",
            }
            sort_col = rank_col_map.get(rank_by, "Total P&L")
            df_scan  = pd.DataFrame(traded).sort_values(sort_col, ascending=False).reset_index(drop=True)
            df_scan.index = range(1, len(df_scan) + 1)

            period_label = (
                f"{meta.get('start','?')} → {meta.get('end','?')}  ·  "
                f"{meta.get('orb','?')}-min ORB  ·  "
                f"TP1={meta.get('tp1_r',0):.1f}R / TP2={meta.get('tp2_r',0):.1f}R  ·  "
                f"VWAP={'on' if meta.get('vwap') else 'off'}  ·  "
                f"Surge={meta.get('surge',0):.1f}×"
            )
            st.markdown(f"**{len(df_scan)} symbols with trades** — {period_label}")

            top = df_scan.iloc[0]
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("🏆 Top Symbol",  top["Symbol"])
            m2.metric("Total P&L",      f"${top['Total P&L']:+,.2f}")
            m3.metric("Win Rate",        f"{top['Win Rate %']:.0f}%")
            m4.metric("Profit Factor",   f"{top['Profit Factor']:.2f}")
            m5.metric("Expectancy",      f"${top['Expectancy']:+.2f}")
            m6.metric("Sharpe",          f"{top['Sharpe']:.2f}")

            st.divider()

            display_cols = ["Symbol", "Trades", "Win Rate %", "Total P&L",
                            "Profit Factor", "Avg R", "Expectancy", "Sharpe",
                            "Max DD ($)", "Total Fees", "Skipped Days"]
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
                    "Expectancy":    st.column_config.NumberColumn(format="$%+.2f"),
                    "Sharpe":        st.column_config.NumberColumn(format="%.2f", width="small"),
                    "Max DD ($)":    st.column_config.NumberColumn(format="$%.2f"),
                    "Total Fees":    st.column_config.NumberColumn(format="$%.2f"),
                    "Skipped Days":  st.column_config.NumberColumn(width="small"),
                },
            )

            st.divider()
            st.subheader("Top 5 Equity Curves")
            top5 = df_scan.head(5)["Symbol"].tolist()
            api_key_scan, api_secret_scan = get_alpaca_credentials(alpaca_paper)
            scan_end_d   = date.today() - timedelta(days=1)
            scan_start_d = scan_end_d - timedelta(days=meta.get("days", scan_days))

            @st.cache_data(ttl=3600)
            def _scan_equity(s, sd, ed, orb, slt, key, sec, tp1, tp2, rpe, eq, vwap, surge, min_vol, min_atr):
                from backtest import BacktestEngine as _BTE
                r = _BTE(key, sec).run_backtest(
                    symbol=s, start_date=sd, end_date=ed,
                    orb_minutes=orb, stop_loss_type=slt,
                    tp1_r=tp1, tp2_r=tp2, risk_pct_equity=rpe,
                    starting_equity=eq, vwap_filter=vwap, volume_surge_mult=surge,
                    min_orb_volume=min_vol, min_atr_pct=min_atr,
                )
                if r.trades:
                    return pd.DataFrame({"pnl": [t.pnl for t in r.trades]})
                return pd.DataFrame()

            eq_cols = st.columns(min(len(top5), 5))
            for col, sym in zip(eq_cols, top5):
                with col:
                    sym_row = df_scan[df_scan["Symbol"] == sym].iloc[0]
                    pnl_color = "#00e676" if sym_row["Total P&L"] >= 0 else "#ef5350"
                    st.caption(f"**{sym}** — ${sym_row['Total P&L']:+,.0f}")
                    try:
                        eq_df = _scan_equity(
                            sym, scan_start_d, scan_end_d,
                            meta.get("orb", orb_minutes), sl_type,
                            api_key_scan, api_secret_scan,
                            meta.get("tp1_r", 0.0), meta.get("tp2_r", 0.0),
                            meta.get("risk_pct", 0.0), 100_000.0,
                            meta.get("vwap", False), meta.get("surge", 0.0),
                            min_orb_volume, min_atr_pct,
                        )
                        if not eq_df.empty:
                            fig_mini = go.Figure(go.Scatter(
                                y=eq_df["pnl"].cumsum(),
                                mode="lines",
                                line=dict(color=pnl_color, width=2),
                                fill="tozeroy",
                                fillcolor=f"rgba({'0,230,118' if pnl_color == '#00e676' else '239,83,80'},0.08)",
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
                    pd.DataFrame([{"Symbol": r["Symbol"], "Reason": r["_reason"]}
                                  for r in no_signals]),
                    use_container_width=True, hide_index=True,
                )


# ── TAB 4 : STRATEGY RULES ────────────────────────────────────────────────────

with tab_rules:

    st.subheader("Dynamic Momentum ORB — Complete Strategy Rules")
    st.caption(
        f"All values reflect the current settings bar: "
        f"**{orb_minutes}-min ORB · Gap ≥{gap_min_pct:.1f}% · RVOL ≥{rvol_min:.1f}× · "
        f"Risk {risk_pct_equity:.1f}% equity · TP1={tp1_r:.1f}R / TP2={tp2_r:.1f}R · "
        f"VWAP={'on' if vwap_filter else 'off'} · Surge={volume_surge_mult:.1f}× · "
        f"Min ORB Vol={min_orb_volume:,} · Min ATR={min_atr_pct:.1f}%**"
    )

    # ── Strategy Overview ──────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Strategy Overview")

    st.info("""
**Dynamic Momentum ORB** combines pre-market scanning with intraday momentum confirmation to produce a high-quality trade list every morning.

- **Pre-market scanner** (4:00 AM – 9:25 AM) ranks stocks by overnight gap and relative volume; only the top N symbols trade that day.
- **Analyst filters** (ORB volume + 14-day ATR) discard low-liquidity and low-volatility days.
- **Intraday momentum filters** (VWAP alignment + volume surge) confirm genuine breakout momentum before any order is sent.
- **Volatility-adjusted sizing** risks a fixed % of account equity per trade — position size automatically scales with the stock's move potential.
- **Two-leg scale-out** locks in profits at TP1 and moves the SL to break-even, letting the second half ride to TP2 risk-free.
- **Bracket orders** sent to Alpaca ensure exits are broker-managed even if the bot crashes.
- **Auto-flatten at 3:55 PM** eliminates overnight exposure.
    """)

    ov1, ov2, ov3, ov4 = st.columns(4)
    with ov1:
        st.metric("TP1 Target", f"{tp1_r:.1f}R", "50% of position")
    with ov2:
        st.metric("TP2 Target", f"{tp2_r:.1f}R", "remaining 50%")
    with ov3:
        st.metric("Risk per trade", f"{risk_pct_equity:.1f}% equity", "volatility-adjusted")
    with ov4:
        st.metric("Top N symbols", str(int(scanner_top_n)), "from pre-market scan")

    # ── Consistency note ──────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Rule Consistency: Live Bot · Backtest · Scanner")

    st.markdown("""
| Rule | Live Bot | Backtest | Scanner | Notes |
|------|----------|----------|---------|-------|
| Pre-market scanner (gap + RVOL) | ✅ Runs 4 AM–9:25 AM | n/a | n/a | Bot selects symbols; backtest tests specific ones |
| Min bars required (≥ 13 of ORB window) | ✅ | ✅ | ✅ | Guards against data gaps at open |
| Min ORB Volume | ✅ | ✅ | ✅ | Same threshold from settings bar |
| Min ATR % (14-day) | ✅ | ✅ | ✅ | Same threshold from settings bar |
| Price breakout (close > ORB High / < ORB Low) | ✅ | ✅ | ✅ | Identical logic |
| VWAP filter | ✅ | ✅ | ✅ | Toggle in settings bar controls all three |
| Volume surge filter | ✅ | ✅ | ✅ | Multiplier in settings bar controls all three |
| Bid-ask spread filter | ✅ Live quotes | ⚠️ Not applied | ⚠️ Not applied | No historical quote data available |
| Two-leg scale-out (TP1 + TP2 + B/E) | ✅ Always | ✅ Always | ✅ Always | Single-leg mode removed |
| Volatility-adjusted sizing (risk % equity) | ✅ Always | ✅ Always | ✅ Always | Fixed-USD mode removed |
| Auto-flatten 3:55 PM | ✅ | ✅ (simulated) | ✅ (simulated) | EOD exit recorded |
| Kill switch (daily loss %) | ✅ Bot only | n/a | n/a | Portfolio-level guard |

> **Spread filter caveat:** The bid-ask spread check is the only rule that differs.
> Historical minute bars do not include real-time quotes, so it cannot be simulated.
> Backtest results are therefore slightly optimistic for thinly-traded names.
> Set `Max Spread %` conservatively and the real bot will auto-filter those out at execution time.
    """)

    # ── Phase 0 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 0 · Pre-Market Scanner  *(4:00 AM → 9:25 AM, live bot only)*")

    sc1, sc2 = st.columns([1, 1])
    with sc1:
        st.markdown(f"""
| Metric | Formula | Threshold |
|--------|---------|-----------|
| **Gap %** | `(first_pm_open − prev_close) / prev_close × 100` | ≥ **{gap_min_pct:.1f}%** |
| **RVOL** | `(pm_vol / 325 min) ÷ (avg_daily_vol / 390 min)` | ≥ **{rvol_min:.1f}×** |
| **Top N** | Symbols ranked by Gap% × RVOL score | Keep **{int(scanner_top_n)}** |

Scanner fires at **9:25 AM EST**. Only these tickers receive intraday signals for the rest of the day.

RVOL normalises by minutes (325 vs 390) so premarket-volume rates compare fairly against full-session averages.
        """)
    with sc2:
        st.info(f"""
**Why scan pre-market?**

Most ORB breakouts with follow-through come from stocks that *already have momentum* before the bell:
- A large gap (≥ {gap_min_pct:.1f}%) signals a catalyst — earnings, news, upgrade.
- High RVOL (≥ {rvol_min:.1f}×) shows institutional participation, not retail noise.

Filtering to the top **{int(scanner_top_n)}** keeps the bot focused on the strongest setups.
        """)

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 1 · Build the Opening Range  *(9:30 AM)*")

    p1c1, p1c2 = st.columns([1, 1])
    with p1c1:
        _min_bars = min(orb_minutes, 13)
        st.markdown(f"""
| Condition | Value |
|-----------|-------|
| Market open | **9:30 AM EST** |
| ORB window | **9:30 → 9:{30 + orb_minutes:02d} AM** ({orb_minutes} bars of 1-min data) |
| Range High | `max(bar.high)` over ORB window |
| Range Low | `min(bar.low)` over ORB window |
| Range Mid | `(High + Low) / 2` — default stop-loss anchor |
| Min bars | ≥ **{_min_bars} of {orb_minutes}** (skips day if data gap at open) |
| Min ORB Volume | Total ORB shares ≥ **{min_orb_volume//1000:,}K** (liquidity check) |
| Min ATR % | 14-day ATR / price ≥ **{min_atr_pct:.1f}%** (volatility check) |
| VWAP | Running `Σ(typical_price × vol) / Σ(vol)` from first bar at 9:30 AM |
        """)
    with p1c2:
        st.info(f"""
**Analyst filters — why they exist**

**Min ORB Volume ({min_orb_volume//1000:,}K shares)**
Ensures you can exit large positions without moving the market.
Low-volume ORBs lead to wide fills and slippage that ruins the theoretical edge.

**Min ATR % ({min_atr_pct:.1f}%)**
14-day Average True Range as % of price measures "how much does this stock normally move per day."
A stock with ATR < {min_atr_pct:.1f}% may never even reach TP1 = {tp1_r:.1f}R before EOD flatten.

**VWAP accumulation**
Starts fresh at 9:30 AM every session. A breakout *above VWAP* means the average intraday buyer is in profit and the stock has strong upward bias.
        """)

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 2 · Breakout Signal + Momentum Confirmation")

    st.markdown(f"""
Every 1-minute bar **after 9:{30 + orb_minutes:02d} AM** is tested.
**All four gates must be true simultaneously** — any single failure skips that bar.

| Gate | LONG | SHORT | Setting |
|------|------|-------|---------|
| **Price breakout** | Bar close `>` ORB High | Bar close `<` ORB Low | Always on |
| **VWAP filter** | Close `>` VWAP | Close `<` VWAP | {"✅ ON" if vwap_filter else "⚠️ OFF"} |
| **Volume surge** | Bar volume `≥` {volume_surge_mult:.1f}× avg ORB vol | same | Always on |
| **Spread filter** | Bid-ask ≤ {max_spread_pct:.3f}% of ask | same | Live bot only |

> **One trade per symbol per day.** Once a signal fires the symbol is locked: `SCANNING → SUBMITTING → IN_TRADE`.
> No second entry is taken even if the first trade closes early.

**VWAP Filter** — a close above ORB High but *below* VWAP means the average intraday buyer is still underwater; they will sell into the move, capping the breakout.

**Volume Surge** — genuine breakouts expand volume. A close above the range on thin volume is a false breakout that likely snaps back.
`avg_orb_volume` = mean per-bar volume across the {orb_minutes} ORB bars.

**Spread Filter (live only)** — fetches a real-time quote via Alpaca's REST API.
Wide spreads (e.g. > {max_spread_pct:.3f}%) on low-float names destroy the theoretical R:R at entry.
    """)

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 3 · Two-Leg Scale-Out Order Execution")

    # Live example numbers
    _rng_ex   = 2.50
    _entry_l  = 100.00
    _entry_s  = 97.50
    _risk     = _rng_ex / 2  # midpoint SL
    _tp1_l    = round(_entry_l + _risk * tp1_r, 2)
    _tp2_l    = round(_entry_l + _risk * tp2_r, 2)
    _sl_l     = round(_entry_l - _risk, 2)
    _tp1_s    = round(_entry_s - _risk * tp1_r, 2)
    _tp2_s    = round(_entry_s - _risk * tp2_r, 2)
    _sl_s     = round(_entry_s + _risk, 2)

    ex1, ex2 = st.columns(2)
    with ex1:
        st.markdown("#### 🟢 Long — Scale-Out Levels")
        st.markdown(f"""
```
ORB High   = {_entry_l - 0.20:.2f}
Entry      ≈ {_entry_l:.2f}  (close above ORB High)
SL         = {_sl_l:.2f}  (midpoint of ORB range)
Risk/share = Entry − SL = {_entry_l - _sl_l:.2f}

Leg 1 — 50% of total qty
  TP1      = Entry + {tp1_r:.1f}R = {_tp1_l:.2f}
  → SL moves to break-even ({_entry_l:.2f}) after TP1 fills

Leg 2 — remaining 50%
  TP2      = Entry + {tp2_r:.1f}R = {_tp2_l:.2f}
  SL       = break-even ({_entry_l:.2f}) once Leg 1 fills
  → Leg 2 is risk-free after TP1

Worst case  → both legs stop at {_sl_l:.2f}  (−1R)
Best case   → Leg1 at {_tp1_l:.2f} (+{tp1_r:.1f}R), Leg2 at {_tp2_l:.2f} (+{tp2_r:.1f}R)
```
        """)
    with ex2:
        st.markdown("#### 🔴 Short — Scale-Out Levels")
        st.markdown(f"""
```
ORB Low    = {_entry_s + 0.20:.2f}
Entry      ≈ {_entry_s:.2f}  (close below ORB Low)
SL         = {_sl_s:.2f}  (midpoint of ORB range)
Risk/share = SL − Entry = {_sl_s - _entry_s:.2f}

Leg 1 — 50% of total qty
  TP1      = Entry − {tp1_r:.1f}R = {_tp1_s:.2f}
  → SL moves to break-even ({_entry_s:.2f}) after TP1 fills

Leg 2 — remaining 50%
  TP2      = Entry − {tp2_r:.1f}R = {_tp2_s:.2f}
  SL       = break-even ({_entry_s:.2f}) once Leg 1 fills
  → Leg 2 is risk-free after TP1

Worst case  → both legs stop at {_sl_s:.2f}  (−1R)
Best case   → Leg1 at {_tp1_s:.2f} (+{tp1_r:.1f}R), Leg2 at {_tp2_s:.2f} (+{tp2_r:.1f}R)
```
        """)

    st.info("""
**Implementation detail:** Two bracket orders are submitted simultaneously — one for each leg, each with its own TP and the same initial SL.
After Leg 1's TP fills, the bot cancels Leg 2's original SL and replaces it with a stop at the entry price (break-even).
Alpaca's OCO mechanism auto-cancels the paired SL when TP fills, so no orphan orders are left.
If the bot crashes after both orders are submitted, Alpaca still manages all exits.
    """)

    st.markdown("---")
    st.markdown("### Visual Setup Diagram — Long Entry")

    fig_diag = go.Figure()

    _orb_h  = 100.00
    _orb_l  = 97.50
    _orb_m  = (_orb_h + _orb_l) / 2
    _e      = _orb_h + 0.20
    _r_diag = _e - _orb_m
    _t1_d   = round(_e + _r_diag * tp1_r, 2)
    _t2_d   = round(_e + _r_diag * tp2_r, 2)
    _sl_d   = round(_orb_m, 2)

    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=_orb_l, y1=_orb_h,
                       fillcolor="rgba(255,214,0,0.08)",
                       line=dict(color="rgba(255,214,0,0.5)", width=1))
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=_e, y1=_t2_d,
                       fillcolor="rgba(0,230,118,0.05)", line=dict(width=0))
    fig_diag.add_shape(type="rect", x0=0, x1=1, y0=_sl_d, y1=_e,
                       fillcolor="rgba(239,83,80,0.05)", line=dict(width=0))

    for y, label, color, dash in [
        (_orb_h, f"ORB High  {_orb_h:.2f}",                  "#FFD700", "dot"),
        (_orb_l, f"ORB Low   {_orb_l:.2f}",                  "#FFA000", "dot"),
        (_orb_m, f"ORB Mid   {_orb_m:.2f}",                  "#FFCC44", "dot"),
        (_t2_d,  f"TP2 ({tp2_r:.1f}R)  {_t2_d:.2f}",         "#00e676", "dash"),
        (_t1_d,  f"TP1 ({tp1_r:.1f}R)  {_t1_d:.2f}",         "#69f0ae", "dash"),
        (_e,     f"Entry / Break-even  {_e:.2f}",             "#82b1ff", "solid"),
        (_sl_d,  f"Initial SL  {_sl_d:.2f}",                  "#ef5350", "dash"),
    ]:
        fig_diag.add_shape(type="line", x0=0, x1=1, y0=y, y1=y,
                           line=dict(color=color, dash=dash, width=1.5))
        fig_diag.add_annotation(x=1.01, y=y, text=label, showarrow=False,
                                xanchor="left", font=dict(size=11, color=color))

    fig_diag.add_annotation(x=0.5, y=_e, ax=0.5, ay=_orb_h + 0.05,
                             text=f"▲ LONG entry ~{_e:.2f}", showarrow=True,
                             arrowhead=2, arrowcolor="#82b1ff",
                             font=dict(color="#82b1ff", size=12))
    fig_diag.add_annotation(x=0.25, y=_t1_d,
                             text=f"← 50% exits here ({tp1_r:.1f}R) → SL moves to B/E",
                             showarrow=False, font=dict(color="#69f0ae", size=10))
    fig_diag.add_annotation(x=0.25, y=_t2_d,
                             text=f"← remaining 50% exits here ({tp2_r:.1f}R) risk-free",
                             showarrow=False, font=dict(color="#00e676", size=10))

    fig_diag.update_layout(
        height=500, margin=dict(l=10, r=200, t=20, b=20),
        xaxis=dict(visible=False, range=[0, 1]),
        yaxis=dict(gridcolor="#1e2130", tickformat="$.2f"),
        showlegend=False,
    )
    _dark_layout(fig_diag)
    st.plotly_chart(fig_diag, use_container_width=True)

    # ── Phase 4 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 4 · Volatility-Adjusted Position Sizing")

    sz1, sz2 = st.columns([1, 1])
    with sz1:
        st.markdown(f"""
```
risk_dollar    = account_equity × {risk_pct_equity:.1f}%
risk_per_share = entry_price − sl_price
total_qty      = floor(risk_dollar ÷ risk_per_share)   [minimum 2]
half_qty       = total_qty ÷ 2                          → Leg 1
remaining_qty  = total_qty − half_qty                   → Leg 2
```

Every trade risks the **same dollar amount** regardless of price.
A high-volatility stock (wide SL) gets fewer shares.
A tight-range stock gets more shares.
The minimum of 2 shares ensures two legs can always be opened.
        """)
    with sz2:
        st.markdown(f"#### Sizing examples at {risk_pct_equity:.1f}% risk, $100,000 equity")
        _eq = 100_000
        _rd = _eq * risk_pct_equity / 100
        ex_rows = []
        for entry_px, sl_dist in [(50, 1.0), (100, 1.5), (200, 2.5), (500, 5.0)]:
            qty = max(2, int(_rd / sl_dist))
            ex_rows.append({
                "Entry ($)":     entry_px,
                "Risk/share ($)": round(sl_dist, 2),
                "Total Qty":     qty,
                "Max Loss ($)":  round(qty * sl_dist, 2),
            })
        st.dataframe(
            pd.DataFrame(ex_rows), use_container_width=True, hide_index=True,
            column_config={
                "Entry ($)":      st.column_config.NumberColumn(format="$%.0f"),
                "Risk/share ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Total Qty":      st.column_config.NumberColumn(),
                "Max Loss ($)":   st.column_config.NumberColumn(format="$%.2f"),
            },
        )

    # ── Bot State Machine ──────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Bot State Machine")
    st.markdown("""
```
WAITING_RANGE
    │  9:30 AM — start accumulating ORB bars
    ▼
BUILDING_RANGE
    │  ORB window complete + all analyst filters pass
    ▼
SCANNING
    │  First bar that passes price + VWAP + surge + spread filters
    ▼
SUBMITTING          ← race-condition guard (no double orders)
    │  Both bracket orders acknowledged by Alpaca
    ▼
IN_TRADE            ← both legs live
    │
    ├─ Leg 1 TP1 fills
    │       ▼
    │   PARTIAL_EXIT → bot cancels original Leg 2 SL
    │       ▼
    │   BREAK_EVEN   → new SL placed at entry price for Leg 2
    │       │
    │       ├─ Leg 2 TP2 fills  →  COMPLETED (exit: TP2)
    │       └─ BE stop fills    →  COMPLETED (exit: BE)
    │
    ├─ Leg 1 SL fills → both legs stopped out → COMPLETED (exit: SL)
    └─ 3:55 PM flatten → all positions closed → COMPLETED (exit: EOD)

INVALID  ← analyst filter failed (low vol, low ATR, data gap)
```
    """)

    # ── Phase 5 ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Phase 5 · Risk Management & Safety")

    rm1, rm2, rm3, rm4 = st.columns(4)
    with rm1:
        st.markdown(f"""
**🛑 Daily Kill Switch**

Checked every **60 seconds**.

If `(start_equity − current_equity) / start_equity × 100 ≥ {cfg.get('max_daily_loss_pct', 2.0):.1f}%`:
- All positions flattened immediately
- All open orders cancelled
- No new signals for the rest of the day
        """)
    with rm2:
        st.markdown("""
**⏰ Auto-Flatten 3:55 PM**

Every trading day at **3:55 PM EST** (5 min before close):
- All open positions closed at market
- All pending orders cancelled

Prevents overnight exposure regardless of TP/SL status.
On early-close days, the flatten time is inferred from the last bar.
        """)
    with rm3:
        st.markdown("""
**📦 Broker-Managed Exits**

Bracket orders sent to Alpaca include both TP and SL.
Alpaca's OCO mechanism auto-cancels the paired order when either leg fills.

**If the bot crashes after submission, Alpaca still manages all exits.
You are never left with a naked position.**
        """)
    with rm4:
        st.markdown("""
**🔍 Orphan Cleanup**

Every **5 minutes** the bot checks open orders against live positions.
If a position no longer exists in the broker account but the bot has open orders:
- All associated orders are cancelled
- Bot state is reset

Prevents stale orders from triggering unexpected fills on future moves.
        """)

    # ── Fees ──────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Regulatory Fees (applied in backtest and live)")
    st.markdown("""
Alpaca is commission-free but passes through SEC and FINRA regulatory fees on every **sell** leg:

| Fee | Rate | Cap |
|-----|------|-----|
| **SEC fee** | $20.60 per $1,000,000 of principal sold | None |
| **FINRA TAF** | $0.000195 per share sold | $9.79 per transaction |

These are applied in backtest to both the TP1 (Leg 1) exit and the final Leg 2 exit.
The `Total Fees` stat in backtest results reflects the full regulatory cost over all trades.
    """)

    # ── Stop-Loss Types ───────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Stop-Loss Types")
    st.markdown(f"""
| Type | LONG SL | SHORT SL | Notes |
|------|---------|----------|-------|
| **midpoint** (current: {"✅" if sl_type == "midpoint" else "—"}) | `(ORB High + ORB Low) / 2` | same | Tighter risk, more trades hit SL before breakout re-tests range |
| **hard** (current: {"✅" if sl_type == "hard" else "—"}) | `ORB Low` | `ORB High` | Wider risk, fewer shakeouts — but position size is smaller |

**Which to use:** Midpoint works best on high-ATR stocks where the ORB range is wide (risk-per-share stays reasonable).
Hard stop works best on tight-range, low-ATR days where midpoint would give near-zero R:R.
    """)
