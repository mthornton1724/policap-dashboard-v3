import streamlit as st
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from src.ingest.loaders import (
    load_events_local, load_congress_trades_local, load_yields_local, load_commods_local,
    load_events_live, load_congress_trades_live, load_yields_live, load_commods_live
)
from src.logic.alert_rules import run_all_alerts
from src.viz.charts import line_chart

st.set_page_config(page_title="Political Capital Flow Dashboard (v2)", layout="wide")
st.title("Political Capital Flow Dashboard (v2)")
st.caption("Stable, resilient build with robust fetchers, safe alerts, and UI guards.")

def has_cols(df, cols):
    return (df is not None) and (not df.empty) and all(c in df.columns for c in cols)

st.sidebar.header("Controls")
if st.sidebar.button("Force refresh"):
    st.cache_data.clear()
    st.rerun()
live_mode = st.sidebar.toggle("Live Mode", value=False)
lookback_days = st.sidebar.slider("Lookback (days)", min_value=30, max_value=180, value=90, step=15)
sectors_default = ["Semis","Defense","Energy","Retail","Utilities","Data Center REITs","Manufacturing","Logistics","Cloud"]
sector_filter = st.sidebar.multiselect("Filter trades by sector", options=sectors_default, default=sectors_default[:3])


@st.cache_data(ttl=60*30, show_spinner=False)
def get_events(live, days):
    return load_events_live(days) if live else load_events_local()

@st.cache_data(ttl=60*30, show_spinner=False)
def get_trades(live, days):
    return load_congress_trades_live(days) if live else load_congress_trades_local()

@st.cache_data(ttl=60*30, show_spinner=False)
def get_yields(live):
    return load_yields_live(120) if live else load_yields_local()

@st.cache_data(ttl=60*30, show_spinner=False)
def get_commods(live):
    return load_commods_live("120d","1d") if live else load_commods_local()

events = get_events(live_mode, lookback_days)
trades = get_trades(live_mode, lookback_days)
yld = get_yields(live_mode)
cmd = get_commods(live_mode)

if has_cols(events, ["date"]):
    events = events.sort_values("date", ascending=False)
if has_cols(trades, ["trade_date"]):
    if sector_filter:
        trades = trades[trades["sector"].fillna("").str.contains("|".join(sector_filter), case=False, na=False)]

st.subheader("Alerts")
alerts = run_all_alerts(events.copy() if events is not None else events,
                        trades.copy() if trades is not None else trades,
                        cmd.copy() if cmd is not None else cmd)
if alerts:
    for a in alerts:
        with st.container(border=True):
            st.markdown(f"**Theme:** {a['theme']} — **Confidence:** {a['confidence']:.0%}")
            st.write(a['why'])
            st.write(f"Suggested action: {a['suggest']}")
else:
    st.info("No alerts at this time based on current simple rules.")

left, right = st.columns([1,1])

with left:
    st.subheader("Policy & Events")
    if not events.empty:
        st.dataframe(events, use_container_width=True)
    else:
        st.warning("No events to show yet.")
    st.subheader("Congressional Trades")
    if not trades.empty:
        st.dataframe(trades, use_container_width=True)
    else:
        st.info("No trades loaded. Add QUIVER_API_KEY in .env for live data.")

with right:
    st.subheader("Rates")
    if has_cols(yld, ["date"]):
        cols_to_plot = [c for c in ["dgs2","dgs10","dgs30"] if c in yld.columns]
        if cols_to_plot:
            fig_y = line_chart(yld, "date", cols_to_plot, "Treasury Yields (2y/10y/30y)")
            st.pyplot(fig_y)
        else:
            st.warning("Yields fetched, but no usable yield columns found.")
    else:
        st.warning("Yields data unavailable right now (feed issue).")

    st.subheader("Commodities")
    if has_cols(cmd, ["date"]) and (("wti" in cmd.columns) or ("gold" in cmd.columns)):
        cols_to_plot = [c for c in ["wti","gold"] if c in cmd.columns]
        fig_c = line_chart(cmd, "date", cols_to_plot, "WTI & Gold")
        st.pyplot(fig_c)
    else:
        st.warning("Commodity data unavailable right now (feed issue).")

st.divider()
st.markdown("### Setup tips")
st.markdown("""- For **Congress trades**, set `QUIVER_API_KEY` in a `.env` file.
- Executive Orders use the **Federal Register API**.
- Yields fetcher is robust (FRED CSV + Yahoo fallback) and the UI won't crash if feeds hiccup.
- Deploy on **Streamlit Cloud** or run locally. Use the sidebar to toggle **Live Mode**.
""")
