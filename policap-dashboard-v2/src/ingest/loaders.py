import os
import pandas as pd
from src.ingest.live_sources import (
    fetch_executive_orders,
    fetch_congress_trades_quiver,
    fetch_treasury_yields,
    fetch_commodities
)

DATA_DIR = "data"

def load_events_local():
    path = os.path.join(DATA_DIR, "events.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date","instrument","title","sectors","source"])
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def load_congress_trades_local():
    path = os.path.join(DATA_DIR, "congress_trades.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["trade_date","member","ticker","action","amount_band","sector","link"])
    df = pd.read_csv(path)
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    return df

def load_yields_local():
    path = os.path.join(DATA_DIR, "yields.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date","dgs2","dgs10","dgs30"])
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def load_commods_local():
    path = os.path.join(DATA_DIR, "commods.csv")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date","wti","gold"])
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def load_events_live(last_n_days: int = 90):
    return fetch_executive_orders(last_n_days=last_n_days)

def load_congress_trades_live(last_n_days: int = 90):
    return fetch_congress_trades_quiver(last_n_days=last_n_days)

def load_yields_live(last_n_days: int = 120):
    try:
        df = fetch_treasury_yields(last_n_days=last_n_days)
    except Exception as e:
        print("[yields] live error:", e)
        df = pd.DataFrame()
    if df is None or df.empty or "date" not in df.columns:
        print("[yields] using local snapshot fallback.")
        return load_yields_local()
    return df

def load_commods_live(period: str = "120d", interval: str = "1d"):
    try:
        df = fetch_commodities(period=period, interval=interval)
    except Exception as e:
        print("[commods] live error:", e)
        df = pd.DataFrame()
    if df is None or df.empty or "date" not in df.columns:
        print("[commods] using local snapshot fallback.")
        return load_commods_local()
    return df

