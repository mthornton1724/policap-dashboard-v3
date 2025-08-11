import os
import io
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

def _days_ago(n: int) -> str:
    return (datetime.utcnow() - timedelta(days=n)).strftime("%Y-%m-%d")

def fetch_executive_orders(last_n_days: int = 90) -> pd.DataFrame:
    base = "https://www.federalregister.gov/api/v1/documents.json"
    params = {
        "per_page": 100,
        "order": "newest",
        "conditions[presidential_document_type]": "executive_order",
        "conditions[publication_date][gte]": _days_ago(last_n_days),
    }
    try:
        r = requests.get(base, params=params, timeout=30)
        r.raise_for_status()
        results = r.json().get("results", [])
    except Exception:
        return pd.DataFrame(columns=["date","instrument","title","sectors","source"])

    rows = []
    for item in results:
        date = item.get("publication_date")
        title = (item.get("title") or "").strip()
        url = item.get("html_url") or item.get("pdf_url")

        tags = []
        t = title.lower()
        if "data center" in t or "ai" in t:
            tags += ["Semiconductors","Utilities","Data Center REITs","Cloud"]
        if "tariff" in t or "reciprocal" in t or "de minimis" in t:
            tags += ["Retail","Logistics","Manufacturing","E-commerce"]
        if "cyber" in t:
            tags += ["Cybersecurity","Defense IT"]
        if "russia" in t or "ukraine" in t:
            tags += ["Energy","Defense","Gold"]

        rows.append({
            "date": date,
            "instrument": "EO",
            "title": title,
            "sectors": ", ".join(sorted(set(tags))) if tags else "",
            "source": url,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date", ascending=False).reset_index(drop=True)
    return df

def fetch_congress_trades_quiver(last_n_days: int = 90) -> pd.DataFrame:
    api_key = (os.getenv("QUIVER_API_KEY") or "").strip()
    if not api_key:
        return pd.DataFrame(columns=["trade_date","member","ticker","action","amount_band","sector","link"])

    url = "https://api.quiverquant.com/beta/live/congresstrading"
    try:
        r = requests.get(url, headers={"Authorization": f"Token {api_key}"}, timeout=30)
        r.raise_for_status()
        items = r.json()
    except Exception:
        return pd.DataFrame(columns=["trade_date","member","ticker","action","amount_band","sector","link"])

    since = datetime.utcnow() - timedelta(days=last_n_days)
    rows = []
    for it in items:
        date_str = (it.get("Date") or "")[:10]
        try:
            d = datetime.fromisoformat(date_str)
        except Exception:
            continue
        if d < since:
            continue
        rows.append({
            "trade_date": d.date().isoformat(),
            "member": it.get("Representative",""),
            "ticker": it.get("Ticker",""),
            "action": it.get("Transaction",""),
            "amount_band": it.get("Amount",""),
            "sector": it.get("Industry",""),
            "link": "https://www.quiverquant.com/congresstrading",
        })
    return pd.DataFrame(rows).sort_values("trade_date", ascending=False).reset_index(drop=True)

def fetch_treasury_yields(last_n_days: int = 120) -> pd.DataFrame:
    """
    Pull 2y/10y/30y. Try FRED CSV first (robust parsing, timezone-safe),
    then fall back to Yahoo yield proxies if needed.
    Returns: date, dgs2, dgs10, dgs30
    """
    import io, pandas as pd, requests, yfinance as yf

    def _norm(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(c).replace("\ufeff","").strip().lower() for c in df.columns]
        if "date" not in df.columns:
            for c in df.columns:
                if str(c).lower().startswith("date"):
                    df = df.rename(columns={c: "date"})
                    break
        ren = {}
        for c in df.columns:
            u = str(c).upper()
            if u == "DGS2": ren[c] = "dgs2"
            if u == "DGS10": ren[c] = "dgs10"
            if u == "DGS30": ren[c] = "dgs30"
        if ren: df = df.rename(columns=ren)
        return df

    # ---- FRED (with UA + timezone normalization) ----
    try:
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2,DGS10,DGS30"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        raw = pd.read_csv(io.StringIO(r.text))
        df = _norm(raw)

        need = {"date","dgs10","dgs30"}
        if not need.issubset(df.columns):
            raise ValueError(f"FRED CSV missing columns: {df.columns.tolist()}")

        # Make both sides timezone-naive before comparing
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_convert(None)
        for c in ["dgs2","dgs10","dgs30"]:
            if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date","dgs10","dgs30"])

        cutoff = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=last_n_days)
        df = df[df["date"] >= cutoff].reset_index(drop=True)
        if "dgs2" not in df.columns: df["dgs2"] = pd.NA
        return df[["date","dgs2","dgs10","dgs30"]]
    except Exception as e:
        print("[yields] FRED fetch failed; falling back:", e)

    # ---- Yahoo fallback (^TNX,^TYX,^IRX) ----
    frames = []
    for name,tkr in {"dgs10":"^TNX", "dgs30":"^TYX", "dgs2":"^IRX"}.items():
        try:
            d = yf.download(tkr, period="6mo", interval="1d", auto_adjust=False, progress=False, threads=False)
            if d.empty or "Close" not in d.columns: continue
            s = d[["Close"]].rename(columns={"Close": name})
            s.index.name = "date"
            frames.append(s)
        except Exception as e:
            print(f"[yields] Yahoo fetch failed for {tkr}:", e)

    if not frames:
        return pd.DataFrame(columns=["date","dgs2","dgs10","dgs30"])

    out = pd.concat(frames, axis=1).dropna(how="all").reset_index()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for c in ["dgs10","dgs30","dgs2"]:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors="coerce")/10.0
    cutoff = pd.Timestamp.utcnow().tz_localize(None).normalize() - pd.Timedelta(days=last_n_days)
    out = out[out["date"] >= cutoff].reset_index(drop=True)
    for c in ["dgs2","dgs10","dgs30"]:
        if c not in out.columns: out[c] = pd.NA
    return out[["date","dgs2","dgs10","dgs30"]]



def fetch_commodities(period: str = "6mo", interval: str = "1d") -> pd.DataFrame:
    """
    WTI (CL=F) & Gold (GC=F) via yfinance with safe defaults.
    """
    import pandas as pd, yfinance as yf
    frames = []
    for name,tkr in {"wti":"CL=F","gold":"GC=F"}.items():
        try:
            df = yf.download(tkr, period=period, interval=interval, auto_adjust=True, progress=False, threads=False)
            if df.empty or "Close" not in df.columns: 
                continue
            part = df[["Close"]].rename(columns={"Close": name})
            part.index.name = "date"
            frames.append(part)
        except Exception as e:
            print(f"[commods] fetch failed for {tkr}:", e)
    if not frames:
        return pd.DataFrame(columns=["date","wti","gold"])
    out = pd.concat(frames, axis=1).dropna(how="all").reset_index()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    return out
