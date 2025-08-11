# Political Capital Flow Dashboard (v2)

A stable Streamlit app that turns **policy + congressional trades + macro** into simple **alerts** and charts.

## Quick Start
```bash
cd policap-dashboard-v2
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config.sample.env .env    # optional; add QUIVER_API_KEY if you have one
streamlit run app.py
```

- In the sidebar, flip **Live Mode** ON for live data.
- UI will show warnings instead of crashing if a feed returns bad/empty data.

## Live Data Sources
- **Executive Orders:** Federal Register API
- **Congressional Trades (optional):** QuiverQuant API (set `QUIVER_API_KEY` in `.env`)
- **Treasury Yields:** FRED CSV (robust header normalization) with Yahoo fallback
- **Commodities:** Yahoo Finance (`CL=F` WTI, `GC=F` Gold) via `yfinance`
