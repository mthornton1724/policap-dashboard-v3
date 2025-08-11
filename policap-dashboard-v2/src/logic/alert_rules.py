import pandas as pd

def detect_ai_infra_opportunity(events_df: pd.DataFrame, trades_df: pd.DataFrame):
    if events_df is None or events_df.empty:
        return None
    ai_events = events_df[events_df['title'].str.contains('AI|data center', case=False, na=False)]
    if ai_events.empty:
        return None

    if trades_df is None or trades_df.empty or 'trade_date' not in trades_df.columns:
        return None

    tdf = trades_df.copy()
    tdf['trade_date'] = pd.to_datetime(tdf['trade_date'], errors='coerce')
    tdf = tdf.dropna(subset=['trade_date'])
    semi_trades = tdf[tdf['sector'].fillna('').str.contains('semi', case=False, na=False)]
    if semi_trades.empty:
        return None

    ai_dates = pd.to_datetime(ai_events['date'], errors='coerce').dropna()
    if ai_dates.empty:
        return None

    start = ai_dates.min() - pd.Timedelta(days=7)
    end   = ai_dates.max() + pd.Timedelta(days=7)
    hits = semi_trades[(semi_trades['trade_date'] >= start) & (semi_trades['trade_date'] <= end)]
    if hits.empty:
        return None

    return {
        "theme": "AI Infrastructure",
        "confidence": 0.80,
        "why": "AI/data-center policy + congressional semiconductor buys in ±7d window.",
        "suggest": "Consider broad AI/semiconductor ETF exposure for small accounts."
    }

def detect_energy_easing(events_df: pd.DataFrame, commods_df: pd.DataFrame):
    if events_df is None or events_df.empty or commods_df is None or commods_df.empty:
        return None

    ru = events_df[events_df['title'].str.contains('Putin|Russia|Ukraine', case=False, na=False)]
    if ru.empty:
        return None

    t = pd.to_datetime(ru['date'], errors='coerce').dropna()
    if t.empty:
        return None
    t = t.max()

    cdf = commods_df.copy()
    cdf['date'] = pd.to_datetime(cdf['date'], errors='coerce')
    cdf = cdf.dropna(subset=['date'])
    if 'wti' not in cdf.columns:
        return None
    window = cdf[(cdf['date'] >= t - pd.Timedelta(days=14)) & (cdf['date'] <= t)]
    if window.empty:
        return None

    wti_start = window.iloc[0]['wti']
    wti_end   = window.iloc[-1]['wti']
    if pd.isna(wti_start) or pd.isna(wti_end) or wti_start <= 0:
        return None

    drop = (wti_start - wti_end) / wti_start
    if drop < 0.05:
        return None

    return {
        "theme": "Energy Easing",
        "confidence": 0.65,
        "why": "Geopolitical de-escalation + >5% 2-week WTI drawdown.",
        "suggest": "Consider trimming energy overweight or rotating toward rate-sensitives."
    }

def detect_tariff_shock(events_df: pd.DataFrame):
    if events_df is None or events_df.empty:
        return None
    t_events = events_df[events_df['title'].str.contains('tariff|de minimis|reciprocal', case=False, na=False)]
    if t_events.empty:
        return None

    e = t_events.iloc[0]
    return {
        "theme": "Tariff Shock",
        "confidence": 0.60,
        "why": f"Recent trade action: '{e['title']}'. Historically pressures import-heavy retail/logistics.",
        "suggest": "Consider hedging retail/logistics; watch domestic manufacturers."
    }

def run_all_alerts(events_df: pd.DataFrame, trades_df: pd.DataFrame, commods_df: pd.DataFrame):
    alerts = []

    try:
        out = detect_ai_infra_opportunity(events_df.copy() if events_df is not None else events_df,
                                          trades_df.copy() if trades_df is not None else trades_df)
        if out: alerts.append(out)
    except Exception as e:
        print(f"[alerts] detect_ai_infra_opportunity error: {e}")

    try:
        out = detect_energy_easing(events_df.copy() if events_df is not None else events_df,
                                   commods_df.copy() if commods_df is not None else commods_df)
        if out: alerts.append(out)
    except Exception as e:
        print(f"[alerts] detect_energy_easing error: {e}")

    try:
        out = detect_tariff_shock(events_df.copy() if events_df is not None else events_df)
        if out: alerts.append(out)
    except Exception as e:
        print(f"[alerts] detect_tariff_shock error: {e}")

    uniq = {a['theme']: a for a in alerts}
    return list(uniq.values())
