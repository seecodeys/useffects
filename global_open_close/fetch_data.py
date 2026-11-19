"""Download raw daily OHLC data from Yahoo Finance for the global open-close study.

Dates are stamped in each exchange's *local* timezone (from Yahoo's metadata),
which matters for aligning Asian / European sessions to the US session.
Prices are left unrounded and unadjusted (Yahoo 'adjclose' is saved separately).
"""
import os
import time
import datetime as dt

import pandas as pd
import requests

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

TICKERS = {
    # Traded instrument
    "SPY": "SPDR S&P 500 ETF",
    "^VIX": "CBOE Volatility Index",
    # Asia-Pacific (close well before 9:30 ET)
    "^N225": "Nikkei 225 (Japan)",
    "^HSI": "Hang Seng (Hong Kong)",
    "^AXJO": "S&P/ASX 200 (Australia)",
    "^KS11": "KOSPI (South Korea)",
    "000001.SS": "Shanghai Composite (China)",
    "^TWII": "TAIEX (Taiwan)",
    "^STI": "Straits Times (Singapore)",
    # Europe (open ~3-4am ET, close 11:30 ET -> close is look-ahead!)
    "^FTSE": "FTSE 100 (UK)",
    "^GDAXI": "DAX (Germany)",
    "^FCHI": "CAC 40 (France)",
    "^STOXX50E": "Euro Stoxx 50",
    # US-listed country ETFs: open at 9:30 ET alongside SPY, so their opening
    # prints carry Europe/Asia pricing *as of the US open* (near-simultaneous
    # with entry -> treated as a best-case / upper-bound feature set)
    "EWJ": "iShares MSCI Japan",
    "EWG": "iShares MSCI Germany",
    "EWU": "iShares MSCI United Kingdom",
    "EZU": "iShares MSCI Eurozone",
    "FXI": "iShares China Large-Cap",
    "EWH": "iShares MSCI Hong Kong",
    "EWY": "iShares MSCI South Korea",
    "EWA": "iShares MSCI Australia",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def fetch(symbol, start="1995-01-01"):
    p1 = int(dt.datetime.fromisoformat(start).replace(tzinfo=dt.timezone.utc).timestamp())
    p2 = int(time.time())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"period1": p1, "period2": p2, "interval": "1d", "events": "div,split"}
    for attempt in range(4):
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            break
        time.sleep(2 ** attempt)
    r.raise_for_status()
    res = r.json()["chart"]["result"][0]
    tz = res["meta"]["exchangeTimezoneName"]
    q = res["indicators"]["quote"][0]
    adj = res["indicators"].get("adjclose", [{}])[0].get("adjclose")
    df = pd.DataFrame({
        "Date": pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert(tz).strftime("%Y-%m-%d"),
        "Open": q["open"], "High": q["high"], "Low": q["low"], "Close": q["close"],
        "Volume": q["volume"],
    })
    if adj is not None:
        df["Adj Close"] = adj
    df = df.dropna(subset=["Open", "Close"]).drop_duplicates("Date", keep="last")
    return df.sort_values("Date").reset_index(drop=True), tz


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    for sym, name in TICKERS.items():
        df, tz = fetch(sym)
        df.to_csv(os.path.join(DATA_DIR, f"{sym.replace('^', '')}.csv"), index=False)
        print(f"{sym:<10} {name:<28} {tz:<20} {df['Date'].iloc[0]} -> {df['Date'].iloc[-1]}  rows={len(df)}")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
