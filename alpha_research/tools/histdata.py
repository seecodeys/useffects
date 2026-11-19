"""Free 1-minute European index CFD bars from HistData.com (no signup).

Symbols: GRXEUR (DAX), FRXEUR (CAC 40), ETXEUR (Euro Stoxx 50), UKXGBP (FTSE 100).
Coverage ~2010 -> now. Timestamps are fixed EST (UTC-5, NO daylight saving), so
9:30 New York time is 09:30 EST in winter but 08:30 in this file during US DST.

The site rate-limits: after ~5 downloads the token comes back empty, so we sleep
between requests and cache every zip under data/histdata/. The current year is
published per month on the site; past years are one zip each.

    python tools/histdata.py GRXEUR 2010 2022
    df = load("GRXEUR", 2015)      # DataFrame indexed by naive EST timestamp
"""
import io
import os
import re
import sys
import time
import zipfile

import pandas as pd
import requests

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.join(HERE, "data", "histdata")


def download(sym, year, month=None, pause=20):
    os.makedirs(CACHE, exist_ok=True)
    tag = f"{sym.upper()}_{year}" + (f"{int(month):02d}" if month else "")
    out = os.path.join(CACHE, f"{tag}.zip")
    if os.path.exists(out) and os.path.getsize(out) > 1000:
        return out
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    page = f"https://www.histdata.com/download-free-forex-historical-data/?/ascii/1-minute-bar-quotes/{sym.lower()}/{year}"
    if month:
        page += f"/{int(month)}"
    for attempt in range(4):
        m = re.search(r'name="tk" id="tk" value="([^"]*)"', s.get(page, timeout=60).text)
        if m and m.group(1):
            break
        time.sleep(pause * (attempt + 2))
    else:
        raise RuntimeError(f"no download token for {tag} (rate limited?)")
    data = dict(tk=m.group(1), date=str(year), datemonth=f"{year}{int(month):02d}" if month else str(year),
                platform="ASCII", timeframe="M1", fxpair=sym.upper())
    r = s.post("https://www.histdata.com/get.php", data=data, headers={"Referer": page}, timeout=300)
    r.raise_for_status()
    if not r.content.startswith(b"PK"):
        raise RuntimeError(f"{tag}: response is not a zip")
    open(out, "wb").write(r.content)
    time.sleep(pause)
    return out


def load(sym, year, month=None):
    path = download(sym, year, month)
    with zipfile.ZipFile(path) as z:
        name = next(n for n in z.namelist() if n.endswith(".csv"))
        df = pd.read_csv(io.BytesIO(z.read(name)), sep=";", header=None,
                         names=["ts", "Open", "High", "Low", "Close", "Vol"])
    df["ts"] = pd.to_datetime(df["ts"], format="%Y%m%d %H%M%S")
    return df.set_index("ts").drop(columns="Vol")


def level_at_ny_time(sym, years, hh=9, mm=29):
    """Close of the 1-minute bar at hh:mm New York local time for each date
    (converted from the file's fixed-EST clock)."""
    df = pd.concat([load(sym, y) for y in years])
    ny = (df.index.tz_localize("Etc/GMT+5").tz_convert("America/New_York").tz_localize(None))
    df.index = ny
    sel = df[(df.index.hour == hh) & (df.index.minute == mm)]
    return pd.Series(sel["Close"].values, index=sel.index.normalize(), name=sym)


if __name__ == "__main__":
    sym, a, b = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    for y in range(a, b + 1):
        print(sym, y, download(sym, y))
