"""Shared toolkit for the alpha research loop.

Data:     get(sym) -> daily OHLC (+ adjusted) from Yahoo, cached in data/ (not committed).
Periods:  TRAIN 2000-2014 | VALID 2015-2022 | HOLDOUT 2023+ (see PROTOCOL.md).
Scoring:  evaluate(pos, ret) and register(...) append every tested variant to
          results/registry.csv so the multiple-testing count is never lost.
"""
import os
import sys
import time
import datetime as dt

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
REGISTRY = os.path.join(HERE, "results", "registry.csv")

TRAIN = ("2000-01-01", "2014-12-31")
VALID = ("2015-01-01", "2022-12-31")
HOLDOUT = ("2023-01-01", "2100-01-01")
PERIODS = {"train": TRAIN, "valid": VALID}          # holdout only via holdout_eval()

_HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


# ------------------------------------------------------------------ data
def _fetch(sym, start="1993-01-01", interval="1d"):
    p1 = int(dt.datetime.fromisoformat(start).replace(tzinfo=dt.timezone.utc).timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    params = {"period1": p1, "period2": int(time.time()), "interval": interval, "events": "div,split"}
    for a in range(5):
        try:
            r = requests.get(url, params=params, headers=_HDR, timeout=30)
            if r.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(2 ** a)
    else:
        raise RuntimeError(f"fetch failed for {sym}")
    res = r.json()["chart"]["result"][0]
    tz = res["meta"]["exchangeTimezoneName"]
    q = res["indicators"]["quote"][0]
    adj = res["indicators"].get("adjclose", [{}])[0].get("adjclose")
    ts = pd.to_datetime(res.get("timestamp", []), unit="s", utc=True).tz_convert(tz)
    df = pd.DataFrame({"Date": ts.strftime("%Y-%m-%d") if interval == "1d" else ts.tz_localize(None),
                       "Open": q["open"], "High": q["high"], "Low": q["low"], "Close": q["close"],
                       "Volume": q["volume"]})
    if adj is not None:
        df["Adj Close"] = adj
    df = df.dropna(subset=["Open", "Close"]).drop_duplicates("Date", keep="last")
    return df.sort_values("Date").reset_index(drop=True)


def get(sym, refresh=False, drop_today=True):
    """Daily bars indexed by local exchange date. Adds adjusted O/H/L/C columns
    (aO, aH, aL, aC) using Yahoo's Adj Close factor (splits + dividends)."""
    os.makedirs(DATA, exist_ok=True)
    path = os.path.join(DATA, sym.replace("^", "").replace("=", "_") + ".csv")
    if refresh or not os.path.exists(path):
        _fetch(sym).to_csv(path, index=False)
        time.sleep(0.3)
    df = pd.read_csv(path, parse_dates=["Date"]).set_index("Date")
    if drop_today:
        now = pd.Timestamp.now(tz="America/New_York")
        today = now.normalize().tz_localize(None)
        if now.hour * 60 + now.minute < 16 * 60 + 30:      # today's bar is live/partial until after the close
            df = df[df.index < today]
    f = df["Adj Close"] / df["Close"] if "Adj Close" in df else 1.0
    for c in ["Open", "High", "Low", "Close"]:
        df["a" + c[0]] = df[c] * f
    df["oc"] = df["Close"] / df["Open"] - 1                  # intraday return (target)
    df["gap"] = df["aO"] / df["aC"].shift(1) - 1               # overnight, div-adjusted
    df["cc"] = df["aC"] / df["aC"].shift(1) - 1
    return df


def asof(series, dates, strict=False):
    s = series.dropna()
    idx = s.index.searchsorted(dates, side="left" if strict else "right") - 1
    vals = np.where(idx >= 0, s.values[np.clip(idx, 0, None)], np.nan)
    return pd.Series(vals, index=dates)


# --------------------------------------------------------------- scoring
def evaluate(pos, ret, cost_bp=0.5):
    """pos: target exposure per day (e.g. -1..1), ret: open->close return.
    Cost charged per side on |pos|. Returns dict of metrics."""
    pos, ret = pos.reindex(ret.index).fillna(0), ret.fillna(0)
    r = pos * ret - pos.abs() * 2 * cost_bp / 1e4
    n = len(r)
    sd = r.std()
    traded = pos != 0
    return {
        "n_days": n,
        "traded_%": 100 * traded.mean(),
        "ann_ret_%": 100 * r.mean() * 252,
        "sharpe": r.mean() / sd * np.sqrt(252) if sd > 0 else np.nan,
        "t": r.mean() / sd * np.sqrt(n) if sd > 0 else np.nan,
        "bp_per_trade": 1e4 * r[traded].mean() / pos[traded].abs().mean() if traded.any() else np.nan,
        "hit_%": 100 * (np.sign(pos[traded]) == np.sign(ret[traded])).mean() if traded.any() else np.nan,
        "max_dd_%": 100 * ((1 + r).cumprod() / (1 + r).cumprod().cummax() - 1).min(),
    }


def daily_pnl(pos, ret, cost_bp=0.5):
    pos = pos.reindex(ret.index).fillna(0)
    return pos * ret.fillna(0) - pos.abs() * 2 * cost_bp / 1e4


def split_eval(pos, ret, cost_bp=0.5):
    out = {}
    for name, (a, b) in PERIODS.items():
        m = evaluate(pos.loc[a:b], ret.loc[a:b], cost_bp)
        out.update({f"{name}_{k}": v for k, v in m.items()})
    return out


def register(experiment, variant, pos, ret, cost_bp=0.5, notes=""):
    """Evaluate on train+valid and append to the registry. Every variant you
    look at must go through here (that's how the test count stays honest)."""
    row = {"timestamp": dt.datetime.now().isoformat(timespec="seconds"),
           "experiment": experiment, "variant": variant, "cost_bp_side": cost_bp,
           **split_eval(pos, ret, cost_bp), "notes": notes}
    df = pd.DataFrame([row])
    os.makedirs(os.path.dirname(REGISTRY), exist_ok=True)
    if os.path.exists(REGISTRY):
        old = pd.read_csv(REGISTRY)
        df = pd.concat([old, df], ignore_index=True)
    df.to_csv(REGISTRY, index=False)
    return row


def passes_gate(row):
    """Promotion gate to be allowed a single holdout look (see PROTOCOL.md)."""
    return (row["train_t"] >= 2.0 and row["valid_t"] >= 2.0 and row["valid_sharpe"] >= 0.5
            and row["valid_bp_per_trade"] >= 1.0)


def holdout_eval(experiment, variant, pos, ret, cost_bp=0.5):
    """ONE look at 2023+. Only for variants that passed the gate. Logged."""
    m = evaluate(pos.loc[HOLDOUT[0]:HOLDOUT[1]], ret.loc[HOLDOUT[0]:HOLDOUT[1]], cost_bp)
    path = os.path.join(HERE, "results", "holdout_log.csv")
    row = pd.DataFrame([{"timestamp": dt.datetime.now().isoformat(timespec="seconds"),
                         "experiment": experiment, "variant": variant, **m}])
    if os.path.exists(path):
        row = pd.concat([pd.read_csv(path), row], ignore_index=True)
    row.to_csv(path, index=False)
    return m


def n_tests():
    return len(pd.read_csv(REGISTRY)) if os.path.exists(REGISTRY) else 0


def fmt(row, keys=("train_sharpe", "train_t", "valid_sharpe", "valid_t", "valid_bp_per_trade", "valid_traded_%")):
    return "  ".join(f"{k}={row[k]:+.2f}" for k in keys)
