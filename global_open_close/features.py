"""Build a look-ahead-free feature table: one row per US trading day D, holding
only information available at the 9:30 ET open of D, plus the target
(SPY open -> close return on D).

Timing rules (all relative to the previous US close P at 16:00 ET):

  Asia-Pacific indices (Tokyo, HK, Seoul, Taipei, Shanghai, Sydney, Singapore)
      Their session on local date D runs ~19:00 ET (D-1) -> ~04:00 ET (D),
      i.e. entirely between the US close on P and the US open on D.
      level at US open (D)  = close of last session with local date <= D
      level at US close (P) = close of last session with local date <= P
      -> return covers every Asian session since the last US close, which
         also handles US-only and Asia-only holidays.

  European indices (DAX, CAC, Euro Stoxx 50)
      Open ~03:00 ET, close 11:30 ET -> the daily CLOSE of D is look-ahead.
      level at US open (D)  = OPEN of session D if it exists, else last close < D
      level at US close (P) = close of last session with local date <= P
      FTSE 100 is excluded: Yahoo's Open for it is stale (= prior close).

  US-listed country ETFs (EWG, EZU, EWU, EWJ, FXI, ...)
      Open at 9:30 ET alongside SPY, so they embed European/Asian pricing as of
      the US open. Their opening print is *simultaneous* with entry, so these
      features are flagged as a best-case upper bound (in live trading you'd
      use pre-market indications instead).

SPY returns are dividend-adjusted using Yahoo's Adj Close factor, so ex-dividend
days don't show up as spurious negative gaps.
"""
import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")

ASIA = ["N225", "HSI", "KS11", "TWII", "000001.SS", "AXJO", "STI"]
EUROPE = ["GDAXI", "FCHI", "STOXX50E"]
ETFS = ["EWG", "EZU", "EWU", "EWJ", "FXI", "EWH", "EWY", "EWA"]


def load(sym):
    df = pd.read_csv(os.path.join(DATA_DIR, f"{sym}.csv"), parse_dates=["Date"])
    return df.sort_values("Date").set_index("Date")


def drop_incomplete_today(df):
    """Yahoo returns a live, partial bar for today's session; drop it."""
    today_ny = pd.Timestamp.now(tz="America/New_York").normalize().tz_localize(None)
    return df[df.index < today_ny]


def adjusted(df):
    f = (df["Adj Close"] / df["Close"]) if "Adj Close" in df else 1.0
    out = df[["Open", "High", "Low", "Close"]].mul(f, axis=0)
    return out


def asof(series, dates):
    """Value of the last observation with index <= each date (NaN if none)."""
    s = series.dropna()
    idx = s.index.searchsorted(dates, side="right") - 1
    vals = np.where(idx >= 0, s.values[np.clip(idx, 0, None)], np.nan)
    return pd.Series(vals, index=dates)


def asof_strict(series, dates):
    """Value of the last observation with index < each date."""
    s = series.dropna()
    idx = s.index.searchsorted(dates, side="left") - 1
    vals = np.where(idx >= 0, s.values[np.clip(idx, 0, None)], np.nan)
    return pd.Series(vals, index=dates)


def asia_return(sym, D, P):
    c = load(sym)["Close"]
    return asof(c, D).values / asof(c, P).values - 1


def europe_return(sym, D, P):
    df = load(sym)
    at_open = df["Open"].reindex(D)  # session on D (NaN if Europe closed)
    at_open = at_open.fillna(asof_strict(df["Close"], D))
    return at_open.values / asof(df["Close"], P).values - 1


def etf_gap(sym, D):
    a = adjusted(drop_incomplete_today(load(sym)))
    gap = a["Open"] / a["Close"].shift(1) - 1
    return gap.reindex(D).values


def build(start="2000-01-01"):
    spy = adjusted(drop_incomplete_today(load("SPY")))
    raw = drop_incomplete_today(load("SPY"))

    feat = pd.DataFrame(index=spy.index)
    feat["prev_date"] = feat.index.to_series().shift(1)
    # Target: raw same-day open->close (no dividend effect intraday)
    feat["y"] = raw["Close"] / raw["Open"] - 1
    feat["spy_high_ret"] = raw["High"] / raw["Open"] - 1
    feat["spy_low_ret"] = raw["Low"] / raw["Open"] - 1
    # Known at the open
    feat["spy_gap"] = spy["Open"] / spy["Close"].shift(1) - 1
    feat["spy_prev_ret"] = spy["Close"].shift(1) / spy["Close"].shift(2) - 1
    feat["spy_prev_intraday"] = (raw["Close"] / raw["Open"] - 1).shift(1)
    feat["vix_prev"] = asof(load("VIX")["Close"], feat["prev_date"]).values

    feat = feat.iloc[2:]
    D, P = feat.index, pd.DatetimeIndex(feat["prev_date"])

    for s in ASIA:
        feat[f"asia_{s}"] = asia_return(s, D, P)
    for s in EUROPE:
        feat[f"eu_{s}"] = europe_return(s, D, P)
    for s in ETFS:
        feat[f"etf_{s}"] = etf_gap(s, D)

    asia_cols = [f"asia_{s}" for s in ASIA]
    eu_cols = [f"eu_{s}" for s in EUROPE]
    feat["asia_avg"] = feat[asia_cols].mean(axis=1)
    feat["eu_avg"] = feat[eu_cols].mean(axis=1)
    feat["global_avg"] = feat[["asia_avg", "eu_avg"]].mean(axis=1)
    feat["etf_eu_avg"] = feat[["etf_EWG", "etf_EZU", "etf_EWU"]].mean(axis=1)
    feat["etf_asia_avg"] = feat[["etf_EWJ", "etf_FXI", "etf_EWH", "etf_EWY", "etf_EWA"]].mean(axis=1)

    feat = feat[feat.index >= start].drop(columns="prev_date")
    feat = feat.dropna(subset=["y", "spy_gap", "spy_prev_ret", "asia_avg", "eu_avg"])
    return feat


if __name__ == "__main__":
    f = build()
    f.to_csv(os.path.join(HERE, "data", "features.csv"))
    print(f.shape, f.index.min().date(), "->", f.index.max().date())
    print(f[["y", "spy_gap", "asia_avg", "eu_avg", "etf_eu_avg"]].describe().round(4).to_string())
