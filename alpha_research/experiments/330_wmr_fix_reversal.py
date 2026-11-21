"""330 — Daily London 4pm WM/Reuters fix: pre-fix run, post-fix reversal (idea card B2).

Story: asset managers/custodians send benchmark FX orders "at the fix"; banks pre-hedge them in the 15-30 min before
16:00 London, pushing price; once the flow is done, price partly reverts. Not 197 (that was a month-end-only
directional move across the fix); here we fade the pre-fix run every day except month-ends.

Data: HistData 1-min USDJPY and AUDUSD (cached), 2001-2022 only (no 2023+ is loaded). EURUSD/GBPUSD not downloaded
(rate limits). Prices: close of the 1-min bar stamped hh:mm (bar open time) = price at hh:mm+1.

CLOCK (checked empirically in this experiment, see find_clock() notes in results/agent_330.md): the FX files are NOT
fixed EST. Weekly Sunday-open / Friday-close bars and the 08:30 NFP spike show
  * years <= 2018: file clock = America/New_York local time (US DST);
  * years >= 2019: file clock = UTC-5 plus EU summer time, i.e. London time minus 5h.
We convert each year accordingly to Europe/London local time and verify that the mean |1-min return| spikes at the
16:00 London minute on all days AND on the US/UK DST-mismatch weeks.

PRE-SPECIFIED VARIANTS (written before any results; pooled equal-weight across USDJPY and AUDUSD; cost 0.5bp/side):
  a) R = ln P(16:00)/P(15:30) London. Every non-month-end London business day: position -sign(R), held 16:00->16:30.
  b) As (a), only when |R| > sigma, sigma = trailing 60-day std of R (prior days only).
  c) Month-end days only (last trading day of the calendar month): -sign(R), 16:00->16:30.
  d) Shorter: R = 15:45->16:00, hold 16:00->16:15, non-month-end days.
Gate: lib.passes_gate. No tuning; no holdout.
"""
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "tools"))
import lib  # noqa: E402
import histdata  # noqa: E402

EXP = "330_wmr_fix_reversal"
PAIRS = ["USDJPY", "AUDUSD"]
YEARS = range(2001, 2023)


def london_minutes(sym):
    parts = []
    for y in YEARS:
        df = histdata.load(sym, y).sort_index()
        df = df[~df.index.duplicated()]
        idx = df.index
        if y <= 2018:
            utc = idx.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT")
        else:  # London time minus 5h
            utc = (idx + pd.Timedelta(hours=5)).tz_localize("Europe/London", ambiguous="NaT", nonexistent="NaT")
        s = pd.Series(df["Close"].values, index=utc)
        s = s[s.index.notna()]
        s.index = s.index.tz_convert("Europe/London").tz_localize(None)
        parts.append(s)
    s = pd.concat(parts).sort_index()
    return s[~s.index.duplicated()]


def at(px, dates, hh, mm, tol=3):
    """Close of the bar stamped hh:mm-1 London (= price at hh:mm), last bar within tol minutes."""
    t = pd.DatetimeIndex(dates) + pd.Timedelta(hours=hh, minutes=mm - 1)
    i = px.index.searchsorted(t, side="right") - 1
    found = px.index[np.clip(i, 0, None)]
    ok = (i >= 0) & ((t - found) <= pd.Timedelta(minutes=tol))
    v = np.where(ok, px.values[np.clip(i, 0, None)], np.nan)
    return pd.Series(v, index=dates)


def clock_check(px, sym):
    """Mean |1-min log return| by London minute 15:50-16:10, all days and UK/US DST-mismatch weeks only."""
    r = np.log(px).diff().abs()
    r = r[(r.index.to_series().diff() == pd.Timedelta(minutes=1)).values]
    r = r.loc[:"2022-12-31"]
    hm = r.index.hour * 100 + r.index.minute
    keep = (hm >= 1550) & (hm <= 1610)
    rr = r[keep]
    d = rr.index.normalize()
    ny_off = d.tz_localize("America/New_York").map(lambda x: x.utcoffset())
    ld_off = d.tz_localize("Europe/London").map(lambda x: x.utcoffset())
    gap = np.asarray((ld_off - ny_off) != pd.Timedelta(hours=5))
    hm2 = rr.index.hour * 100 + rr.index.minute
    tab = pd.DataFrame({"all": rr.groupby(hm2).mean(), "dst_gap": rr[gap].groupby(hm2[gap]).mean()}) * 1e4
    print(f"\n{sym} clock check: mean |1m ret| bp by London bar-start minute (bar 15:59 covers 15:59-16:00)")
    print(tab.round(2).T.to_string())
    return tab


def build(px):
    days = pd.DatetimeIndex(sorted(set(px.index.normalize())))
    days = days[days.weekday < 5]
    p = {k: at(px, days, *k) for k in [(15, 30), (15, 45), (16, 0), (16, 15), (16, 30)]}
    df = pd.DataFrame({f"p{h}{m:02d}": v for (h, m), v in p.items()})
    df = df.dropna(subset=["p1530", "p1600", "p1630"])
    df["R30"] = np.log(df.p1600 / df.p1530)
    df["F30"] = np.log(df.p1630 / df.p1600)
    df["R15"] = np.log(df.p1600 / df.p1545)
    df["F15"] = np.log(df.p1615 / df.p1600)
    ym = df.index.to_period("M")
    df["month_end"] = df.index == pd.Series(df.index, index=df.index).groupby(ym).transform("max")
    df["sig"] = df["R30"].rolling(60, min_periods=40).std().shift(1)
    return df


def pooled(dfs, sigfn, retcol):
    """Equal-weight across pairs that trade the day. Returns (pos, ret) with pos=1 when any pair trades."""
    contrib = []
    for d in dfs.values():
        s = sigfn(d)
        contrib.append((s * d[retcol]).where(s != 0))
    c = pd.concat(contrib, axis=1)
    ret = c.mean(axis=1)                          # average of traded pairs' signed returns
    idx = c.index
    pos = pd.Series(np.where(c.notna().any(axis=1), 1.0, 0.0), index=idx)
    return pos, ret.fillna(0.0), c.notna().sum(axis=1)


if __name__ == "__main__":
    dfs = {}
    for sym in PAIRS:
        px = london_minutes(sym)
        clock_check(px, sym)
        dfs[sym] = build(px).loc[:"2022-12-31"]
        print(sym, "days", len(dfs[sym]), dfs[sym].index[0].date(), dfs[sym].index[-1].date())

    V = {
        "a_daily_nonME_30m": (lambda d: (-np.sign(d.R30)).where(~d.month_end, 0.0), "F30"),
        "b_nonME_abs_gt_1sig": (lambda d: (-np.sign(d.R30)).where(~d.month_end & (d.R30.abs() > d.sig), 0.0), "F30"),
        "c_monthend_only_30m": (lambda d: (-np.sign(d.R30)).where(d.month_end, 0.0), "F30"),
        "d_nonME_15m": (lambda d: (-np.sign(d.R15)).where(~d.month_end, 0.0).fillna(0.0), "F15"),
    }
    rows = []
    for name, (fn, rc) in V.items():
        pos, ret, npair = pooled(dfs, lambda d: fn(d).fillna(0.0), rc)
        row = lib.register(EXP, name, pos, ret, 0.5, notes="pooled USDJPY+AUDUSD, London 16:00 fix, fade pre-fix run")
        row["gate"] = lib.passes_gate(row)
        row["events_train"] = int((pos.loc[:"2014"] != 0).sum())
        row["events_valid"] = int((pos.loc["2015":"2022"] != 0).sum())
        e1 = lib.split_eval(pos, ret, 1.0)
        row["valid_bp_1bp"] = e1["valid_bp_per_trade"]
        row["train_bp_1bp"] = e1["train_bp_per_trade"]
        g = lib.split_eval(pos, ret, 0.0)
        row["train_gross_bp"], row["valid_gross_bp"] = g["train_bp_per_trade"], g["valid_bp_per_trade"]
        yr = lib.daily_pnl(pos, ret, 0.5).groupby(pos.index.year).sum() * 1e4
        row["years_pos"] = f"{(yr > 0).sum()}/{len(yr)}"
        rows.append(row)
        print(f"\n{name}: {lib.fmt(row)} train_bp={row['train_bp_per_trade']:+.2f} "
              f"gross tr/va={row['train_gross_bp']:+.2f}/{row['valid_gross_bp']:+.2f} gate={row['gate']}")
        print("  yearly net bp sum:", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))
    out = pd.DataFrame(rows)
    cols = ["variant", "events_train", "events_valid", "train_t", "valid_t", "train_sharpe", "valid_sharpe",
            "train_gross_bp", "valid_gross_bp", "train_bp_per_trade", "valid_bp_per_trade", "valid_bp_1bp",
            "years_pos", "gate"]
    print("\n", out[cols].round(2).to_string(index=False))
    out[cols].to_csv(os.path.join(HERE, "results", "330_wmr_fix_reversal.csv"), index=False)
    print("tests in registry:", lib.n_tests())
