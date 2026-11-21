"""346 — London gold PM fix (15:00 London): gold weakens into the fix and recovers after it (idea card X2).

Story: benchmark physical orders (ETF creations, central banks, miners' hedges) execute at the LBMA PM fix; dealers
pre-position, so price is pushed into the fix and reverts after (Caminschi & Heaney 2014; Abrantes-Metz & Metz).
2015-03-20 the bank-panel fix was replaced by the IBA electronic auction -> prior: decay after 2015.

Data: HistData XAUUSD 1-min (cached, 2009-2022 loaded only; no 2023+). Bar stamped hh:mm close = price at hh:mm+1.
CLOCK (checked in this experiment on US/UK DST-mismatch days via the 08:30 NY data-release |1m ret| spike, same
finding as 330): years <= 2018 file clock = America/New_York local; years >= 2019 = London time minus 5h. Converted
to Europe/London accordingly. Train = 2009-2014 (data start), valid = 2015-2022. Cost 1bp/side (spot spread).

PRE-SPECIFIED VARIANTS (written before any results; one trade per London weekday with prices):
  a) Short gold 14:30 -> 15:00 London (pre-fix pressure).
  b) Long gold 15:05 -> 16:00 London (post-fix recovery).
  c) a + b combined on the same day (two round trips; second round trip's 2bp deducted inside ret).
  d) Clock placebo: on US/UK DST-mismatch days only, short the NY-clock window 09:30 -> 10:00 NY (= 13:30 -> 14:00
     London those days, when the fix is at 11:00 NY). A real fix effect follows London, so d should be ~0.
Descriptive (not new variants): a and b split by era (panel 2009..2015-03-19 vs IBA 2015-03-20..2022), a/b on
mismatch days only (real London window), yearly P&L. Gate: lib.passes_gate. No tuning; no holdout.
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

EXP = "346_gold_pm_fix"
YEARS = range(2009, 2023)
COST = 1.0


def london_minutes():
    parts = []
    for y in YEARS:
        df = histdata.load("XAUUSD", y).sort_index()
        df = df[~df.index.duplicated()]
        idx = df.index
        if y <= 2018:
            utc = idx.tz_localize("America/New_York", ambiguous="NaT", nonexistent="NaT")
        else:
            utc = (idx + pd.Timedelta(hours=5)).tz_localize("Europe/London", ambiguous="NaT", nonexistent="NaT")
        s = pd.Series(df["Close"].values, index=utc)
        s = s[s.index.notna()]
        s.index = s.index.tz_convert("Europe/London").tz_localize(None)
        parts.append(s)
    s = pd.concat(parts).sort_index()
    return s[~s.index.duplicated()].loc[:"2022-12-31"]


def at(px, dates, hh, mm, tol=3):
    """Price at hh:mm London = close of bar stamped hh:mm-1 (last bar within tol minutes)."""
    t = pd.DatetimeIndex(dates) + pd.Timedelta(hours=hh, minutes=mm - 1)
    i = px.index.searchsorted(t, side="right") - 1
    found = px.index[np.clip(i, 0, None)]
    ok = (i >= 0) & ((t - found) <= pd.Timedelta(minutes=tol))
    return pd.Series(np.where(ok, px.values[np.clip(i, 0, None)], np.nan), index=dates)


if __name__ == "__main__":
    px = london_minutes()
    days = pd.DatetimeIndex(sorted(set(px.index.normalize())))
    days = days[days.weekday < 5]
    ny = days.tz_localize("America/New_York").map(lambda x: x.utcoffset())
    ld = days.tz_localize("Europe/London").map(lambda x: x.utcoffset())
    gap = pd.Series(np.asarray((ld - ny) != pd.Timedelta(hours=5)), index=days)   # London-NY = 4h
    P = {k: at(px, days, *k) for k in [(13, 30), (14, 0), (14, 30), (15, 0), (15, 5), (16, 0)]}
    Ra = np.log(P[(15, 0)] / P[(14, 30)])
    Rb = np.log(P[(16, 0)] / P[(15, 5)])
    Rd = np.log(P[(14, 0)] / P[(13, 30)])          # = 09:30->10:00 NY on mismatch days
    one = pd.Series(1.0, index=days)
    V = {
        "a_short_1430_1500": (-one.where(Ra.notna(), 0.0), Ra),
        "b_long_1505_1600": (one.where(Rb.notna(), 0.0), Rb),
        "c_combined_a_plus_b": (one.where(Ra.notna() & Rb.notna(), 0.0), (-Ra + Rb) - 2 * COST / 1e4),
        "d_placebo_NYclock_0930_1000_gapdays": (-one.where(Rd.notna() & gap, 0.0), Rd),
    }
    rows = []
    for name, (pos, ret) in V.items():
        ret = ret.fillna(0.0)
        row = lib.register(EXP, name, pos, ret, COST, notes="XAUUSD HistData, London PM fix 15:00, 1bp/side")
        row["gate"] = lib.passes_gate(row)
        row["variant"] = name
        row["ev_train"] = int((pos.loc[:"2014"] != 0).sum())
        row["ev_valid"] = int((pos.loc["2015":"2022"] != 0).sum())
        g = lib.split_eval(pos, ret, 0.0)
        row["train_gross_bp"], row["valid_gross_bp"] = g["train_bp_per_trade"], g["valid_bp_per_trade"]
        yr = lib.daily_pnl(pos, ret, COST).groupby(pos.index.year).sum() * 1e4
        row["years_pos"] = f"{(yr > 0).sum()}/{len(yr)}"
        rows.append(row)
        print(f"\n{name}: {lib.fmt(row)} train_bp={row['train_bp_per_trade']:+.2f} "
              f"gross tr/va={row['train_gross_bp']:+.2f}/{row['valid_gross_bp']:+.2f} gate={row['gate']}")
        print("  yearly net bp sum:", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))

    print("\nDescriptive: mean signed gross bp per trade (t) by era / day type")
    def st(x):
        x = x.dropna() * 1e4
        return f"{x.mean():+.2f}bp t={x.mean() / x.std() * np.sqrt(len(x)):+.2f} n={len(x)}"
    eras = {"panel 2009-2015-03-19": (None, "2015-03-19"), "IBA 2015-03-20..2022": ("2015-03-20", "2022-12-31")}
    for lab, s in [("a short pre-fix", -Ra), ("b long post-fix", Rb), ("d placebo window (all days)", -Rd)]:
        for e, (a0, b0) in eras.items():
            print(f"  {lab:28s} {e:22s} {st(s.loc[a0:b0])}")
        print(f"  {lab:28s} {'mismatch days only':22s} {st(s[gap])}")
    out = pd.DataFrame(rows)
    cols = ["variant", "ev_train", "ev_valid", "train_t", "valid_t", "train_sharpe", "valid_sharpe",
            "train_gross_bp", "valid_gross_bp", "train_bp_per_trade", "valid_bp_per_trade", "years_pos", "gate"]
    print("\n", out[cols].round(2).to_string(index=False))
    out[cols].to_csv(os.path.join(HERE, "results", "346_gold_pm_fix.csv"), index=False)
    print("tests in registry:", lib.n_tests())
