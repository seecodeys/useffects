"""379 - Target-date-fund glide-path resets in early January.

Story: many target-date series step their equity/bond mix down once a year (often effective in January, some
quarterly). That is a mechanical, UNCONDITIONAL sale of equities / purchase of bonds, independent of past returns
(unlike 013/213/250 which fade return gaps). Prediction: bonds beat equities (vol-matched) over the first sessions of
January more than over other month-starts, and more so post-2010 as TDF AUM grew.

Spread: short 1 SPY, long h bonds, h = trailing-252d vol(SPY)/vol(bond) through the decision close (capped at 5).
Calendar-known schedule -> lag 0 (weights set at the close before session 1, held close->close).
Costs 0.5bp/side SPY, 1bp/side TLT/AGG (charged on turnover per leg).

Pre-specified variants (fixed before any results were seen):
  379a  -SPY + h*TLT, first 5 sessions of January
  379b  -SPY + h*TLT, first 10 sessions of January
  379c  -SPY + h*AGG, first 5 sessions of January
  379d  -SPY + h*TLT, first 5 sessions of Jan/Apr/Jul/Oct (quarterly glide steps)
Descriptive (not strategies): Jan vs other month-starts (first 5 sessions), by year, pre (2003-09) / post (2010-22),
component legs. Nothing after 2022-12-31 is printed.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "379_tdf_glide_january"
END = "2022-12-31"
spy, tlt, agg = lib.get("SPY"), lib.get("TLT"), lib.get("AGG")
R = pd.DataFrame({"SPY": spy.cc, "TLT": tlt.cc, "AGG": agg.cc}).loc["2002-08-01":]
idx = R.index
ym = idx.to_period("M")
sess = pd.Series(1, index=idx).groupby(ym).cumsum()          # session number within month (calendar-known)
vol = R.rolling(252, min_periods=120).std()


def weights(bond, months, nsess):
    hold = pd.Series(idx.month.isin(months) & (sess.values <= nsess), index=idx)
    on = hold.shift(-1, fill_value=False)                     # set at close t, earns t+1's return
    h = (vol.SPY / vol[bond]).clip(upper=5)
    W = pd.DataFrame(0.0, index=idx, columns=["SPY", bond])
    W.loc[on, "SPY"] = -1.0
    W.loc[on, bond] = h[on]
    return W.dropna(), hold


def tt(x):
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


spec = {"a": ("-SPY+h*TLT first 5 sessions Jan", "TLT", [1], 5, "2003-01-01"),
        "b": ("-SPY+h*TLT first 10 sessions Jan", "TLT", [1], 10, "2003-01-01"),
        "c": ("-SPY+h*AGG first 5 sessions Jan", "AGG", [1], 5, "2004-01-01"),
        "d": ("-SPY+h*TLT first 5 sessions Jan/Apr/Jul/Oct", "TLT", [1, 4, 7, 10], 5, "2003-01-01")}
COST = {"SPY": 0.5, "TLT": 1.0, "AGG": 1.0}
for k, (desc, bond, months, n, start) in spec.items():
    W, hold = weights(bond, months, n)
    Rk = R[W.columns].reindex(W.index).fillna(0.0)
    gross = lib.backtest_weights(W, Rk, 0.0, lag=0)
    tc = sum(W[c].diff().abs().fillna(W[c].abs()) * COST[c] / 1e4 for c in W.columns).shift(1).fillna(0.0)
    net = (gross - tc).loc[start:END]
    g = W.shift(1).abs().sum(axis=1).reindex(net.index).fillna(0.0)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register(EXP, f"379{k} {desc}", g, ret, 0.0, notes="TDF glide-path reset, vol-matched, lag0 calendar")
    print(f"379{k} {desc}\n   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    hm = hold.reindex(net.index).values
    ev = net[hm].groupby(net.index[hm].to_period("M")).sum()
    for nm, a, b in [("train", start[:7], "2014-12"), ("valid", "2015-01", "2022-12")]:
        s_ = ev.loc[a:b]
        print(f"   {nm}: events={len(s_)}  net bp/event={s_.mean()*1e4:+.1f}  t={tt(s_):+.2f}  pos={int((s_>0).sum())}/{len(s_)}")

# ---------------- descriptive: Jan vs other month-starts (gross vol-matched spread, first 5 sessions)
print("\nDescriptive, gross, first-5-session sums per month (bp), through 2022:")
for bond, start in [("TLT", "2003-01"), ("AGG", "2004-01")]:
    h = (vol.SPY / vol[bond]).clip(upper=5).shift(1)
    spread = (h * R[bond] - R.SPY)
    s5 = spread.where(sess <= 5).groupby(ym).sum().loc[start:"2022-12"] * 1e4
    all5 = spread.groupby(ym).mean().loc[start:"2022-12"] * 5 * 1e4      # exposure-matched: avg 5-session return
    jan = s5.index.month == 1
    for nm, a, b in [("2003/4-09", start, "2009-12"), ("2010-22", "2010-01", "2022-12"),
                     ("train", start, "2014-12"), ("valid", "2015-01", "2022-12")]:
        m = (s5.index >= pd.Period(a)) & (s5.index <= pd.Period(b))
        x, y = s5[m & jan], s5[m & ~jan]
        d = (x.mean() - y.mean()) / np.sqrt(x.var() / len(x) + y.var() / len(y))
        tv = (s5 - all5)[m & jan]
        print(f"  {bond} {nm:9s}: Jan {x.mean():+6.1f} (n={len(x)})  other months {y.mean():+6.1f} (n={len(y)})  "
              f"diff t={d:+.2f} | Jan timing value vs same-month avg {tv.mean():+6.1f} t={tt(tv):+.2f}")
    if bond == "TLT":
        print("  Jan-by-year (TLT spread, bp):", " ".join(f"{p.year % 100:02d}:{v:+.0f}" for p, v in s5[jan].items()))
        legs = pd.DataFrame({"SPY": R.SPY.where(sess <= 5).groupby(ym).sum(),
                             "TLT": R.TLT.where(sess <= 5).groupby(ym).sum()}).loc["2003-01":"2022-12"] * 1e4
        lj = legs.index.month == 1
        print(f"  legs Jan mean: SPY {legs.SPY[lj].mean():+.1f}  TLT {legs.TLT[lj].mean():+.1f} | other months: "
              f"SPY {legs.SPY[~lj].mean():+.1f}  TLT {legs.TLT[~lj].mean():+.1f}")
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
