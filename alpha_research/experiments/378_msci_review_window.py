"""378 - MSCI index-review windows: close-in pressure and post-effective reversal, review month-ends vs other month-ends.

Story: MSCI quarterly (Feb/Aug) and semi-annual (May/Nov, the big ones) reviews are implemented at the close of the last
business day of the month. Passive EAFE/EM/ACWI trackers trade at that close; arbitrageurs pre-position in the sessions
before and unwind after. If that leaves an aggregate footprint, it should show in the EM/EAFE vs US spread in the last 3
sessions (pressure) and in the 1-2 sessions after (reversal), and it should be stronger in review month-ends than in
ordinary month-ends (which carry the 013-type stock-bond and 017 SPY-EFA rebalance flows). Extends 178, which tested
only review-day open->close and close->next open vs SPY (dead).

Event day D = last US trading day of the month (SPY calendar). Spreads are beta-hedged: X - beta*SPY with beta = 250-day
rolling close->close beta of X on SPY, measured up to the entry close (no look-ahead). Calendar-known -> lag 0.
Costs: 2bp/side EEM, 1bp/side EFA and SPY (turnover-based via lib.backtest_weights per leg).
Direction for A-C = sign of the TRAIN mean of the event spread (as in 178); D uses B's sign.
Per-event net P&L is booked on the exit day (pos=1 there) so registry bp/trade = net bp per event.

Pre-specified variants (fixed before any results were seen):
  A  EEM-b*SPY, close D-3 -> close D (last 3 sessions, pressure), all review months Feb/May/Aug/Nov.
  B  EEM-b*SPY, close D -> close D+2 (reversal), semi-annual May/Nov only.
  C  EFA-b*SPY, close D -> close D+2 (reversal), May/Nov only.
  D  placebo/control: B's trade (same sign) on NON-review month-ends (Jan/Mar/Apr/Jun/Jul/Sep/Oct/Dec).
     B minus D (Welch t on per-event net) isolates the MSCI part from the generic month-end effect.
VWO is not used as a traded variant (it switched from MSCI EM to FTSE EM in 2013, so it is not a clean MSCI tracker;
keeping within 4 variants). Nothing after 2022-12-31 is printed.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "378_msci_review_window"
END = "2022-12-31"
COST = {"EEM": 2.0, "EFA": 1.0, "SPY": 1.0}

px = {s: lib.get(s) for s in ["SPY", "EEM", "EFA"]}
idx = px["SPY"].index
for s in ["EEM", "EFA"]:
    idx = idx.intersection(px[s].index)
idx = idx[idx >= "2003-06-01"]
R = pd.DataFrame({s: px[s]["cc"].reindex(idx) for s in px}).fillna(0.0)

ym = idx.to_period("M")
pos_in_month = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
is_last = (pos_in_month == 1) & pd.Series(ym != ym[-1], index=idx)
last_days = idx[is_last.values]
ipos = {d: i for i, d in enumerate(idx)}


def beta(x):
    c = R[x].rolling(250).cov(R["SPY"]); v = R["SPY"].rolling(250).var()
    return (c / v).shift(0)          # measured through close t (the entry close) -> known at entry


B = {x: beta(x) for x in ["EEM", "EFA"]}


def event_series(x, start_off, end_off, months):
    """Per-event gross spread return and net (sign applied later). Window: close(D+start_off) -> close(D+end_off)."""
    rows = []
    for d in last_days:
        if d.month not in months:
            continue
        i = ipos[d]; a, b = i + start_off, i + end_off
        if a < 0 or b >= len(idx):
            continue
        bt = B[x].iloc[a]
        if not np.isfinite(bt):
            continue
        rows.append((idx[b], d, a, b, bt))
    return rows


def build(x, start_off, end_off, months, sign):
    """Weights decided at close a, held through close b; per-leg costs via backtest_weights; P&L booked on exit day."""
    rows = event_series(x, start_off, end_off, months)
    Wx = pd.Series(0.0, index=idx); Ws = pd.Series(0.0, index=idx)
    for (exit_d, d, a, b, bt) in rows:
        Wx.iloc[a:b] = sign; Ws.iloc[a:b] = -sign * bt
    net_daily = (lib.backtest_weights(Wx.to_frame(x), R[[x]], COST[x], lag=0)
                 + lib.backtest_weights(Ws.to_frame("SPY"), R[["SPY"]], COST["SPY"], lag=0))
    ev = pd.Series({exit_d: net_daily.iloc[a + 1:b + 2].sum() for (exit_d, d, a, b, bt) in rows})
    gross = pd.Series({exit_d: (sign * (np.prod(1 + R[x].iloc[a + 1:b + 1]) - 1)
                                - sign * bt * (np.prod(1 + R["SPY"].iloc[a + 1:b + 1]) - 1))
                       for (exit_d, d, a, b, bt) in rows})
    # timing value: subtract the same-sign unconditional mean of this window-length hedged spread (all days, same period)
    L = end_off - start_off
    sx = (1 + R[x]).rolling(L).apply(np.prod, raw=True) - 1
    ss = (1 + R["SPY"]).rolling(L).apply(np.prod, raw=True) - 1
    uncond = sign * (sx - B[x].shift(L) * ss)
    return ev.sort_index(), gross.sort_index(), uncond


def train_sign(x, s0, s1, months):
    _, g, _ = build(x, s0, s1, months, +1)
    return 1.0 if g.loc[:lib.TRAIN[1]].mean() >= 0 else -1.0


REV = [2, 5, 8, 11]; SEMI = [5, 11]; OTHER = [1, 3, 4, 6, 7, 9, 10, 12]
sA = train_sign("EEM", -3, 0, REV)
sB = train_sign("EEM", 0, 2, SEMI)
sC = train_sign("EFA", 0, 2, SEMI)
spec = {
    "A EEM-bSPY last3 pressure (Feb/May/Aug/Nov)": ("EEM", -3, 0, REV, sA),
    "B EEM-bSPY D->D+2 reversal (May/Nov)": ("EEM", 0, 2, SEMI, sB),
    "C EFA-bSPY D->D+2 reversal (May/Nov)": ("EFA", 0, 2, SEMI, sC),
    "D placebo: B trade on non-review month-ends": ("EEM", 0, 2, OTHER, sB),
}
res = {}
print(f"{'variant':<46} sign  n_tr n_va  train_t valid_t valid_Sh  va_bp/ev  tr_bp/ev  timing_t(tr/va) gate")
for name, (x, s0, s1, months, sg) in spec.items():
    ev, gross, uncond = build(x, s0, s1, months, sg)
    ev = ev.loc[:END]
    full = pd.Series(0.0, index=idx[idx <= END]); full.loc[ev.index] = ev.values
    pos = pd.Series(0.0, index=full.index); pos.loc[ev.index] = 1.0
    r = lib.register(EXP, f"378 {name} sign{int(sg):+d}", pos, full, 0.0,
                     notes=f"per-event net on exit day; costs EEM2/EFA1/SPY1 bp/side; lag0 calendar")
    tv = {}
    for per, (a, b) in lib.PERIODS.items():
        e = ev.loc[a:b]; u = uncond.loc[a:b].dropna().mean()
        t = (e - u)
        tv[per] = t.mean() / t.std() * np.sqrt(len(t)) if len(t) > 2 else np.nan
    res[name] = ev
    print(f"{name:<46} {int(sg):+d}  {len(ev.loc[:lib.TRAIN[1]]):4d} {len(ev.loc[lib.VALID[0]:]):4d}  "
          f"{r['train_t']:+6.2f} {r['valid_t']:+6.2f} {r['valid_sharpe']:+7.2f}  {r['valid_bp_per_trade']:+8.1f}  "
          f"{r['train_bp_per_trade']:+8.1f}  {tv['train']:+5.2f}/{tv['valid']:+5.2f}  {lib.passes_gate(r)}")

# B vs D: MSCI semi-annual review month-ends vs ordinary month-ends (same trade, same sign)
print("\nB minus placebo D (Welch t on per-event net bp):")
for per, (a, b) in lib.PERIODS.items():
    b_, d_ = res[list(spec)[1]].loc[a:b] * 1e4, res[list(spec)[3]].loc[a:b] * 1e4
    t = (b_.mean() - d_.mean()) / np.sqrt(b_.var() / len(b_) + d_.var() / len(d_))
    print(f"  {per}: B {b_.mean():+.1f}bp (n={len(b_)})  D {d_.mean():+.1f}bp (n={len(d_)})  diff t={t:+.2f}")

print("\nYearly net bp sums (<=2022):")
yr = pd.DataFrame({k[:1]: v.groupby(v.index.year).sum() * 1e4 for k, v in res.items()}).round(0)
print(yr.to_string())
print("agent registry size:", lib.n_tests())
