"""373 — Fallen angels around month-end IG-index exits (card E5, results/ideas_extensions.md).

Hypothesis: bonds downgraded below IG leave IG indices at the month-end rebalance; IG holders sell into it and
fallen-angel trackers buy only after it. So the fallen-angel spread S = ANGL - h*HYG should be weak into month-end
(W3) and rebound in the first 3 sessions of the new month.
Data: Yahoo adjusted closes (lib.get 'cc', dividends included) for ANGL (2012-04+), HYG, LQD, IEF; FALN (2016-06+)
descriptive only. TRAIN IS SHORT: ANGL starts 2012-04, so train = 2012-05..2014-12 (~32 month-ends). Results are
also reported by year. Descriptive tables stop 2022-12-31; holdout never touched.
Hedge ratios h: OLS beta of daily returns, train period only (2012-05..2014-12), fixed thereafter.
Signals are calendar-only (known in advance). Implementation: decision weights fed to lib.backtest_weights(..., lag=1),
placed 2 rows before each held return row so the held days are exactly the window.
Windows: W3 = close of 4th-last session -> close of last session of the month (earns last 3 sessions' returns).
         F3 = close of last session of month -> close of 3rd session of the new month.
Costs: 2bp/side per leg (caller instruction; turnover-based). 1bp/side shown as a second cost level.

PRE-SPECIFIED VARIANTS (fixed before seeing results; signs fixed):
  V1 short S over W3.
  V2 long S over F3.
  V3 V2 only when LQD - IEF total return over the 21 sessions ending at the W3 entry close < -1% (known in advance).
  V4 long LQD - h2*IEF over F3 (h2 = train 2003-2014 beta; LQD/IEF train is full 2003-14). Descriptive card item,
     registered anyway.
Descriptive (not registered): FALN - h*HYG event means over W3/F3 (2016-06..2022), per-year tables, timing value vs
exposure-matched unconditional S holding.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lib

EXP = "373_fallen_angel_month_end"
COST = 2.0
END = "2022-12-31"
OUT = os.path.join(lib.HERE, "results")
SY = ["ANGL", "HYG", "LQD", "IEF", "FALN"]
px = {s: lib.get(s) for s in SY}
R = pd.DataFrame({s: px[s]["cc"] for s in SY}).loc["2003-01-01":END]
idx = R.index
print("first dates:", {s: str(px[s].index[0].date()) for s in SY})


def beta(y, x, a, b):
    d = pd.concat([R[y], R[x]], axis=1).loc[a:b].dropna()
    return np.cov(d[y], d[x])[0, 1] / d[x].var()


h = beta("ANGL", "HYG", "2012-05-01", lib.TRAIN[1])
h2 = beta("LQD", "IEF", "2003-01-01", lib.TRAIN[1])
print(f"h(ANGL~HYG, 2012-05..2014) = {h:.3f}   h2(LQD~IEF, 2003-14) = {h2:.3f}")

# month structure
ym = pd.Series(idx.to_period("M"), index=idx)
pos_of = {d: i for i, d in enumerate(idx)}
last_rows = [pos_of[g.index[-1]] for _, g in ym.groupby(ym)]
ANGL0 = pd.Timestamp("2012-05-01")


def windows(kind):
    """list of (event_date, list_of_held_return_rows). Event date = month-end date."""
    ev = []
    for L in last_rows:
        if kind == "W3":
            rows = [L - 2, L - 1, L]
        else:
            rows = [L + 1, L + 2, L + 3]
        if rows[0] - 2 < 0 or rows[-1] + 1 >= len(idx):
            continue
        ev.append((idx[L], rows))
    return ev


# V3 condition: LQD-IEF return over 21 sessions ending at W3 entry close (row L-3)
cum = lambda s, a, b: np.prod(1 + R[s].iloc[a:b + 1].fillna(0)) - 1
def widen(L):
    e = L - 3
    return (cum("LQD", e - 20, e) - cum("IEF", e - 20, e)) < -0.01


def build(legs, kind, sign, start, cond=None, cost=COST):
    cols = list(legs)
    W = pd.DataFrame(0.0, index=idx, columns=cols)
    book = {}
    for d, rows in windows(kind):
        if d < start:
            continue
        L = pos_of[d]
        if cond is not None and not cond(L):
            continue
        if any(R[c].iloc[rows].isna().any() for c in cols):
            continue
        for r in rows:
            for c, w in legs.items():
                W.iloc[r - 2, cols.index(c)] += sign * w
        book[d] = (idx[rows[0]], idx[rows[-1] + 1])   # include exit-cost day
    net = lib.backtest_weights(W, R[cols].fillna(0), cost, lag=1)
    pnl = pd.Series(0.0, index=idx)
    for d, (a, b) in book.items():
        pnl[d] = net.loc[a:b].sum()
    pnl = pnl.loc[start:]
    return (pnl != 0).astype(float), pnl, book


S = {"ANGL": 1.0, "HYG": -h}
variants = {
    "V1 short ANGL-hHYG W3": (S, "W3", -1, ANGL0, None),
    "V2 long ANGL-hHYG F3": (S, "F3", +1, ANGL0, None),
    "V3 V2 if LQD-IEF<-1% prior month": (S, "F3", +1, ANGL0, widen),
    "V4 long LQD-h2IEF F3": ({"LQD": 1.0, "IEF": -h2}, "F3", +1, pd.Timestamp("2003-01-01"), None),
}


def t(x):
    x = pd.Series(x).dropna()
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


summ = []
yearly = {}
for name, (legs, kind, sg, st, cond) in variants.items():
    pos, pnl, book = build(legs, kind, sg, st, cond)
    row = lib.register(EXP, name, pos, pnl, 0.0, notes="event P&L net 2bp/side/leg booked on month-end date; lag=1")
    _, pnl1, _ = build(legs, kind, sg, st, cond, cost=1.0)
    ev = pnl[pos > 0]; ev1 = pnl1[pos > 0]
    tr = ev.loc[:lib.TRAIN[1]]; va = ev.loc[lib.VALID[0]:]
    gate = lib.passes_gate(row)
    print(f"{name:34s} {lib.fmt(row)} gate={gate}")
    print(f"   n tr/va {len(tr)}/{len(va)}  net bp/event tr {1e4*tr.mean():+.1f} va {1e4*va.mean():+.1f} "
          f"| @1bp tr {1e4*ev1.loc[:lib.TRAIN[1]].mean():+.1f} va {1e4*ev1.loc[lib.VALID[0]:].mean():+.1f} "
          f"| hit va {100*(va>0).mean():.0f}%")
    yearly[name] = (1e4 * ev.groupby(ev.index.year).sum()).round(0)
    yearly[name + " n"] = ev.groupby(ev.index.year).size()
    summ.append(dict(variant=name, n_train=len(tr), n_valid=len(va), train_t=row["train_t"], valid_t=row["valid_t"],
                     valid_sharpe=row["valid_sharpe"], net_bp_ev_train=1e4 * tr.mean(), net_bp_ev_valid=1e4 * va.mean(),
                     gate=gate))
pd.DataFrame(summ).to_csv(os.path.join(OUT, "373_summary.csv"), index=False)
print("\n=== per-year net P&L (bp, sum over events) ===")
yt = pd.DataFrame(yearly).loc[:2022]
print(yt.to_string())

# ---------------- descriptive: gross window returns of S, timing value vs unconditional exposure, FALN
print("\n=== descriptive gross window returns (bp, mean per event), 2012-05..2022 ===")
def spread(y, x, hh):
    return R[y] - hh * R[x]
sA = spread("ANGL", "HYG", h)
hF = beta("FALN", "HYG", "2016-07-01", "2018-12-31")   # FALN has no train; hedge from its first 2.5y (descriptive)
sF = spread("FALN", "HYG", hF)
for lab, s, st in [("ANGL-hHYG", sA, ANGL0), ("FALN-hHYG (h=%.2f, 2016-07+)" % hF, sF, pd.Timestamp("2016-07-01"))]:
    uncond = s.loc[st:END].mean() * 3
    for kind in ["W3", "F3"]:
        g = pd.Series({d: (np.prod(1 + s.iloc[rows]) - 1) for d, rows in windows(kind) if d >= st})
        for per, (a, b) in [("train", lib.TRAIN), ("valid", lib.VALID)]:
            x = g.loc[a:b]
            if len(x) < 3:
                continue
            print(f"{lab:32s} {kind} {per}: n={len(x)} mean {1e4*x.mean():+.1f}bp t={t(x):+.2f} "
                  f"| timing vs uncond 3-day {1e4*(x.mean()-uncond):+.1f}bp (t={t(x-uncond):+.2f})")
    if "FALN" in lab:
        for kind in ["W3", "F3"]:
            g = pd.Series({d: (np.prod(1 + s.iloc[rows]) - 1) for d, rows in windows(kind) if d >= st})
            print(f"   FALN {kind} by year bp:", (1e4 * g.groupby(g.index.year).mean()).round(1).to_dict())
print("N registered tests in this registry:", lib.n_tests(), " luck t ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
