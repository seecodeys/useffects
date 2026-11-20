"""013d - Extensions of the month-end rebalancing finding (SPY, 0.5bp/side).
  E1 first trading day of next month: rebalancing pressure unwinds -> pos = +sign(previous month's full stock-bond gap)
  E2 last day, sized: pos = -clip(gap_mtd / median|gap_mtd| on last days in TRAIN, -2, 2) / 2  (avg |pos| ~ 0.5-1)
  E3 second-to-last day only: pos = -sign(gap_mtd)
  E4 last day with IEF instead of TLT as the bond leg (robustness of the bond proxy)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
import contextlib, io
with contextlib.redirect_stdout(io.StringIO()):
    src = open(os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")).read().split("S = {")[0]
    exec(src)
EXP = "013_month_end_rebalance"
# E1: previous month's full gap = gap through the last day's CLOSE of prev month
full_month = (s["aC"] / prev_month_end_close(s) - 1) - (t["aC"] / prev_month_end_close(t) - 1)
prev_full = full_month.where(rank_bwd == 1).groupby(ym).transform("last")      # month's final value
prev_full = prev_full.shift(1).where(pd.Series(idx, index=idx).dt.to_period("M") != pd.Series(idx, index=idx).shift(1).dt.to_period("M"))
first_day = prev_full.notna()
E1 = np.sign(prev_full).where(first_day, 0)
med = gap_mtd[last1].loc[lib.TRAIN[0]:lib.TRAIN[1]].abs().median()
E2 = (-(gap_mtd / med).clip(-2, 2) / 2).where(last1, 0)
E3 = (-np.sign(gap_mtd)).where((rank_bwd == 2) & complete, 0)
ief = lib.get("IEF").loc[idx]
gap_ief = mtd(s) - mtd(ief)
E4 = (-np.sign(gap_ief)).where(last1, 0)
if "--holdout" not in sys.argv:
  for name, pos in {"E1 first day of month: follow prior month's stock-bond gap (unwind)": E1,
                  "E2 last day sized by |gap| (clip 2x median)": E2,
                  "E3 second-to-last day only": E3,
                  "E4 last day, IEF as bond leg": E4}.items():
      r = lib.register(EXP, f"013d {name}", pos.fillna(0), s.oc, 0.5)
      print(f"{name:<66} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("E1 sanity: first-day count", int(first_day.sum()), " months", len(set(ym)))
print("registry size:", lib.n_tests())

if "--holdout" in sys.argv:
    for c in [0.5, 2.0]:
        h = lib.holdout_eval(EXP, f"013d E2 last day sized by |gap| (cost {c}bp)", E2.fillna(0), s.oc, c)
        print("HOLDOUT E2", c, {k: round(float(v), 2) for k, v in h.items()})
