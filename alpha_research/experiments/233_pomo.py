"""233 - Fed QE purchase days (POMO; NY Fed operation results via markets.newyorkfed.org API, schedules pre-announced
monthly so the dates are known in advance). Outright Treasury PURCHASES 2008-11 .. 2022-12.
  a) long SPY open->close on purchase days (liquidity injection; Fed pays dealers who rebalance into risk) 0.5bp
  b) long TLT open->close on days with long-maturity purchases (range end >= 15y out), 1bp
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ops = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "nyfed_tsy.json")))["treasury"]["auctions"])
ops["d"] = pd.to_datetime(ops.operationDate)
buy = ops[(ops.operationDirection == "P") & ops.operationType.str.contains("Purchase")]
print("purchase ops:", len(buy), buy.d.min().date(), "->", buy.d.max().date(), " by year:", buy.d.dt.year.value_counts().sort_index().to_dict())
days = set(buy.d)
longd = set(buy[pd.to_datetime(buy.maturityRangeEnd, errors="coerce") - buy.d >= pd.Timedelta(days=15 * 365)].d)
spy, tlt = lib.get("SPY"), lib.get("TLT")
pa = pd.Series(spy.index.isin(list(days)), index=spy.index).astype(float).loc["2008-11":]
pb = pd.Series(tlt.index.isin(list(longd)), index=tlt.index).astype(float).loc["2008-11":]
r = lib.register("233_pomo", "233a long SPY open->close on Fed Treasury purchase days", pa, spy.oc.loc["2008-11":], 0.5)
print(f"233a {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(pa.sum())}")
r = lib.register("233_pomo", "233b long TLT open->close on long-maturity purchase days", pb, tlt.oc.loc["2008-11":], 1.0)
print(f"233b {lib.fmt(r)}  gate={lib.passes_gate(r)}  days={int(pb.sum())}")
print("registry size:", lib.n_tests())
