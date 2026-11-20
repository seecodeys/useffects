"""127 - 013 with an overnight leg: same signal (MTD through the close of the 2nd-to-last day), held from that close to
      the last day's close (close->close) instead of open->close. 0.5bp/side.
129 - Mid-month rebalancing: 013 rule (−sign(SPY MTD − TLT MTD), MTD through prior close) on the first trading day on or
      after the 15th, SPY open->close. 0.5bp.
133 - Treasury auction supply (Lou-Yan-Zhang 2013, 'Anticipated and repeated shocks'): around long-duration auctions
      (10y, 20y, 30y incl. reopenings; dates from fiscaldata.treasury.gov): (a) short TLT close->close over the 3 sessions
      before auction day (closes d-4 -> d-1); (b) long TLT from the auction-day close to the close of d+2. 1bp/side.
"""
import os, sys, json, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap, idx = ns["s"], ns["last1"], ns["gap_mtd"], ns["idx"]
# 127: position decided at close of day -2 (= signal of last day, which uses MTD through that close), earns last day cc
pos = (-np.sign(gap)).where(last1, 0).shift(-1).fillna(0)          # held from close t (2nd-last) into t+1 (last)
net = lib.backtest_weights(pos.to_frame("S"), s.cc.to_frame("S"), 0.5, lag=0)
if not set(sys.argv) & {"--combined","--holdout"}:
  r = lib.register("013_month_end_rebalance", "127 013 held close->close (adds the overnight before the last day)",
                 pd.Series(1.0, index=net.index), net.loc["2002-08":], 0.0)
  print(f"127  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
ym = idx.to_period("M")
mid = pd.Series(idx.day >= 15, index=idx) & ~pd.Series(idx.day >= 15, index=idx).shift(1, fill_value=False).groupby(ym).cummax()
mid = mid & pd.Series(idx.day >= 15, index=idx)
first_after15 = pd.Series(idx.day >= 15, index=idx).astype(int).groupby(ym).cumsum() == 1
if not set(sys.argv) & {"--combined","--holdout"}:
  r = lib.register("129_mid_month", "129 013 rule on first trading day >= 15th", (-np.sign(gap)).where(first_after15, 0).fillna(0), s.oc, 0.5)
  print(f"129  {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(first_after15.sum())}")

a = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "auctions.json")))["data"])
long_terms = a.security_term.str.contains(r"^(10-Year|9-Year|20-Year|19-Year|30-Year|29-Year)")
ad = pd.to_datetime(a[long_terms].auction_date).drop_duplicates().sort_values()
tlt = lib.get("TLT"); ti = tlt.index
pre = pd.Series(0.0, index=ti); post = pd.Series(0.0, index=ti)
for d in ad:
    if d not in ti:
        continue
    k = ti.get_loc(d)
    if k >= 4:
        pre.iloc[k - 4:k - 1] = -1.0          # positions set at closes d-4, d-3, d-2 -> earn d-3, d-2, d-1
    if k + 2 < len(ti):
        post.iloc[k:k + 2] = 1.0               # set at closes d, d+1 -> earn d+1, d+2
for name, p in ({} if set(sys.argv) & {"--combined","--holdout"} else {"133a short TLT 3 sessions before long-duration auctions": pre,
                "133b long TLT 2 sessions after long-duration auctions": post}).items():
    net = lib.backtest_weights(p.to_frame("T"), tlt.cc.to_frame("T"), 1.0, lag=0).loc["2002-08":]
    r = lib.register("133_treasury_auctions", name, pd.Series(1.0, index=net.index), net, 0.0)
    print(f"{name:<58} {lib.fmt(r)}  gate={lib.passes_gate(r)}  days%={100*(p!=0).loc['2003':'2022'].mean():.0f}")
print("auctions used:", len(ad), " registry size:", lib.n_tests())

if "--combined" in sys.argv or "--holdout" in sys.argv:
    both = pre + post
    net = lib.backtest_weights(both.to_frame("T"), tlt.cc.to_frame("T"), 1.0, lag=0).loc["2002-08":]
    if "--combined" in sys.argv:
        r = lib.register("133_treasury_auctions", "133c full auction cycle: short d-3..d-1, long d+1..d+2", pd.Series(1.0, index=net.index), net, 0.0)
        print(f"133c combined  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
        yr = (net.groupby(net.index.year).sum() * 100).loc[:2022]
        print("   yearly %:", yr.round(1).to_dict(), " positive years", (yr > 0).sum(), "/", len(yr))
    if "--holdout" in sys.argv:
        for c in [1.0, 2.0]:
            n2 = lib.backtest_weights(both.to_frame("T"), tlt.cc.to_frame("T"), c, lag=0).loc["2002-08":]
            h = lib.holdout_eval("133_treasury_auctions", f"133c auction cycle (cost {c}bp)", pd.Series(1.0, index=n2.index), n2, 0.0)
            print("HOLDOUT", c, {k: round(float(v), 2) for k, v in h.items()})
