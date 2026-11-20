"""314b - Extend 314A (long SPY 5 sessions after z(equity P/C) > 1.5) through 2022 by splicing the CBOE daily JSON
(Oct 2019+) onto the CSV history. Same rule, no new variants; registered as the extended-valid run of 314A.
314A failed the gate on train t (1.62), so no holdout look is allowed regardless of the result."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
x = pd.read_csv(os.path.join(lib.DATA, "cboe_equitypc.csv"), skiprows=2); x.columns = [c.strip() for c in x.columns]
csv = pd.Series(pd.to_numeric(x["P/C Ratio"], errors="coerce").values, index=pd.to_datetime(x.DATE.str.strip(), format="mixed"))
j = pd.read_csv(os.path.join(lib.DATA, "cboe_pc_daily.csv"), parse_dates=["date"]).set_index("date").equity.sort_index()
print("level check: CSV last 60d mean %.3f | JSON first 60d mean %.3f" % (csv.iloc[-60:].mean(), j.iloc[:60].mean()))
pc = pd.concat([csv, j[j.index > csv.index[-1]]]).sort_index()
z = (pc - pc.rolling(60, min_periods=40).mean()) / pc.rolling(60, min_periods=40).std()
spy = lib.get("SPY"); idx = spy.index[(spy.index >= z.dropna().index[0]) & (spy.index <= "2022-12-31")]
w = (z > 1.5).astype(float).reindex(idx).fillna(0).rolling(5, min_periods=1).max()
net = lib.backtest_weights(w.to_frame("X"), spy.cc.loc[idx].to_frame("X"), 0.5, lag=1)
held = w.shift(2).fillna(0); tv = net - held.mean() * spy.cc.loc[idx]
r = lib.register("314_put_call", "314b A extended through 2022 (CSV + CBOE daily JSON)", pd.Series(1.0, index=net.index), net, 0.0)
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
print(f"314b: {lib.fmt(r)}  timing t 2007-14 {tt(tv.loc[:'2014']):+.2f} | 2015-19.9 {tt(tv.loc['2015':'2019-10-04']):+.2f} | "
      f"2019.10-22 {tt(tv.loc['2019-10-07':]):+.2f}")
print("registry size:", lib.n_tests())
