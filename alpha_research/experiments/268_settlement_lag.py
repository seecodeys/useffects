"""268 - Settlement-lag rebalancing: funds needing month-end settlement trade 'settlement-lag' sessions before the month's
last day. US equities: T+3 until 2017-09-05, T+2 until 2024-05-28, T+1 after. Pre-specified: 013's signal (MTD through the
prior close) applied on the session that is (lag) sessions before the last trading day -> day −3 (T+3 era), day −2 (T+2
era), day −1 (T+1 era = 013's last day... no: lag 1 means the day before the last day). SPY open->close, 0.5bp."""
import os, sys, contextlib, io
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, rb, complete, gap, idx = ns["s"], ns["rank_bwd"], ns["complete"], ns["gap_mtd"], ns["idx"]
lag = pd.Series(np.where(idx < pd.Timestamp("2017-09-05"), 3, np.where(idx < pd.Timestamp("2024-05-28"), 2, 1)), index=idx)
day = (rb == lag + 1) & complete          # rank_bwd==1 is the last day; lag sessions before it -> rank lag+1
pos = (-np.sign(gap)).where(day, 0).fillna(0)
r = lib.register("013_month_end_rebalance", "268 013 signal on the settlement-lag day before month-end (T+3/T+2/T+1 eras)", pos, s.oc, 0.5)
tt = (pos * s.oc)[pos != 0] * 1e4
print(f"268  {lib.fmt(r)}  gate={lib.passes_gate(r)}  per-trade T+3 era {tt.loc[:'2017-09'].mean():+.1f}, T+2 era (to 2022) {tt.loc['2017-09':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
