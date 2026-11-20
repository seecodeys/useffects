"""212 - Intraday momentum / hedging demand (Gao-Han-Li-Zhou 2018; Baltussen-Da-Lammers-Martens 2021): the return from the
previous close to ~10:30 predicts the last half hour (15:30->close). Yahoo 1h bars (Dec 2023-Nov 2026) -> DESCRIPTIVE,
not registered (holdout era).
213 - Target-date-fund rebalancing (Parker-Schoar-Sun, JF 2023): TDFs trade against the prior month's stock-bond move with
a lag. Signal at the month-end close: d = −sign(SPY monthly return − AGG monthly return); hold SPY close->close over the
first 3 sessions of the new month, 0.5bp/side. (013 uses the LAST day; this tests the following days.)
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["SPY", "QQQ"]:
    h = pd.read_csv(os.path.join(lib.DATA, f"{sym}_1h.csv"), parse_dates=["ts"]); h["d"] = h.ts.dt.normalize()
    p1030 = h[(h.ts.dt.hour == 10) & (h.ts.dt.minute == 30)].set_index("d")["Open"]
    p1530 = h[(h.ts.dt.hour == 15) & (h.ts.dt.minute == 30)].set_index("d")["Open"]
    d = lib.get(sym)
    days = p1030.index.intersection(p1530.index).intersection(d.index)
    first = p1030.reindex(days) / d["Close"].shift(1).reindex(days) - 1
    lasthh = d["Close"].reindex(days) / p1530.reindex(days) - 1
    x = (np.sign(first) * lasthh * 1e4).dropna()
    print(f"212 {sym}: {len(x)} days; last half-hour in direction of first-hour-incl-overnight move: mean {x.mean():+.2f}bp "
          f"hit {100*(x>0).mean():.0f}% t={x.mean()/x.std()*np.sqrt(len(x)):+.2f}")

spy, agg = lib.get("SPY"), lib.get("AGG")
idx = spy.index.intersection(agg.index); ym = idx.to_period("M")
last = pd.Series(ym, index=idx) != pd.Series(ym, index=idx).shift(-1); last.iloc[-1] = False
ms = spy.aC.reindex(idx).where(last).dropna().pct_change(); ma = agg.aC.reindex(idx).where(last).dropna().pct_change()
sig_at_me = (-np.sign(ms - ma)).dropna()
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
want = pd.Series(0.0, index=idx)
for me, s_ in sig_at_me.items():
    nxt = idx[(idx > me) & (fwd.reindex(idx) <= 3).values & (idx.to_period("M") == (me + pd.offsets.MonthBegin(1)).to_period("M"))]
    want[nxt] = s_
pos = want.shift(-1).fillna(0)
net = lib.backtest_weights(pos.to_frame("S"), spy.cc.reindex(idx).to_frame("S"), 0.5).loc["2003-10":]
r = lib.register("213_tdf_rebalance", "213 first 3 sessions of month: -sign(prior month SPY - AGG)", pd.Series(1.0, index=net.index), net, 0.0)
print(f"213  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
