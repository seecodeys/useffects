"""258 - Japan month-end stock-bond rule: 1306.T (TOPIX ETF) vs 2510.T (JGB ETF, 2017-12+). Local last trading day,
      pos = −sign(TOPIX MTD − JGB MTD), 1306.T open->close, 3bp/side. TRAIN empty (data starts 2018) -> valid 2018-22 only.
263 - BOJ ETF purchase support (2010-12 .. 2024-03): the BOJ bought TOPIX ETFs on days the market fell in the morning.
      Pre-specified: long 1306.T open->close on days it OPENS > 0.5% below the prior close (gap down known at the open).
      Control: same rule after the BOJ stopped (2024-04+ is holdout, so only reported via the registry split). 3bp/side.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
e, b = lib.get("1306.T"), lib.get("2510.T")
idx = e.index.intersection(b.index); ee, bb = e.loc[idx], b.loc[idx]
ym = pd.Series(idx.to_period("M"), index=idx)
last = (ym != ym.shift(-1)) & (ym != ym.iloc[-1])
mtd = lambda x: x["aC"].shift(1) / x["aC"].where(ym != ym.shift(-1)).shift(1).ffill().where(ym != ym.shift(1)).ffill() - 1
pos = (-np.sign(mtd(ee) - mtd(bb))).where(last, 0).fillna(0)
r = lib.register("258_japan_monthend", "258 Japan 1306.T/2510.T month-end rule (2018+)", pos, ee.oc, 3.0)
t = (pos * ee.oc)[pos != 0].loc[:"2022"] * 1e4
print(f"258  {lib.fmt(r)}  per-trade 2018-22 {t.mean():+.1f}bp n={len(t)}")
gapdn = (e.gap < -0.005)
pos = gapdn.astype(float).loc["2009":]
r = lib.register("263_boj_etf", "263 long 1306.T open->close after >0.5% gap-down open (BOJ buying era)", pos, e.oc.loc["2009":], 3.0)
x = (e.oc[gapdn] * 1e4)
print(f"263  {lib.fmt(r)}  gate={lib.passes_gate(r)}  mean bp by era: 2009-12 {x.loc['2009':'2012'].mean():+.1f}, "
      f"2013-20 {x.loc['2013':'2020'].mean():+.1f}, 2021-22 {x.loc['2021':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
