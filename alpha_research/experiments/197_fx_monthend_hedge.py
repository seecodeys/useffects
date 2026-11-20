"""197 - Month-end FX hedge rebalancing (foreign holders of US equities re-hedge at the WM/Reuters 4pm London fix,
11:00 ET, on the last business day): if US equities outperformed foreign equities month-to-date, foreign investors'
USD exposure grew -> they sell USD at the fix. Pre-specified: on the last trading day, pos_USD = −sign(SPY MTD − EFA MTD)
(MTD through the prior close).
  a) UUP (USD index ETF, 2007+) open->close, 2bp/side
  b) USDJPY (HistData 1-min) from 09:29 to 11:05 NY on the last day (spans the fix), 2001-2025, 0.5bp/side
"""
import os, sys, contextlib, io, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
spy, efa = lib.get("SPY"), lib.get("EFA")
idx = spy.index.intersection(efa.index); idx = idx[idx >= "2001-09-01"]
ym = idx.to_period("M")
rb = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last = (rb == 1) & pd.Series(ym != ym[-1], index=idx)
first = pd.Series(ym != pd.Series(ym, index=idx).shift(1).values, index=idx)
def mtd(x):
    x = x.reindex(idx)
    base = x["aC"].where(rb == 1).ffill().shift(1).where(first).ffill()
    return x["aC"].shift(1) / base - 1
sig = (-np.sign(mtd(spy) - mtd(efa))).where(last, 0).fillna(0)
uup = lib.get("UUP").reindex(idx)
r = lib.register("197_fx_monthend", "197a UUP open->close on last day, short USD if SPY beat EFA MTD", sig.where(uup.oc.notna(), 0), uup.oc.fillna(0), 2.0)
print(f"197a UUP     {lib.fmt(r)}  gate={lib.passes_gate(r)}")
px = fx.ny_minute_close("USDJPY", range(2001, 2026))
D = idx[last.values & (idx <= "2025-12-31")]
move = pd.Series(fx.level_at(px, D, 11, 5).values / fx.level_at(px, D, 9, 29).values - 1, index=D)
ser = (sig.reindex(D) * move - 2 * 0.5e-4).reindex(idx).fillna(0)
r = lib.register("197_fx_monthend", "197b USDJPY 09:29->11:05 on last day (spans the fix), same signal", pd.Series(1.0, index=idx), ser, 0.0)
t = (sig.reindex(D) * move * 1e4).dropna()
print(f"197b USDJPY  {lib.fmt(r)}  per-trade bp train {t.loc[:'2014'].mean():+.1f} valid {t.loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
