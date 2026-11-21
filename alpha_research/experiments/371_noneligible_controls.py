"""371 - Index-eligibility controls for the month-end bond bid (idea E2, results/ideas_extensions.md). Prediction: securities
OUTSIDE the main bond indices should show little or no last-3-session bid. STRIPS (EDV/ZROZ — zero-coupon, not in the
Bloomberg Aggregate) vs TLT (in-index, similar duration); leveraged loans (BKLN, floating-rate, not in bond indices) vs
HYG (in HY index). Registered: last-3-session timing value vs always-long, 1bp/side, close->close (calendar-known)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
for sym in ["EDV", "ZROZ", "TLT", "BKLN", "HYG"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    pos = ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0)
    net = net.loc[max(net.index[0], pd.Timestamp("2008-01-01")):]
    held = pos.shift(1).loc[net.index]
    tv = net - held.loc[:"2022"].mean() * d.cc.loc[net.index]
    r = lib.register("275_bond_tom", f"371 {sym} last-3 timing value (index-eligibility control)", pd.Series(1.0, index=tv.index), tv, 0.0)
    win = d.cc[(bwd <= 3) & (ym != ym.iloc[-1])].groupby(ym).sum().loc["2008":"2022"] * 1e4
    print(f"371 {sym:5s} start {d.index[0].date()}  {lib.fmt(r)}  last-3 bp/month 2008-22 {win.mean():+.1f} (t={win.mean()/win.std()*np.sqrt(len(win)):.1f})")
print("registry size:", lib.n_tests())
