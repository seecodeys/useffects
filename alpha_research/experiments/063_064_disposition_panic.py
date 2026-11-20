"""063 - Disposition effect / capital-gains overhang (Grinblatt-Han), survivorship-free ETF cross-section.
      overhang = price / (252-day volume-weighted average adjusted price) - 1. Investors sitting on big gains sell →
      high-overhang ETFs under-perform. Pre-specified: long 4 lowest overhang, short 4 highest, rebalance monthly
      (hold 21 days, overlapping), lag-1, 1bp/side. NB the literature finds the OPPOSITE sign at stock level in some
      samples (overhang ~ momentum); we test the disposition-pressure direction as pre-registered.
064 - Panic selling in global stress: when VIX (prior close) > 30, long the 3 countries that fell most vs the
      cross-section over the past 5 days, short the 3 best; hold 5 days, lag-1, 2bp/side. Otherwise flat.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

ETFS = ["SPY", "QQQ", "IWM", "DIA", "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY",
        "EWJ", "EWG", "EWU", "EWH", "EWA", "EWY", "EWT", "EWZ", "FXI", "EFA", "EEM", "TLT", "GLD"]
R, OH = {}, {}
for s in ETFS:
    d = lib.get(s)
    vwap = (d.aC * d.Volume).rolling(252).sum() / d.Volume.rolling(252).sum()
    R[s], OH[s] = d.cc, d.aC / vwap - 1
R, OH = pd.DataFrame(R).loc["1999":], pd.DataFrame(OH).loc["1999":]
rk = OH.rank(axis=1); n = OH.notna().sum(axis=1)
W = (rk.le(4).astype(float) / 8 - rk.ge(n.values[:, None] - 3).astype(float) / 8).where(n >= 12, 0).fillna(0)
W = W.rolling(21, min_periods=1).mean()
net = lib.backtest_weights(W, R, 1.0, lag=1).loc["2000":]
r = lib.register("063_disposition", "063 long low-overhang / short high-overhang ETFs (monthly)", pd.Series(1.0, index=net.index), net, 0.0)
print(f"063 disposition                          {lib.fmt(r)}  gate={lib.passes_gate(r)}")

C = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL",
     "EWN", "EWD", "EWK", "EWO", "EWM"]
RC = pd.DataFrame({c: lib.get(c).cc for c in C}).loc["1998":]
vix = lib.get("^VIX")["Close"].reindex(RC.index).ffill()
past = RC.rolling(5).sum(); rel = past.sub(past.mean(axis=1), axis=0)
rk = rel.rank(axis=1); n = rel.notna().sum(axis=1)
W = (rk.le(3).astype(float) / 6 - rk.ge(n.values[:, None] - 2).astype(float) / 6).where((n >= 10) & (vix > 30), 0).fillna(0)
W = W.rolling(5, min_periods=1).mean()
net = lib.backtest_weights(W, RC, 2.0, lag=1).loc["2000":]
r = lib.register("064_panic_countries", "064 country 1w reversal only when VIX>30", pd.Series(1.0, index=net.index), net, 0.0)
print(f"064 panic country reversal (VIX>30)      {lib.fmt(r)}  gate={lib.passes_gate(r)}  active%={100*(W.abs().sum(axis=1)>0).loc['2000':].mean():.0f}")
print("registry size:", lib.n_tests())
