"""225 - LETF rebalancing where leveraged products are LARGE relative to the underlying (stronger version of 112):
GDX (NUGT/DUST), GDXJ (JNUG/JDST), SMH (SOXL/SOXS), XBI (LABU/LABD), TLT (TMF/TBT). After a big day (|close->close| > 2x the
ETF's trailing 60d daily std), LETFs trade in the move's direction into the close -> overshoot -> fade from that close to
the next open (signal known at the close it trades on: best-case MOC). Pooled equal-weight across triggered ETFs.
2010+ (LETF era for these). 2bp/side (TLT 1bp). Also each ETF separately.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
U = {"GDX": 2.0, "GDXJ": 2.0, "SMH": 2.0, "XBI": 2.0, "TLT": 1.0}
P = {}
for sym, c in U.items():
    d = lib.get(sym)
    z = d.cc / d.cc.rolling(60).std().shift(1)
    sig = pd.Series(np.where(z > 2, -1.0, np.where(z < -2, 1.0, 0.0)), index=d.index)
    co = d["Open"].shift(-1) / d["Close"] - 1
    P[sym] = ((sig * co - sig.abs() * 2 * c / 1e4).shift(1)).loc["2010":]   # booked on the next day
    s = P[sym].fillna(0)
    r = lib.register("225_letf_heavy", f"225 {sym} fade |z|>2 day close->next open", pd.Series(1.0, index=s.index), s, 0.0)
    print(f"225 {sym:<5} {lib.fmt(r)}  events={int(sig.loc['2010':'2022'].abs().sum())}")
Pdf = pd.DataFrame(P)
n = Pdf.ne(0).sum(axis=1).replace(0, np.nan)
pooled = (Pdf.fillna(0).sum(axis=1) / n).fillna(0)
r = lib.register("225_letf_heavy", "225 pooled fade big days overnight in LETF-heavy ETFs", pd.Series(1.0, index=pooled.index), pooled, 0.0)
print(f"225 pooled {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
