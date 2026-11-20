"""313 - Treasury leveraged-ETF rebalancing (TMF/TMV, 3x 20+yr, since 2009): after a big TLT day both bull and bear
3x funds must trade in the direction of the move into the close (negative-gamma rebalancing), pushing the close too far
-> next-day reversal. Equity analogue 112 was weak-but-consistent. Pre-specified (next-day open->close avoids trading on
the signal's own closing print; also an overnight close->open leg):
 A fade TLT next-day open->close after |TLT cc| > 1.5%    B same, threshold 2%
 C fade TLT close->next open (overnight) after |cc| > 1.5%  (2bp/side)"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
t = lib.get("TLT")
prev = t.cc.shift(1)
for name, thr, tgt in [("A fade next-day oc |cc|>1.5%", 0.015, t.oc), ("B fade next-day oc |cc|>2%", 0.02, t.oc),
                       ("C fade overnight |cc|>1.5%", 0.015, t.gap)]:
    pos = (-np.sign(prev)).where(prev.abs() > thr, 0.0)
    r = lib.register("313_tlt_letf", f"313{name}", pos.loc["2002":], tgt.loc["2002":], 2.0)
    print(f"313{name}: {lib.fmt(r)}  pre-2009 vs post: valid only matters (TMF 2009+)")
print("registry size:", lib.n_tests())
