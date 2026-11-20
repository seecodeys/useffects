"""016/017 - Other rebalancing flows on the last trading day of a completed month.
MTD returns through the PRIOR close (known pre-open); positions held open->close; 1bp/side per leg.
017a IWM-SPY spread: pos = -sign(IWM_mtd - SPY_mtd)            (size/style rebalancing)
017b EFA vs bonds:   EFA pos = -sign(EFA_mtd - TLT_mtd)         (intl equity-bond)
017c EEM vs bonds:   EEM pos = -sign(EEM_mtd - TLT_mtd)
017d SPY-EFA spread: pos = -sign(SPY_mtd - EFA_mtd)             (home vs intl)
016a sector XS (9 SPDRs), every month-end: long 3 worst MTD, short 3 best
016b same, quarter-ends only
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "016_017_rebalance_family"
COST = 1.0
spy = lib.get("SPY")
idx = spy.index[spy.index >= "2000-01-01"]
ym = idx.to_period("M")
rank_bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
last = (rank_bwd == 1) & pd.Series(ym != ym[-1], index=idx)
qend = last & (idx.month % 3 == 0)


def mtd(sym):
    d = lib.get(sym).reindex(idx)
    month_end_close = d["aC"].where(rank_bwd == 1).ffill().shift(1)   # previous month-end close as of each day
    first = pd.Series(ym != pd.Series(ym, index=idx).shift(1).values, index=idx)
    base = month_end_close.where(~first, d["aC"].shift(1))        # on 1st day, base = prior close
    base = base.where(first).ffill()                               # hold the month's base constant
    return d["aC"].shift(1) / base - 1, d.oc


def spread_test(name, a, b):
    ma, oa = mtd(a); mb, ob = mtd(b)
    pos = (-np.sign(ma - mb)).where(last, 0)
    net = pos * (oa - ob) - pos.abs() * 2 * 2 * COST / 1e4
    return lib.register(EXP, name, pd.Series(1.0, index=net.index), net.fillna(0), 0.0, notes="spread, 1bp/side/leg in series")


def single_test(name, a, bond="TLT"):
    ma, oa = mtd(a); mb, _ = mtd(bond)
    pos = (-np.sign(ma - mb)).where(last, 0)
    return lib.register(EXP, name, pos.fillna(0), oa.fillna(0), COST)


rows = {
    "017a IWM-SPY spread: fade size MTD gap": spread_test("017a IWM-SPY spread: fade size MTD gap", "IWM", "SPY"),
    "017b EFA: fade EFA-TLT MTD gap": single_test("017b EFA: fade EFA-TLT MTD gap", "EFA"),
    "017c EEM: fade EEM-TLT MTD gap": single_test("017c EEM: fade EEM-TLT MTD gap", "EEM"),
    "017d SPY-EFA spread: fade home-vs-intl MTD gap": spread_test("017d SPY-EFA spread: fade home-vs-intl MTD gap", "SPY", "EFA"),
}
SECT = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"]
M, O = {}, {}
for s_ in SECT:
    M[s_], O[s_] = mtd(s_)
M, O = pd.DataFrame(M), pd.DataFrame(O)
rk = M.rank(axis=1)
w = pd.DataFrame(0.0, index=idx, columns=SECT)
w[rk.le(3)] = 1 / 6; w[rk.ge(7)] = -1 / 6
for name, mask in [("016a sector XS month-end: long 3 worst MTD, short 3 best", last),
                   ("016b sector XS quarter-end only", qend)]:
    ww = w.where(mask, 0.0)
    net = (ww * O.fillna(0)).sum(axis=1) - ww.abs().sum(axis=1) * 2 * COST / 1e4
    rows[name] = lib.register(EXP, name, pd.Series(1.0, index=idx), net, 0.0, notes="1bp/side in series")
for n, r in rows.items():
    print(f"{n:<55} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
# sanity: our mtd() vs 013's SPY-TLT signal sign agreement on last days
ms, _ = mtd("SPY"); mt, _ = mtd("TLT")
print("sanity SPY MTD on last days, mean abs %:", round(ms[last].abs().mean() * 100, 2))
print("registry size:", lib.n_tests())
