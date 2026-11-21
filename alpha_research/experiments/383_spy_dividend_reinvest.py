"""383 - SPY dividend-payment reinvestment flow (Jan/Apr/Jul/Oct turn of month vs other turns of month).
Story: SPY pays its quarterly dividend (recently ~$15-20bn/qtr incl. IVV/VOO peers) on the LAST BUSINESS DAY of
January, April, July and October (SPDR S&P 500 ETF Trust prospectus, SEC 497 2018: "Payments of dividends are made
quarterly, on the last Business Day ... of April, July, October and January"; ex-date = 3rd Friday of Mar/Jun/Sep/Dec,
confirmed on Yahoo events=div, 133/136 ex-dates on Fridays). DRIP programmes and rebalancers reinvest the cash in the
sessions after the pay date P, so SPY should be bid on P+1..P+3 = first 3 sessions of Feb/May/Aug/Nov.
Confound: those sessions are the turn of the month (110, dead) and P is a month-end (013). The dividend part is
isolated by comparing the dividend months' turn-of-month with the other 8 months' turn-of-month (diff-in-diff).
Calendar-known -> position set at the prior close (lag 0 legitimate: no price information used). Close->close SPY,
0.5bp/side. Related: LOG 114 (Hartzmark-Solomon aggregate dividend-payment price pressure, parked for lack of
aggregate pay dates; SPY's own pay date is the one large, exactly known payment we can test).

Pre-specified variants (written before any results):
 A. Long SPY over sessions P+1..P+3 (first 3 sessions of Feb/May/Aug/Nov); timing value vs exposure-matched always-long.
 B. Diff-in-diff, zero average exposure: +1 SPY on first 3 sessions of Feb/May/Aug/Nov, -0.5 SPY on first 3 sessions
    of the other 8 months (4*3*1 = 8*3*0.5 exposure-days per year).
 C. Diff-in-diff on the pay day itself: +1 SPY on P (last session of Jan/Apr/Jul/Oct), -0.5 on the last session of the
    other 8 months (separates any pay-day effect from 013's month-end effect).
 D. Placebo diff-in-diff: same as B but for the turn of Jan/Apr/Jul/Oct (first 3 sessions of the pay months, i.e.
    BEFORE payment) vs the other 8 months. Expect ~0.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "383_spy_div_reinvest"
PAY_M = {1, 4, 7, 10}
POST_M = {2, 5, 8, 11}


def timing_value(pos, cc, cost):
    """pos(t)=exposure held over session t (decided at close t-1). Net minus exposure-matched buy&hold per period."""
    W = pos.shift(-1, fill_value=0.0).to_frame("S")
    net = lib.backtest_weights(W, cc.to_frame("S"), cost)
    held = pos.reindex(net.index).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * cc.loc[a:b].fillna(0)
    return net, out


def reg(variant, series, held, notes=""):
    s = series.loc["2000":]
    r = lib.register(EXP, variant, pd.Series(1.0, index=s.index), s, 0.0, notes)
    h = held.abs().clip(upper=1) > 0
    ev = {k: int((h.loc[a:b] & ~h.loc[a:b].shift(1, fill_value=False)).sum()) for k, (a, b) in lib.PERIODS.items()}
    bp = {k: 1e4 * s.loc[a:b].sum() / max(ev[k], 1) for k, (a, b) in lib.PERIODS.items()}
    print(f"{variant[:62]:62s} {lib.fmt(r)} gate={lib.passes_gate(r)} blocks={ev} net_bp/block(train,valid)="
          f"{bp['train']:+.1f},{bp['valid']:+.1f}")
    return r


spy = lib.get("SPY"); idx = spy.index; cc = spy.cc
d = pd.DataFrame(index=idx); d["ym"] = idx.to_period("M"); d["m"] = idx.month
d["k"] = d.groupby("ym").cumcount() + 1                         # session number in month
d["last"] = d["ym"] != d["ym"].shift(-1)                          # last session of month
tom3 = d["k"] <= 3
post = tom3 & d["m"].isin(POST_M); oth = tom3 & ~d["m"].isin(POST_M)
payd = d["last"] & d["m"].isin(PAY_M); othlast = d["last"] & ~d["m"].isin(PAY_M)
pre = tom3 & d["m"].isin(PAY_M); othpre = tom3 & ~d["m"].isin(PAY_M)

# descriptive (<=2022 only)
for k, (a, b) in lib.PERIODS.items():
    c = cc.loc[a:b]; f = lambda m: f"{1e4*c[m.loc[a:b]].mean():+.2f}bp (n={int(m.loc[a:b].sum())})"
    print(f"{k}: TOM1-3 post-pay {f(post)} | other TOM1-3 {f(oth)} | pay day {f(payd)} | other month-end {f(othlast)}"
          f" | all {1e4*c.mean():+.2f}bp")

pA = post.astype(float)
_, tvA = timing_value(pA, cc, 0.5); reg("383A long SPY P+1..P+3 (timing value)", tvA, pA)
pB = post.astype(float) - 0.5 * oth.astype(float)
nB = lib.backtest_weights(pB.shift(-1, fill_value=0.0).to_frame("S"), cc.to_frame("S"), 0.5)
reg("383B DiD +1 post-pay TOM1-3 / -0.5 other TOM1-3", nB, pB)
pC = payd.astype(float) - 0.5 * othlast.astype(float)
nC = lib.backtest_weights(pC.shift(-1, fill_value=0.0).to_frame("S"), cc.to_frame("S"), 0.5)
reg("383C DiD +1 pay day / -0.5 other month-ends", nC, pC)
pD = pre.astype(float) - 0.5 * othpre.astype(float)
nD = lib.backtest_weights(pD.shift(-1, fill_value=0.0).to_frame("S"), cc.to_frame("S"), 0.5)
reg("383D placebo DiD pre-pay TOM1-3 (Jan/Apr/Jul/Oct) / -0.5 other", nD, pD)

for nm, s in [("383A", tvA), ("383B", nB)]:
    y = s.loc["2000":"2022"]; print(nm, "yearly bp:", (y.groupby(y.index.year).sum() * 1e4).round(0).astype(int).to_dict())
print("registry size:", lib.n_tests())
