"""381 - Social Security payment Wednesdays (retirement-income cash flows).
Story: since June 1997 SSA pays new beneficiaries on the 2nd/3rd/4th Wednesday of each month (by birth date; paid on the
preceding business day if the Wednesday is a holiday), ~$80-120bn/month recently. Part flows into brokerage/retirement
accounts and may be invested in equities that day / the next session. Calendar-known -> position set at the prior close
(lag 0 is legitimate: no price information used). The 3rd-of-month cohort (pre-1997) is skipped: it sits inside the
turn-of-month window already tested (110, dead). The 16th payday (165) is also dead; this tests the specific SS dates.

Pre-specified variants (written before any results):
 A. Long SPY close->close on each SS payment day (2nd/3rd/4th Wed, holiday -> prior session); timing value vs
    exposure-matched always-long (per period); 0.5bp/side.
 B. Same, held over the payment day and the following session (2 sessions); timing value; 0.5bp/side.
 C. Long SPY on SS payment Wednesdays, short SPY on the other Wednesdays (1st/5th) - 'vs other Wednesdays'; 0.5bp/side.
 D. Placebo: long BIL on SS payment days, timing value vs always-long BIL; 1bp/side (expect ~0).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "381_ss_wednesdays"


def ss_days(idx):
    """Trading dates on which an SS payment (2nd/3rd/4th Wed) lands; holiday -> preceding trading day."""
    out = []
    for p in pd.period_range(idx[0], idx[-1], freq="M"):
        weds = pd.date_range(p.start_time, p.end_time, freq="W-WED")
        for w in weds[1:4]:
            j = idx.searchsorted(w, side="right") - 1          # w if trading day, else preceding session
            if j >= 0 and (w - idx[j]).days < 7:
                out.append(idx[j])
    return pd.DatetimeIndex(sorted(set(out)))


def timing_value(pos, cc, cost):
    """pos(t)=exposure held over session t (decided at close t-1). Net minus exposure-matched buy&hold per period."""
    W = pos.shift(-1, fill_value=0.0).to_frame("S")             # weight decided at close t-1
    net = lib.backtest_weights(W, cc.to_frame("S"), cost)
    held = pos.reindex(net.index).fillna(0); out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * cc.loc[a:b].fillna(0)
    return net, out


def reg(variant, series, held, notes=""):
    s = series.loc["2000":]
    r = lib.register(EXP, variant, pd.Series(1.0, index=s.index), s, 0.0, notes)
    trades = {k: int((held.loc[a:b].diff().fillna(held.loc[a:b]) > 0).sum()) for k, (a, b) in lib.PERIODS.items()}
    bp = {k: 1e4 * s.loc[a:b].sum() / max(trades[k], 1) for k, (a, b) in lib.PERIODS.items()}
    print(f"{variant[:60]:60s} {lib.fmt(r)} gate={lib.passes_gate(r)} trades={trades} net_bp/trade(train,valid)="
          f"{bp['train']:+.1f},{bp['valid']:+.1f}")
    return r


spy = lib.get("SPY"); idx = spy.index
ssd = ss_days(idx)
pA = pd.Series(idx.isin(ssd).astype(float), index=idx)
pB = ((pA + pA.shift(1).fillna(0)) > 0).astype(float)
wed = pd.Series(idx.weekday == 2, index=idx)
pC = pA - (wed & (pA == 0)).astype(float)

# descriptive (<=2022 only): mean SPY cc on SS Wed vs other Wed vs all days
cc = spy.cc
for k, (a, b) in lib.PERIODS.items():
    c = cc.loc[a:b]; m = pA.loc[a:b] == 1; ow = wed.loc[a:b] & ~m
    print(f"{k}: SS-day mean {1e4*c[m].mean():+.2f}bp (n={m.sum()}), other-Wed {1e4*c[ow].mean():+.2f}bp (n={ow.sum()}), "
          f"next-session {1e4*c[pA.loc[a:b].shift(1, fill_value=0)==1].mean():+.2f}bp, all {1e4*c.mean():+.2f}bp")

_, tvA = timing_value(pA, cc, 0.5); reg("381A long SPY on SS payment days (timing value)", tvA, pA)
_, tvB = timing_value(pB, cc, 0.5); reg("381B long SPY SS day + next session (timing value)", tvB, pB)
netC = lib.backtest_weights(pC.shift(-1, fill_value=0.0).to_frame("S"), cc.to_frame("S"), 0.5)
reg("381C long SPY SS Wed / short SPY other Wed", netC, pC.abs())

bil = lib.get("BIL"); pD = pd.Series(bil.index.isin(ss_days(bil.index)).astype(float), index=bil.index)
_, tvD = timing_value(pD, bil.cc, 1.0); reg("381D placebo long BIL on SS days (timing value)", tvD, pD)

# yearly P&L of A (<=2022)
ya = tvA.loc["2000":"2022"].groupby(tvA.loc["2000":"2022"].index.year).sum() * 1e4
print("381A yearly timing-value bp:", ya.round(0).astype(int).to_dict())
print("registry size:", lib.n_tests())
