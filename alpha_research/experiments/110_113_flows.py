"""110-113 Participant-behavior / flow tests on SPY (close->close unless noted; timing value = excess over an
exposure-matched always-long position so the equity premium doesn't count). 0.5bp/side on turnover.
110 TOM inflows: long from the close of the 2nd-last trading day of the month through the close of the 3rd trading day.
111 overnight split: long SPY close->open only (flat intraday), costs 0.5bp/side x2 daily. Raw t (not timing value)
    and timing value vs always-long; plus by-era mean overnight vs intraday.
112 LETF rebalancing reversal: after a |close->close| > 2% day, bet against it from that close to the NEXT OPEN
    (overnight) — signal known at the close it trades on (best-case: MOC near 15:50 needs an estimate).
113 Buyback blackout proxy: flat during calendar days 14-45 after each quarter-end (peak earnings blackout),
    long otherwise.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
idx = spy.index
r = spy.cc


def timing(name, exp, pos, ret=r, cost=0.5, lag=0):
    net = lib.backtest_weights(pos.to_frame("S"), ret.to_frame("S"), cost, lag=lag)
    held = pos.shift(1 + lag).fillna(0)
    out = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * ret.loc[a:b].fillna(0)
    out = out.loc["2000":]
    res = lib.register(exp, name + " (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
    print(f"{name:<54} {lib.fmt(res)}  gate={lib.passes_gate(res)}  exposure={100*held.loc['2000':'2022'].mean():.0f}%")
    return res


ym = idx.to_period("M")
fwd = pd.Series(1, index=idx).groupby(ym).cumsum()
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
# position set at close of day t earns day t+1. Window: hold from close of bwd==2 day through close of fwd==3 day
hold_days = (bwd == 1) | (fwd <= 3)                    # days whose return we want
pos110 = hold_days.shift(-1, fill_value=False).astype(float)   # decide at prior close
if "--letf-era" not in sys.argv: timing("110 TOM: long last trading day + first 3 days", "110_tom_flows", pos110)

# 111 overnight
co = spy["Open"] / spy["Close"].shift(1) - 1           # raw overnight (dividend drop on ex-days ignored -> small bias)
oc = spy.oc
d = pd.DataFrame({"on": co, "id": oc}).loc["2000":]
print("111 mean bp/day overnight vs intraday by era:",
      {k: (round(1e4 * g.on.mean(), 2), round(1e4 * g.id.mean(), 2)) for k, g in d.groupby((d.index.year // 5) * 5)})
on_net = (co - 2 * 0.5e-4).loc["2000":]
if "--letf-era" in sys.argv: pass
else:
 res = lib.register("111_overnight", "111 long SPY close->open only (raw, 0.5bp/side x2)", pd.Series(1.0, index=on_net.index), on_net.fillna(0), 0.0)
 print(f"{'111 long overnight only (raw)':<54} {lib.fmt(res)}")
diff = (co - oc).loc["2000":] - 4 * 0.5e-4
if "--letf-era" not in sys.argv:
 res = lib.register("111_overnight", "111 long overnight / short intraday SPY (raw)", pd.Series(1.0, index=diff.index), diff.fillna(0), 0.0)
 print(f"{'111 long overnight / short intraday':<54} {lib.fmt(res)}  gate={lib.passes_gate(res)}")

# 112 LETF reversal overnight
sig = pd.Series(np.where(r > 0.02, -1.0, np.where(r < -0.02, 1.0, 0.0)), index=idx)
ser = (sig * co.shift(-1) - sig.abs() * 2 * 0.5e-4).shift(1).loc["2000":]       # P&L lands on the next day
if "--letf-era" not in sys.argv:
 res = lib.register("112_letf_rebalance", "112 fade |day|>2% from close to next open", pd.Series(1.0, index=ser.index), ser.fillna(0), 0.0)
 print(f"{'112 LETF overnight reversal':<54} {lib.fmt(res)}  gate={lib.passes_gate(res)}  n={int(sig.abs().loc['2000':].sum())}")

# 113 buyback blackout
qe = idx.to_period("Q").start_time - pd.Timedelta(days=1)          # last quarter's end date
days_after = (idx - qe).days
blackout = pd.Series((days_after >= 14) & (days_after <= 45), index=idx)
pos113 = (~blackout).shift(-1, fill_value=False).astype(float)
if "--letf-era" not in sys.argv: timing("113 flat in buyback-blackout weeks, long otherwise", "113_buyback_blackout", pos113)
print("registry size:", lib.n_tests())

# ---- 112b (run with --letf-era): same rule, sample restricted to the leveraged-ETF era (2009+). POST-HOC choice
# (motivated by the mechanism after seeing 112's train/valid split) -> logged as such; gate then holdout only if it passes.
if "--letf-era" in sys.argv:
    ser9 = ser.loc["2009-01-01":]
    res = lib.register("112_letf_rebalance", "112b fade |day|>2% close->next open, 2009+ only (post-hoc era)",
                       pd.Series(1.0, index=ser9.index), ser9.fillna(0), 0.0, notes="post-hoc sample start 2009 (LETF era)")
    print(f"112b 2009+  {lib.fmt(res)}  gate={lib.passes_gate(res)}")
    t = ser9[sig.shift(1).loc["2009":].abs() > 0]
    print("   per-trade bp by period:", {p: round(1e4 * t.loc[a:b].mean(), 1) for p, (a, b) in
          {"2009-14": ("2009", "2014"), "valid": lib.VALID}.items()}, " n:", len(t.loc[:"2022"]))
    if lib.passes_gate(res):
        h = lib.holdout_eval("112_letf_rebalance", "112b fade big days overnight (2009+ spec)", pd.Series(1.0, index=ser9.index), ser9.fillna(0), 0.0)
        print("   HOLDOUT", {k: round(float(v), 2) for k, v in h.items()}, " per-trade", round(1e4 * t.loc["2023":].mean(), 1), "bp n=", len(t.loc["2023":]))
