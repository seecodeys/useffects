"""083 - Expressing the month-end signal (013) with options, MODEL-PRICED.
On the last trading day: direction d = -sign(SPY MTD - TLT MTD). Options struck at the open, expiring at the close
(6.5h; T = 6.5/24/365 years), Black-Scholes with implied vol = VIX9D (2011+) else VIX, taken at the prior close,
zero rates. Premium haircut 5% per side (pay 105% when buying, receive 95% when selling). P&L per $1 of underlying.
  a) BUY the ATM call (d=+1) or put (d=-1)
  b) SELL the ATM put (d=+1) or call (d=-1)      [collect premium + directional tilt]
  c) baseline: SPY open->close with the same direction (013), 0.5bp/side
Caveats: same-day SPX options only became broadly available 2016-2022 (pre-2016 is hypothetical); real 0DTE implied vol
usually trades above VIX9D (check vs ^VIX1D 2023+), which would make buying dearer and selling richer than modelled.
"""
import os, sys, math, contextlib, io
import numpy as np, pandas as pd
from scipy.stats import norm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
ns = {"__file__": os.path.join(lib.HERE, "experiments", "013_month_end_rebalance.py")}
with contextlib.redirect_stdout(io.StringIO()):
    exec(open(ns["__file__"]).read().split("S = {")[0], ns)
s, last1, gap = ns["s"], ns["last1"], ns["gap_mtd"]
d = (-np.sign(gap)).where(last1, 0)
iv = lib.get("^VIX9D")["Close"].reindex(s.index).combine_first(lib.get("^VIX")["Close"].reindex(s.index)) / 100
iv = iv.shift(1)                                                      # prior close
T = 6.5 / 24 / 365
S0, S1 = s["Open"], s["Close"]
prem = S0 * iv * math.sqrt(T) * (norm.pdf(0.5 * iv * math.sqrt(T)) / 0.5 / iv / math.sqrt(T) * 0.5)  # placeholder
# exact BS ATM (K=S0, r=0): C = P = S0*(2*N(sigma*sqrt(T)/2) - 1)
prem = S0 * (2 * norm.cdf(iv * math.sqrt(T) / 2) - 1)
call_pay, put_pay = (S1 - S0).clip(lower=0), (S0 - S1).clip(lower=0)
pay = np.where(d > 0, call_pay, np.where(d < 0, put_pay, 0.0))
buy = pd.Series(np.where(d != 0, (pay - 1.05 * prem) / S0, 0.0), index=s.index)
sell_pay = np.where(d > 0, put_pay, np.where(d < 0, call_pay, 0.0))      # sell the opposite-side option
sell = pd.Series(np.where(d != 0, (0.95 * prem - sell_pay) / S0, 0.0), index=s.index)
for name, ser in ({} if ("--calibrated" in sys.argv or "--holdout" in sys.argv) else {"083a BUY ATM same-day option in signal direction": buy,
                  "083b SELL ATM same-day option against signal direction": sell}).items():
    r = lib.register("083_month_end_options", name + " (model-priced)", pd.Series(1.0, index=ser.index), ser.fillna(0), 0.0,
                     notes="BS w/ VIX9D|VIX, 5% premium haircut")
    t = ser[d != 0]
    print(f"{name:<56} {lib.fmt(r)}  gate={lib.passes_gate(r)}  mean bp/trade 2002-22={1e4*t.loc[:'2022'].mean():+.1f}")
print(f"avg modelled ATM premium on month-ends: {1e4*(prem/S0)[d!=0].mean():.1f}bp of spot;"
      f" avg |open->close| move: {1e4*(S1/S0-1).abs()[d!=0].mean():.1f}bp")
v1 = lib.get("^VIX1D")["Close"]; v9 = lib.get("^VIX9D")["Close"]
both = pd.concat([v1, v9], axis=1, keys=["v1", "v9"]).dropna()
print(f"VIX1D / VIX9D ratio 2023+: median {both.v1.div(both.v9).median():.2f} (real 0DTE IV vs model IV)")
print("registry size:", lib.n_tests())

# ---- 083c/d (run with --calibrated): fix the intraday-variance underpricing ----
if "--calibrated" in sys.argv and "--holdout" not in sys.argv:
    allprem = (S0 * (2 * norm.cdf(iv * math.sqrt(T) / 2) - 1)) / S0
    absmove = (S1 / S0 - 1).abs()
    trn = slice(lib.TRAIN[0], lib.TRAIN[1])
    k = 1.10 * absmove.loc[trn].mean() / (2 * allprem.loc[trn]).mean()
    print(f"calibration multiplier k = {k:.2f} (straddle priced at 110% of avg realized |open->close| in TRAIN)")
    pc = prem * k
    buy_c = pd.Series(np.where(d != 0, (pay - 1.05 * pc) / S0, 0.0), index=s.index)
    sell_c = pd.Series(np.where(d != 0, (0.95 * pc - sell_pay) / S0, 0.0), index=s.index)
    # sanity: unconditional straddle buyer on NON-month-end days should lose ~VRP
    strad = ((call_pay + put_pay) - 2 * 1.05 * pc) / S0
    print(f"sanity: buying a straddle every non-month-end day: mean {1e4*strad[d==0].loc['2000':'2022'].mean():+.1f}bp/day")
    for name, ser in {"083c BUY ATM option in signal direction (calibrated)": buy_c,
                      "083d SELL ATM option against signal direction (calibrated)": sell_c}.items():
        r = lib.register("083_month_end_options", name, pd.Series(1.0, index=ser.index), ser.fillna(0), 0.0,
                         notes=f"BS x{k:.2f} calibration, 5% haircut")
        t = ser[d != 0]
        per_trade = {p: round(1e4 * t.loc[a:b].mean(), 1) for p, (a, b) in {"train": lib.TRAIN, "valid": lib.VALID}.items()}
        print(f"{name:<58} {lib.fmt(r)}  per-trade bp {per_trade}  (gate on per-trade bp: "
              f"{r['train_t']>=2 and r['valid_t']>=2 and r['valid_sharpe']>=0.5 and per_trade['valid']>=1})")

if "--holdout" in sys.argv:
    allprem = (S0 * (2 * norm.cdf(iv * math.sqrt(T) / 2) - 1)) / S0
    trn = slice(lib.TRAIN[0], lib.TRAIN[1])
    k = 1.10 * (S1 / S0 - 1).abs().loc[trn].mean() / (2 * allprem.loc[trn]).mean()
    pc = prem * k
    sell_c = pd.Series(np.where(d != 0, (0.95 * pc - sell_pay) / S0, 0.0), index=s.index)
    h = lib.holdout_eval("083_month_end_options", "083d SELL ATM option against signal (calibrated, model-priced)",
                         pd.Series(1.0, index=sell_c.index), sell_c.fillna(0), 0.0)
    t = sell_c[d != 0].loc["2023":]
    print("HOLDOUT", {kk: round(float(v), 2) for kk, v in h.items()})
    print(f"holdout per-trade: mean {1e4*t.mean():+.1f}bp, hit {100*(t>0).mean():.0f}%, n={len(t)}, worst {1e4*t.min():.0f}bp")
    # stock version for comparison on same days
    st = (d * (S1 / S0 - 1) - 1e-4)[d != 0].loc["2023":]
    print(f"stock (013) on same days: mean {1e4*st.mean():+.1f}bp, hit {100*(st>0).mean():.0f}%, worst {1e4*st.min():.0f}bp")
