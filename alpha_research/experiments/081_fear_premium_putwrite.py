"""081 - Behavioral: investors overpay for crash insurance when fear is elevated -> put-writing earns more then.
Instruments: CBOE PutWrite indices (real option prices): PUT (monthly ATM SPX puts, 1991+), WPUT (weekly, 2006+).
Fear premium FP = VIX - 20d realized vol of SPY (annualised %), at the prior close.
Decision only at each index roll date (PUT: 3rd Friday; WPUT: every Friday), held until the next roll:
  write if FP > TRAIN median, else stay in T-bills.
Measured as TIMING VALUE: excess-over-T-bill return of the timed strategy minus exposure-matched always-on writing
(per period), so the general put-writing premium doesn't count. Switching cost 10bp of notional per entry/exit.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib


def cboe(name):
    df = pd.read_csv(os.path.join(lib.DATA, "cboe", f"{name}.csv"))
    df["DATE"] = pd.to_datetime(df["DATE"], format="%m/%d/%Y")
    return df.set_index("DATE").iloc[:, 0].astype(float).sort_index()


spy = lib.get("SPY")
vix = lib.get("^VIX")["Close"]
irx = lib.get("^IRX")["Close"] / 100 / 252                    # daily T-bill
rv = spy.cc.rolling(20).std() * np.sqrt(252) * 100
fp = (vix.reindex(spy.index) - rv)
med = fp.loc[lib.TRAIN[0]:lib.TRAIN[1]].median()
for name, roll_rule in ([] if "--daily" in sys.argv else [("PUT", "monthly"), ("WPUT", "weekly")]):
    lvl = cboe(name)
    idx = lvl.index.intersection(spy.index)
    lvl = lvl.reindex(idx)
    x = lvl.pct_change() - irx.reindex(idx).ffill().fillna(0)      # excess return of writing
    if roll_rule == "monthly":
        roll = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
    else:
        roll = pd.Series(idx.dayofweek == 4, index=idx)
    dec = (fp.reindex(idx).shift(1) > med).astype(float)            # info at prior close
    pos = dec.where(roll).ffill().shift(1).fillna(0)                # set at roll close, earns from next day
    cost = pos.diff().abs().fillna(0) * 10 / 1e4
    strat = pos * x - cost
    out = pd.Series(0.0, index=idx)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        m = pos.loc[a:b].mean()
        out.loc[a:b] = strat.loc[a:b] - m * x.loc[a:b]
    out = out.loc["2000":].fillna(0)
    r = lib.register("081_fear_putwrite", f"081 {name}: write only when fear premium > median (timing value)",
                     pd.Series(1.0, index=out.index), out, 0.0, notes="excess over exposure-matched always-on; 10bp switch cost")
    print(f"081 {name:<5} {lib.fmt(r)}  gate={lib.passes_gate(r)}  time-writing={100*pos.loc['2000':'2022'].mean():.0f}%")
    # descriptive: excess return of writing by FP tercile (train+valid)
    k = pd.DataFrame({"x": x, "fp": fp.reindex(idx).shift(1)}).loc["2000":"2022"].dropna()
    print("     ann. excess of always-writing by prior-day FP tercile (%):",
          (k.groupby(pd.qcut(k.fp, 3, labels=["low", "mid", "high"]), observed=True).x.mean() * 252 * 100).round(1).to_dict())
print("registry size:", lib.n_tests())

# ---- 081b (run with --daily): daily exposure decision (hypothesis formed from the tercile table above) ----
if "--daily" in sys.argv:
    for name, cost_bp in [("PUT", 10), ("PUT", 25)]:
        lvl = cboe(name); idx = lvl.index.intersection(spy.index); lvl = lvl.reindex(idx)
        x = lvl.pct_change() - irx.reindex(idx).ffill().fillna(0)
        pos = (fp.reindex(idx).shift(1) > med).astype(float)        # decided at prior close, held today
        strat = pos * x - pos.diff().abs().fillna(0) * cost_bp / 1e4
        out = pd.Series(0.0, index=idx)
        for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
            out.loc[a:b] = strat.loc[a:b] - pos.loc[a:b].mean() * x.loc[a:b]
        out = out.loc["2000":].fillna(0)
        r = lib.register("081_fear_putwrite", f"081b PUT daily: exposure only when FP>median (timing value, {cost_bp}bp/switch)",
                         pd.Series(1.0, index=out.index), out, 0.0)
        print(f"081b {cost_bp}bp/switch  {lib.fmt(r)}  gate={lib.passes_gate(r)}  switches/yr={pos.diff().abs().loc['2000':'2022'].sum()/23:.0f}")
