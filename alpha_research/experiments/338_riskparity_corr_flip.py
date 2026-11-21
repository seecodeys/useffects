"""338 - Risk-parity deleveraging when the stock-bond correlation flips positive (idea card P8, results/ideas_papers.md).
Mechanism: risk-parity / vol-target stock-bond books scale to a portfolio-vol target. When the SPY-TLT correlation turns
positive, portfolio vol jumps even if single-asset vols don't, forcing sales of both legs (esp. the levered bond leg)
over the following days. Different trigger from the equity-vol tests (018, 325) and the vol-control overlay (122).

Pre-specified BEFORE looking at results (no tuning):
  corr = 21-session rolling correlation of SPY and TLT daily close->close returns (known at close t).
  Up-trigger at close t: corr[t] > +0.2 and corr[t-1] <= +0.2, min(corr[t-63..t-1]) < 0, and no other raw up-crossing
  of +0.2 in the prior 42 sessions. Positions run from close t+1 (lag=1) for 10 sessions, close->close.
  A: short 50/50 SPY+TLT (weights -0.5/-0.5) for 10 sessions.
  B: short TLT alone (weight -1) for 10 sessions.
  C: A, only when 21d realized vol of the 50/50 mix at t is above its trailing 252-session median.
  D: placebo / mirror: corr crosses below -0.2 (having been > 0 in prior 63, first down-crossing in 42) -> long 50/50.
Costs 0.5bp/side SPY, 1bp/side TLT. Data cut at 2022-12-31 (holdout never touched). Train 2003-14, valid 2015-22.
Timing value = net P&L minus (period-mean exposure x underlying return), i.e. vs an exposure-matched constant holding.
Registered as (pos=1, ret=net daily P&L, cost 0) like 325, so bp_per_trade is bp per calendar day of the book.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
spy, tlt = lib.get("SPY"), lib.get("TLT")
R = pd.concat([spy.cc.rename("SPY"), tlt.cc.rename("TLT")], axis=1).dropna().loc[:END]
corr = R.SPY.rolling(21).corr(R.TLT)
mix = 0.5 * R.SPY + 0.5 * R.TLT
mvol = mix.rolling(21).std()
mvol_med = mvol.rolling(252).median()
COST = {"SPY": 0.5, "TLT": 1.0}
H, GAP, LOOK = 10, 42, 63


def triggers(up=True):
    if up:
        raw = (corr > 0.2) & (corr.shift(1) <= 0.2)
        prior = corr.shift(1).rolling(LOOK).min() < 0
    else:
        raw = (corr < -0.2) & (corr.shift(1) >= -0.2)
        prior = corr.shift(1).rolling(LOOK).max() > 0
    recent = raw.astype(float).shift(1).rolling(GAP, min_periods=1).max().fillna(0) > 0
    return (raw & prior & ~recent).fillna(False)


def run(name, trig, wvec):
    on = trig.astype(float).rolling(H, min_periods=1).max()        # 1 for H sessions starting at trigger close
    W = pd.DataFrame({k: on * v for k, v in wvec.items()}, index=R.index)
    net = sum(lib.backtest_weights(W[[k]], R[[k]], COST[k], lag=1) for k in W)
    held = W.shift(2).fillna(0)                                     # exposure actually earning each day
    under = (held * R).sum(axis=1)
    net = net.loc["2003":END]
    row = lib.register("338_riskparity_corr_flip", name, pd.Series(1.0, index=net.index), net, 0.0,
                       notes="lag1; SPY 0.5bp TLT 1bp; pos=1 ret=net convention")
    tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
    out = {"variant": name}
    for p, (a, b) in {"train": ("2003", "2014"), "valid": ("2015", END)}.items():
        # exposure-matched constant holding: period-mean weights x asset returns
        wbar = held.loc[a:b].mean()
        bench = (R.loc[a:b] * wbar).sum(axis=1)
        tv = net.loc[a:b] - bench
        ev = trig.loc[a:b]
        evr = []
        for d in ev[ev].index:                                       # per-event gross P&L of the held book
            i = R.index.get_loc(d)
            evr.append(under.iloc[i + 2:i + 2 + H].sum())
        out.update({f"{p}_events": int(ev.sum()), f"{p}_t": row[f"{p}_t"], f"{p}_sharpe": row[f"{p}_sharpe"],
                    f"{p}_timing_t": tt(tv), f"{p}_event_mean_bp": 1e4 * np.mean(evr) if evr else np.nan,
                    f"{p}_event_hit_%": 100 * np.mean(np.array(evr) > 0) if evr else np.nan})
    yr = net.groupby(net.index.year).sum() * 1e4
    out["yearly_bp"] = " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items() if v != 0)
    out["gate"] = lib.passes_gate(row)
    print(f"{name}: ev {out['train_events']}/{out['valid_events']}  {lib.fmt(row)}  timing t "
          f"{out['train_timing_t']:+.2f}/{out['valid_timing_t']:+.2f}  ev mean bp "
          f"{out['train_event_mean_bp']:+.0f}/{out['valid_event_mean_bp']:+.0f}  hit "
          f"{out['train_event_hit_%']:.0f}/{out['valid_event_hit_%']:.0f}  gate {out['gate']}")
    print("   yearly bp:", out["yearly_bp"])
    return out


up, dn = triggers(True), triggers(False)
upC = up & (mvol > mvol_med)
res = [run("338A short 50/50 SPY+TLT 10d on corr flip >+0.2", up, {"SPY": -0.5, "TLT": -0.5}),
       run("338B short TLT 10d on corr flip >+0.2", up, {"TLT": -1.0}),
       run("338C = A only when 21d mix vol > 1y median", upC, {"SPY": -0.5, "TLT": -0.5}),
       run("338D placebo: corr flip <-0.2 -> long 50/50 10d", dn, {"SPY": 0.5, "TLT": 0.5})]
pd.DataFrame(res).to_csv(os.path.join(lib.HERE, "results", "338_riskparity_corr_flip.csv"), index=False)
print("registry size:", lib.n_tests(), " luck t ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
