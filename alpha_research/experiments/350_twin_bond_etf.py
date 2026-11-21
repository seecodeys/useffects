"""350 - Twin bond-ETF residual reversion (idea card 1, results/ideas_options_etf.md).
Mechanism: bond ETFs absorb flow faster than their illiquid baskets; APs create/redeem slowly and at a cost, so a fund
hit by a flow shock trades at a temporary premium/discount. Two funds holding nearly the same bonds get different flow
shocks; the gap between their returns is mostly a premium/discount difference that should close as arbitrage catches up.
No NAV data used (issuer NAV pages block scripts; not bypassed) - the twin's return is the NAV proxy.

Signal (all known at close t): r = Yahoo adjusted close->close returns (dividend-corrected; ex-dates differ by fund).
  beta_t = trailing 250d OLS slope of r_A on r_B (days t-249..t).   e_t = r_A,t - beta_t * r_B,t
  S_t = e_t + e_{t-1} + e_{t-2};  z_t = (S_t - mean250(S)) / sd250(S)  (trailing 250 days through t)
  Trigger |z_t| > 2: short the rich leg, long the cheap leg, beta-neutral: w_A = -sign(z)/(1+|b|), w_B = +sign(z)*b/(1+|b|)
  (gross 1). Hold 3 sessions; a new trigger restarts the hold with the new sign/beta.
Implementation: lib.backtest_weights(W, R, 1bp/side, lag=1) - signal at close t, position from close t+1 (REALISTIC).
Costs 1bp/side per leg (bond ETFs), charged on turnover of both legs.
Pre-specified variants (max 4, fixed before results; no tuning after):
  A HYG vs JNK (A=HYG, B=JNK)
  B A, only triggers when HYG close / max(HYG close, last 5 sessions) - 1 < -2% (stress, AP capacity binds)
  C MUB vs VTEB (VTEB 2015-08+, so valid-only; train empty -> descriptive)
  D EMB vs PCY
Each variant is ALSO registered at lag=0 (position from close t, same closing print that made the signal) labelled
BEST-CASE reference only; the lag-1 rows are the verdict rows.
Registered as pos = gross exposure, ret = net pnl / exposure (cost inside, cost_bp=0) so bp_per_trade = net bp per
unit-gross day held. Timing value = net - static mean weights (2000-2022) * leg returns. Nothing from 2023+ printed.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "350_twin_bond_etf"; C = 1.0; W_ = 250; HOLD = 3
PAIRS = {"A": ("HYG", "JNK", False), "B": ("HYG", "JNK", True), "C": ("MUB", "VTEB", False), "D": ("EMB", "PCY", False)}
DESC = {"A": "HYG vs JNK", "B": "HYG vs JNK, HYG 5d dd<-2%", "C": "MUB vs VTEB", "D": "EMB vs PCY"}
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x)) if x.std() > 0 else np.nan
D = {}
def px(s):
    if s not in D: D[s] = lib.get(s)
    return D[s]

def weights(a, b, stress):
    da, db = px(a), px(b)
    idx = da.index.intersection(db.index)
    ra, rb = da["cc"].reindex(idx), db["cc"].reindex(idx)
    beta = ra.rolling(W_, min_periods=W_).cov(rb) / rb.rolling(W_, min_periods=W_).var()
    e = ra - beta * rb
    S = e.rolling(3, min_periods=3).sum()
    z = (S - S.rolling(W_, min_periods=W_).mean()) / S.rolling(W_, min_periods=W_).std()
    trig = (z.abs() > 2)
    if stress:
        c = da["aC"].reindex(idx)
        trig &= (c / c.rolling(5).max() - 1) < -0.02
    sgn = np.sign(z).where(trig)
    bt = beta.where(trig)
    # hold 3 sessions (trigger day + 2 more weight-decision days); new trigger restarts
    sgn_h = sgn.ffill(limit=HOLD - 1); b_h = bt.ffill(limit=HOLD - 1)
    gross = 1 + b_h.abs()
    W = pd.DataFrame({a: -sgn_h / gross, b: sgn_h * b_h / gross}).fillna(0.0)
    R = pd.DataFrame({a: ra, b: rb}).fillna(0.0)
    return W, R, trig

rows = []
for k, (a, b, st) in PAIRS.items():
    W, R, trig = weights(a, b, st)
    for lag in (1, 0):
        net = lib.backtest_weights(W, R, C, lag=lag).loc[:"2022"]       # never touch 2023+ beyond registry eval
        P = W.shift(1 + lag).fillna(0.0).loc[:"2022"]
        g = P.abs().sum(axis=1)
        ret = (net / g).where(g > 0, 0.0)
        name = f"350{k}{'' if lag else '0'} {DESC[k]} lag{lag}" + ("" if lag else " BEST-CASE")
        # register needs full-index series; pass 2022-truncated (register only scores train/valid anyway)
        r = lib.register(EXP, name, g, ret, 0.0, notes=f"{a}/{b} resid z>2 fade 3d, beta-neutral, 1bp/side/leg, lag{lag}")
        Rr = R.loc[:"2022"]
        beta_static = P.mean()
        tv = net - (Rr * beta_static).sum(axis=1)
        legpnl = (P * Rr.reindex(P.index)).loc["2000":"2022"]
        ev_tr = int(trig.loc[:"2014"].sum()); ev_va = int(trig.loc["2015":"2022"].sum())
        # net bp per trade (trigger episode): total net bp / number of triggers
        nbp = lambda per: 1e4 * net.loc[per[0]:per[1]].sum() / max(int(trig.loc[per[0]:per[1]].sum()), 1)
        tv_tr = tt(tv.loc[:"2014"][g.loc[:"2014"] >= 0]) if g.loc[:"2014"].sum() > 0 else np.nan
        tv_va = tt(tv.loc["2015":"2022"])
        print(f"{name}: triggers tr/va {ev_tr}/{ev_va}  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
        print(f"   net bp/trigger tr {nbp(lib.TRAIN):+.1f} va {nbp(lib.VALID):+.1f} | timing t tr {tv_tr:+.2f} va {tv_va:+.2f}"
              f" | leg bp tr {a} {1e4*legpnl[a].loc[:'2014'].sum():+.0f} {b} {1e4*legpnl[b].loc[:'2014'].sum():+.0f}"
              f" va {a} {1e4*legpnl[a].loc['2015':].sum():+.0f} {b} {1e4*legpnl[b].loc['2015':].sum():+.0f}")
        yr = net.groupby(net.index.year).sum() * 1e4
        yr = yr[yr.index >= R.index[0].year]
        print("   yearly net bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items() if y <= 2022))
        r.update(variant=name, trig_tr=ev_tr, trig_va=ev_va, nbp_tr=nbp(lib.TRAIN), nbp_va=nbp(lib.VALID),
                 tv_tr=tv_tr, tv_va=tv_va, gate=lib.passes_gate(r))
        rows.append(r)
for s in ("HYG", "JNK", "MUB", "VTEB", "EMB", "PCY"):
    print(s, "data from", px(s).index[0].date())
pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "350_summary.csv"), index=False)
print("registry size:", lib.n_tests(), " luck t ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
