"""372 - Idea card E1: month-end bond-index-extension bid in currency-HEDGED global bonds and local markets.
Pre-specified (written before any results were seen). Signal = calendar only: long over the last 3 sessions of
each month, where 'session' and 'month' follow EACH instrument's own exchange calendar (Yahoo bars are dated in
local exchange time, so ASX/TSE month-ends are local). Entry at the close of the 4th-to-last session, exit at the
month-end close (same convention as 277/278/284; the calendar is known in advance, so no look-ahead).
Close->close adjusted returns, cost 2bp/side (coordinator). Train <=2014, valid 2015-22. No holdout look.
  V1 BNDX   (Vanguard Total Intl Bond, USD-hedged; Jun 2013+, US calendar)
  V2 IAF.AX (iShares Core Composite Bond, Australia; AUD returns, ASX calendar)
  V3 2510.T (NEXT FUNDS NOMURA-BPI; JPY returns, TSE calendar)
  V4 EMLC - beta*CEW (EM local-ccy bonds with FX basket removed; beta = OLS of EMLC on CEW daily cc returns over
     train period only; US calendar; turnover cost on both legs)
Also reported (descriptive, not new strategies): timing value vs an exposure-matched always-long holding
(window P&L minus frac_in_window * always-long return, daily), and a Stouffer pooled t of V1-V3.
Comparison with 277/278/284 (IGLT.L, XBB.TO, EUNH.DE, BWX, EMB): those were unhedged/USD-sovereign; this card adds
hedged and local-currency series only.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

COST = 2.0
END = "2022-12-31"          # descriptive output never goes past this


def w3(idx):
    ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    # position decided at close t applies to t+1's return (backtest_weights shifts by 1)
    return ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)


def tstat(x):
    x = x.dropna(); return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


rows = {}
def run(name, W, R):
    net = lib.backtest_weights(W, R, COST).dropna()
    r = lib.register("372_monthend_bond_hedged_local", name, pd.Series(1.0, index=net.index), net, 0.0,
                     notes="last 3 local sessions of month, 2bp/side")
    rows[name] = r
    # timing value vs exposure-matched always-long (gross position path = W.sum(axis=1))
    pos = W.sum(axis=1).reindex(R.index).fillna(0).shift(1).fillna(0)
    alw = (R * W.max().where(W.max() > 0, W.min())).sum(axis=1)   # always-long, same leg weights
    out = {}
    for p, (a, b) in lib.PERIODS.items():
        n, al, ps = net.loc[a:b], alw.loc[a:b], pos.loc[a:b]
        if len(n) < 50: out[p] = None; continue
        frac = ps.mean()
        tv = n - frac * al
        ev = int((ps.diff() > 0).sum())
        out[p] = dict(t=tstat(n), sh=n.mean() / n.std() * np.sqrt(252), bp=1e4 * n[ps > 0].mean() * 3, ev=ev,
                      tv_t=tstat(tv), tv_bp=1e4 * tv.sum() / max(ev, 1), start=n.index[0].date())
    print(f"{name:<28} {lib.fmt(r)}")
    for p, o in out.items():
        if o: print(f"   {p}: start {o['start']} events {o['ev']} t {o['t']:+.2f} Sh {o['sh']:+.2f} net bp/trade {o['bp']:+.1f}"
                    f" | timing value t {o['tv_t']:+.2f}, {o['tv_bp']:+.1f} bp/event")
    return net, out


nets = {}
for v, sym in [("V1", "BNDX"), ("V2", "IAF.AX"), ("V3", "2510.T")]:
    d = lib.get(sym).loc[:END]
    W = w3(d.index).to_frame(sym)
    nets[v] = run(f"{v} {sym} last3 local", W, d.cc.to_frame(sym))

e, c = lib.get("EMLC").loc[:END], lib.get("CEW").loc[:END]
R = pd.concat([e.cc.rename("EMLC"), c.cc.rename("CEW")], axis=1).dropna()
tr = R.loc[lib.TRAIN[0]:lib.TRAIN[1]]
beta = np.cov(tr.EMLC, tr.CEW)[0, 1] / tr.CEW.var()
print(f"V4 beta (train {tr.index[0].date()}..{tr.index[-1].date()}): {beta:.3f}")
p = w3(R.index)
W = pd.DataFrame({"EMLC": p, "CEW": -beta * p})
nets["V4"] = run("V4 EMLC-beta*CEW last3", W, R)

print("\nPooled Stouffer t (V1-V3, equal weight):")
for per in ["train", "valid"]:
    ts = [nets[v][1][per]["t"] for v in ["V1", "V2", "V3"] if nets[v][1][per]]
    tvs = [nets[v][1][per]["tv_t"] for v in ["V1", "V2", "V3"] if nets[v][1][per]]
    print(f"  {per}: n={len(ts)} pooled t {np.sum(ts)/np.sqrt(len(ts)):+.2f}  pooled timing-value t {np.sum(tvs)/np.sqrt(len(tvs)):+.2f}")

print("\nYearly net bp (2015-22):")
yr = pd.DataFrame({k: v[0].loc["2015":END] for k, v in nets.items()}).groupby(lambda x: x.year).sum() * 1e4
print(yr.round(1).to_string())
print("gate:", {k: lib.passes_gate(r) for k, r in rows.items()})
print("registry size:", lib.n_tests())
