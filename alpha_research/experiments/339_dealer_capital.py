"""339 - Intermediary (broker-dealer) capital shocks -> risk premia in dealer-intermediated assets (idea card P9;
He, Kelly & Manela 2017). Mechanism: primary dealers are marginal investors in credit; negative shocks to their
equity capital force balance-sheet cuts and the assets they intermediate then earn higher expected returns.
State variable (month-end, last trading day m): D_m = 1-month return of an equal-weight basket GS, MS, JPM, C, BAC
(adjusted closes, equal-weight of the 5 monthly returns) minus SPY's 1-month return. SURVIVORSHIP: Lehman, Bear,
Merrill are missing (hindsight-chosen survivors). Thresholds/z-scores use TRAIN months only (2000-2014).
Position: decided at month-end close m, held through the next month-end; lib.backtest_weights lag=1 (position from
close of first trading day after m to close one day after next month-end). Duration matching (pre-set, approximate
modified durations HYG~4, LQD~8.5, IEF~7.5): HYG - 0.5*IEF, LQD - 1.1*IEF.
Pre-specified variants (max 4, no tuning after):
  A D in bottom TRAIN quintile -> long HYG - 0.5 IEF next month, else flat. 1bp/side.
  B same trigger -> long LQD - 1.1 IEF. 1bp/side.
  C same trigger -> long SPY (timing value). 0.5bp/side.
  D continuous HYG - 0.5 IEF, weight = clip(-z(D), -2, 2)/2 (z with TRAIN mean/sd). 1bp/side.
Evaluation windows: from asset availability (HYG 2007-04, LQD/IEF 2002-08, SPY 2000) to 2022-12-31; train <=2014,
valid 2015-22. Timing value = net P&L minus exposure-matched holdings (held weight x that asset's mean daily return
in the same period). Never reads 2023+."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
px = {s: lib.get(s).aC.loc[:END] for s in ["GS", "MS", "JPM", "C", "BAC", "SPY", "HYG", "LQD", "IEF"]}
P = pd.DataFrame(px)
R = P.pct_change()
idx = P.index[(P.index >= "2000-01-01")]
me = P.groupby(P.index.to_period("M")).apply(lambda x: x.index[-1]).values      # month-end trading days
me = pd.DatetimeIndex(me)
Pm = P.loc[me]
mret = Pm.pct_change()
D = (mret[["GS", "MS", "JPM", "C", "BAC"]].mean(axis=1) - mret.SPY).dropna()
D = D.loc["2000-01-01":]
Dtr = D.loc[:"2014-12-31"]
q20 = Dtr.quantile(0.2); mu, sd = Dtr.mean(), Dtr.std()
trig = (D <= q20).astype(float)
zc = (-(D - mu) / sd).clip(-2, 2) / 2
print(f"D months: {len(D)} train {len(Dtr)}  q20={q20:+.4f}  mu={mu:+.4f} sd={sd:.4f}")

def daily(sig):          # month-end value carried over the following month (value on day m decided at close m)
    return sig.reindex(idx).ffill().fillna(0.0)

def weights(sig, legs):
    s = daily(sig)
    return pd.DataFrame({a: s * w for a, w in legs.items()})

ev = trig[trig > 0]
for per, (a, b) in {"train": ("2000", "2014"), "train07": ("2007-04", "2014"), "valid": ("2015", "2022")}.items():
    print(f"trigger months {per}: {int(ev.loc[a:b].sum())}")
print("trigger month-ends:", [d.strftime("%Y-%m") for d in ev.index])

runs = [("A bottom-quintile D -> HYG-0.5IEF next month lag-1", trig, {"HYG": 1.0, "IEF": -0.5}, 1.0, "2007-04-12"),
        ("B bottom-quintile D -> LQD-1.1IEF next month lag-1", trig, {"LQD": 1.0, "IEF": -1.1}, 1.0, "2002-08-01"),
        ("C bottom-quintile D -> long SPY next month lag-1", trig, {"SPY": 1.0}, 0.5, "2000-02-01"),
        ("D continuous HYG-0.5IEF ~ -z(D) lag-1", zc, {"HYG": 1.0, "IEF": -0.5}, 1.0, "2007-04-12")]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
for name, sig, legs, cb, start in runs:
    W = weights(sig, legs)
    Ra = R[list(legs)].loc[idx]
    net = lib.backtest_weights(W, Ra, cb, lag=1).loc[start:END]
    r = lib.register("339_dealer_capital", f"339{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes=f"cost {cb}bp/side in net")
    held = W.reindex(Ra.index).fillna(0).shift(2).fillna(0).loc[net.index]
    parts = []
    for a, b in ((start, "2014"), ("2015", "2022")):
        bench = (held.loc[a:b] * Ra.loc[a:b].mean()).sum(axis=1)       # exposure-matched holdings
        parts.append(net.loc[a:b] - bench)
    tv = pd.concat(parts)
    on = held.abs().sum(axis=1) != 0
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"339{name}\n   {lib.fmt(r)}  train_bp/day={r['train_bp_per_trade']:+.2f} gate={lib.passes_gate(r)}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}   timing bp/held-day "
          f"{1e4*tv.loc[:'2014'][on.loc[:'2014']].mean():+.2f} / {1e4*tv.loc['2015':][on.loc['2015':]].mean():+.2f}")
    print("   yearly bp:", yr.to_dict())
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2*np.log(max(lib.n_tests(), 2))), 2))
