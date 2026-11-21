"""337 (idea P6) - TIPS seasonal inflation accrual: does TIP-vs-nominal return carry the KNOWN seasonal accrual?
Mechanism: TIPS principal accretes daily by NSA CPI with a 3-month lag. Accrual over month M =
CPI_NSA(M-2)/CPI_NSA(M-3) - 1. CPI(M-2) is released mid-month M-1, so it is known at the M-1 month-end close.
TIP's Yahoo adjusted price already contains the accrual mechanically (index ratio + monthly accrual distributions).
Under efficient pricing the nominal value depends on expected CPI at maturity, not on the near-term path, so a known
high-accrual month should be offset by a quoted-real-yield/clean-price decline and the TIP-minus-nominal return
should NOT vary with the known accrual. If quoted real yields are sticky (priced off trend breakevens), the
hedged spread earns the known excess carry. That is the test.

Point-in-time CPI: FRED CPIAUCNS (NSA, essentially never revised). For decision at end of month M-1 we use only
CPI through month M-2 (released ~10-15th of M-1). No SA series is used (avoids revised seasonal factors).
  a_M   = CPI(M-2)/CPI(M-3) - 1                 (known total accrual for month M)
  trend = mean NSA m/m over the 12 months ending M-2   (what a trend/breakeven-based quote implies)
  x_M   = a_M - trend                            (known excess / seasonal carry)

Trade: decided at month-end close (M-1), held all of month M (weights applied from the first session of M through the
last close of M), close->close adjusted returns, lag=0 (signal uses no price from the decision close).
Hedge ("duration-matched"): h = OLS beta of TIP daily cc on IEF daily cc over the 252 sessions ending at the decision
close (TIP real duration ~7.5-8 vs IEF ~7.5; beta also absorbs the real-vs-nominal yield beta < 1).
Costs 1bp/side on turnover. Pre-specified variants (max 4, no tuning after results):
  A: sign(x_M) * (TIP - h*IEF).                   long TIP/short IEF when known excess carry > 0, reversed when < 0.
  B: clip(a_M / 0.2%, -2, 2) * (TIP - h*IEF).     sized by known total accrual (0.2% = typical monthly CPI change).
  C: sign(x_M) * (VTIP - h*SHY), h from VTIP on SHY beta (VTIP 2012-10+; short TIPS, where the seasonal accrual is
     the biggest share of return per Gapen). Train window only ~2 years.
  D: control: sign(x_M) * IEF alone (nominal bond, no accrual: should be ~0).
Registered as pos = gross exposure, ret = net pnl / exposure (cost inside, cost_bp=0) so bp_per_trade is per unit of
gross exposure. TRAIN Jan 2004-2014 (TIP starts Dec 2003), VALID 2015-2022. Nothing from 2023+ is printed or used
beyond construction; holdout untouched.
"""
import os, sys
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

C = 1.0; EXP = "337_tips_seasonal"


def fred(s):
    p = os.path.join(lib.DATA, f"fred_{s}.csv")
    if not os.path.exists(p):
        txt = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s}", timeout=60).text
        open(p, "w").write(txt)
    df = pd.read_csv(p, na_values=".")
    df.index = pd.to_datetime(df.iloc[:, 0])
    return pd.to_numeric(df.iloc[:, 1], errors="coerce").dropna()


cpi = fred("CPIAUCNS")                       # monthly, indexed by first of reference month
cpi.index = cpi.index.to_period("M")
mm = cpi.pct_change()
# signals for holding month M (Period): use reference month M-2
acc = mm.shift(2)                            # a_M = CPI(M-2)/CPI(M-3)-1
trend = mm.rolling(12).mean().shift(2)
x = acc - trend

TK = ["TIP", "IEF", "VTIP", "SHY"]
D = {s: lib.get(s) for s in TK}
idx = D["IEF"].index
idx = idx[idx >= "2003-12-31"]
R = pd.DataFrame({s: D[s]["cc"].reindex(idx) for s in TK})

# decision dates: last session of each month; weights set there apply from next session (backtest_weights shifts 1)
per = pd.Series(idx.to_period("M"), index=idx)
me = per[per != per.shift(-1)].index           # month-end sessions


def beta(y, x_, d, n=252):
    w = pd.concat([R[y], R[x_]], axis=1).loc[:d].dropna().iloc[-n:]
    if len(w) < 200:
        return np.nan
    c = np.cov(w.iloc[:, 0], w.iloc[:, 1])
    return c[0, 1] / c[1, 1]


W = {k: pd.DataFrame(0.0, index=idx, columns=TK) for k in "ABCD"}
log = []
for d in me:
    M = d.to_period("M") + 1                   # holding month
    if M not in x.index or np.isnan(x.get(M, np.nan)):
        continue
    s = np.sign(x[M]); a = acc[M]
    h = beta("TIP", "IEF", d)
    hv = beta("VTIP", "SHY", d)
    sl = (idx > d) & (idx.to_period("M") == M)
    days = idx[sl]
    if len(days) == 0:
        continue
    # weights decided at close t apply to t+1: set the weight on the decision day and on days of M except the last
    rows = [d] + list(days[:-1])
    if not np.isnan(h):
        W["A"].loc[rows, ["TIP", "IEF"]] = [s, -s * h]
        b = float(np.clip(a / 0.002, -2, 2))
        W["B"].loc[rows, ["TIP", "IEF"]] = [b, -b * h]
        W["D"].loc[rows, "IEF"] = s
    if not np.isnan(hv):
        W["C"].loc[rows, ["VTIP", "SHY"]] = [s, -s * hv]
    log.append({"month": M, "acc": a, "x": x[M], "h": h, "hv": hv})
L = pd.DataFrame(log).set_index("month")
# flatten at end of each month: the last session of M is also the next decision date (overwritten above if traded)

desc = {"A": "sign(excess known accrual) TIP-hIEF", "B": "size=known accrual/0.2% TIP-hIEF",
        "C": "sign(excess known accrual) VTIP-hSHY", "D": "control sign(x) IEF alone"}


def tt(s):
    s = s.dropna(); return s.mean() / s.std() * np.sqrt(len(s)) if s.std() > 0 else np.nan


Lp = L.loc[:"2022-12"]
print("months with signal (2004-22):", len(Lp), " x>0 share:", f"{(Lp.x > 0).mean():.2f}",
      " mean |x| %:", f"{Lp.x.abs().mean()*100:.3f}", " mean hedge h:", f"{Lp.h.mean():.2f}")
cal = Lp.groupby(Lp.index.month)[["acc", "x"]].mean() * 100
print("mean known accrual / excess by holding calendar month (% , 2004-22):")
print("   ", " ".join(f"{m}:{a:+.2f}/{e:+.2f}" for m, (a, e) in cal.iterrows()))

rows = {}
for k in "ABCD":
    Wk = W[k]
    if k == "C":
        Wk = Wk.loc["2012-10-01":]
    Rk = R[Wk.columns].reindex(Wk.index).fillna(0.0)
    net = lib.backtest_weights(Wk, Rk, C, lag=0).loc["2004-01-01":"2022-12-31"]
    P = Wk.shift(1).fillna(0.0).reindex(net.index)
    g = P.abs().sum(axis=1)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register(EXP, f"337{k} {desc[k]}", g, ret, 0.0, notes="P6 TIPS seasonal accrual, monthly, 1bp/side")
    rows[k] = r
    print(f"337{k} {desc[k]}\n   {lib.fmt(r)}  train_bp/unit/day={r['train_bp_per_trade']:+.2f}  gate={lib.passes_gate(r)}")
    mon = net.groupby(net.index.to_period("M")).sum()
    print(f"   monthly net bp: train mean {mon.loc[:'2014'].mean()*1e4:+.1f} (t {tt(mon.loc[:'2014']):+.2f}, "
          f"n={len(mon.loc[:'2014'])})  valid mean {mon.loc['2015':].mean()*1e4:+.1f} (t {tt(mon.loc['2015':]):+.2f})")
    yr = net.groupby(net.index.year).sum() * 1e4
    print("   yearly bp:", " ".join(f"{yy % 100:02d}:{v:+.0f}" for yy, v in yr.items()))

# descriptive: monthly hedged TIP-hIEF spread return (gross, no sign) regressed on known excess accrual x
sp = []
for M, rw in L.iterrows():
    if M > pd.Period("2022-12", "M") or np.isnan(rw.h):
        continue
    dd = idx[idx.to_period("M") == M]
    r_ = (R.loc[dd, "TIP"] - rw.h * R.loc[dd, "IEF"]).sum()
    sp.append({"month": M, "sp": r_, "x": rw.x, "acc": rw.acc})
S = pd.DataFrame(sp).set_index("month")
for nm, a, b in [("train", "2004-01", "2014-12"), ("valid", "2015-01", "2022-12")]:
    s_ = S.loc[a:b]
    X = np.c_[np.ones(len(s_)), s_.x.values]
    beta_, res, *_ = np.linalg.lstsq(X, s_.sp.values, rcond=None)
    e = s_.sp.values - X @ beta_
    se = np.sqrt(np.diag(np.linalg.inv(X.T @ X)) * (e @ e) / (len(s_) - 2))
    print(f"descr {nm}: monthly hedged spread on x: slope {beta_[1]:+.2f} (t {beta_[1]/se[1]:+.2f}); "
          f"slope=1 means full pass-through of the known excess carry; spread sd {s_.sp.std()*1e4:.0f}bp, n={len(s_)}")
print("registered tests in this registry:", lib.n_tests())
