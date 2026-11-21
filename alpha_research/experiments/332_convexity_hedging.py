"""332 (idea B3) - Mortgage convexity hedging: continuation of big 10y-yield moves inside the "convexity zone".
Mechanism: MBS holders/servicers are short convexity. When yields jump, MBS duration extends and hedgers sell
Treasuries (pay swaps); when yields fall they buy. That amplifies the move -> continuation over the next days, most when
current mortgage rates sit near the outstanding coupon stack (prepayment sensitivity is highest).

Data: FRED DGS10 (daily; value for day t is the close-t level, published by H.15 next business day -> known before
the lag-1 entry at close t+1), FRED MORTGAGE30US (Freddie Mac PMMS, weekly, dated the Thursday of release; used only
from the session AFTER its date: lib.asof(strict=True)), Yahoo TLT/IEF/MBB close->close.
Zone (known at t): latest released MORTGAGE30US within +-75bp of the mean of its weekly values 3-7 years earlier
(weeks t-364..t-156, a proxy for the outstanding coupon stack).
Trigger at close t: |DGS10_t - DGS10_{t-5}| > 90th pct of that quantity over the TRAIN period 2002-2014 (fixed number).
Trade follows the yield move: rate up -> short bonds, rate down -> long bonds. Held 5 sessions (the most recent trigger
sets the sign if triggers overlap), entered lag-1 at the close (position from close t+1 to close t+6).
Costs 1bp/side. Pre-specified variants (max 4, no tuning after results):
  A TLT, all triggers (unconditional).
  B TLT, in-zone triggers only.  Interaction = B vs the out-of-zone remainder (A's triggers not in zone), reported as a
    Welch t on per-event 5-day trade returns (descriptive; not a separate tradeable variant).
  C IEF, in-zone triggers only (most hedging at the 5-10y point).
  D MBB - IEF (short MBB, long IEF, 1 unit each) after any in-zone trigger regardless of direction (does MBS
    underperform duration-matched Treasuries while hedgers trade?), MBB 2007+.
  (Card variant (b), the 1-day >10bp trigger, is dropped to keep to 4 variants.)
Registered as pos = gross exposure, ret = net pnl / exposure (cost inside, cost_bp=0 in register) so bp_per_trade is
per unit traded. Timing value = net - mean signed exposure (2002-2022) * underlying returns (strips static beta).
Nothing from 2023+ is printed; holdout untouched."""
import os, sys, io
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

C = 1.0; EXP = "332_convexity_hedging"


def fred(s):
    p = os.path.join(lib.DATA, f"fred_{s}.csv")
    if not os.path.exists(p):
        txt = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s}", timeout=60).text
        open(p, "w").write(txt)
    df = pd.read_csv(p, na_values=".")
    df.index = pd.to_datetime(df.iloc[:, 0])
    return pd.to_numeric(df.iloc[:, 1], errors="coerce").dropna()


TK = ["TLT", "IEF", "MBB"]
D = {s: lib.get(s) for s in TK}
idx = D["TLT"].index.intersection(D["IEF"].index)
idx = idx[(idx >= "2002-07-31")]
R = pd.DataFrame({s: D[s]["cc"].reindex(idx) for s in TK}).fillna(0.0)

dgs = fred("DGS10")
y = dgs.reindex(idx).ffill()                       # close-t level (known before close t+1)
d5 = y - y.shift(5)
thr = d5.abs().loc["2002":"2014"].quantile(0.90)
trig = d5.abs() > thr
mort = fred("MORTGAGE30US")
stack = mort.shift(156).rolling(209, min_periods=200).mean()
zone_w = ((mort - stack).abs() <= 0.75)
zone_w = zone_w[stack.notna()].astype(float)
zone = lib.asof(zone_w, idx, strict=True).fillna(0.0).astype(bool)   # only values dated strictly before t
gap = (lib.asof(mort, idx, strict=True) - lib.asof(stack[stack.notna()], idx, strict=True))


def hold(ev_sign, n=5):
    """event sign at close t -> held for n sessions (most recent event wins). Weight decided at close t."""
    s = ev_sign.replace(0.0, np.nan)
    return s.ffill(limit=n - 1).fillna(0.0)


sgn = -np.sign(d5)                                  # rate up -> short bonds
ev_all = sgn.where(trig, 0.0).fillna(0.0)
ev_in = sgn.where(trig & zone, 0.0).fillna(0.0)
ev_out = sgn.where(trig & ~zone, 0.0).fillna(0.0)

W = {
    "A": pd.DataFrame({"TLT": hold(ev_all)}),
    "B": pd.DataFrame({"TLT": hold(ev_in)}),
    "C": pd.DataFrame({"IEF": hold(ev_in)}),
}
spread_on = hold(ev_in.abs()) * (R.index >= "2007-04-01")
W["D"] = pd.DataFrame({"MBB": -spread_on, "IEF": spread_on})
desc = {"A": "TLT follow big 5d DGS10 move, all triggers, hold5 lag1",
        "B": "TLT follow, in-zone triggers only, hold5 lag1",
        "C": "IEF follow, in-zone triggers only, hold5 lag1",
        "D": "short MBB / long IEF after in-zone trigger, hold5 lag1"}
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x))

print(f"5d |dDGS10| 90th pct (2002-14) = {thr*100:.1f}bp; in-zone share of days 2002-22: "
      f"{zone.loc[:'2022'].mean()*100:.0f}%")
zy = zone.loc[:"2022"].groupby(zone.loc[:"2022"].index.year).mean()
print("   zone frac by year:", " ".join(f"{k % 100:02d}:{v:.2f}" for k, v in zy.items()))
for nm, ev in [("all", ev_all), ("in", ev_in), ("out", ev_out)]:
    e = ev.loc[:"2022"] != 0
    print(f"trigger days {nm}: train {int(e.loc[:'2014'].sum())} / valid {int(e.loc['2015':].sum())}")

rows = {}
for k in "ABCD":
    Wk = W[k]; Rk = R[Wk.columns]
    net = lib.backtest_weights(Wk, Rk, C, lag=1).loc[:"2022"]
    P = Wk.shift(2).fillna(0.0).reindex(net.index)
    g = P.abs().sum(axis=1)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register(EXP, f"332{k} {desc[k]}", g, ret, 0.0, notes="B3 convexity hedging, 1bp/side, lag1")
    rows[k] = r
    beta = P.mean()
    tv = net - (Rk.reindex(net.index) * beta).sum(axis=1)
    print(f"332{k} {desc[k]}\n   {lib.fmt(r)}  train_bp/unit={r['train_bp_per_trade']:+.2f}  "
          f"timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':'2022']):+.2f}  gate={lib.passes_gate(r)}")
    yr = net.groupby(net.index.year).sum() * 1e4
    print("   yearly bp:", " ".join(f"{yy % 100:02d}:{v:+.0f}" for yy, v in yr.items()))

# interaction: per-event 5-session forward TLT/IEF return in trade direction, entry close t+1 -> close t+6
for tk in ["TLT", "IEF"]:
    lr = np.log1p(R[tk])
    fwd = lr.rolling(5).sum().shift(-6)               # sum of returns t+2..t+6
    for per, a, b in [("train", "2002", "2014"), ("valid", "2015", "2022")]:
        res = {}
        for nm, ev in [("in", ev_in), ("out", ev_out)]:
            e = ev.loc[a:b]; e = e[e != 0]
            x = (e * fwd.reindex(e.index)).dropna() * 1e4
            res[nm] = x
        i, o = res["in"], res["out"]
        wt = (i.mean() - o.mean()) / np.sqrt(i.var() / len(i) + o.var() / len(o))
        print(f"{tk} {per}: in-zone n={len(i)} mean {i.mean():+.1f}bp (t {tt(i):+.2f}) | out n={len(o)} "
              f"mean {o.mean():+.1f}bp (t {tt(o):+.2f}) | diff t {wt:+.2f}  [overlapping events, t overstated]")
print("registry size:", lib.n_tests())
