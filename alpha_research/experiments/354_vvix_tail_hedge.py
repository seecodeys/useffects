"""354 - Volatility-of-volatility (VVIX) -> subsequent tail-hedge returns (card 5, results/ideas_options_etf.md;
Park 2015 / FEDS 2013-54 "Volatility of Volatility and Tail Risk Premiums").

Hypothesis: when VVIX is high, tail-hedge buyers have bid VIX calls / OTM SPX puts above fair value, so the hedges
subsequently under-perform -> be (more) short tail hedges after high-VVIX closes.

Data: cdn.cboe.com VVIX_History.csv and VXTH_History.csv (copied to data/cboe/), data/cboe/PPUT.csv, Yahoo VIXY
(2011+), ^VIX, ^SP500TR. VVIX prints < 40 (a handful of 2006-07 bad ticks such as 15.71) are dropped as data errors
(rule set before results). VVIX/VIX/index closes are published at the close of day t -> known at the decision close.
All positions via lib.backtest_weights(lag=1): signal at close t, position from close t+1.

Pre-specified variants (4, registered BEFORE seeing results). Each registered series is the TIMING VALUE: strategy
net return minus (average exposure in that period) x always-on return of the same leg (exposure-matched), periods
TRAIN (to 2014) / VALID (2015-22) computed separately.
  A: short VIXY for 5 sessions after any close where VVIX / its 60-session mean is above its TRAIN (2006-14) 80th
     percentile; cost 5bp/side (as 082).  VIXY only exists 2011+, so TRAIN is 2011-14.
  B: protective-put leg L = daily return of PPUT minus daily return of S&P 500 TR. Short L (= underweight the hedge) for
     20 sessions after any close where VVIX is in its TRAIN top tercile (> 67th pct, 2006-14). Cost 2bp/side on L.
  C: as B, signal = VVIX/VIX in its TRAIN top tercile.
  D: as B, leg = VXTH (SPX + 1% VIX calls) minus S&P 500 TR, signal = VVIX TRAIN top tercile. Cost 2bp/side.
Note: B-D trade index differences; the 2bp/side cost understates real option spreads -> flag if anything passes.
Descriptive tables (forward 20-session leg return by VVIX tercile) stop at 2022-12-31.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "354_vvix_tail_hedge"
END = "2022-12-31"


def cboe(name, col):
    p = pd.read_csv(os.path.join(lib.DATA, "cboe", f"{name}.csv"))
    p["DATE"] = pd.to_datetime(p.DATE, format="%m/%d/%Y")
    return p.set_index("DATE")[col].astype(float).sort_index()


vvix = cboe("VVIX", "VVIX")
vvix = vvix[vvix >= 40]
vxth = cboe("VXTH", "VXTH")
pput = cboe("PPUT", "PPUT")
vix = lib.get("^VIX")["Close"]
sptr = lib.get("^SP500TR")["Close"]
vixy = lib.get("VIXY")
spy = lib.get("SPY")
idx = spy.loc["2006-04-03":].index

vv = vvix.reindex(idx)                                  # NaN where Cboe has no print (no signal that day)
ratio60 = vv / vvix.rolling(60).mean().reindex(idx)
vvvix = vv / vix.reindex(idx)

tr_r = sptr.reindex(idx).ffill().pct_change()
leg_pput = (pput.reindex(idx).ffill().pct_change() - tr_r).fillna(0)
leg_vxth = (vxth.reindex(idx).ffill().pct_change() - tr_r).fillna(0)
vixy_r = vixy["cc"].reindex(idx)
vixy_x = (-vixy_r).fillna(0)                            # short-VIXY return
TRAIN0 = {"A": "2011-01-01"}


def hold(ind, n):
    """1 for n sessions starting at each signal close (signal close included; lag handled by backtest_weights)."""
    return ind.astype(float).rolling(n, min_periods=1).max().fillna(0.0)


def timing_value(net, x, wl, t0):
    out = pd.Series(0.0, index=net.index)
    for a, b in [(t0, "2014-12-31"), lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - wl.loc[a:b].mean() * x.loc[a:b]
    return out.loc[t0:]


def run(name, sig, q, x, cost, n, t0="2006-04-03"):
    thr = sig.loc["2006":"2014"].dropna().quantile(q)
    ind = (sig > thr).fillna(False)
    w = hold(ind, n)
    net = lib.backtest_weights(w.to_frame("a"), x.to_frame("a"), cost, lag=1)
    wl = w.shift(2).fillna(0.0)
    if t0 != "2006-04-03":
        net, wl, xx = net.loc[t0:], wl.loc[t0:], x.loc[t0:]
    else:
        xx = x
    tv = timing_value(net, xx, wl, t0)
    row = lib.register(EXP, name, pd.Series(1.0, index=tv.index), tv, 0.0,
                       notes=f"timing value vs exposure-matched; thr={thr:.3f}; cost {cost}bp/side incl.")
    print(f"{name}\n   thr={thr:.3f}  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    for per, (a, b) in {"train": (t0, "2014-12-31"), "valid": lib.VALID}.items():
        ev = int((ind.loc[a:b].astype(int).diff() == 1).sum())
        s_on = lib.evaluate(pd.Series(1.0, index=net.loc[a:b].index), net.loc[a:b], 0)
        s_al = lib.evaluate(pd.Series(1.0, index=xx.loc[a:b].index), xx.loc[a:b], 0)
        print(f"   {per}: signal episodes {ev}, avg exposure {wl.loc[a:b].mean():.2f}, strat Sharpe {s_on['sharpe']:.2f} "
              f"vs always-on {s_al['sharpe']:.2f}, timing value {1e4*tv.loc[a:b].mean()*252:+.0f} bp/yr")
    yr = tv.loc[:END].groupby(tv.loc[:END].index.year).sum() * 1e4
    print("   timing value bp by year:", yr.round(0).astype(int).to_dict())
    return row


def describe(label, sig, leg, t0="2006-04-03"):
    """Forward 20-session cumulative leg return (from close t+1, i.e. lag-1) by TRAIN VVIX tercile, <=2022."""
    f = leg[::-1].rolling(20).sum()[::-1].shift(-2)     # sum of leg returns over t+2..t+21
    c = sig.loc["2006":"2014"].dropna().quantile([1 / 3, 2 / 3]).values
    t = pd.cut(sig, [-np.inf, c[0], c[1], np.inf], labels=["low", "mid", "high"])
    for per, (a, b) in {"train": (t0, "2014-12-31"), "valid": (lib.VALID[0], "2022-11-25")}.items():
        g = (f.loc[a:b] * 1e4).groupby(t.loc[a:b], observed=True).agg(["mean", "count"])
        print(f"   {label} {per}: " + "  ".join(f"{k} {r['mean']:+.0f}bp (n={int(r['count'])})" for k, r in g.iterrows()))


print("VVIX first", vvix.index[0].date(), "| VXTH first", vxth.index[0].date(), "| VIXY first", vixy.index[0].date())
print("Descriptive: forward 20-session leg return (bp, non-overlap NOT enforced) by TRAIN tercile of signal")
describe("PPUT-SPXTR | VVIX", vv, leg_pput)
describe("PPUT-SPXTR | VVIX/VIX", vvvix, leg_pput)
describe("VXTH-SPXTR | VVIX", vv, leg_vxth)
describe("short VIXY | VVIX/MA60", ratio60, vixy_x, "2011-01-04")
rows = [run("354A short VIXY 5d after VVIX/MA60 > train p80 (timing value)", ratio60, 0.8, vixy_x, 5.0, 5, "2011-01-04"),
        run("354B short PPUT-SPXTR 20d after VVIX top train tercile (timing value)", vv, 2 / 3, -leg_pput, 2.0, 20),
        run("354C short PPUT-SPXTR 20d after VVIX/VIX top train tercile (timing value)", vvvix, 2 / 3, -leg_pput, 2.0, 20),
        run("354D short VXTH-SPXTR 20d after VVIX top train tercile (timing value)", vv, 2 / 3, -leg_vxth, 2.0, 20)]
print("registry size:", lib.n_tests(), "| luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
