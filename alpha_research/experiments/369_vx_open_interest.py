"""369 - Cboe CFE VX futures open interest -> VIXY / SPY (data source 6, results/data_sources_new.md, H6a/H6b).

Data: per-contract daily files from cdn.cboe.com (archive CFE_{M}{YY}_VX.csv for 2005-2012 expiries,
VX_{expiry}.csv for 2013+ expiries; monthly contracts only, cached in data/cfe_vx/). Columns used: Trade Date,
Settle (Close if Settle==0), Open Interest. Contract expiry = last trade date in its file.
Publication lag: OI for day t is published the next morning. Every feature is indexed at its OI date t; it is first
known before the open of t+1, so the position is decided at close t+1 -> lib.backtest_weights(..., lag=1)
(position earns from the close-(t+1) -> close-(t+2) return onward). Settles used for the contango flag are also day t.

Not a duplicate: 082 (VIXY short in contango / fear overshoot, no OI), 154 (VIXY big-day fade), 301 (weekly CFTC TFF
trader-category VIX positioning), 354 (VVIX), 032 (VIX-expiry Wednesday SPY, no OI).

Features (fixed before results):
  totOI_t  = sum of OI over all listed monthly VX contracts on day t.
  z_t      = (log totOI_t - log totOI_{t-20} - 252d rolling mean of that change) / 252d rolling std  (min 126 obs).
  contango_t = front settle < second settle on day t (front = nearest contract expiring after t).
  share_t  = OI_front / (OI_front + OI_second); k_t = sessions (SPY calendar) from t to front expiry.

Pre-specified variants (4). Registered series = TIMING VALUE: net strategy return minus (period-average exposure) x
always-on return of the same leg, computed separately for TRAIN and VALID (as 354). Costs 2bp/side VIXY, 0.5bp SPY.
  A (H6a): z_t > +2 and contango_t -> LONG VIXY for 5 sessions (new short-vol supply / ETP-dealer hedging gets
     unwound). VIXY exists 2011+, so TRAIN = 2011-14.
  B (H6a): same signal -> SHORT SPY for 5 sessions. TRAIN 2006-14.
  C (H6b): roll-window OI migration. On days with k_t in 1..5 (final week before VX expiry), "slow roll" = share_t
     above the TRAIN (2006-14) 67th percentile of share for the same k. Slow roll -> LONG VIXY for the remaining
     sessions up to and including front expiry (holders still in the front must sell it / buy the second month,
     which is what VIXY mostly holds late in the cycle). TRAIN 2011-14.
  D (hedging-demand peak): z_t > +2 and NOT contango_t (backwardation: OI surge during stress) -> LONG SPY 10 sessions.
Descriptive tables stop at 2022-12-31.
"""
import os, sys, glob
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "369_vx_open_interest"
END = "2022-12-31"
D = os.path.join(lib.DATA, "cfe_vx")


def load():
    rows = []
    for f in sorted(glob.glob(os.path.join(D, "*.csv"))):
        p = pd.read_csv(f, index_col=False, usecols=range(11))
        p.columns = [c.strip() for c in p.columns]
        p["date"] = pd.to_datetime(p["Trade Date"], format="mixed")
        px = p["Settle"].where(p["Settle"] > 0, p["Close"]).replace(0, np.nan)
        exp = p["date"].max()
        rows.append(pd.DataFrame({"date": p["date"], "exp": exp, "px": px, "oi": p["Open Interest"].astype(float)}))
    return pd.concat(rows, ignore_index=True)


vx = load()
spy = lib.get("SPY")
vixy = lib.get("VIXY")
idx = spy.loc["2005-11-01":].index
vx = vx[vx.date.isin(idx)]
tot = vx.groupby("date")["oi"].sum().reindex(idx)
tot = tot.where(tot > 0)

# front / second per day
vx = vx[vx.exp > vx.date].sort_values(["date", "exp"])
vx["rank"] = vx.groupby("date").cumcount()
fr = vx[vx["rank"] == 0].set_index("date")
se = vx[vx["rank"] == 1].set_index("date")
fpx, spx_ = fr.px.reindex(idx), se.px.reindex(idx)
contango = (fpx < spx_).where(fpx.notna() & spx_.notna())
share = fr.oi.reindex(idx) / (fr.oi.reindex(idx) + se.oi.reindex(idx))
pos_i = pd.Series(np.arange(len(idx)), index=idx)
exp_pos = fr.exp.map(lambda e: pos_i.index.searchsorted(e)).reindex(idx)
k = exp_pos - pos_i                                       # sessions from t to front expiry

lc = np.log(tot).diff(20)
z = (lc - lc.rolling(252, min_periods=126).mean()) / lc.rolling(252, min_periods=126).std()

spy_r = spy["cc"].reindex(idx).fillna(0)
vixy_r = vixy["cc"].reindex(idx).fillna(0)


def hold(ind, n):
    return ind.astype(float).rolling(n, min_periods=1).max().fillna(0.0)


def timing_value(net, x, wl, t0):
    out = pd.Series(0.0, index=net.index)
    for a, b in [(t0, "2014-12-31"), lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - wl.loc[a:b].mean() * x.loc[a:b]
    return out.loc[t0:]


def run(name, ind, w, x, cost, t0):
    net = lib.backtest_weights(w.to_frame("a"), x.to_frame("a"), cost, lag=1).loc[t0:]
    wl = w.shift(2).fillna(0.0).loc[t0:]
    xx = x.loc[t0:]
    tv = timing_value(net, xx, wl, t0)
    row = lib.register(EXP, name, pd.Series(1.0, index=tv.index), tv, 0.0,
                       notes=f"timing value vs exposure-matched leg; cost {cost}bp/side incl; OI lag-1")
    print(f"{name}\n   {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    for per, (a, b) in {"train": (t0, "2014-12-31"), "valid": lib.VALID}.items():
        ev = int((ind.loc[a:b].astype(int).diff() == 1).sum())
        on = wl.loc[a:b]
        trades = (w.loc[a:b].diff().abs() > 0).sum() / 2
        s_on = lib.evaluate(pd.Series(1.0, index=net.loc[a:b].index), net.loc[a:b], 0)
        s_al = lib.evaluate(pd.Series(1.0, index=xx.loc[a:b].index), xx.loc[a:b], 0)
        bp_trade = 1e4 * net.loc[a:b].sum() / max(trades, 1)
        print(f"   {per}: episodes {ev}, exposure {on.mean():.3f}, net {bp_trade:+.1f} bp/round-trip, strat Sharpe "
              f"{s_on['sharpe']:.2f} vs always-on {s_al['sharpe']:.2f}, timing value {1e4*tv.loc[a:b].mean()*252:+.0f} bp/yr")
    yr = tv.loc[:END].groupby(tv.loc[:END].index.year).sum() * 1e4
    print("   timing value bp by year:", yr.round(0).astype(int).to_dict())
    return row


print("VX OI first", tot.first_valid_index().date(), "| contracts", vx.exp.nunique(), "| VIXY first", vixy.index[0].date())
print("total OI (k contracts) yearly mean <=2022:", (tot.loc[:END].groupby(tot.loc[:END].index.year).mean() / 1e3).round(0).to_dict())
print("contango share <=2022:", round(contango.loc[:END].mean(), 3))

# Descriptive (<=2022): forward 5-session VIXY / SPY return (lag-1 timing) by z bucket
fwd = lambda r, n: r[::-1].rolling(n).sum()[::-1].shift(-2)
zb = pd.cut(z, [-np.inf, -2, 2, np.inf], labels=["z<-2", "mid", "z>2"])
for nm, r, t0 in [("VIXY", vixy_r, "2011-01-04"), ("SPY", spy_r, "2006-01-01")]:
    for per, (a, b) in {"train": (t0, "2014-12-31"), "valid": (lib.VALID[0], "2022-12-20")}.items():
        f = fwd(r, 5).loc[a:b] * 1e4
        g = f.groupby([zb.loc[a:b], contango.loc[a:b]], observed=True).agg(["mean", "count"])
        print(f"   fwd5 {nm} {per} by (z bucket, contango): " +
              "  ".join(f"{i}: {rr['mean']:+.0f}bp n={int(rr['count'])}" for i, rr in g.iterrows()))

# signals
sigA = ((z > 2) & (contango == True)).fillna(False)
sigD = ((z > 2) & (contango == False)).fillna(False)
roll = k.between(1, 5)
thr = {kk: share[(k == kk)].loc["2006":"2014"].quantile(2 / 3) for kk in range(1, 6)}
thr_s = k.map(thr)
sigC = (roll & (share > thr_s)).fillna(False)
print("C thresholds (front OI share, train p67 by sessions-to-expiry):", {kk: round(v, 3) for kk, v in thr.items()})
# C: hold from signal day until front expiry (inclusive of expiry close -> weight 1 while k>=1 after first slow-roll flag)
grp = exp_pos
first = (sigC.astype(float).groupby(grp.fillna(-1)).cummax().reindex(idx).fillna(0) > 0) & roll
wC = first.astype(float)
wC = wC.where(k >= 1, 0.0)          # weight at close t held over (t, t+1]; with lag=1 the last return earned ends at expiry+... see note
# with lag=1 the weight set at t (OI date) is applied over (t+1, t+2]; k>=1 at t means t+1 <= expiry, so exposure ends
# at most one session after front expiry -- accepted (pre-specified as 'remaining sessions').

rows = [run("369A long VIXY 5d after VX OI 20d z>2 in contango (timing value)", sigA, hold(sigA, 5), vixy_r, 2.0, "2011-01-04"),
        run("369B short SPY 5d after VX OI 20d z>2 in contango (timing value)", sigA, hold(sigA, 5), -spy_r, 0.5, "2006-04-03"),
        run("369C long VIXY final week when front OI share > train p67 (slow roll; timing value)", sigC, wC, vixy_r, 2.0, "2011-01-04"),
        run("369D long SPY 10d after VX OI 20d z>2 in backwardation (timing value)", sigD, hold(sigD, 10), spy_r, 0.5, "2006-04-03")]
n = lib.n_tests()
print("registry size:", n, "| luck t for best of N ~", round(np.sqrt(2 * np.log(max(n, 2))), 2))
