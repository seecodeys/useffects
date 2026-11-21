"""376 - Idea card E3: month-end bid in hybrid benchmarks (convertibles CWB, preferreds PFF) AFTER removing their
equity (SPY) and rate (IEF) exposure. LOG 371 showed the Treasury month-end bid passes to anything priced off the
curve, so the question is whether there is an EXTRA bid beyond the IEF hedge.
Index schedule: CWB tracks the Bloomberg US Convertible Liquid Bond Index, "rebalanced on the last business day of
the month" (SSGA CWB page). PFF tracks ICE Exchange-Listed Preferred & Hybrid Securities (S&P US Preferred before
2019-ish); rebalance frequency not stated on the iShares page.

Pre-specified (written before any results were seen):
Window W3 = enter at close of 4th-last session of the month, exit at the month-end close (calendar, known ahead).
Hedge ratios: Dimson OLS on TRAILING 252 daily cc returns: r_X ~ SPY_t + SPY_{t-1} + IEF_t + IEF_{t-1}; beta_s =
sum of SPY coefs, beta_b = sum of IEF coefs (lag terms absorb stale closes in the thinner ETFs). Betas use data
through the close BEFORE the entry decision close (one-day lag) and are frozen for the window.
Costs: 2bp/side on every leg (turnover-based, lib.backtest_weights). Train <=2014 (from first window with 252d
history), valid 2015-22. No holdout look; nothing after 2022-12-31 is computed or printed.
  V1 CWB - bs*SPY - bb*IEF over W3
  V2 PFF - bs*SPY - bb*IEF over W3
  V3 0.5*V1 + 0.5*V2 (pooled residual)
  V4 raw 0.5 CWB + 0.5 PFF long over W3 (timing value vs exposure-matched always-long reported)
Descriptive (not strategies): timing value of each residual vs the always-on residual (exposure matched); per-event
gross/net bp; permutation control = mean residual over a random contiguous 3-session window from each month
excluding the W3 window (2000 draws), p-value of the actual W3 mean; yearly valid P&L.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

COST = 2.0
END = "2022-12-31"
EXP = "376_hybrid_monthend_resid"


def w3(idx):
    ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    return ((bwd <= 3) & (ym != ym.iloc[-1])).astype(float).shift(-1).fillna(0)   # decided at close t, earns t+1


def tstat(x):
    x = x.dropna(); return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


px = {s: lib.get(s).loc[:END] for s in ["CWB", "PFF", "SPY", "IEF"]}
R = pd.concat({s: d.cc for s, d in px.items()}, axis=1)


def dimson_betas(y, R, win=252):
    X = pd.concat([R.SPY, R.SPY.shift(1), R.IEF, R.IEF.shift(1)], axis=1)
    D = pd.concat([y, X], axis=1).dropna()
    out = {}
    vals = D.values
    for i in range(win, len(D) + 1):
        blk = vals[i - win:i]
        A = np.column_stack([np.ones(win), blk[:, 1:]])
        c = np.linalg.lstsq(A, blk[:, 0], rcond=None)[0]
        out[D.index[i - 1]] = (c[1] + c[2], c[3] + c[4])
    return pd.DataFrame(out, index=["bs", "bb"]).T


def resid_weights(sym):
    r = R[[sym, "SPY", "IEF"]].dropna()
    p = w3(r.index)
    b = dimson_betas(r[sym], r).reindex(r.index).shift(1)        # data through the close before the decision close
    entry = (p > 0) & (p.shift(1).fillna(0) == 0)
    bf = b.where(entry).ffill().where(p > 0)                      # frozen for the window
    ok = bf.notna().all(axis=1)
    p = p.where(ok, 0.0)
    W = pd.DataFrame({sym: p, "SPY": -bf.bs.fillna(0) * p, "IEF": -bf.bb.fillna(0) * p})
    print(f"{sym} betas at entries (<=2022): bs mean {bf.bs[entry].mean():.2f} [{bf.bs[entry].min():.2f},"
          f"{bf.bs[entry].max():.2f}], bb mean {bf.bb[entry].mean():.2f} [{bf.bb[entry].min():.2f},{bf.bb[entry].max():.2f}]")
    # always-on residual (daily betas, lagged) for exposure-matched timing value + permutation
    resid = r[sym] - b.bs * r.SPY - b.bb * r.IEF
    return W, r, p, resid


rows, nets = {}, {}
def run(name, W, Rr, p, alw):
    net = lib.backtest_weights(W, Rr, COST).loc[:END]
    gross = lib.backtest_weights(W, Rr, 0.0).loc[:END]
    start = W.index[(W.abs().sum(axis=1) > 0)][0]
    net, gross = net.loc[start:], gross.loc[start:]
    row = lib.register(EXP, name, pd.Series(1.0, index=net.index), net, 0.0, notes="W3 last 3 sessions, 2bp/side/leg")
    rows[name], nets[name] = row, net
    pos = p.reindex(net.index).shift(1).fillna(0)
    print(f"\n{name}\n  registry: {lib.fmt(row)}")
    for per, (a, b) in lib.PERIODS.items():
        n, g, ps, al = net.loc[a:b], gross.loc[a:b], pos.loc[a:b], alw.reindex(net.index).loc[a:b].fillna(0)
        if len(n) < 50: continue
        ev = int((ps.diff() > 0).sum())
        tv = n - ps.mean() * al
        print(f"  {per}: {n.index[0].date()}..  events {ev}  t {tstat(n):+.2f}  Sh {n.mean()/n.std()*np.sqrt(252):+.2f}"
              f"  gross {1e4*g.sum()/ev:+.1f} / net {1e4*n.sum()/ev:+.1f} bp/event | timing value vs always-on "
              f"t {tstat(tv):+.2f}, {1e4*tv.sum()/ev:+.1f} bp/event")
    return net


rng = np.random.default_rng(376)
def permutation(resid, p, label):
    s = resid.loc[:END].dropna()
    pp = p.reindex(s.index).shift(1).fillna(0)
    ym = s.index.to_period("M")
    df = pd.DataFrame({"r": s.values, "in": pp.values, "m": ym})
    for per, (a, b) in lib.PERIODS.items():
        d = df[(s.index >= a) & (s.index <= b)]
        actual, draws = [], []
        groups = [g for _, g in d.groupby("m") if g["in"].sum() == 3]
        actual = np.mean([g.r[g["in"] > 0].sum() for g in groups])
        sims = np.empty(2000)
        for k in range(2000):
            tot = []
            for g in groups:
                rr = g.r[g["in"] == 0].values
                j = rng.integers(0, len(rr) - 2)
                tot.append(rr[j:j + 3].sum())
            sims[k] = np.mean(tot)
        print(f"  perm {label} {per}: W3 resid {1e4*actual:+.1f} bp/mo vs random-window mean {1e4*sims.mean():+.1f}"
              f" (sd {1e4*sims.std():.1f}), one-sided p {np.mean(sims >= actual):.3f}, n months {len(groups)}")


W1, R1, p1, res1 = resid_weights("CWB")
W2, R2, p2, res2 = resid_weights("PFF")
n1 = run("V1 CWB-bs*SPY-bb*IEF W3", W1, R1, p1, res1)
n2 = run("V2 PFF-bs*SPY-bb*IEF W3", W2, R2, p2, res2)

W3c = pd.concat([0.5 * W1.add_suffix("_c"), 0.5 * W2.add_suffix("_p")], axis=1).fillna(0)
R3c = pd.concat([R1.add_suffix("_c"), R2.add_suffix("_p")], axis=1)
p3 = ((p1.reindex(R3c.index).fillna(0) + p2.reindex(R3c.index).fillna(0)) > 0).astype(float)
alw3 = 0.5 * res1.reindex(R3c.index) + 0.5 * res2.reindex(R3c.index)
run("V3 pooled 0.5 resid CWB + 0.5 resid PFF W3", W3c, R3c, p3, alw3)

Rr = R[["CWB", "PFF"]].dropna()
p4 = w3(Rr.index)
W4 = pd.DataFrame({"CWB": 0.5 * p4, "PFF": 0.5 * p4})
run("V4 raw 0.5 CWB + 0.5 PFF W3", W4, Rr, p4, 0.5 * Rr.CWB + 0.5 * Rr.PFF)

print("\nPermutation control (gross residual, random 3-session window in same month):")
permutation(res1, p1, "CWB")
permutation(res2, p2, "PFF")

print("\nYearly net bp (2015-22):")
print((pd.DataFrame(nets).loc["2015":END].groupby(lambda x: x.year).sum() * 1e4).round(1).to_string())
print("\ngate:", {k: lib.passes_gate(r) for k, r in rows.items()})
print("registry size:", lib.n_tests())
