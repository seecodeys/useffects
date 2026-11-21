"""360 - Idea card P7 (results/ideas_papers.md): pension funded-status glide paths buy long duration.
Klingler & Sundaresan (JF 2019), Greenwood & Vayanos (AER P&P 2010). When corporate DB funded status improves
(equities up, discount rates up), glide paths sell equity and buy long-duration bonds / receiver swaps, so the long
end outperforms the belly over the following month (10s-20s/30s flatten).

Not a duplicate: 013/250 are 1-day month-/quarter-end stock-bond rebalancing trades; 256 is a last-day TLT-IEF trade on
the MTD gap; 275-319 are the turn-of-month bond bid. None hold a month-ahead long-end flattener on funded status.

Decision: last trading day of month t (SPY calendar), using closes through t. Position held over month t+1 via
lib.backtest_weights(..., lag=1) (from close of the first day of t+1 to close of the first day of t+2).
Costs 1bp/side on turnover (bond ETFs). All data truncated at 2022-12-31 (no holdout).
FS = trailing-3-month SPY total return minus TLT total return (month-end to month-end, Yahoo adjusted closes).
Tercile threshold = 67th pct of FS over TRAIN month-ends only (<= 2014-12).

PRE-SPECIFIED VARIANTS (fixed before results):
  A  FS in top TRAIN tercile -> long TLT - 1.9 x IEF over month t+1 (duration-neutral flattener), else flat.
  B  FRED version 1994+: synthetic DV01-neutral 20y-vs-10y flattener, daily ret = -14 x (dy20 - dy10)/100;
     FS = 3m ^GSPC price return minus 3m 20y bond return proxy (y20_{t-3}/4 - 14 x dy20), yields strictly before t
     (H.15 publication lag). Top tercile (threshold from 1994-2014 month-ends). Descriptive/synthetic, 1bp/side.
  C  A only when the holding month t+1 is in Q4/Q1 (Oct-Mar; year-end measurement-date de-risking).
  D  continuous: weight = clip(z(FS), -2, 2) on the TLT - 1.9 x IEF flattener, z from TRAIN mean/sd.
Each registered twice: raw net, and timing value = net - mean_signed_pos(period) x always-on flattener (uncosted).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP, COST, END = "360_pension_ldi_flattener", 1.0, "2022-12-31"
HR = 1.9


def fred(s):
    return pd.read_csv(os.path.join(lib.DATA, f"fred_{s}.csv"), parse_dates=["observation_date"], index_col=0,
                       na_values=".").iloc[:, 0].astype(float)


def month_ends(idx):
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby(idx.to_period("M")).max().values)


def hold_weights(me, sig, idx):
    """sig indexed by month-end decision dates -> daily target weight (decided at close t, held until next ME)."""
    w = pd.Series(np.nan, index=idx)
    w.loc[me] = sig.reindex(me).values
    return w.ffill().fillna(0.0)


def run(name, w, R, hr_cols, F, notes):
    """w: daily target weight on the flattener; R: returns DataFrame; hr_cols: dict col->multiplier."""
    W = pd.DataFrame({c: w * m for c, m in hr_cols.items()})
    net = lib.backtest_weights(W, R[list(hr_cols)], COST, lag=1)
    held = w.shift(2).fillna(0.0)                         # position actually earning on day t (shift 1 + lag 1)
    act = ((held != 0) | (net != 0)).astype(float)
    raw = lib.register(EXP, name + " (raw, 1bp/side)", act * held.abs().where(held != 0, 1.0), net.where(act > 0, 0.0),
                       0.0, notes=notes)
    tv = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID]:
        tv.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * F.loc[a:b]
    tvr = lib.register(EXP, name + " (timing value vs exposure-matched always-on)", pd.Series(1.0, index=net.index),
                       tv, 0.0, notes="net minus mean signed pos x always-on flattener (uncosted)")
    yr = net.loc[lib.TRAIN[0]:END].groupby(net.loc[lib.TRAIN[0]:END].index.year).sum() * 1e4
    return raw, tvr, yr


def main():
    out = []
    spy, tlt, ief = (lib.get(s)["aC"].loc[:END] for s in ["SPY", "TLT", "IEF"])
    px = pd.concat({"SPY": spy, "TLT": tlt, "IEF": ief}, axis=1).dropna()
    R = px.pct_change()
    idx = px.index
    me = month_ends(idx)[:-1] if idx[-1] < pd.Timestamp(END) else month_ends(idx)
    pm = px.loc[me]
    fs = (pm["SPY"] / pm["SPY"].shift(3) - 1) - (pm["TLT"] / pm["TLT"].shift(3) - 1)
    fs = fs.dropna()
    tr = fs.loc[:lib.TRAIN[1]]
    thr, mu, sd = tr.quantile(2 / 3), tr.mean(), tr.std()
    F = R["TLT"] - HR * R["IEF"]
    cols = {"TLT": 1.0, "IEF": -HR}

    top = (fs > thr).astype(float)
    nxt_month = (fs.index + pd.offsets.MonthBegin(1)).month
    q41 = pd.Series(np.isin(nxt_month, [10, 11, 12, 1, 2, 3]).astype(float), index=fs.index)
    z = ((fs - mu) / sd).clip(-2, 2)
    res = {}
    for name, sig in [("A top-tercile FS -> TLT-1.9IEF", top), ("C A in Q4/Q1 holding months", top * q41),
                      ("D z(FS) continuous", z)]:
        w = hold_weights(fs.index, sig, idx)
        res[name] = run(name, w, R, cols, F, f"thr={thr:.4f} n_sig_months={int((sig != 0).sum())}")

    # B: FRED synthetic
    y = pd.concat({"y10": fred("DGS10"), "y20": fred("DGS20")}, axis=1).loc["1993-10-01":END].dropna()
    g = lib.get("GSPC")["Close"].loc["1993-10-01":END]
    gi = g.index
    yl = y.reindex(gi, method="ffill").shift(1)            # yields known at close t: through t-1
    dyy = y.diff()
    dyy = dyy.groupby(dyy.index.map(lambda d: gi[gi.searchsorted(d)] if gi.searchsorted(d) < len(gi) else pd.NaT)).sum()
    dyy = dyy.reindex(gi).fillna(0.0)
    Fsyn = -14 * (dyy["y20"] - dyy["y10"]) / 100
    meg = month_ends(gi)
    meg = meg[meg < pd.Timestamp(END)]
    gm, ym = g.loc[meg], yl.loc[meg]
    bond3 = ym["y20"].shift(3) / 100 / 4 - 14 * (ym["y20"] - ym["y20"].shift(3)) / 100
    fsb = ((gm / gm.shift(3) - 1) - bond3).dropna()
    thrb = fsb.loc[:lib.TRAIN[1]].quantile(2 / 3)
    sigb = (fsb > thrb).astype(float)
    wb = hold_weights(fsb.index, sigb, gi)
    Rb = pd.DataFrame({"FLAT": Fsyn})
    res["B FRED synthetic 20s-10s flattener"] = run("B FRED synthetic 20s-10s flattener", wb, Rb, {"FLAT": 1.0}, Fsyn,
                                                     f"thr={thrb:.4f} synthetic DV01-neutral; 1994+ descriptive")

    # descriptive: next-month flattener return by FS tercile (<=2022), and pre-2000 FRED block
    Fm = (1 + F).groupby(F.index.to_period("M")).prod() - 1
    ev = pd.DataFrame({"FS": fs, "top": top, "z": z})
    ev["next_month_flattener_bp"] = [1e4 * Fm.get(p + 1, np.nan) for p in fs.index.to_period("M")]
    ev.loc[:END].to_csv(os.path.join(lib.HERE, "results", "360_events.csv"))
    ev["terc"] = pd.qcut(ev["FS"], 3, labels=["low", "mid", "high"])
    ev["per"] = np.where(ev.index <= lib.TRAIN[1], "train", "valid")
    print(f"FS thr (train 67pct) = {thr:.4f}; months train={len(tr)}, valid={len(fs.loc[lib.VALID[0]:END])}")
    print(ev.groupby(["per", "terc"], observed=True)["next_month_flattener_bp"].agg(["count", "mean", "std"]).round(1))
    for p in ["train", "valid"]:
        e = ev[ev.per == p].dropna()
        print(p, "corr(FS, next-month flattener) =", round(e["FS"].corr(e["next_month_flattener_bp"]), 3))
    Fbm = Fsyn.groupby(Fsyn.index.to_period("M")).sum()
    evb = pd.DataFrame({"FS": fsb, "nxt": [1e4 * Fbm.get(p + 1, np.nan) for p in fsb.index.to_period("M")]}).dropna()
    for lab, a, b in [("1994-99 (descr.)", "1994", "1999"), ("train", "2000", "2014"), ("valid", "2015", END)]:
        e = evb.loc[a:b]
        print(f"B {lab}: n={len(e)} corr={e.FS.corr(e.nxt):+.3f} top-terc mean={e.nxt[e.FS > thrb].mean():+.1f}bp "
              f"other mean={e.nxt[e.FS <= thrb].mean():+.1f}bp")

    print()
    for name, (raw, tvr, yr) in res.items():
        print(name)
        print("  raw:", lib.fmt(raw), "gate", lib.passes_gate(raw))
        print("  TV :", lib.fmt(tvr), "gate", lib.passes_gate(tvr))
        print("  yearly bp:", " ".join(f"{k}:{v:+.0f}" for k, v in yr.items()))
    print("registry rows:", lib.n_tests())


if __name__ == "__main__":
    main()
