"""374 — Equal-weight quarterly reset: fade the QTD equal-weight vs cap-weight drift (idea card E6).

Rebalance day (confirmed from public documents before running):
  * S&P 500 Equal Weight family: Invesco SEC filing for an S&P 500 EW sector ETF (485APOS, 2018,
    sec.gov/Archives/edgar/data/1209466/000119312518245722): "The Index Provider rebalances the Underlying Index on a
    quarterly basis after the close of business on the third Friday in March, June, September and December."
    RSP's 2025 summary prospectus confirms quarterly rebalancing and that the Fund follows the index schedule.
    (S&P DJI pages return 403 from this proxy; not bypassed.)
  * Nasdaq-100 Equal Weighted (NDXE, tracked by QQQE): indexes.nasdaq.com/docs/methodology_NDXE.pdf, change log:
    before 5/1/2026 "The quarterly Index Rebalance uses the Last Sale Prices as of the close of trading on the third
    Friday in March, June, September and December" (now last trading day of Feb/May/Aug/Nov; outside our window).
  Event day D = 3rd Friday of Mar/Jun/Sep/Dec (previous trading day if a holiday). Funds trade at the close of D.

Signal g = QTD(EW ETF) - QTD(cap ETF), cumulative close->close from the last close of the prior quarter through
close of D-1 (known before D's open). Position -sign(g) in (EW - cap): EW lagged -> reset buys laggards -> long EW.

Pre-specified variants (written before any results were seen):
  V1  RSP-SPY,  -sign(g), D open->close.                      cost 1bp RSP + 0.5bp SPY per side.
  V2  RSP-SPY,  -sign(g') with g' through close D-2, held close D-1 -> close D (backtest_weights lag=1).
  V3  QQQE-QQQ, -sign(g), D open->close (QQQE 2012+).          cost 1bp QQQE + 0.5bp QQQ per side.
  V4  RSP-SPY,  +sign(g) close D -> close D+1 (post-reset reversal; g through D-1, backtest_weights lag=1).
  Benchmark (registered, not a candidate): unconditional +1 (RSP-SPY) D open->close -> timing value = V1 - |V1| x this.
Also reported: pooled V1+V3 t (event-level).
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lib  # noqa: E402

EXP = "374_equal_weight_reset"
END = "2022-12-31"


def third_fridays(idx):
    out = []
    for y in range(2000, 2023):
        for m in (3, 6, 9, 12):
            d = pd.Timestamp(y, m, 1)
            fri = d + pd.Timedelta(days=(4 - d.weekday()) % 7 + 14)
            k = idx.searchsorted(fri, side="right") - 1          # previous trading day if holiday
            if k >= 0 and idx[k].month == m:
                out.append(idx[k])
    return pd.DatetimeIndex(out)


def setup(ew, cap):
    a, b = lib.get(ew), lib.get(cap)
    idx = a.index.intersection(b.index)
    idx = idx[(idx <= END)]
    a, b = a.loc[idx], b.loc[idx]
    first = max(a["aC"].first_valid_index(), b["aC"].first_valid_index())
    idx = idx[idx >= first]
    a, b = a.loc[idx], b.loc[idx]
    q = idx.to_period("Q")
    qend_close = {}  # last close of prior quarter
    for qq in q.unique():
        prev = idx[q < qq]
        if len(prev):
            qend_close[qq] = prev[-1]
    events = [d for d in third_fridays(idx) if d in idx and d.to_period("Q") in qend_close]
    rows = []
    for D in events:
        k = idx.get_loc(D)
        if k < 3 or k + 1 >= len(idx):
            continue
        q0 = qend_close[D.to_period("Q")]
        d1, d2 = idx[k - 1], idx[k - 2]
        g1 = (a.aC[d1] / a.aC[q0] - b.aC[d1] / b.aC[q0]) if d1 > q0 else np.nan
        g2 = (a.aC[d2] / a.aC[q0] - b.aC[d2] / b.aC[q0]) if d2 > q0 else np.nan
        rows.append(dict(D=D, d1=d1, g1=g1, g2=g2, oc=a.oc[D] - b.oc[D],
                         cc_D=a.cc[D] - b.cc[D], cc_D1=a.cc[idx[k + 1]] - b.cc[idx[k + 1]]))
    return idx, a, b, pd.DataFrame(rows).set_index("D")


def reg_oc(name, idx, ev, sign, cost):
    pos = pd.Series(0.0, index=idx)
    pos.loc[ev.index] = sign
    ret = pd.Series(0.0, index=idx)
    ret.loc[ev.index] = ev["oc"]
    return lib.register(EXP, name, pos, ret, cost, notes="spread EW-cap, cost=sum of legs per side"), pos * ret - pos.abs() * 2 * cost / 1e4


def reg_w(name, idx, a, b, wdates, signs, cost_ew, cost_cap):
    """Weights decided at close of wdates (EW +s, cap -s), implemented with lag=1, one-day hold."""
    W = pd.DataFrame(0.0, index=idx, columns=["ew", "cap"])
    W.loc[wdates, "ew"] = signs
    W.loc[wdates, "cap"] = -signs
    R = pd.DataFrame({"ew": a.cc, "cap": b.cc}, index=idx)
    gross = lib.backtest_weights(W, R, 0.0, lag=1)        # lib timing convention (signal close t, hold (t+1, t+2])
    held = W.shift(2).fillna(0.0)["ew"] != 0
    # isolated one-day round trip: enter + exit both legs -> 2*(cost_ew+cost_cap) bp, booked on the holding day
    pnl = (gross - 2 * (cost_ew + cost_cap) / 1e4).where(held, 0.0)
    pos = held.astype(float)
    return lib.register(EXP, name, pos, pnl, 0.0, notes="weights lag=1, net legs costs booked on holding day"), pnl


def yearly(p):
    return (p.groupby(p.index.year).sum() * 1e4).round(0)


def main():
    out = []
    idx, a, b, ev = setup("RSP", "SPY")
    ev = ev.dropna(subset=["g1"])
    s1 = -np.sign(ev["g1"])
    r1, p1 = reg_oc("V1_RSP-SPY_oc_fade", idx, ev, s1, 1.5)
    rb, pb = reg_oc("BENCH_RSP-SPY_oc_long", idx, ev, pd.Series(1.0, index=ev.index), 1.5)
    e2 = ev.dropna(subset=["g2"])
    k = [idx.get_loc(d) for d in e2.index]
    r2, p2 = reg_w("V2_RSP-SPY_cc_fade_lag1", idx, a, b, idx[[i - 2 for i in k]], (-np.sign(e2["g2"])).values, 1.0, 0.5)
    r4, p4 = reg_w("V4_RSP-SPY_post_reversal_lag1", idx, a, b, ev["d1"].values, np.sign(ev["g1"]).values, 1.0, 0.5)
    idx3, a3, b3, ev3 = setup("QQQE", "QQQ")
    ev3 = ev3.dropna(subset=["g1"])
    r3, p3 = reg_oc("V3_QQQE-QQQ_oc_fade", idx3, ev3, -np.sign(ev3["g1"]), 1.5)

    for nm, r in [("V1", r1), ("V2", r2), ("V3", r3), ("V4", r4), ("BENCH", rb)]:
        n_tr = r["train_traded_%"] * r["train_n_days"] / 100
        n_va = r["valid_traded_%"] * r["valid_n_days"] / 100
        out.append(f"{nm}: train t={r['train_t']:+.2f} bp/trade={r['train_bp_per_trade']:+.1f} (n={n_tr:.0f}) | "
                   f"valid t={r['valid_t']:+.2f} Sh={r['valid_sharpe']:+.2f} bp/trade={r['valid_bp_per_trade']:+.1f} "
                   f"(n={n_va:.0f}) | gate={lib.passes_gate(r)}")
    # pooled V1+V3 event-level t
    ev1 = p1[p1 != 0]
    ev3p = p3[p3 != 0]
    for per, (lo, hi) in lib.PERIODS.items():
        x = pd.concat([ev1.loc[lo:hi], ev3p.loc[lo:hi]])
        out.append(f"pooled V1+V3 {per}: n={len(x)} mean={1e4 * x.mean():+.1f}bp t={x.mean() / x.std() * np.sqrt(len(x)):+.2f}")
    # timing value V1 vs unconditional long spread
    for per, (lo, hi) in lib.PERIODS.items():
        out.append(f"V1 timing value {per}: signed {1e4 * ev1.loc[lo:hi].mean():+.1f}bp vs unconditional "
                   f"{1e4 * pb[pb != 0].loc[lo:hi].mean():+.1f}bp/event")
    # descriptive: correlation of g with D oc spread (≤2022)
    for per, (lo, hi) in lib.PERIODS.items():
        e = ev.loc[lo:hi]
        out.append(f"RSP {per}: corr(g, D oc spread)={e['g1'].corr(e['oc']):+.2f}, mean|g|={1e4 * e['g1'].abs().mean():.0f}bp, "
                   f"n={len(e)}")
    out.append("V1 yearly net bp: " + yearly(p1).to_dict().__repr__())
    out.append("V3 yearly net bp: " + yearly(p3).to_dict().__repr__())
    n = lib.n_tests()
    out.append(f"registry rows (agent file): {n}")
    txt = "\n".join(out)
    print(txt)
    with open(os.path.join(lib.HERE, "results", "374_equal_weight_reset.txt"), "w") as f:
        f.write(txt + "\n")


if __name__ == "__main__":
    main()
