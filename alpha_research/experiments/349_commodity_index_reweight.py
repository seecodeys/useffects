"""349 — BCOM / S&P GSCI January annual reweight (idea card X1, results/ideas_crossasset.md).

Mechanism: BCOM (bd 6-10) and GSCI (bd 5-9) reset target weights inside the January roll. Trackers sell last year's
outperformers and buy the laggards -> prior-year losers should outperform prior-year winners in the window.

Universe (Yahoo ETFs/ETNs, used only once listed, so survivorship-free): USO, UNG, GLD, SLV, DBB, DBA, CORN, WEAT,
SOYB, CPER. An asset is eligible for a ranking if it has a close on/before the 15th of the first month of the
look-back (i.e. a full look-back). Need n>=4 eligible; k = min(3, n//2) longs (losers) and k shorts (winners), weights
+1/k and -1/k (gross 2, dollar neutral). Signal = return over the prior calendar year (Jan) / trailing 12 months to the
prior month-end (placebo months), all known at the prior month-end, long before entry.
Holding: close of bd4 -> close of bd10 (bd = NYSE trading day of the month, GLD calendar), implemented with
lib.backtest_weights(lag=1) by stamping weights on bd3..bd9. Costs 2bp/side.

Pre-specified variants (3, no tuning after):
  V1  January bd4->bd10, long bottom-k / short top-k by prior-calendar-year return  (the test)
  V2  Placebo: same rule in Feb-Dec bd4->bd10 with trailing-12m signal (should be ~0 if the effect is the reweight)
  V3  Reversal: opposite of V1 from close bd10 -> close bd20 of January
Descriptive (not a trading variant): per-year Spearman rank-IC of the January signal vs window return, per-year P&L.
Descriptive tables stop at 2022-12-31.
First run dropped 19-trading-day Januaries (bug, len<20 filter); those 3 rows stay in the registry, superseded.
"""
import os, sys
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "349_commodity_index_reweight"
UNIV = ["USO", "UNG", "GLD", "SLV", "DBB", "DBA", "CORN", "WEAT", "SOYB", "CPER"]
COST = 2.0
END = "2022-12-31"

px = pd.DataFrame({s: lib.get(s)["aC"] for s in UNIV}).sort_index()
px = px.loc[:END]
first = {s: px[s].first_valid_index() for s in UNIV}
R = px.pct_change(fill_method=None)
cal = lib.get("GLD").index
cal = cal[cal <= pd.Timestamp(END)]
months = pd.Series(cal, index=cal).groupby([cal.year, cal.month]).apply(list)


def signal(y, m, lookback_start, lookback_end):
    elig = [s for s in UNIV if first[s] is not None and first[s] <= lookback_start + pd.Timedelta(days=14)]
    p = px.loc[:lookback_end, elig].ffill()
    p0 = px.loc[:lookback_start - pd.Timedelta(days=1), elig].ffill()
    if len(p0) == 0:
        return None
    a = p0.iloc[-1] if len(p0) else None
    b = p.iloc[-1]
    # if asset listed after lookback_start (within 14d), use its first close as start
    a = a.copy()
    for s in elig:
        if pd.isna(a[s]):
            a[s] = px[s].dropna().iloc[0]
    sig = (b / a - 1).dropna()
    return sig if len(sig) >= 4 else None


def build(kind):
    W = pd.DataFrame(0.0, index=cal, columns=UNIV)
    events = []
    for (y, m), days in months.items():
        if len(days) < (20 if kind == "rev" else 10):   # fix: Jan can have 19 trading days (2010/16/21)
            continue
        if kind in ("jan", "rev") and m != 1:
            continue
        if kind == "placebo" and m == 1:
            continue
        mstart = pd.Timestamp(y, m, 1)
        lb_start = pd.Timestamp(y - 1, m, 1)
        sig = signal(y, m, lb_start, mstart - pd.Timedelta(days=1))
        if sig is None:
            continue
        k = min(3, len(sig) // 2)
        sig = sig.sort_values()
        w = pd.Series(0.0, index=UNIV)
        w[sig.index[:k]] = 1.0 / k
        w[sig.index[-k:]] = -1.0 / k
        if kind == "rev":
            w = -w
            stamp, d0, d1 = days[8:18], days[9], days[19]      # hold close bd10 -> close bd20
        else:
            stamp, d0, d1 = days[2:9], days[3], days[9]        # hold close bd4 -> close bd10
        W.loc[stamp, :] = w.values
        wr = (px.loc[d1] / px.loc[d0] - 1)
        ic = sig.rank().corr(wr[sig.index].rank()) if kind == "jan" else np.nan
        events.append({"year": y, "month": m, "n": len(sig), "k": k, "long": ",".join(sig.index[:k]),
                       "short": ",".join(sig.index[-k:]), "gross_bp": 1e4 * float((w * wr.fillna(0)).sum()),
                       "rank_ic": ic})
    net = lib.backtest_weights(W, R.reindex(cal), COST, lag=1)
    held = W.shift(2).reindex(cal).abs().sum(axis=1) > 0
    ev = pd.DataFrame(events)
    ev["net_bp"] = ev["gross_bp"] - 4 * COST     # gross 2 in + 2 out at 2bp/side (approx; exact in net series)
    return net, held, ev


out = []
for tag, kind, desc in [("V1", "jan", "Jan bd4->bd10 L losers/S winners prior-yr"),
                        ("V2", "placebo", "PLACEBO Feb-Dec bd4->bd10 trailing-12m"),
                        ("V3", "rev", "Jan bd10->bd20 reversal (opposite of V1)")]:
    net, held, ev = build(kind)
    row = lib.register(EXP, f"349{tag} {desc} (lag1, 2bp/side) [fix: 19-day Jans kept]", held.astype(float), net, 0.0,
                       notes="dollar-neutral gross 2; costs inside net")
    def tstat(x):
        return x.mean() / x.std(ddof=1) * np.sqrt(len(x)) if len(x) > 1 else np.nan
    tr, va = ev[ev.year <= 2014], ev[(ev.year >= 2015) & (ev.year <= 2022)]
    print(f"\n{tag} {desc}\n  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    print(f"  events train={len(tr)} mean net {tr.net_bp.mean():+.1f}bp (t {tstat(tr.net_bp):+.2f}, hit "
          f"{100*(tr.net_bp>0).mean():.0f}%) | valid={len(va)} mean net {va.net_bp.mean():+.1f}bp "
          f"(t {tstat(va.net_bp):+.2f}, hit {100*(va.net_bp>0).mean():.0f}%)")
    if kind != "placebo":
        print(ev.to_string(index=False, float_format=lambda v: f"{v:+.1f}"))
        if kind == "jan":
            print(f"  mean rank-IC train {tr.rank_ic.mean():+.2f}  valid {va.rank_ic.mean():+.2f}")
    else:
        print(ev.groupby("year")[["gross_bp", "net_bp"]].mean().round(1).T.to_string())
    out.append(ev.assign(variant=tag))
pd.concat(out).to_csv(os.path.join(lib.HERE, "results", "349_events.csv"), index=False)
print("\nregistry tests in this file:", lib.n_tests())
