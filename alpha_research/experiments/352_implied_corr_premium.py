"""352 - Option-implied correlation (Cboe COR3M) and the correlation risk premium -> SPY and put-write returns (card 4,
results/ideas_options_etf.md; Buss-Schoenleber-Vilkov 2018, Driessen-Maenhout-Vilkov 2009).

Data: cdn.cboe.com COR3M_History.csv (2006-01-03+; Cboe launched COR3M in 2022 and the pre-launch history is Cboe's
own backfill - flag). The older KCJ/ICJ/JCJ files return 403 AccessDenied, so COR3M is the only series used.
Realized correlation: average pairwise correlation of daily returns of the 9 original sector SPDRs (XLB XLE XLF XLI
XLK XLP XLU XLV XLY) over the trailing 63 sessions, x100 to match COR3M units. CRP = COR3M - realized corr.
Put-write: CBOE PUT index (real option prices), data/cboe/PUT.csv. Excess returns = daily return - ^IRX/252.

Timing: decision at the LAST close of each month using that close's COR3M / realized corr (known at the decision
close); positions via lib.backtest_weights(lag=1), i.e. entered at the close of the next session and held to the
first session after the next month-end decision (about one month). Tercile cut-offs (67th percentile) are fixed on
TRAIN month-end values 2006-2014 and never changed. Costs 0.5bp/side SPY, 2bp/side PUT index.

Pre-specified variants (4, registered BEFORE seeing results). Each registered series is the TIMING VALUE:
strategy excess return minus (average exposure in that period) x always-long excess return, so a pass means the
signal adds value beyond simply holding the asset that often.
  A: SPY long next month when COR3M is in its top TRAIN tercile, else flat.
  B: SPY long next month when CRP (COR3M - realized 63d sector corr) is in its top TRAIN tercile, else flat.
  C: PUT index long next month when CRP is in its top TRAIN tercile, else flat (long top / flat otherwise).
  D: as A at a 3-month horizon: decisions only at quarter-end closes (Mar/Jun/Sep/Dec), hold 3 months.
Descriptive tables stop at 2022-12-31.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "352_implied_corr_premium"
END = "2022-12-31"


def cboe(name, col):
    p = pd.read_csv(os.path.join(lib.DATA, "cboe", f"{name}.csv"))
    p["DATE"] = pd.to_datetime(p.DATE, format="%m/%d/%Y")
    return p.set_index("DATE")[col].astype(float).sort_index()


cor3m = cboe("COR3M", "CLOSE")
put = cboe("PUT", "PUT")
spy = lib.get("SPY")
irx = lib.get("^IRX")["Close"] / 100 / 252
idx = spy.loc["2006-01-01":].index
rf = irx.reindex(idx).ffill().fillna(0)

sect = ["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"]
R = pd.DataFrame({s: lib.get(s)["cc"] for s in sect}).loc["2005-06-01":]
iu = np.triu_indices(len(sect), 1)
rc = pd.Series({d: np.nanmean(R.iloc[i - 62:i + 1].corr().values[iu]) for i, d in enumerate(R.index) if i >= 62}) * 100

cor = cor3m.reindex(idx)                         # value at close t (published end of day t)
crp = cor - rc.reindex(idx)
me = idx.to_series().groupby(idx.to_period("M")).max().values     # last session of each month
me = pd.DatetimeIndex(me)
me = me[me < idx[-1]]                                              # last month-end only if month completed

spy_x = (spy["cc"].reindex(idx) - rf).fillna(0)
pr = put.reindex(idx).ffill()
put_x = (pr.pct_change() - rf).fillna(0)


def cut(sig):
    s = sig.reindex(me).loc["2006":"2014"].dropna()
    return s.quantile(2 / 3)


def monthly_weights(sig, thr, dates):
    """Weight decided at each decision close, held until the next decision close (forward-filled)."""
    w = pd.Series(np.nan, index=idx)
    w.loc[dates] = (sig.reindex(dates) > thr).astype(float).where(sig.reindex(dates).notna(), 0.0)
    return w.ffill().fillna(0.0)


def timing_value(net, x, w_lagged):
    out = pd.Series(0.0, index=net.index)
    for a, b in [("2006-01-01", "2014-12-31"), lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = net.loc[a:b] - w_lagged.loc[a:b].mean() * x.loc[a:b]
    return out


def run(name, sig, x, cost, dates):
    thr = cut(sig)
    w = monthly_weights(sig, thr, dates)
    net = lib.backtest_weights(w.to_frame("a"), x.to_frame("a"), cost, lag=1)
    wl = w.shift(2).fillna(0.0)                                   # exposure actually held on each day
    tv = timing_value(net, x, wl)
    row = lib.register(EXP, name, pd.Series(1.0, index=tv.index), tv, 0.0,
                       notes=f"timing value vs exposure-matched; thr={thr:.2f}; cost {cost}bp/side incl.")
    # descriptive (<=2022): events, mean net excess bp per event, raw strategy Sharpe vs always-long
    on = (sig.reindex(dates) > thr)
    seg = pd.Series(np.arange(len(idx)), index=idx)
    stats = {}
    for per, (a, b) in {"train": ("2006-01-01", "2014-12-31"), "valid": lib.VALID}.items():
        d = dates[(dates >= a) & (dates <= b) & (dates < pd.Timestamp("2022-11-30"))]   # holding must end <=2022
        ev = d[on.reindex(d).fillna(False).values]
        nb = [1e4 * net.iloc[seg[e] + 2: (seg[dates[dates > e][0]] + 2 if (dates > e).any() else len(idx)) ].sum() for e in ev]
        s_on = lib.evaluate(pd.Series(1.0, index=net.loc[a:b].index), net.loc[a:b], 0)
        s_bh = lib.evaluate(pd.Series(1.0, index=x.loc[a:b].index), x.loc[a:b], 0)
        stats[per] = (len(ev), len(d), np.mean(nb) if nb else np.nan, s_on["sharpe"], s_bh["sharpe"], wl.loc[a:b].mean())
    print(f"{name}\n   thr={thr:.2f}  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    for per, (ne, nd, mb, so, sb, ex) in stats.items():
        print(f"   {per}: events {ne}/{nd} decisions, mean net excess bp/event {mb:+.1f}, strat Sharpe {so:.2f} "
              f"vs always-long {sb:.2f}, avg exposure {ex:.2f}")
    yr = tv.loc[:END].groupby(tv.loc[:END].index.year).sum() * 1e4
    print("   timing value bp by year:", yr.round(0).astype(int).to_dict())
    return row


print("COR3M first", cor3m.index[0].date(), "| realized corr first", rc.index[0].date())
print("month-end COR3M / CRP train mean:", round(cor.reindex(me).loc[:"2014"].mean(), 1), round(crp.reindex(me).loc[:"2014"].mean(), 1))
q = me[me.month.isin([3, 6, 9, 12])]
rows = [run("352A SPY long next month if COR3M top train tercile (timing value)", cor, spy_x, 0.5, me),
        run("352B SPY long next month if CRP=COR3M-RC63 top train tercile (timing value)", crp, spy_x, 0.5, me),
        run("352C PUT index long next month if CRP top train tercile (timing value)", crp, put_x, 2.0, me),
        run("352D SPY long next quarter if COR3M top train tercile, quarterly decisions (timing value)", cor, spy_x, 0.5, q)]
print("registry size:", lib.n_tests(), "| luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
