"""140 - S&P 500 additions: index funds buy at the close of the day before the effective date ('Date added').
Event study for CURRENT constituents added since 2001 (Wikipedia 'List of S&P 500 companies', data/sp500_wiki.html).
Survivorship: stocks later removed are missing — they tended to do worse, which biases AGAINST the short below
(so a positive result would be conservative). Pre-specified trade: short the added stock / long SPY (beta 1) from the
close of the effective date for 5 sessions (post-inclusion reversal), 3bp/side/leg. Also prints the run-up (abnormal
return over the 10 sessions before the effective date), descriptive.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
t = pd.read_html(os.path.join(lib.DATA, "sp500_wiki.html"), flavor="lxml")[0]
t["added"] = pd.to_datetime(t["Date added"], errors="coerce")
ev = t[(t.added >= "2001-01-01") & (t.added <= "2026-10-31")][["Symbol", "added"]]
spy = lib.get("SPY")
rows, daily = [], {}
for sym, dt in ev.itertuples(index=False):
    ysym = sym.replace(".", "-")
    try:
        d = lib.get(ysym)
    except Exception:
        continue
    if dt not in d.index:
        nxt = d.index[d.index >= dt]
        if not len(nxt):
            continue
        dt = nxt[0]
    k = d.index.get_loc(dt)
    if k < 12 or k + 6 >= len(d):
        continue
    ab = (d.cc - spy.cc.reindex(d.index)).fillna(0)
    run_up = ab.iloc[k - 10:k].sum()                    # 10 sessions before effective date (through its eve)
    eff = ab.iloc[k]
    post = ab.iloc[k + 1:k + 6]
    rows.append({"sym": sym, "date": dt, "run_up": run_up, "eff_day": eff, "post5": post.sum()})
    for j, (dd, v) in enumerate(post.items()):
        daily.setdefault(dd, []).append(-v - (2 * 2 * 3e-4 / 5 if j in (0, 4) else 0))   # short added vs SPY; costs at entry/exit
E = pd.DataFrame(rows).set_index("date").sort_index()
print(f"events: {len(E)}  ({E.index.min().date()}..{E.index.max().date()})")
for per, (a, b) in {"2001-14": ("2001", "2014"), "2015-22": lib.VALID, "2023+": lib.HOLDOUT}.items():
    x = E.loc[a:b]
    if len(x):
        print(f"  {per}: n={len(x)}  run-up {1e4*x.run_up.mean():+.0f}bp  eff-day {1e4*x.eff_day.mean():+.0f}bp  post-5d {1e4*x.post5.mean():+.0f}bp (t={x.post5.mean()/x.post5.std()*np.sqrt(len(x)):+.2f})")
pnl = pd.Series({k: np.mean(v) * min(len(v), 1) for k, v in daily.items()}).sort_index()
pnl = pnl.reindex(spy.loc["2001":].index).fillna(0) * 0.25          # ~25% capital per event-day basket
r = lib.register("140_sp500_additions", "140 short added stock vs SPY, 5 sessions after effective date", pd.Series(1.0, index=pnl.index), pnl, 0.0,
                 notes="current constituents only (survivorship biases against)")
print(f"140 {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())
