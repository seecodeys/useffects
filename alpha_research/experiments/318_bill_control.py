"""318 (reuses 315 code) - Index-eligibility control: 3-month bills (DTB3) are outside the bond indices (>1y maturity), 1-year (DGS1) is at the edge; 2-year (DGS2) inside. Expect no bid in bills. 315 - Back-cast of the month-end bond bid on never-used data (descriptive): FRED daily constant-maturity yields
(DGS10 from 1962, DGS2 from 1976, DGS30 1977-2002 & 2006+). Bond 'return' over the last 3 sessions of each month is
proxied by minus the yield change (bp). The bond bid predicts falling yields into month-end. Bond index tracking (Lehman
indices, 1973+) and month-end extension grew through the 1980s-90s, so the effect should strengthen over time.
Reported by decade, plus a random-window permutation baseline (like 276e). Not registered (descriptive backcast)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
rng = np.random.default_rng(5)
for s in ["DTB3", "DGS1", "DGS2"]:
    y = pd.read_csv(os.path.join(lib.DATA, f"fred_{s}.csv"), parse_dates=["observation_date"], index_col=0,
                    na_values=".").iloc[:, 0].dropna()
    dy = y.diff().dropna() * 100                                             # bp
    ym = pd.Series(dy.index.to_period("M"), index=dy.index)
    g = [(m, v.values) for m, v in dy.groupby(ym) if len(v) >= 15]
    w = pd.Series({m: -v[-3:].sum() for m, v in g})                         # bp of yield decline in last 3 sessions
    w.index = w.index.to_timestamp()
    out = []
    for a, b in [("1973", "1982"), ("1983", "1992"), ("1993", "2001"), ("2002", "2022")]:
        x = w.loc[a:b]
        if len(x) > 20: out.append(f"{a}-{b[2:]}: {x.mean():+.2f}bp (t={x.mean()/x.std()*np.sqrt(len(x)):.1f}, n={len(x)})")
    pre = [v for m, v in g if m.year <= 2001]
    act = np.mean([-v[-3:].sum() for v in pre])
    sims = [np.mean([-v[s0:s0 + 3].sum() for v, s0 in zip(pre, [rng.integers(0, len(v) - 3) for v in pre])]) for _ in range(2000)]
    print(f"318 {s}: " + " | ".join(out) + f" || pre-2002 perm: actual {act:+.2f}bp vs random mean {np.mean(sims):+.2f}, "
          f"p={np.mean(np.array(sims) >= act):.4f}")
