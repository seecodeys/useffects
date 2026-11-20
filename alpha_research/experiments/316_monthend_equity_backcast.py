"""316 - Back-cast of the month-end stock-bond rebalancing trade (013) to 1962-1992 (never-used data; 040 covered
1993-2002). ^GSPC daily closes (opens unreliable pre-1990s -> last-day close->close, which 296 showed has no pre-open
component on modern data) and the 10y bond leg proxied by -7 x DGS10 change (duration ~7). Signal from MTD through the
prior close; rule = -sign(SPX MTD - bond MTD) on the last trading day. Descriptive by decade, plus 2002-22 on the same
proxy construction for comparison. Not registered."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spx = pd.read_csv(os.path.join(lib.DATA, "GSPC_long.csv"), parse_dates=["Date"], index_col="Date")["Close"].loc["1962":"2022"]
y = pd.read_csv(os.path.join(lib.DATA, "fred_DGS10.csv"), parse_dates=["observation_date"], index_col=0, na_values=".").iloc[:, 0]
idx = spx.index.intersection(y.dropna().index)
s, b = spx.loc[idx], (-7 * y.loc[idx].diff() / 100).fillna(0)
sr = s.pct_change().fillna(0)
ym = pd.Series(idx.to_period("M"), index=idx)
mtd = lambda r: r.groupby(ym).cumsum().shift(1).where(ym == ym.shift(1), 0.0)     # through the prior close, same month
gap = mtd(sr) - mtd(b)
last = ym != ym.shift(-1)
last.iloc[-1] = False
pnl = (-np.sign(gap) * sr)[last]
for a, e in [("1962", "1972"), ("1973", "1982"), ("1983", "1992"), ("2002", "2014"), ("2015", "2022")]:
    x = pnl.loc[a:e] * 1e4
    print(f"316 {a}-{e}: {x.mean():+.1f}bp/trade (t={x.mean()/x.std()*np.sqrt(len(x)):.2f}, hit {100*(x>0).mean():.0f}%, n={len(x)})")
