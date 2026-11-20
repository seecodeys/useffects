"""306 - Weekend crypto moves and Monday US equities (idea from arXiv 2610.35702 'Weekend Crypto Volatility and Monday
Market Risk'). Crypto trades all weekend; leveraged retail/crypto-linked accounts that lose (gain) over the weekend
de-risk (add) in other risk assets on Monday. BTC-USD daily bars are UTC days: weekend move = close of Sunday UTC
(~20:00 ET Sun) / close of Friday UTC (~20:00 ET Fri) - 1, fully known before Monday's open.
Target: SPY/QQQ Monday open->close (the gap already prices whatever's public). Variants (registered):
 A SPY Mon oc = sign(BTC weekend)  B QQQ same  C QQQ only when |BTC weekend| > 5%.
Data start 2014-09 => there's no 2000-14 train sample; valid (2015-22) is the only pre-holdout evidence.
Also reported: corr(BTC weekend, Monday gap) to show how much the open already prices."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
btc = lib.get("BTC-USD", drop_today=False)["Close"]
fri = btc[btc.index.dayofweek == 4]; sun = btc[btc.index.dayofweek == 6]
wk = pd.Series(sun.values, index=sun.index + pd.Timedelta(days=1)) / lib.asof(fri, sun.index).values - 1   # keyed on Monday
for sym in ["SPY", "QQQ"]:
    d = lib.get(sym); mon = d.index[d.index.dayofweek == 0]
    x = wk.reindex(mon).dropna()
    print(f"306 {sym}: corr(BTC weekend, Monday gap) 2015-22 = {np.corrcoef(x.loc['2015':'2022'], d.gap[x.loc['2015':'2022'].index])[0,1]:+.2f}; "
          f"corr with Monday oc = {np.corrcoef(x.loc['2015':'2022'], d.oc[x.loc['2015':'2022'].index])[0,1]:+.2f}")
for name, sym, f in [("A SPY Mon oc sign(BTC wknd)", "SPY", lambda x: np.sign(x)),
                     ("B QQQ Mon oc sign(BTC wknd)", "QQQ", lambda x: np.sign(x)),
                     ("C QQQ Mon oc sign(BTC wknd) |x|>5%", "QQQ", lambda x: np.sign(x).where(x.abs() > 0.05, 0.0))]:
    d = lib.get(sym)
    pos = f(wk).reindex(d.index).fillna(0.0)
    r = lib.register("306_crypto_weekend", f"306{name}", pos, d.oc, 0.5, notes="no 2000-14 data; valid only")
    print(f"306{name}: {lib.fmt(r)}")
print("registry size:", lib.n_tests())
