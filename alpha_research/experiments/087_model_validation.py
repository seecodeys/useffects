"""087 - How wrong is Black-Scholes-with-VIX option pricing? Monthly ATM put-write priced with VIX (prior close), held
from each 3rd-Friday roll to the next, vs the real CBOE PUT index (same rolls, collateralised in T-bills).
Compares monthly excess returns (over T-bills): mean, vol, correlation. Descriptive (not registered)."""
import os, sys, math
import numpy as np, pandas as pd
from scipy.stats import norm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY"); vix = lib.get("^VIX")["Close"]; irx = lib.get("^IRX")["Close"] / 100
put = pd.read_csv(os.path.join(lib.DATA, "cboe", "PUT.csv")); put["DATE"] = pd.to_datetime(put.DATE, format="%m/%d/%Y")
put = put.set_index("DATE").PUT
idx = spy.index.intersection(put.index)
rolls = [d for d in idx if d.dayofweek == 4 and 15 <= d.day <= 21 and d >= pd.Timestamp("1997-01-01")]
rows = []
for a, b in zip(rolls[:-1], rolls[1:]):
    S0, S1 = spy.loc[a, "Close"], spy.loc[b, "Close"]          # SPY ~ SPX/10 (dividends ignored)
    sig, r = vix.get(a, np.nan) / 100, irx.get(a, np.nan)
    T = (b - a).days / 365
    if np.isnan(sig) or np.isnan(r):
        continue
    d1 = (math.log(1) + (r + 0.5 * sig ** 2) * T) / (sig * math.sqrt(T)); d2 = d1 - sig * math.sqrt(T)
    P = S0 * math.exp(-r * T) * norm.cdf(-d2) - S0 * norm.cdf(-d1)        # ATM put
    model_excess = (P - max(S0 - S1, 0)) / S0                               # premium kept minus payoff, per $ notional
    real = put.loc[b] / put.loc[a] - 1 - r * T
    rows.append({"date": b, "model": model_excess, "real": real})
k = pd.DataFrame(rows).set_index("date")
for per, (a, b) in {"1997-2014": ("1997", "2014"), "2015-2022": ("2015", "2022"), "2023+": ("2023", "2100")}.items():
    x = k.loc[a:b]
    print(f"{per}: n={len(x)}  mean model {100*x.model.mean():+.2f}%/mo  real {100*x.real.mean():+.2f}%/mo  "
          f"corr {x.model.corr(x.real):.2f}  vol model {100*x.model.std():.2f} real {100*x.real.std():.2f}")
print(f"overall model-minus-real mean: {100*(k.model-k.real).mean():+.2f}%/mo")
