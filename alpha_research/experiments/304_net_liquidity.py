"""304 - 'Fed net liquidity' flow narrative: NL = Fed total assets (WALCL) - Treasury General Account (WTREGEN)
- overnight reverse repo (RRPONTSYD), all FRED. Claim: reserve injections flow into risk assets. Weekly H.4.1 data
(Wednesday level, published Thursday 16:30 ET) -> made available from the Friday after (strict asof => first used at
Monday's close). Pre-specified variants on SPY, close->close, 0.5bp/side:
 A sign(4-week change in NL)   B sign(13-week change in NL)   C long when 4w change > 0, else flat (overlay)"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
rd = lambda s: pd.read_csv(os.path.join(lib.DATA, f"fred_{s}.csv"), parse_dates=["observation_date"], index_col=0,
                           na_values=".").iloc[:, 0].astype(float)
walcl, tga, rrp = rd("WALCL"), rd("WTREGEN"), rd("RRPONTSYD") * 1000   # all in $ millions
wk = walcl.index
nl = walcl - lib.asof(tga, wk).values - lib.asof(rrp, wk).fillna(0).values
nl.index = nl.index + pd.Timedelta(days=2)            # Wednesday level -> available Friday
d = lib.get("SPY")
variants = {"A sign(4w chg NL)": np.sign(nl.diff(4)), "B sign(13w chg NL)": np.sign(nl.diff(13)),
            "C long if 4w chg NL>0 else flat": (nl.diff(4) > 0).astype(float).where(nl.diff(4).notna())}
for name, sig in variants.items():
    w = lib.asof(sig.dropna(), d.index, strict=True).fillna(0.0)
    net = lib.backtest_weights(w.to_frame("X"), d.cc.to_frame("X"), 0.5).loc["2003-04":]
    r = lib.register("304_net_liquidity", f"304{name}", pd.Series(1.0, index=net.index), net, 0.0)
    held = w.shift(1).loc["2003-04":"2022"]
    ex = (net.loc[:"2022"] - held.mean() * d.cc.loc["2003-04":"2022"]).dropna()
    print(f"304{name}: {lib.fmt(r)}  mean pos {held.mean():+.2f}  timing t (2003-22) {ex.mean()/ex.std()*np.sqrt(len(ex)):+.2f}")
print("registry size:", lib.n_tests())
