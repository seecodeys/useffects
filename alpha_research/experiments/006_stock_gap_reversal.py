"""006/007 - Cross-sectional intraday signals in US large caps.

Universe: ~90 large caps listed since ~2000 (hand-picked => SURVIVORSHIP BIAS; less
harmful for dollar-neutral L/S than for long-only, but flag any result).
Only stocks with prior close >= $20 (tick <= 5bp) on the day are eligible, to limit
the opening-print bid/ask artifact found in 004.

006  gap reversal:   rg = stock gap - SPY gap; long bottom decile rg, short top decile.
007a tug-of-war:     past 20d sum of overnight returns; long lowest decile, short highest (intraday).
007b intraday mom:   past 20d sum of intraday returns; long highest decile, short lowest.
All open->close, equal-weight, dollar neutral (0.5 long / 0.5 short gross). Costs 2bp/side.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "006_stock_intraday_xs"
UNIVERSE = """AAPL MSFT INTC CSCO ORCL IBM HPQ TXN QCOM ADBE AMAT MU AMZN EBAY ADP
JPM BAC C WFC GS MS AXP USB PNC BK SCHW AIG MET TRV ALL
JNJ PFE MRK ABT LLY BMY AMGN GILD MDT UNH CVS CI BAX SYK BDX
XOM CVX COP SLB OXY HAL EOG DVN APA
PG KO PEP WMT HD LOW TGT COST MCD NKE SBUX DIS CMCSA KMB CL GIS K HSY MO
GE MMM HON CAT DE BA LMT RTX UNP CSX NSC FDX UPS EMR ITW GD
T VZ DUK SO D NEE AEP EXC SPG F""".split()
COST = 2.0


def load_panel():
    O, C, G = {}, {}, {}
    for s in UNIVERSE:
        try:
            d = lib.get(s).loc["2000":]
        except Exception as e:
            print("skip", s, e); continue
        O[s], C[s], G[s] = d.oc, d["Close"].shift(1), d.gap
    return pd.DataFrame(O), pd.DataFrame(C), pd.DataFrame(G)


def decile_ls(signal, ret, long_low=True, q=0.1, min_n=30):
    rk = signal.rank(axis=1, pct=True)
    n = signal.notna().sum(axis=1)
    lo, hi = rk.le(q), rk.gt(1 - q)
    w = (lo.astype(float).div(lo.sum(axis=1), axis=0) * 0.5 - hi.astype(float).div(hi.sum(axis=1), axis=0) * 0.5)
    if not long_low:
        w = -w
    w = w.where(n >= min_n, 0.0).fillna(0.0)
    gross = (w * ret.fillna(0)).sum(axis=1)
    return gross, gross - w.abs().sum(axis=1) * 2 * COST / 1e4


if __name__ == "__main__":
    OC, PC, GAP = load_panel()
    spy = lib.get("SPY")
    elig = PC >= 20
    print("stocks:", OC.shape[1], " avg eligible/day 2015-22:", elig.loc["2015":"2022"].sum(axis=1).mean().round(1))
    rg = GAP.sub(spy.gap.reindex(GAP.index), axis=0).where(elig & GAP.abs().lt(0.3))
    on20 = GAP.rolling(20).sum().shift(1).where(elig)
    id20 = OC.rolling(20).sum().shift(1).where(elig)
    tests = {"006 gap reversal (long low resid gap)": (rg, True),
             "007a tug-of-war (long low past-20d overnight)": (on20, True),
             "007b intraday momentum (long high past-20d intraday)": (id20, False)}
    for name, (sig, long_low) in tests.items():
        gross, net = decile_ls(sig, OC, long_low)
        r = lib.register(EXP, name, pd.Series(1.0, index=net.index), net, 0.0, notes="2bp/side in series; survivorship-biased universe")
        print(f"{name:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
        g = gross.loc[:"2022"]
        print("   gross bp/day by year:", (g.groupby(g.index.year).mean() * 1e4).round(1).to_dict())
    print("registry size:", lib.n_tests())
