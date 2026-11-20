"""134 - Treasury auction cycle (133c) conditioned on dealer-stress regime, pre-specified:
  a) TLT, long-duration auctions, only when TLT 20d realized vol (at the close before the cycle starts) > its trailing
     252d median.
  b) IEF with 10-year auctions only (incl. reopenings), unconditional.
1bp/side, close->close.
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
a = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "auctions.json")))["data"])


def cycle(etf, pattern, vol_filter):
    d = lib.get(etf); ti = d.index
    ad = pd.to_datetime(a[a.security_term.str.contains(pattern)].auction_date).drop_duplicates()
    rv = d.cc.rolling(20).std(); hi = rv > rv.rolling(252).median()
    p = pd.Series(0.0, index=ti)
    for dd in ad:
        if dd not in ti:
            continue
        k = ti.get_loc(dd)
        if k < 5 or k + 2 >= len(ti):
            continue
        if vol_filter and not bool(hi.iloc[k - 5]):
            continue
        p.iloc[k - 4:k - 1] = -1.0
        p.iloc[k:k + 2] = 1.0
    return lib.backtest_weights(p.to_frame("T"), d.cc.to_frame("T"), 1.0, lag=0).loc["2002-08":], p


for name, etf, pat, vf in [("134a TLT auction cycle, high rate-vol regime only", "TLT", r"^(10-Year|9-Year|20-Year|19-Year|30-Year|29-Year)", True),
                           ("134b IEF auction cycle, 10-year auctions", "IEF", r"^(10-Year|9-Year)", False)]:
    net, p = cycle(etf, pat, vf)
    r = lib.register("133_treasury_auctions", name, pd.Series(1.0, index=net.index), net, 0.0)
    v = net.loc["2015":"2022"]; v2 = v[v.index.year != 2020]
    print(f"{name:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}  valid ex-2020 t={v2.mean()/v2.std()*np.sqrt(len(v2)):+.2f}  days%={100*(p!=0).loc['2003':'2022'].mean():.0f}")
print("registry size:", lib.n_tests())
