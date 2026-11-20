"""298 - Dose-response for the month-end bond bid: is it larger when more Treasury coupon supply (notes/bonds/TIPS) was
issued during the month? New issuance lengthens the index at month-end -> more extension buying by index trackers.
Issuance by issue month (fiscaldata auctions, total accepted), scaled by its trailing 12-month mean (known in advance).
Descriptive terciles on 2002-22 (rule 4b) + one registered sized variant (weights 0.5/1/1.5 by tercile, cut points
from 2002-14 only)."""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
a = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "auct_issue_1.json")))["data"])
a["amt"] = pd.to_numeric(a.total_accepted, errors="coerce").fillna(pd.to_numeric(a.offering_amt, errors="coerce"))
a["m"] = pd.to_datetime(a.issue_date).dt.to_period("M")
iss = a.groupby("m").amt.sum()
iss = iss.reindex(pd.period_range(iss.index.min(), iss.index.max(), freq="M")).fillna(0)
ratio = iss / iss.shift(1).rolling(12).mean()
cuts = ratio.loc["2002-01":"2014-12"].quantile([1 / 3, 2 / 3]).values
print("tercile cuts (2002-14):", cuts.round(2))
for sym in ["IEF", "TLT"]:
    d = lib.get(sym); idx = d.index; ym = pd.Series(idx.to_period("M"), index=idx)
    bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    win = d.cc[(bwd <= 3) & (ym != ym.iloc[-1])].groupby(ym).sum().loc["2002-08":"2022-12"]
    rr = ratio.reindex(win.index)
    terc = pd.Series(np.digitize(rr, cuts), index=win.index)
    print(f"{sym} last-3-session bp by issuance tercile (2002-22):")
    for per, (s, e) in {"2002-14": ("2002-08", "2014-12"), "2015-22": ("2015-01", "2022-12")}.items():
        out = []
        for t in range(3):
            x = win.loc[s:e][terc.loc[s:e] == t]
            out.append(f"T{t+1} {1e4*x.mean():+.1f} (t={x.mean()/x.std()*np.sqrt(len(x)):.1f}, n={len(x)})")
        print("  ", per, " | ".join(out))
    print("   corr(ratio, window ret) 2002-22:", round(np.corrcoef(rr.dropna(), win[rr.notna()])[0, 1], 3))
    w = pd.Series(np.digitize(ratio.reindex(ym).values, cuts), index=idx).map({0: 0.5, 1: 1.0, 2: 1.5})
    pos = (((bwd <= 3) & (ym != ym.iloc[-1])).astype(float) * w).shift(-1).fillna(0)
    net = lib.backtest_weights(pos.to_frame("X"), d.cc.to_frame("X"), 1.0).loc["2002-08":]
    r = lib.register("275_bond_tom", f"298 {sym} last-3 bid sized by issuance tercile 0.5/1/1.5",
                     pd.Series(1.0, index=net.index), net, 0.0)
    print("  sized:", lib.fmt(r))
print("registry size:", lib.n_tests())
