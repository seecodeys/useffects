"""305 - Tax-date liquidity drain. On quarterly tax dates (corporate: 15th of Mar/Apr/Jun/Sep/Dec; individual estimated:
Jan/Apr/Jun/Sep 15) billions move from bank reserves into the Treasury General Account (TGA), a predictable reserve
drain; liquidity narratives say risk assets soften. (a) Descriptive: how big are the TGA jumps on those dates and what
did SPY do on the largest-drain days (2005-22, DTS data from fiscaldata, rule 4b). (b) Registered, calendar-only rule
(known in advance): short SPY close->close on the first trading day >= the 15th of Jan/Mar/Apr/Jun/Sep/Dec, and
variant over that day and the next (2-day)."""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
x = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "dts_cash.json")))["data"] +
                 json.load(open(os.path.join(lib.DATA, "dts_cash2.json")))["data"])
x = x.replace("null", np.nan)
v = pd.concat([x[x.account_type.isin(["Federal Reserve Account", "Treasury General Account (TGA)"])].set_index("record_date").close_today_bal,
               x[x.account_type == "Treasury General Account (TGA) Closing Balance"].set_index("record_date").open_today_bal])
tga = pd.to_numeric(v, errors="coerce").groupby(level=0).last(); tga.index = pd.to_datetime(tga.index); tga = tga.sort_index()
dtga = tga.diff()
spy = lib.get("SPY"); idx = spy.index
ym = pd.Series(idx.to_period("M"), index=idx)
taxday = pd.Series(False, index=idx)
for m, g in pd.Series(idx, index=idx).groupby(ym):
    if m.month in (1, 3, 4, 6, 9, 12):
        c = g[g.dt.day >= 15]
        if len(c): taxday[c.iloc[0]] = True
dd = dtga.reindex(idx)
print("305a median TGA change $bn: tax days %.1f vs other days %.1f (2005-22)" % (
    dd[taxday].loc["2005":"2022"].median() / 1e3, dd[~taxday].loc["2005":"2022"].median() / 1e3))
top = dd.loc["2005-10":"2022"]; big = top >= top.quantile(0.98)
print("305a share of top-2%% TGA-jump days that are tax days: %.0f%%" % (100 * taxday[big[big].index].mean()))
print("305a SPY cc on top-2%% TGA-jump days: %+.1fbp (n=%d) vs all days %+.1fbp" % (
    1e4 * spy.cc[big[big].index].mean(), big.sum(), 1e4 * spy.cc.loc["2005-10":"2022"].mean()))
for name, w in {"A short SPY cc on tax day": -taxday.astype(float).shift(-1).fillna(0),
                "B short SPY cc tax day + next": -(taxday | taxday.shift(1, fill_value=False)).astype(float).shift(-1).fillna(0)}.items():
    net = lib.backtest_weights(w.to_frame("X"), spy.cc.to_frame("X"), 0.5).loc["2000":]
    r = lib.register("305_tax_drain", f"305{name}", pd.Series(1.0, index=net.index), net, 0.0)
    days = (w != 0).shift(1, fill_value=False)
    print(f"305{name}: {lib.fmt(r)}  bp/day 2000-14 {1e4*net[days].loc[:'2014'].mean():+.1f}, 2015-22 {1e4*net[days].loc['2015':'2022'].mean():+.1f}")
print("registry size:", lib.n_tests())
