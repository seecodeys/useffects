"""331 (idea card B5) - Auction takedown: do IEF/TLT underperform after primary dealers are stuffed at a 10y/30y auction?

Mechanism: when end-investor demand is weak, primary dealers absorb an unusually large share of the auction, carry
excess inventory and sell it down over the following days -> the tenor's ETF underperforms. Very strong end-demand
(low dealer share / high bid-to-cover) -> outperforms. Direction is FIXED by this hypothesis (no choice on train).

Data: fiscaldata v1/accounting/od/auctions_query (cached data/auct331/p1.json, p2.json; only auctions <= 2022-12-31 are
downloaded). Nominal (non-TIPS, non-FRN) auctions with original_security_term 10-Year -> IEF, 30-Year -> TLT, new issues
and reopenings. Dealer share = primary_dealer_accepted / comp_accepted (competitive accepted; excludes SOMA add-ons and
non-competitive awards, which would otherwise distort 2008-14 shares). Dealer fields exist from 2008-05, so dealer
variants are 2008-14 train / 2015-22 valid; the bid-to-cover variant runs from IEF/TLT inception (2002-07).
Surprise = value - mean of the same tenor's previous 6 auctions (known before the auction).
Timing: results are released ~13:00 ET on auction day d; entry at the close of d (tradable, after the result), weights
decided at close d. Costs 1bp/side, turnover-based (lib.backtest_weights). Book = IEF and TLT columns, each in [-1, 1].

PRE-SPECIFIED VARIANTS (written before any result was seen; no tuning afterwards):
  A dealer contrast, 3 days: share surprise > +5pp -> short, < -5pp -> long; hold close d -> close d+3.
  B as A but held close d -> close d+5 (slower inventory distribution).
  C bid-to-cover contrast, 3 days: btc surprise < -0.2 -> short, > +0.2 -> long; close d -> close d+3 (2002+).
  D as A with lag 1 (enter close d+1, exit close d+4): the realistic-if-delayed check.
Timing value = net P&L - held weight x the ETF's mean daily cc return in the same period (removes drift/beta); t by
period. Descriptive tables stop at 2022-12-31; lib.holdout_eval is never called.
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "331_auction_dealer_takedown"
COST = 1.0
END = "2022-12-31"

raw = []
for p in ("p1.json", "p2.json"):
    raw += json.load(open(os.path.join(lib.DATA, "auct331", p)))["data"]
a = pd.DataFrame(raw)
a = a[(a.inflation_index_security == "No") & (a.floating_rate == "No")
      & a.original_security_term.isin(["10-Year", "30-Year"])].copy()
for c in ["bid_to_cover_ratio", "primary_dealer_accepted", "comp_accepted"]:
    a[c] = pd.to_numeric(a[c].replace("null", None))
a["date"] = pd.to_datetime(a.auction_date)
a = a[a.date <= END].sort_values("date")
a["etf"] = a.original_security_term.map({"10-Year": "IEF", "30-Year": "TLT"})
a["share"] = a.primary_dealer_accepted / a.comp_accepted
a["btc"] = a.bid_to_cover_ratio
for c in ["share", "btc"]:
    a[c + "_sur"] = a.groupby("etf")[c].transform(lambda s: s - s.shift(1).rolling(6, min_periods=6).mean())

px = {s: lib.get(s) for s in ["IEF", "TLT"]}
R = pd.DataFrame({s: px[s].cc for s in px}).loc[:END]
idx = R.index


def weights(sig_col, thr, hold, lag):
    W = pd.DataFrame(0.0, index=idx, columns=R.columns)
    ev = []
    for _, e in a.dropna(subset=[sig_col]).iterrows():
        x = e[sig_col]
        s = -1.0 if x > thr else (1.0 if x < -thr else 0.0)
        if sig_col == "btc_sur":               # high bid-to-cover = strong demand -> long
            s = -s
        if s == 0:
            continue
        i = idx.searchsorted(e.date)
        if i >= len(idx) or idx[i] != e.date:
            continue                            # auction day not a trading day (should not happen)
        W.iloc[i + lag:i + lag + hold, W.columns.get_loc(e.etf)] += s
        ev.append((e.date, e.etf, s))
    return W.clip(-1, 1), pd.DataFrame(ev, columns=["date", "etf", "side"])


def tt(x):
    x = x.dropna()
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


variants = {
    "A dealer-share surprise +-5pp, close d -> d+3": ("share_sur", 0.05, 3, 0, "2008-05-01"),
    "B dealer-share surprise +-5pp, close d -> d+5": ("share_sur", 0.05, 5, 0, "2008-05-01"),
    "C bid-to-cover surprise +-0.2, close d -> d+3": ("btc_sur", 0.2, 3, 0, "2002-08-01"),
    "D dealer-share surprise +-5pp, lag1 close d+1 -> d+4": ("share_sur", 0.05, 3, 1, "2008-05-01"),
}
for name, (col, thr, hold, lag, start) in variants.items():
    W, ev = weights(col, thr, hold, lag)
    W, Rs = W.loc[start:], R.loc[start:]
    net = lib.backtest_weights(W, Rs, COST)
    r = lib.register(EXP, name, pd.Series(1.0, index=net.index), net, 0.0,
                     notes=f"cost {COST}bp/side via backtest_weights; IEF(10y)+TLT(30y) book")
    held = W.shift(1).fillna(0)
    tv = net.copy()
    for per in [(start, "2014-12-31"), ("2015-01-01", END)]:
        mu = Rs.loc[per[0]:per[1]].mean()
        tv.loc[per[0]:per[1]] = net.loc[per[0]:per[1]] - (held.loc[per[0]:per[1]] * mu).sum(axis=1)
    ev["per"] = np.where(ev.date <= "2014-12-31", "train", "valid")
    cnt = ev.groupby(["per", "etf", "side"]).size().unstack(["etf", "side"]).fillna(0).astype(int)
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"\n331{name}\n   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    print(f"   timing t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}")
    print("   events:\n" + cnt.to_string())
    print("   yearly bp:", yr.to_dict())

# descriptive: mean ETF return close d -> d+3 by dealer-share surprise tercile (cuts from 2008-14), <= 2022
fw = (R.shift(-1) + R.shift(-2) + R.shift(-3))
a["fwd3"] = [fw.loc[d, e] if d in fw.index else np.nan for d, e in zip(a.date, a.etf)]
d = a.dropna(subset=["share_sur", "fwd3"])
cuts = d[d.date <= "2014-12-31"].share_sur.quantile([1 / 3, 2 / 3]).values
d["terc"] = np.digitize(d.share_sur, cuts)
print("\nd+1..d+3 bp by dealer-share-surprise tercile (cuts 2008-14):", cuts.round(3))
for per, m in [("2008-14", d.date <= "2014-12-31"), ("2015-22", d.date > "2014-12-31")]:
    g = d[m].groupby("terc").fwd3.agg(["mean", "count"])
    print("  ", per, " | ".join(f"T{t+1} {1e4*g.loc[t,'mean']:+.1f} (n={int(g.loc[t,'count'])})" for t in g.index))
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
