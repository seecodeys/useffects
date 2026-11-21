"""340 - Primary-dealer Treasury inventory build-ups -> subsequent IEF/TLT returns (idea card B4; Fleming, Nguyen &
Rosenberg, NY Fed SR299: dealers are compensated for absorbing supply through later price appreciation).

Data: NY Fed markets API, weekly as-of-Wednesday net outright positions (USD mn), fetched per series break:
  SBP2013 (2001-07..2013-03): Treasury ex-TIPS = PDPUSGTNOP - PDPUSGTIISNOP (verified: total = bills + coupon
          buckets + TIPS exactly); coupons ex-TIPS = PDPUSGCS3L + CS36 + CS611 + CSM11.
  SBN2013 / SBN2015 / SBN2022: Treasury ex-TIPS = PDPOSGST-TOT; coupons = PDPOSGST-TOT - PDPOSGS-B (bills)
          (FRNs, 2014+, stay in the coupon bucket). SBP2001 ends 2001-06, before IEF/TLT exist, so it is unused.
  Values are the API's current vintage (any revisions are not point-in-time; small for FR 2004 aggregates).
Release lag: as-of Wednesday W is published Thursday W+8 at ~16:15 ET (after the close). Signal date = first trading
  day >= W+8; lib.backtest_weights(lag=1) then puts the position on from the next close (normally Friday's close).
Signal (fixed before looking): d4 = 4-week change in net position; z = (d4 - rolling 52-week mean of d4) / rolling
  52-week sd of d4, both computed WITHIN a series break only (min 26 d4 observations, causal; no cross-break
  splicing). Trigger s = +1 if z > +1, -1 if z < -1, else 0.
Pre-specified variants (max 4, no tuning after):
  a) z > +1 -> long IEF for 4 weeks: 4 overlapping weekly tranches, daily weight = mean of the last 4 releases'
     long triggers (each tranche 1/4, rolls off after 4 releases ~ 20 trading days). 1bp/side.
  b) symmetric: same tranche ladder with s in {+1,-1} (long build-ups, short draw-downs) on IEF. 1bp/side.
  c) z > +1 -> long TLT for 1 week (weight = latest release's long trigger only). 1bp/side.
  d) coupon sub-series (ex bills) z > +1 -> long IEF for 4 weeks, same ladder as (a). 1bp/side.
Windows: IEF/TLT from 2002-07-31; train <= 2014-12-31, valid 2015-2022. Nothing after 2022-12-31 is read into
  returns or printed. Registration: pos = 1 on days with non-zero held exposure, ret = net daily P&L (costs inside),
  so registry bp_per_trade = mean net bp per held day. Timing value = net minus exposure-matched holdings (held weight x
  the asset's mean daily return within the same period).
"""
import os, sys, json
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
CACHE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "nyfed_pd")
os.makedirs(CACHE, exist_ok=True)


def pdget(sb, keys):
    path = os.path.join(CACHE, f"{sb}_{'_'.join(keys)}.json")
    if not os.path.exists(path):
        r = requests.get(f"https://markets.newyorkfed.org/api/pd/get/{sb}/timeseries/{'_'.join(keys)}.json", timeout=90)
        r.raise_for_status()
        open(path, "w").write(r.text)
    d = pd.DataFrame(json.load(open(path))["pd"]["timeseries"])
    d["value"] = pd.to_numeric(d["value"], errors="coerce")
    return d.pivot(index="asofdate", columns="keyid", values="value").rename(index=pd.Timestamp).sort_index()


old = pdget("SBP2013", ["PDPUSGTNOP", "PDPUSGTIISNOP", "PDPUSGCS3LNOP", "PDPUSGCS36NOP", "PDPUSGCS611NOP",
                        "PDPUSGCSM11NOP"])
series = {"SBP2013": pd.DataFrame({"tot": old.PDPUSGTNOP - old.PDPUSGTIISNOP,
                                   "cpn": old[["PDPUSGCS3LNOP", "PDPUSGCS36NOP", "PDPUSGCS611NOP",
                                               "PDPUSGCSM11NOP"]].sum(axis=1)})}
for sb in ["SBN2013", "SBN2015", "SBN2022"]:
    n = pdget(sb, ["PDPOSGST-TOT", "PDPOSGS-B"])
    series[sb] = pd.DataFrame({"tot": n["PDPOSGST-TOT"], "cpn": n["PDPOSGST-TOT"] - n["PDPOSGS-B"]})


def zscore(x):
    d4 = x.diff(4)
    m, s = d4.rolling(52, min_periods=26).mean(), d4.rolling(52, min_periods=26).std()
    return (d4 - m) / s


Z = pd.concat([pd.DataFrame({c: zscore(df[c]) for c in ["tot", "cpn"]}) for df in series.values()]).sort_index()
Z = Z.loc[:END].dropna(how="all")

px = {s: lib.get(s).aC.loc[:END] for s in ["IEF", "TLT"]}
P = pd.DataFrame(px)
R = P.pct_change()
idx = P.index[P.index >= "2002-07-31"]
pos_rel = idx.searchsorted(Z.index + pd.Timedelta(days=8))           # first trading day >= Wed + 8 (Thursday release)
ok = pos_rel < len(idx)
Zr = Z[ok].copy()
Zr.index = idx[pos_rel[ok]]
Zr = Zr[~Zr.index.duplicated(keep="last")]


def trig(z, sym):
    return ((z > 1).astype(float) - (sym * (z < -1)).astype(float)).where(z.notna(), 0.0)


def ladder(s, k):        # mean of the last k releases' triggers, held between release dates
    return s.rolling(k, min_periods=1).sum().div(k).reindex(idx).ffill().fillna(0.0)


ev_tot, ev_cpn = trig(Zr.tot, False), trig(Zr.cpn, False)
for nm, e in [("tot z>+1", ev_tot), ("tot z<-1", trig(Zr.tot, True).clip(upper=0)), ("cpn z>+1", ev_cpn)]:
    e = e[e != 0]
    print(f"events {nm}: train {len(e.loc[:'2014'])}  valid {len(e.loc['2015':END])}")
print("releases with z: train", Zr.tot.loc[:"2014"].notna().sum(), " valid", Zr.tot.loc["2015":END].notna().sum())

runs = [("a tot z>+1 long IEF 4w ladder lag-1", ladder(ev_tot, 4), "IEF", ev_tot, 4),
        ("b tot sym +-1 IEF 4w ladder lag-1", ladder(trig(Zr.tot, True), 4), "IEF", trig(Zr.tot, True), 4),
        ("c tot z>+1 long TLT 1w lag-1", ladder(ev_tot, 1), "TLT", ev_tot, 1),
        ("d cpn z>+1 long IEF 4w ladder lag-1", ladder(ev_cpn, 4), "IEF", ev_cpn, 4)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s))
start = "2002-08-01"
out = []
for name, w, a, ev, k in runs:
    W = pd.DataFrame({a: w})
    Ra = R[[a]].loc[idx]
    net = lib.backtest_weights(W, Ra, 1.0, lag=1).loc[start:END]
    held = W.reindex(Ra.index).fillna(0).shift(2).fillna(0).loc[net.index][a]
    on = held != 0
    r = lib.register("340_dealer_inventory", f"340{name}", on.astype(float), net, 0.0, notes="cost 1bp/side in net")
    tv = pd.concat([net.loc[x:y] - held.loc[x:y] * Ra[a].loc[x:y].mean() for x, y in ((start, "2014"), ("2015", END))])
    # net bp per full-notional trade: each event is a 1/k-notional tranche, so bp/trade = k * sum(net) / n_events
    bpt = [1e4 * k * net.loc[x:y].sum() / max((ev.loc[x:y] != 0).sum(), 1) for x, y in ((start, "2014"), ("2015", END))]
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"340{name}\n   {lib.fmt(r)}  train_bp/heldday={r['train_bp_per_trade']:+.2f} gate={lib.passes_gate(r)}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}   held-day% "
          f"{100*on.loc[:'2014'].mean():.0f} / {100*on.loc['2015':].mean():.0f}   avg|w| held "
          f"{held.loc[:'2014'][on.loc[:'2014']].abs().mean():.2f} / {held.loc['2015':][on.loc['2015':]].abs().mean():.2f}")
    print(f"   net bp per full-notional trade {bpt[0]:+.1f} / {bpt[1]:+.1f}")
    print("   yearly bp:", yr.to_dict())
    out.append((name, r, tv))
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
