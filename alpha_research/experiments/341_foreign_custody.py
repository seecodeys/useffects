"""341 - Foreign official Treasury custody drawdowns at the NY Fed -> Treasury supply (idea card B7).
Mechanism: reserve managers (Japan, EM CBs) sell custody Treasuries to fund FX intervention/dollar needs; this is
rate-outlook-insensitive supply that should weigh on Treasury prices for a few weeks.
Data: FRED WMTSECL1 (Wednesday level, $mn; nonzero from 2007-07; current vintage - custody levels are essentially not
revised). H.4.1 is released Thursday 16:30 ET (after the close), and on holiday weeks later. Availability rule:
a Wednesday observation is usable at the close of the first trading day >= Wednesday+2 (Friday). The signal is dated
that Friday and lib.backtest_weights(lag=1) puts the position on from the NEXT trading day's close (conservative,
one extra day over the card's 'enter at Friday close').
Signal: g = 4-week % change of WMTSECL1 (obs t vs t-4). Thresholds = TRAIN-only (obs dated <= 2014-12-31,
first usable g 2007-08) 10th / 90th percentiles. Hold H trading days after each trigger (re-triggers extend).
Pre-specified variants (max 4, no tuning after):
  a g < train p10 -> short TLT for 20 trading days. 1bp/side.
  b as (a), only if UUP 4-week (20d) return at the signal close > 0. 1bp/side.
  c g < train p10 -> short IEF for 10 trading days. 1bp/side.
  d g > train p90 -> long IEF for 20 trading days. 1bp/side.
Train 2007-08..2014 only (reduced power, ~7 years); valid 2015-22. Timing value = net minus exposure-matched holdings
(held weight x asset mean daily return within each period). Never reads 2023+."""
import os, sys
import numpy as np, pandas as pd, requests, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
path = os.path.join(lib.DATA, "fred_WMTSECL1.csv")
if not os.path.exists(path):
    r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=WMTSECL1", headers=lib._HDR, timeout=60)
    open(path, "w").write(r.text)
c = pd.read_csv(path, parse_dates=["observation_date"]).set_index("observation_date").iloc[:, 0]
c = pd.to_numeric(c, errors="coerce")
c = c[c > 0].loc[:END]
g = (c / c.shift(4) - 1).dropna()
gtr = g.loc[:"2014-12-31"]
p10, p90 = gtr.quantile(0.1), gtr.quantile(0.9)
print(f"weekly obs {len(c)} first {c.index[0].date()}  train g n={len(gtr)} p10={p10:+.4f} p90={p90:+.4f}")

P = pd.DataFrame({s: lib.get(s).aC.loc[:END] for s in ["TLT", "IEF", "UUP"]})
R = P.pct_change()
days = P.index[P.index >= "2007-07-01"]

def avail_day(wed):                       # first trading day >= Wed+2 (Friday)
    i = days.searchsorted(wed + pd.Timedelta(days=2))
    return days[i] if i < len(days) else None

gd = pd.Series({avail_day(w): v for w, v in g.items() if avail_day(w) is not None})
gd = gd[~gd.index.duplicated(keep="last")]
uup20 = P.UUP.pct_change(20)

def hold(trig_days, H):
    s = pd.Series(0.0, index=days)
    pos = days.get_indexer(trig_days)
    for p in pos:
        s.iloc[p:p + H] = 1.0             # dated signal days; backtest lag shifts implementation
    return s

lo = gd[gd < p10].index
hi = gd[gd > p90].index
lo_usd = [d for d in lo if uup20.get(d, np.nan) > 0]
runs = [("a g<p10 -> short TLT 20d lag-1", lo, "TLT", -1, 20),
        ("b g<p10 & UUP20d>0 -> short TLT 20d lag-1", pd.DatetimeIndex(lo_usd), "TLT", -1, 20),
        ("c g<p10 -> short IEF 10d lag-1", lo, "IEF", -1, 10),
        ("d g>p90 -> long IEF 20d lag-1", hi, "IEF", 1, 20)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s)) if s.std() > 0 else np.nan
start = "2007-08-03"
for name, trig, a, sgn, H in runs:
    w = hold(trig, H) * sgn
    W = pd.DataFrame({a: w})
    Ra = R[[a]].loc[days]
    net = lib.backtest_weights(W, Ra, 1.0, lag=1).loc[start:END]
    r = lib.register("341_foreign_custody", f"341{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="cost 1bp/side in net")
    held = W.reindex(Ra.index).fillna(0).shift(2).fillna(0).loc[net.index]
    on = held[a] != 0
    parts = [net.loc[x:y] - held.loc[x:y, a] * Ra.loc[x:y, a].mean() for x, y in ((start, "2014"), ("2015", END))]
    tv = pd.concat(parts)
    tr_ev = sum(1 for d in trig if d <= pd.Timestamp("2014-12-31")); va_ev = len(trig) - tr_ev
    # episodes = trigger runs separated by > H days
    ti = days.get_indexer(trig); ep = int((np.diff(ti) > H).sum() + 1) if len(ti) else 0
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    bpt = lambda x, y: 1e4 * net.loc[x:y][on.loc[x:y]].sum() / max(1, (held.loc[x:y, a].diff().abs() > 0).sum() / 2)
    print(f"341{name}\n   trigger weeks train/valid {tr_ev}/{va_ev}, episodes {ep}")
    print(f"   {lib.fmt(r)}  train_bp/day={r['train_bp_per_trade']:+.2f} gate={lib.passes_gate(r)}")
    print(f"   net bp per round-trip trade train/valid {bpt(start,'2014'):+.1f} / {bpt('2015',END):+.1f}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}  timing bp/held-day "
          f"{1e4*tv.loc[:'2014'][on.loc[:'2014']].mean():+.2f} / {1e4*tv.loc['2015':][on.loc['2015':]].mean():+.2f}")
    print("   yearly bp:", yr.to_dict())
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2*np.log(max(lib.n_tests(), 2))), 2))
