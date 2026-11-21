"""358 - SNB weekly sight deposits as an FX-intervention gauge -> EURCHF the following week (idea card X6,
results/ideas_crossasset.md).
Mechanism: the SNB pays for FX purchases by crediting banks' sight deposits, so the weekly change in domestic banks'
sight deposits is a near-real-time proxy of SNB franc selling (or buying, 2022). Interventions come in campaigns, so a
large week may predict more official flow / a persistent EURCHF move the next week.
Data: SNB data portal, cube snbgwdchfsgw (public CSV API, https://data.snb.ch/api/cube/snbgwdchfsgw/data/csv/en),
item GI = sight deposits of domestic banks, CHF mn, Friday (week-end) observations from 2009-01-09. Current vintage
(weekly sight deposits are not materially revised). Published Monday ~09:00 CET for the week ending the previous
Friday; if that Monday is a Swiss bank holiday (Easter Monday, Whit Monday, 1-2 Jan, 25-26 Dec, 1 Aug) the release is
taken as the next weekday. FX prices: Yahoo EURCHF=X, CHF=X (USDCHF), daily close->close (spot only, no carry).
Timing: the signal is DATED at the first trading day >= release day (Monday) and lib.backtest_weights(lag=1) puts the
position on at the NEXT trading day's close (Tuesday), held 5 trading days. This is one day more conservative than the
card's Monday-close entry (brief rule: close->close signals use lag=1).
Floor: all signals dated 2011-09-06 .. 2015-01-15 are dropped (EURCHF 1.20 floor), and the P&L series excludes that
window entirely. TRAIN therefore = 2009-01 .. 2011-09-05 only (~2.7 years, very low power); VALID 2015-01-16 .. 2022.
dGI = GI_t - GI_{t-1} (CHF mn). Costs 2bp/side (turnover-based, inside the net series).
Pre-specified variants (max 4, no tuning after):
  a dGI > +2000 -> long EURCHF 5 trading days.
  b dGI < -2000 -> short EURCHF 5 trading days.
  c continuous: z = dGI / trailing 52-week std of dGI (weeks t-52..t-1), clipped to [-2,2], scaled /2 -> weight in
    [-1,1] on EURCHF for 5 trading days; sign = sign of the TRAIN correlation between z and the next-5-day EURCHF
    return (decided from train only, printed before valid).
  d as (a) in USDCHF (long USDCHF).
Timing value = net minus exposure-matched holding (held weight x asset mean daily return within each period).
Never reads 2023+."""
import os, sys
import numpy as np, pandas as pd, requests, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
FLOOR = ("2011-09-06", "2015-01-15")
COST = 2.0
H = 5
path = os.path.join(lib.DATA, "snb_snbgwdchfsgw.csv")
if not os.path.exists(path):
    r = requests.get("https://data.snb.ch/api/cube/snbgwdchfsgw/data/csv/en", headers=lib._HDR, timeout=60)
    r.raise_for_status()
    open(path, "w", encoding="utf-8").write(r.text)
raw = pd.read_csv(path, sep=";", skiprows=3, encoding="utf-8-sig")
gi = raw[raw.D0 == "GI"].dropna(subset=["Value"])
gi = pd.Series(pd.to_numeric(gi.Value).values, index=pd.to_datetime(gi.Date)).sort_index().loc[:END]
d = gi.diff().dropna()
sd52 = gi.diff().rolling(52, min_periods=26).std().shift(1)
z = (d / sd52).clip(-2, 2) / 2
print(f"GI weekly obs {len(gi)} {gi.index[0].date()}..{gi.index[-1].date()}  dGI>+2bn {int((d>2000).sum())}"
      f"  dGI<-2bn {int((d<-2000).sum())}")


def easter(y):
    a = y % 19; b = y // 100; c = y % 100; dd = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - dd - (c % 4)) % 7; f = dd + e - 7 * ((a + 11 * dd + 22 * e) // 451) + 114
    return pd.Timestamp(y, f // 31, f % 31 + 1)


def release(fri):
    t = fri + pd.Timedelta(days=3)
    hol = {easter(t.year) + pd.Timedelta(days=1), easter(t.year) + pd.Timedelta(days=50)} | \
          {pd.Timestamp(t.year, m, dd_) for m, dd_ in ((1, 1), (1, 2), (12, 25), (12, 26), (8, 1))}
    while t in hol or t.dayofweek >= 5:
        t += pd.Timedelta(days=1)
    return t


P = pd.DataFrame({"EURCHF": lib.get("EURCHF=X").Close, "USDCHF": lib.get("CHF=X").Close}).loc["2008-12-01":END]
P = P.dropna()
R = P.pct_change()
days = P.index


def sig_day(fri):
    i = days.searchsorted(release(fri))
    return days[i] if i < len(days) else None


def infloor(t):
    return pd.Timestamp(FLOOR[0]) <= t <= pd.Timestamp(FLOOR[1])


ev = pd.DataFrame({"dGI": d, "z": z})
ev["sday"] = [sig_day(f) for f in ev.index]
ev = ev.dropna(subset=["sday"])
ev = ev[[not infloor(t) for t in ev.sday]]


def weights(sel, valcol=None, sgn=1.0):
    w = pd.Series(0.0, index=days)
    for _, row in ev[sel].iterrows():
        p = days.get_loc(row.sday)
        w.iloc[p:p + H] = sgn * (row[valcol] if valcol else 1.0)   # later signals overwrite (re-trigger extends)
    return w


# train sign for (c): corr(z, next-5d EURCHF return from the implemented entry) on train events only
fwd = np.log(P.EURCHF).shift(-(H + 1)) - np.log(P.EURCHF).shift(-1)   # from close sday+1 to sday+1+H
ev["fwd"] = fwd.reindex(ev.sday.values).values
trv = ev[(ev.sday <= pd.Timestamp(FLOOR[0])) & ev.z.notna()]
sgn_c = float(np.sign(trv[["z", "fwd"]].corr().iloc[0, 1]))
print(f"(c) train corr(z, fwd5) = {trv[['z','fwd']].corr().iloc[0,1]:+.3f} over {len(trv)} weeks -> sign {sgn_c:+.0f}")

runs = [("a dGI>+2bn long EURCHF 5d lag-1", weights(ev.dGI > 2000), "EURCHF"),
        ("b dGI<-2bn short EURCHF 5d lag-1", weights(ev.dGI < -2000, sgn=-1.0), "EURCHF"),
        ("c z-scaled dGI (train sign) EURCHF 5d lag-1", weights(ev.z.notna(), "z", sgn_c), "EURCHF"),
        ("d dGI>+2bn long USDCHF 5d lag-1", weights(ev.dGI > 2000), "USDCHF")]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s)) if s.std() > 0 else np.nan
start = "2009-01-15"
keep = lambda s: s[(s.index < FLOOR[0]) | (s.index > FLOOR[1])]
rows = []
for name, w, a in runs:
    W = pd.DataFrame({a: w})
    net = keep(lib.backtest_weights(W, R[[a]], COST, lag=1).loc[start:END])
    r = lib.register("358_snb_sight_deposits", f"358{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="cost 2bp/side in net; floor 2011-09-06..2015-01-15 excluded")
    held = keep(W.shift(2).fillna(0).loc[start:END])[a]
    on = held != 0
    parts = [net.loc[x:y] - held.loc[x:y] * R[a].loc[x:y].mean() for x, y in ((start, "2014"), ("2015", END))]
    tv = pd.concat(parts)
    trig = (w != 0) & (w.shift(1).fillna(0) == 0)
    ntr = int(trig.loc[:"2014"].sum()); nva = int(trig.loc["2015":END].sum())
    bpt = lambda x, y: 1e4 * net.loc[x:y][on.loc[x:y]].sum() / max(1, int(trig.loc[x:y].sum()))
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"358{name}\n   episodes train/valid {ntr}/{nva}")
    print(f"   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    print(f"   net bp per episode train/valid {bpt(start,'2014'):+.1f} / {bpt('2015',END):+.1f}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}")
    print("   yearly bp:", yr.to_dict())
    rows.append({"variant": name, "train_ep": ntr, "valid_ep": nva, "train_t": r["train_t"], "valid_t": r["valid_t"],
                 "valid_sharpe": r["valid_sharpe"], "bp_ep_train": bpt(start, "2014"),
                 "bp_ep_valid": bpt("2015", END), "gate": lib.passes_gate(r)})
pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "358_summary.csv"), index=False)
ev[["dGI", "z", "sday"]].to_csv(os.path.join(lib.HERE, "results", "358_events.csv"))
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2*np.log(max(lib.n_tests(), 2))), 2))
