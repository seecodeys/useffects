"""355 - Idea card 7 (results/ideas_options_etf.md): scheduled-event volatility premium (Londono-Samadi IFDP 1376;
Wright NBER 28306). Hedgers bid implied vol into scheduled FOMC and CPI releases; once the event passes, the event
variance leaves the option and implied vol is crushed. Sellers of event-spanning vol should earn more than normal.

Not a duplicate: 024 traded SPY direction on FOMC days, 291 faded the 14:00 FOMC reaction on the SPX CFD, 220 split
WPUT by weekday. None measured the option/VIX-ETP premium around scheduled releases.

Data (all used only through 2022-12-31; nothing from 2023+ is loaded or printed):
  * FOMC: tools/fomc_dates.csv, type == scheduled (statement 14:00 ET, intraday).
  * CPI release dates: ALFRED vintage dates of CPIAUCSL (alfredgraph.csv?id=CPIAUCSL&vintage_date=...). A vintage is
    a CPI release iff it adds a new latest observation month (drops seasonal-factor-revision vintages). CPI prints at
    08:30 ET, before the open, so the event day is the release day itself. Cached to data/cpi_release_dates.csv.
  * VIXY (Yahoo, 2011+; split-adjusted cc), CBOE WPUT index (data/cboe/WPUT.csv, 2006+; weekly ATM put-write).
Timing: event dates are scheduled and published weeks ahead, so weights are decided at close t-2 and implemented with
lib.backtest_weights(..., lag=1) -> the position is held close(t-1) -> close(t) for event day t. Costs 2bp/side
(VIXY and WPUT are 'others'). TRAIN = data start..2014 (VIXY 2011-14; WPUT 2006-14), VALID = 2015-22.

PRE-SPECIFIED VARIANTS (fixed before looking at results; no tuning):
  V1  short VIXY close(t-1)->close(t), t = scheduled FOMC announcement day
  V2  short VIXY close(t-1)->close(t), t = CPI release day
  V3  long WPUT only in weeks (Fri close -> Fri close) containing a scheduled FOMC announcement
  V4  long WPUT only in weeks containing a CPI release
Each is registered twice (336 convention): raw net P&L, and timing value = net - mean_signed_pos(period) * asset cc,
i.e. vs an exposure-matched always-on position (always-short VIXY / always-long WPUT, uncosted benchmark).
The verdict is on the timing-value rows (raw short-VIXY / long-WPUT carry is not an event effect).
"""
import os, sys, time
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP, COST = "355_event_vol_premium", 2.0
END = "2022-12-31"


def cpi_release_dates():
    path = os.path.join(lib.DATA, "cpi_release_dates.csv")
    if os.path.exists(path):
        return pd.to_datetime(pd.read_csv(path)["date"])
    html = requests.get("https://alfred.stlouisfed.org/series/downloaddata?seid=CPIAUCSL",
                        timeout=60).text
    import re
    vint = sorted({d for d in re.findall(r"(?:19|20)\d\d-\d\d-\d\d", html) if "1999-06-01" <= d <= "2026-12-31"})
    rows, last = [], None
    for d in vint:
        url = f"https://alfred.stlouisfed.org/graph/alfredgraph.csv?id=CPIAUCSL&vintage_date={d}&cosd=1998-01-01"
        for a in range(4):
            try:
                df = pd.read_csv(url).dropna()
                break
            except Exception:
                time.sleep(2 ** a)
        else:
            raise RuntimeError(d)
        lo = df.iloc[:, 0].max()
        if pd.isna(lo):
            continue
        rows.append({"vintage": d, "last_obs": lo, "new_month": last is None or lo > last})
        last = lo if last is None or lo > last else last
        time.sleep(0.2)
    v = pd.DataFrame(rows)
    v.to_csv(os.path.join(lib.DATA, "cpi_vintages.csv"), index=False)
    out = v.loc[v["new_month"], ["vintage"]].rename(columns={"vintage": "date"})
    out.to_csv(path, index=False)
    return pd.to_datetime(out["date"])


fomc = pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"])
fomc = pd.DatetimeIndex(fomc.loc[fomc["type"] == "scheduled", "date"])
cpi = pd.DatetimeIndex(cpi_release_dates())

vixy = lib.get("VIXY").loc[:END]
wp = pd.read_csv(os.path.join(lib.DATA, "cboe", "WPUT.csv"))
wp = pd.Series(wp["WPUT"].values, index=pd.to_datetime(wp["DATE"], format="%m/%d/%Y")).sort_index().loc[:END]


def day_events(idx, ev):
    return pd.Series(idx.isin(ev).astype(float), index=idx)


def week_events(idx, ev):
    wk = idx.to_period("W-FRI")
    evw = set(pd.DatetimeIndex(ev).to_period("W-FRI"))
    return pd.Series([1.0 if w in evw else 0.0 for w in wk], index=idx)


def run(name, sign, ind, cc):
    """ind: 1 on days whose close(t-1)->close(t) return is held. Decided at close t-2 (calendar), lag=1."""
    W = (sign * ind).shift(-2).fillna(0.0).to_frame("a")
    net = lib.backtest_weights(W, cc.to_frame("a"), COST, lag=1)
    signed = sign * ind
    act = ((ind != 0) | (net != 0)).astype(float)      # include exit-cost day as traded
    raw = lib.register(EXP, name + " (raw, 2bp/side)", act, net.where(act > 0, 0.0), 0.0,
                       notes=f"events held={int(ind.sum())}")
    tv = pd.Series(0.0, index=cc.index)
    for a, b in [lib.TRAIN, lib.VALID]:
        tv.loc[a:b] = net.loc[a:b] - signed.loc[a:b].mean() * cc.loc[a:b]
    one = pd.Series(1.0, index=cc.index)
    tvr = lib.register(EXP, name + " (timing value vs exposure-matched always-on)", one, tv, 0.0,
                       notes="net minus mean signed pos * cc (uncosted benchmark)")
    # per-event stats: event blocks
    blk = (ind.diff().fillna(ind) > 0).cumsum().where(ind > 0)
    rows = []
    for lab, r in [("raw", raw), ("TV", tvr)]:
        rows.append({"variant": name, "metric": lab, "train_t": r["train_t"], "valid_t": r["valid_t"],
                     "train_sharpe": r["train_sharpe"], "valid_sharpe": r["valid_sharpe"],
                     "valid_bp_per_trade": r["valid_bp_per_trade"], "gate": lib.passes_gate(r)})
    ev = {}
    for p, (a, b) in lib.PERIODS.items():
        n_ev = blk.loc[a:b].nunique()
        netp = net.loc[a:b].sum()
        base = (sign * cc.loc[a:b]).mean()    # always-on per-day mean (sign applied)
        days_per = ind.loc[a:b].sum() / max(n_ev, 1)
        ev[p] = {"events": n_ev, "net_bp_per_event": 1e4 * netp / max(n_ev, 1),
                 "always_on_bp_same_days": 1e4 * base * days_per,
                 "TV_bp_per_event": 1e4 * tv.loc[a:b].sum() / max(n_ev, 1)}
    yr = 1e4 * tv.groupby(tv.index.year).sum()
    return rows, ev, yr, net


pd.set_option("display.width", 220)
rows, evs, yrs = [], {}, {}
vcc = vixy["cc"].fillna(0.0)
wcc = wp.pct_change().fillna(0.0)
specs = [("V1 short VIXY FOMC day", -1.0, day_events(vixy.index, fomc), vcc),
         ("V2 short VIXY CPI day", -1.0, day_events(vixy.index, cpi), vcc),
         ("V3 long WPUT FOMC weeks", 1.0, week_events(wp.index, fomc), wcc),
         ("V4 long WPUT CPI weeks", 1.0, week_events(wp.index, cpi), wcc)]
for name, s, ind, cc in specs:
    r, ev, yr, _ = run(name, s, ind, cc)
    rows += r
    evs[name] = ev
    yrs[name] = yr

print(pd.DataFrame(rows).round(2).to_string(index=False))
print("\nPer-event (net, bp):")
for k, ev in evs.items():
    print(k, {p: {kk: round(vv, 1) for kk, vv in d.items()} for p, d in ev.items()})
print("\nTiming value bp by year (sum, <=2022):\n", pd.DataFrame(yrs).round(0).to_string())
print("\nCPI release dates found (<=2022):", (cpi <= END).sum(), " first:", cpi.min().date(),
      " non-trading-day releases dropped (VIXY idx):", int((~cpi[(cpi >= "2011-01-04") & (cpi <= END)]
                                                            .isin(vixy.index)).sum()))
n = lib.n_tests()
print("registry size:", n, " luck t ~", round(np.sqrt(2 * np.log(max(n, 2))), 2))
pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "355_event_vol_premium.csv"), index=False)
