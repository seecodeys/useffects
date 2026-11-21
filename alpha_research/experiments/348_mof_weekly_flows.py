"""348 - Japan MOF weekly 'International Transactions in Securities' -> next-week USDJPY / TLT / EWJ / DXJ (idea X5).
Mechanism: Japanese lifers/pensions/toshin buy foreign bonds via multi-week programmes, and non-resident buying of
Japanese equities is programme-like; flows are autocorrelated, so a large week signals more flow the following week.

Data: MOF week.csv (public, 2005-01 onward, 100m JPY; designated major investors), downloaded from
  https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv
  Columns used: residents' net acquisition of foreign long-term debt (col 6), non-residents' net acquisition of
  Japanese equity & fund shares (col 14). The file is the CURRENT history (weekly figures are rarely revised; no
  first-release vintages available), so small revision leakage is possible.
Timing: week Sun..Sat (ending Saturday S). Release 08:50 JST on the 4th Japanese business day after S (normally
  Thursday S+5; pushed later by Japanese holidays / Dec 31-Jan 3), i.e. NY evening of the previous calendar day,
  after the US close. Signal assigned to the first US trading day >= (release JST date - 1 day) and traded with
  lib.backtest_weights(lag=1): position from the NEXT trading day's close (normally Thursday close), held until the
  next weekly signal replaces it (~5 trading days). This is one session later than the card's 'Thursday open' entry
  (daily closes only); conservative.
Signal: z = flow_w / std(flow over the PRIOR 52 weeks, current excluded). Position +1 if z > +1.5, -1 if z < -1.5.
Pre-specified variants (4, no tuning after):
  a USDJPY (Yahoo JPY=X close; long = long USD vs JPY) ~ z(residents' net foreign LT bond purchases). 2bp/side.
  b TLT, long-only on z(bond) > +1.5 (flat otherwise). 1bp/side.
  c EWJ ~ z(non-residents' net Japanese equity purchases), long/short. 2bp/side.
  d DXJ (yen-hedged, 2006-06+) ~ same signal as c, long/short. 2bp/side.
Windows: from data availability to 2022-12-31; train <=2014, valid 2015-22. Timing value = net minus
exposure-matched holdings (held weight x asset mean daily return in the same period). Never reads 2023+."""
import datetime as dt, os, re, sys
import numpy as np, pandas as pd, requests
import holidays
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
EXP = "348_mof_weekly_flows"
URL = "https://www.mof.go.jp/policy/international_policy/reference/itn_transactions_in_securities/week.csv"


def load_mof():
    path = os.path.join(lib.DATA, "mof_week.csv")
    if not os.path.exists(path):
        r = requests.get(URL, headers=lib._HDR, timeout=60)
        r.raise_for_status()
        open(path, "wb").write(r.content)
    lines = open(path, "rb").read().decode("cp932", errors="replace").splitlines()
    import csv
    recs = []
    for row in csv.reader([l for l in lines if l[:4].isdigit()]):
        lab = row[0].replace("．", ".").replace("～", "~").replace("〜", "~").replace(" ", "")
        m = re.match(r"(\d{4})\.(\d+)\.(\d+)~(?:(\d{4})\.)?(\d+)\.(\d+)", lab)
        y, m1, d1, y2, m2, d2 = m.groups()
        y, m1, m2, d2 = int(y), int(m1), int(m2), int(d2)
        end = dt.date(int(y2) if y2 else y + (m2 < m1), m2, d2)
        num = lambda s: float(s.replace(",", "").strip())
        recs.append((pd.Timestamp(end), num(row[6]), num(row[14])))
    df = pd.DataFrame(recs, columns=["end", "bond", "eq"]).set_index("end").sort_index()
    return df[~df.index.duplicated(keep="last")]


def release_date(sat):
    jh = holidays.Japan(years=range(sat.year, sat.year + 2))
    d, k = sat.date(), 0
    while k < 4:
        d += dt.timedelta(days=1)
        closed = d.weekday() >= 5 or d in jh or (d.month == 12 and d.day == 31) or (d.month == 1 and d.day <= 3)
        k += not closed
    return pd.Timestamp(d)


def zsig(x):
    sd = x.shift(1).rolling(52, min_periods=52).std()
    return (x / sd).dropna()


mof = load_mof()
rel = pd.DatetimeIndex([release_date(s) for s in mof.index])
print("weeks", len(mof), mof.index[0].date(), "->", mof.index[mof.index <= END][-1].date(),
      "; release not Thursday:", int((rel.dayofweek != 3).sum()))
zb, ze = zsig(mof.bond), zsig(mof["eq"])
TH = 1.5
sig_b = np.sign(zb) * (zb.abs() > TH)
sig_bl = (zb > TH).astype(float)
sig_e = np.sign(ze) * (ze.abs() > TH)

P = pd.DataFrame({"USDJPY": lib.get("JPY=X").Close, **{s: lib.get(s).aC for s in ["TLT", "EWJ", "DXJ"]}}).loc[:END]
R = P.pct_change(fill_method=None)
# Yahoo JPY=X has occasional bad prints; null any |daily move| > 8% (none plausible outside 2008 crash days ~<5%)
bad = R.USDJPY.abs() > 0.08
print("USDJPY bad-print days nulled:", int(bad.sum()))
R.loc[bad, "USDJPY"] = np.nan
R = R.fillna(0.0)
relmap = pd.Series(rel, index=mof.index)


def to_daily(sig, tdays):
    ny = relmap.loc[sig.index] - pd.Timedelta(days=1)          # NY calendar date of the release (evening)
    i = tdays.searchsorted(ny.values)
    ok = i < len(tdays)
    s = pd.Series(sig.values[ok], index=tdays[i[ok]])
    s = s[~s.index.duplicated(keep="last")]
    return s.reindex(tdays).ffill().fillna(0.0)


runs = [("a USDJPY ~ z(resid net foreign LT bonds) +-1.5", sig_b, "USDJPY", 2.0),
        ("b TLT long ~ z(resid net foreign LT bonds) > 1.5", sig_bl, "TLT", 1.0),
        ("c EWJ ~ z(nonresid net JP equity) +-1.5", sig_e, "EWJ", 2.0),
        ("d DXJ ~ z(nonresid net JP equity) +-1.5", sig_e, "DXJ", 2.0)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s)) if len(s) > 2 and s.std() > 0 else np.nan
out = []
for name, sig, a, cb in runs:
    tdays = P[a].dropna().index
    dpos = to_daily(sig, tdays)
    W = pd.DataFrame({a: dpos})
    Ra = R.loc[tdays, [a]]
    start = max(tdays[0] + pd.Timedelta(days=5), dpos[dpos != 0].index.min())
    net = lib.backtest_weights(W, Ra, cb, lag=1).loc[start:END]
    r = lib.register(EXP, f"348{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes=f"cost {cb}bp/side in net; MOF current history; entry close after release (lag=1)")
    held = W.reindex(Ra.index).fillna(0).shift(2).fillna(0).loc[net.index][a]
    blk = (held != held.shift()).cumsum()
    first = held.groupby(blk).first()
    trades = net.groupby(blk).sum()[first != 0]
    tdate = held.groupby(blk).apply(lambda x: x.index[0])[trades.index]
    trades.index = pd.DatetimeIndex(tdate.values)
    tv = pd.concat([net.loc[p0:p1] - held.loc[p0:p1] * Ra[a].loc[p0:p1].mean()
                    for p0, p1 in ((start, "2014-12-31"), ("2015-01-01", END))])
    on = held != 0
    tr_tr, tr_va = trades.loc[:"2014"], trades.loc["2015":END]
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"\n348{name}  start {start.date()}  gate={lib.passes_gate(r)}")
    print(f"   {lib.fmt(r)}")
    print(f"   trade blocks train {len(tr_tr)} valid {len(tr_va)};  net bp/trade {1e4*tr_tr.mean():+.1f} / "
          f"{1e4*tr_va.mean():+.1f};  per-trade t {tt(tr_tr):+.2f} / {tt(tr_va):+.2f}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}   timing bp/held-day "
          f"{1e4*tv.loc[:'2014'][on.loc[:'2014']].mean():+.2f} / {1e4*tv.loc['2015':][on.loc['2015':]].mean():+.2f}"
          f"   long/short held-days {int((held>0).sum())}/{int((held<0).sum())}")
    print("   yearly bp:", yr.to_dict())
    out.append(dict(variant=name, **{k: r[k] for k in ["train_t", "valid_t", "valid_sharpe", "valid_bp_per_trade"]},
                    train_trades=len(tr_tr), valid_trades=len(tr_va),
                    train_bp_trade=1e4 * tr_tr.mean(), valid_bp_trade=1e4 * tr_va.mean(),
                    tv_train_t=tt(tv.loc[:'2014']), tv_valid_t=tt(tv.loc['2015':]), gate=lib.passes_gate(r)))

# descriptive: flow persistence (weekly autocorrelation), <= 2022
m = mof.loc[:END]
print("\nflow AR(1) train/valid: bond", round(m.bond.loc[:"2014"].autocorr(), 3), round(m.bond.loc["2015":].autocorr(), 3),
      " eq", round(m["eq"].loc[:"2014"].autocorr(), 3), round(m["eq"].loc["2015":].autocorr(), 3))
pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "348_summary.csv"), index=False)
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
