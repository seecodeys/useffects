"""345 - Commercial-bank securities buying (Fed H.8) -> next-week Treasury / MBS returns (idea card P10;
Drechsler, Savov & Schnabl 2021). Mechanism: banks are large, price-insensitive duration buyers funded by deposits;
weeks of heavy bank buying/selling of Treasuries+agencies are demand flows that may persist into the next week.

Data: FRED H.8 weekly, seasonally adjusted, all commercial banks, Wednesday-dated levels:
  TASACBW027SBOG  Treasury and agency securities (1973+)
  TMBACBW027SBOG  of which MBS (2009-07+)
LIMITATION: FRED serves the CURRENT revised history, not first-release vintages. ALFRED vintages were not reachable
without an API key (alfredgraph.csv 404; fredgraph.csv ignores vintage_date). Revisions (annual seasonal-factor
re-estimation, benchmark revisions, reclassifications) therefore leak a little future information into the signal,
and H.8 levels are not break-adjusted (thrift conversions / nonbank structure activity create level jumps).
Publication lag: week ending Wednesday w is released Friday w+9d at ~16:15 ET (after the close). The signal is
assigned to the first trading day >= that Friday and traded with lib.backtest_weights(lag=1), i.e. position from the
close of the next trading day (normally Monday close), held until the next weekly signal replaces it (~5 days).

Signal: d_w = log-change of the level (1 week, or 4 weeks for C); z_w = (d_w - mean) / std, mean/std over the PRIOR
52 weekly observations of d (current week excluded). Position = +1 if z > +1, -1 if z < -1, else 0.
Pre-specified variants (max 4, no tuning after):
  A IEF,  z of 1-week change in Treasury+agency securities. 1bp/side.
  B MBB,  z of 1-week change in the MBS sub-series (2009-07+; usable ~2010-07+). 1bp/side.
  C IEF,  z of 4-week cumulative change in Treasury+agency securities (std of overlapping 4w changes). 1bp/side.
  D SPY,  A's signal (control, should be ~0). 0.5bp/side.
Windows: from ETF/signal availability to 2022-12-31; train <=2014, valid 2015-22. Timing value = net minus
exposure-matched holdings (held weight x asset mean daily return in the same period). Never reads 2023+."""
import io, os, sys
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
EXP = "345_bank_securities_h8"


def fred(s):
    path = os.path.join(lib.DATA, f"fred_{s}.csv")
    if not os.path.exists(path):
        for a in range(5):
            try:
                r = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s}", headers=lib._HDR, timeout=60)
                r.raise_for_status()
                break
            except requests.RequestException:
                if a == 4:
                    raise
                import time; time.sleep(3 * (a + 1))
        open(path, "w").write(r.text)
    df = pd.read_csv(path)
    return pd.Series(pd.to_numeric(df.iloc[:, 1], errors="coerce").values,
                     index=pd.to_datetime(df.iloc[:, 0])).dropna()


def zsig(level, k):
    d = np.log(level).diff(k)
    mu = d.shift(1).rolling(52, min_periods=52).mean()
    sd = d.shift(1).rolling(52, min_periods=52).std()
    z = ((d - mu) / sd).dropna()
    return np.sign(z) * (z.abs() > 1)


P = pd.DataFrame({s: lib.get(s).aC for s in ["IEF", "MBB", "SPY"]}).loc[:END]
R = P.pct_change()
tdays = P.index


def to_daily(pos_w):
    """Wednesday-dated weekly positions -> value on first trading day >= release Friday (w+9d), ffilled."""
    rel = pos_w.index + pd.Timedelta(days=9)
    i = tdays.searchsorted(rel)
    ok = i < len(tdays)
    s = pd.Series(pos_w.values[ok], index=tdays[i[ok]])
    s = s[~s.index.duplicated(keep="last")]
    return s.reindex(tdays).ffill().fillna(0.0)


TA, MB = fred("TASACBW027SBOG"), fred("TMBACBW027SBOG")
sA, sB, sC = zsig(TA, 1), zsig(MB, 1), zsig(TA, 4)

runs = [("A IEF ~ z(1w dTreasury+agency) +-1", sA, "IEF", 1.0),
        ("B MBB ~ z(1w dMBS) +-1", sB, "MBB", 1.0),
        ("C IEF ~ z(4w dTreasury+agency) +-1", sC, "IEF", 1.0),
        ("D SPY ~ A signal (control)", sA, "SPY", 0.5)]
tt = lambda s: s.mean() / s.std() * np.sqrt(len(s)) if len(s) > 2 and s.std() > 0 else np.nan
out = []
for name, sig, a, cb in runs:
    dpos = to_daily(sig)
    W = pd.DataFrame({a: dpos})
    Ra = R[[a]]
    start = max(P[a].first_valid_index() + pd.Timedelta(days=5), dpos[dpos != 0].index.min())
    net = lib.backtest_weights(W, Ra, cb, lag=1).loc[start:END]
    r = lib.register(EXP, f"345{name}", pd.Series(1.0, index=net.index), net, 0.0,
                     notes=f"cost {cb}bp/side in net; FRED revised data (no ALFRED)")
    held = W.reindex(Ra.index).fillna(0).shift(2).fillna(0).loc[net.index][a]
    # per-trade (per weekly holding block with nonzero position)
    blk = (held != held.shift()).cumsum()
    trades = net.groupby(blk).sum()[held.groupby(blk).first() != 0]
    tdate = held.groupby(blk).apply(lambda x: x.index[0])[trades.index]
    trades.index = pd.DatetimeIndex(tdate.values)
    tv_parts = []
    for p0, p1 in ((start, "2014-12-31"), ("2015-01-01", END)):
        bench = held.loc[p0:p1] * Ra[a].loc[p0:p1].mean()
        tv_parts.append(net.loc[p0:p1] - bench)
    tv = pd.concat(tv_parts)
    on = held != 0
    tr_tr, tr_va = trades.loc[:"2014"], trades.loc["2015":END]
    yr = net.groupby(net.index.year).sum().mul(1e4).round(0).astype(int)
    print(f"345{name}  start {start.date()}  gate={lib.passes_gate(r)}")
    print(f"   {lib.fmt(r)}")
    print(f"   trades train {len(tr_tr)} (long {int((held.loc[:'2014'].groupby(blk).first()>0).sum())}) "
          f"valid {len(tr_va)};  net bp/trade {1e4*tr_tr.mean():+.1f} / {1e4*tr_va.mean():+.1f};  "
          f"per-trade t {tt(tr_tr):+.2f} / {tt(tr_va):+.2f}")
    print(f"   timing-value t {tt(tv.loc[:'2014']):+.2f} / {tt(tv.loc['2015':]):+.2f}   timing bp/held-day "
          f"{1e4*tv.loc[:'2014'][on.loc[:'2014']].mean():+.2f} / {1e4*tv.loc['2015':][on.loc['2015':]].mean():+.2f}")
    print("   yearly bp:", yr.to_dict())
    out.append(dict(variant=name, **{k: r[k] for k in ["train_t", "valid_t", "valid_sharpe"]},
                    train_trades=len(tr_tr), valid_trades=len(tr_va),
                    train_bp_trade=1e4 * tr_tr.mean(), valid_bp_trade=1e4 * tr_va.mean(),
                    tv_train_t=tt(tv.loc[:'2014']), tv_valid_t=tt(tv.loc['2015':]), gate=lib.passes_gate(r)))
pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "345_summary.csv"), index=False)
print("registry size:", lib.n_tests(), " luck t for best of N ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
