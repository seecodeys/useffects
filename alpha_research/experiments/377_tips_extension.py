"""377 (idea E4) - TIPS-specific month-end extension: does the TIPS-vs-nominal spread / 10y breakeven widen over the
last 3 sessions (W3) of the month, and more so in months when a new/reopened TIPS issue settles?

Data / timing
- TIPS issue calendar: fiscaldata auctions_query, filter inflation_index_security=Yes (the API has no 'TIPS Note'
  security_type; TIPS are security_type 'Note'/'Bond' with inflation_index_security='Yes'). A month is a "TIPS month"
  when a TIPS issue_date falls in it AND its announcement date is before the first W3 holding session (point-in-time).
- FRED T10YIE (= DGS10 - DFII10, H.15 end-of-day yields, published the next business day ~4:15pm). The breakeven is
  only MEASURED over the window (outcome), never used as a signal; the signal is the pre-announced calendar, so the
  FRED release lag does not create look-ahead. Breakeven pseudo-return = 8 (duration) x daily change in T10YIE / 100,
  i.e. a duration-8 long-TIPS / short-nominal position.
- TIP, IEF Yahoo adjusted close->close. h = static OLS beta of TIP on IEF daily cc over TRAIN 2004-2014 only.
- Holding: last 3 sessions of month (returns of sessions with 3,2,1 sessions left), weights via backtest_weights lag=1
  (weights set two sessions before each held return). Costs 1bp/side per leg (2 legs -> spread turnover x 1bp).
Pre-specified variants (<=4; card V4 'DFII10 decline minus DGS10 decline' is identical to T10YIE change, so replaced
by the unconditional tradable spread, which is also the exposure-matched comparison for V3):
  V1  long breakeven (8 x dT10YIE) over W3, all months.            Prediction: positive.
  V2  same, TIPS months only (plus descriptive TIPS vs non-TIPS difference-of-means t).
  V3  TIP - h*IEF over W3, TIPS months only.                       Prediction: positive, > V4.
  V4  TIP - h*IEF over W3, all months.
TRAIN 2003/04-2014, VALID 2015-2022; nothing 2023+ printed. Never holdout_eval.
"""
import os, sys, json
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "377_tips_extension"; C = 1.0; DUR = 8.0


def fred(s):
    p = os.path.join(lib.DATA, f"fred_{s}.csv")
    if not os.path.exists(p):
        open(p, "w").write(requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={s}", timeout=60).text)
    df = pd.read_csv(p, na_values=".")
    df.index = pd.to_datetime(df.iloc[:, 0])
    return pd.to_numeric(df.iloc[:, 1], errors="coerce").dropna()


def tips_calendar():
    p = os.path.join(lib.DATA, "tips_auctions.json")
    if not os.path.exists(p):
        url = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"
               "?fields=security_type,security_term,auction_date,issue_date,announcemt_date,offering_amt,reopening,cusip"
               "&filter=inflation_index_security:eq:Yes&page%5Bsize%5D=10000")
        open(p, "w").write(requests.get(url, timeout=60).text)
    df = pd.DataFrame(json.load(open(p))["data"])
    for c in ["issue_date", "announcemt_date", "auction_date"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


tips = tips_calendar()
D = {s: lib.get(s) for s in ["TIP", "IEF"]}
idx = D["IEF"].index
idx = idx[(idx >= "2003-01-01") & (idx <= "2022-12-31")]
R = pd.DataFrame({s: D[s]["cc"].reindex(idx) for s in ["TIP", "IEF"]})
be = fred("T10YIE").reindex(idx).ffill()
R["BE"] = DUR * be.diff() / 100

ym = pd.Series(idx.to_period("M"), index=idx)
bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
hold = (bwd <= 3)                                   # sessions whose return is held
first_hold = hold & ~hold.shift(1, fill_value=False)
# point-in-time TIPS month flag
flag = pd.Series(False, index=idx)
for M, d0 in zip(ym[first_hold], idx[first_hold]):
    t = tips[(tips.issue_date.dt.to_period("M") == M) & (tips.announcemt_date < d0 - pd.Timedelta(days=2))]
    if len(t):
        flag[ym == M] = True
tm = ym[flag].unique()

tr = R.loc["2004-01-01":"2014-12-31", ["TIP", "IEF"]].dropna()
h = np.cov(tr.TIP, tr.IEF)[0, 1] / tr.IEF.var()
print(f"static train hedge h={h:.3f};  TIPS months 2003-22: {len(tm)} of {ym.nunique()}")

# W at t applies (with lag=1) to return at t+2 -> W = hold.shift(-2)
Wbase = hold.astype(float).shift(-2).fillna(0.0)
Wtm = (hold & flag).astype(float).shift(-2).fillna(0.0)
spec = {
    "V1": ("long BE (8xdT10YIE) W3 all months", pd.DataFrame({"BE": Wbase}), "2003-02-01"),
    "V2": ("long BE (8xdT10YIE) W3 TIPS months", pd.DataFrame({"BE": Wtm}), "2003-02-01"),
    "V3": ("TIP-h*IEF W3 TIPS months", pd.DataFrame({"TIP": Wtm, "IEF": -h * Wtm}), "2004-01-01"),
    "V4": ("TIP-h*IEF W3 all months", pd.DataFrame({"TIP": Wbase, "IEF": -h * Wbase}), "2004-01-01"),
}


def tt(s):
    s = s.dropna(); return s.mean() / s.std() * np.sqrt(len(s)) if len(s) > 2 and s.std() > 0 else np.nan


def per_event(net, g):
    # event = held sessions plus the following session (which carries the exit cost); keyed by the holding month
    on = (g > 0) | (g.shift(1, fill_value=0) > 0)
    key = pd.Series((net.index - pd.Timedelta(days=4)).to_period("M"), index=net.index)  # exit day -> prior month
    key[g > 0] = net.index[g > 0].to_period("M")
    return net[on].groupby(key[on]).sum()


nets = {}
for k, (desc, W, start) in spec.items():
    # BE leg: two legs (TIPS + nominal) -> charge 2x1bp per unit of BE turnover
    cost = 2 * C if "BE" in W.columns else C
    Rk = R[W.columns].fillna(0.0)
    net = lib.backtest_weights(W, Rk, cost, lag=1).loc[start:"2022-12-31"]
    P = W.shift(2).fillna(0.0).reindex(net.index)
    g = P.abs().sum(axis=1)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register(EXP, f"377{k} {desc}", g, ret, 0.0, notes="E4 TIPS month-end extension, 1bp/side/leg, lag=1")
    nets[k] = net
    act = per_event(net, g)
    print(f"377{k} {desc}\n   {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    for nm, a, b in [("train", start[:7], "2014-12-31"), ("valid", "2015-01", "2022-12-31")]:
        s_ = act.loc[a:b]
        print(f"   {nm}: events={len(s_)}  net bp/event={s_.mean()*1e4:+.2f}  t={tt(s_):+.2f}")
    yr = net.groupby(net.index.year).sum() * 1e4
    print("   yearly bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items()))

# descriptive: TIPS vs non-TIPS months (gross W3 sums), difference of means; and exposure-matched baseline
print("\nDescriptive (gross, W3 sums per month, bp):")
for lab, ser, start in [("BE 8xdT10YIE", R.BE, "2003-02"), ("TIP-h*IEF", R.TIP - h * R.IEF, "2004-01")]:
    s = ser.where(hold).groupby(ym).sum().loc[start:"2022-12"] * 1e4
    base = ser.groupby(ym).mean().loc[start:"2022-12"] * 3 * 1e4   # avg 3-session return in same month
    isT = s.index.isin(tm)
    for nm, a, b in [("train", start, "2014-12"), ("valid", "2015-01", "2022-12")]:
        ss, bb, ii = s.loc[a:b], base.loc[a:b], isT[(s.index >= pd.Period(a)) & (s.index <= pd.Period(b))]
        x, y = ss[ii], ss[~ii]
        dtt = (x.mean() - y.mean()) / np.sqrt(x.var() / len(x) + y.var() / len(y)) if len(y) > 2 else np.nan
        tv = ss - bb
        print(f"  {lab:13s} {nm}: TIPS-mo {x.mean():+.2f} (n={len(x)})  other {y.mean():+.2f} (n={len(y)})  "
              f"diff t={dtt:+.2f} | W3 minus avg-3-day timing value {tv.mean():+.2f} (t {tt(tv):+.2f})")
print("registry size:", lib.n_tests())
