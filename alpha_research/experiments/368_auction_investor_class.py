"""368 - Treasury auction allotments by investor class (non-dealer classes) -> IEF / TLT / SHY.

Data (data_sources_new.md item 4): home.treasury.gov investor-class auction allotments, coupon files
  current  : /system//files/276/November-9_2026-IC-Coupons.xls (issue dates 2009-10 -> present; $bn)
  history  : /system/files/276/Website-PDO-4-A-Coupons-Jan-2000-Sep%202009.xls (2000-01 -> 2009-09; $mn)
  (file name of the current file scraped from https://home.treasury.gov/data/investor-class-auction-allotments,
  cached in data/ic368/). Rows are keyed by ISSUE date. Nominal 10-Year Notes -> IEF, 30-Year Bonds -> TLT
  (TIPS, FRN, 20y excluded). Only rows with issue date <= 2022-12-31 are used.

PUBLICATION TIMING (conservative, no real-time vintages exist):
  * The page today says "Data released at 3:00 PM" on ~2 dates per month (current file includes issues up to ~1 week
    before the file date). Early online releases (2009-10+) were roughly monthly. Assumption: an auction issued in
    month m is public by the LAST calendar day of month m+1.
  * Before Oct-2009 the table was only published as PDO-4 in the QUARTERLY Treasury Bulletin. Assumption: public by the
    last day of the 3rd month after the end of the issue quarter (e.g. Feb-2005 issue -> 2005-06-30).
  * Signal decided at the close of the first trading day >= that availability date; lib.backtest_weights(lag=1), so the
    position starts the following close. This is therefore a SLOW signal (2-6 months after the auction), not an
    auction-day one: entering "in the days after the auction" is impossible with this publication schedule.

HOW THIS AVOIDS DUPLICATING 331 (dealer takedown, fiscaldata bidder stats, auction-day entry):
  every share here is computed over END-INVESTOR allotments only, denominator = total - SOMA - dealers&brokers, so the
  dealer takedown is removed by construction (the signal is the COMPOSITION of non-dealer demand: investment funds,
  foreign & international, pension/insurance). Dealer share is not used anywhere. The correlation of each surprise with
  the dealer-share surprise is printed as a diagnostic.

Definitions: fund = investment funds / EI, foreign = foreign & intl / EI, realmoney = (funds + pension&ins) / EI,
  EI = total - SOMA - dealers. Per-auction surprise z = (x - mean of same tenor's previous 8 auctions) / std of same 8.

PRE-SPECIFIED VARIANTS (written before any result was seen; no tuning afterwards). Costs 1bp/side, turnover based.
  A  fund-share surprise per auction: z > +1 -> long that tenor's ETF (strong real-money demand), z < -1 -> short;
     enter at availability (lag 1), hold 10 trading days. Multiple auctions of a tenor released together: mean sign.
  B  same as A with the foreign share (z > +1 long, z < -1 short).
  C  H4a regime, monthly: realmoney share of 10y+30y allotments (EI-weighted) over the trailing 3 issue-months
     available, minus its trailing 24-issue-month average. Low (< 0) -> TLT underperforms IEF: +1 IEF / -0.45 TLT
     (approx DV01-neutral); high (>= 0) -> -1 IEF / +0.45 TLT. Rebalanced each availability month-end (always in).
  D  H4b: foreign share of ALL nominal coupon allotments (EI-weighted, by issue month) fell 3 issue-months in a row
     (latest available month) -> -1 TLT / +1 SHY until the next availability date; flat otherwise.
Train 2002-07 (ETF inception) -> 2014, valid 2015-22. Timing value = net P&L - held weight x the asset's mean daily cc
return in the same period (removes drift / exposure; exposure-matched). Never calls lib.holdout_eval; nothing 2023+.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "368_auction_investor_class"
COST = 1.0
END = "2022-12-31"
D = os.path.join(lib.DATA, "ic368")
COLS = ["issue", "type", "rate", "cusip", "mat", "total", "soma", "dep", "indiv", "dealer", "pension", "funds",
        "foreign", "other"]

cur = pd.read_excel(os.path.join(D, "ic_coupons.xls"), header=None, skiprows=5)
cur = cur.iloc[:, :14]; cur.columns = COLS
cur["issue"] = pd.to_datetime(cur.issue, errors="coerce"); cur = cur.dropna(subset=["issue"])
old = pd.read_excel(os.path.join(D, "pdo4_2000_2009.xls"), header=None, skiprows=4)
old = old.iloc[:, :15]; old["issue"] = pd.to_datetime(old[0], errors="coerce"); old = old.dropna(subset=["issue"])
o = pd.DataFrame({"issue": old.issue, "type": old[1].astype(str).str.replace(" ", "") + "|" + old[2].astype(str)})
for j, c in enumerate(COLS[5:]):
    o[c] = pd.to_numeric(old[6 + j], errors="coerce") / 1000.0
cur_t = cur.type.astype(str).str.strip()
cur["kind"] = np.where(cur_t.str.contains("TIPS|FRN"), "x", np.where(cur_t.eq("10-Year Note"), "10",
                       np.where(cur_t.eq("30-Year Bond"), "30", "nom")))
o["kind"] = np.where(o.type.str.contains("TIPS"), "x", np.where(o.type.eq("NOTE|10 YR"), "10",
                     np.where(o.type.eq("BOND|30 YR"), "30", "nom")))
a = pd.concat([o[o.issue < "2009-10-01"], cur[cur.issue >= "2009-10-01"]], ignore_index=True)
for c in COLS[5:]:
    a[c] = pd.to_numeric(a[c], errors="coerce").fillna(0.0)
a = a[(a.kind != "x") & (a.issue <= END)].sort_values("issue").reset_index(drop=True)
a["ei"] = a.total - a.soma - a.dealer
a["fund"] = a.funds / a.ei
a["foreign_s"] = a.foreign / a.ei
a["realmoney"] = (a.funds + a.pension) / a.ei
a["dealer_s"] = a.dealer / (a.total - a.soma)


def avail(d):
    if d >= pd.Timestamp("2009-10-01"):
        return (d + pd.offsets.MonthEnd(0)) + pd.offsets.MonthEnd(1)
    qe = d + pd.offsets.QuarterEnd(0)
    return qe + pd.offsets.MonthEnd(3)


a["avail"] = a.issue.map(avail)

px = {s: lib.get(s) for s in ["IEF", "TLT", "SHY"]}
R = pd.DataFrame({s: px[s].cc for s in px}).loc[:END]
idx = R.index


def tday(d):
    i = idx.searchsorted(d)
    return idx[i] if i < len(idx) else None


a["dec"] = a.avail.map(tday)

# ---------------------------------------------------------------- per-auction surprises (A, B)
t = a[a.kind.isin(["10", "30"])].copy()
t["etf"] = t.kind.map({"10": "IEF", "30": "TLT"})
for c in ["fund", "foreign_s", "dealer_s"]:
    g = t.groupby("etf")[c]
    m = g.transform(lambda s: s.shift(1).rolling(8, min_periods=8).mean())
    sd = g.transform(lambda s: s.shift(1).rolling(8, min_periods=8).std())
    t[c + "_z"] = (t[c] - m) / sd
print("auctions 10y/30y:", len(t), " corr of z with dealer-share z:",
      {c: round(t[[c + "_z", "dealer_s_z"]].corr().iloc[0, 1], 2) for c in ["fund", "foreign_s"]})


def event_weights(col, hold=10):
    W = pd.DataFrame(0.0, index=idx, columns=R.columns)
    ev = t.dropna(subset=[col, "dec"]).copy()
    ev["s"] = np.where(ev[col] > 1, 1.0, np.where(ev[col] < -1, -1.0, 0.0))
    grp = ev.groupby(["dec", "etf"]).s.mean().apply(np.sign)
    rows = []
    for (d, etf), s in grp.items():
        if s == 0:
            continue
        i = idx.get_loc(d)
        W.iloc[i:i + hold, W.columns.get_loc(etf)] = s
        rows.append((d, etf, s))
    return W, pd.DataFrame(rows, columns=["dec", "etf", "sign"])


# ---------------------------------------------------------------- monthly regimes (C, D)
a["im"] = a.issue.dt.to_period("M")
a["avm"] = a.avail
mon_av = a.groupby("im").avail.max()


def monthly_share(sub, num):
    g = sub.groupby("im")
    return g[num].sum() / g.ei.sum()


lt = a[a.kind.isin(["10", "30"])].assign(rm=lambda x: x.funds + x.pension)
rm = monthly_share(lt, "rm")                      # issue-month series
fx = monthly_share(a, "foreign")                  # all nominal coupons
rm_full = rm.reindex(pd.period_range(rm.index.min(), rm.index.max(), freq="M"))
fx_full = fx.reindex(pd.period_range(fx.index.min(), fx.index.max(), freq="M"))


def regime_at(d):
    """Latest issue-months whose availability <= d."""
    known = mon_av[mon_av <= d].index
    return known.max() if len(known) else None


dec_days = sorted({tday(x) for x in mon_av.values if tday(x) is not None})
WC = pd.DataFrame(np.nan, index=idx, columns=R.columns)
WD = pd.DataFrame(np.nan, index=idx, columns=R.columns)
logC, logD = [], []
for d in dec_days:
    last = regime_at(d)
    if last is None:
        continue
    s = rm_full.loc[:last]
    tr3 = s.iloc[-3:].mean()
    base = s.iloc[-24:].mean() if s.dropna().shape[0] >= 24 else np.nan
    if np.isfinite(tr3) and np.isfinite(base):
        lo = tr3 < base
        WC.loc[d] = [1.0, -0.45, 0.0] if lo else [-1.0, 0.45, 0.0]
        logC.append((d, last, tr3 - base))
    f = fx_full.loc[:last].iloc[-4:]
    fall = f.notna().all() and len(f) == 4 and (np.diff(f.values) < 0).all()
    WD.loc[d] = [0.0, -1.0, 1.0] if fall else [0.0, 0.0, 0.0]
    logD.append((d, last, bool(fall)))
WC = WC[["IEF", "TLT", "SHY"]].ffill().fillna(0.0)
WD = WD[["IEF", "TLT", "SHY"]].ffill().fillna(0.0)

WA, evA = event_weights("fund_z")
WB, evB = event_weights("foreign_s_z")
variants = {
    "A_fundshare_z1_10d": (WA, evA),
    "B_foreignshare_z1_10d": (WB, evB),
    "C_realmoney_regime_IEFvsTLT": (WC, pd.DataFrame(logC, columns=["dec", "last_im", "dev"])),
    "D_foreign_fall3_shortTLT_SHY": (WD, pd.DataFrame(logD, columns=["dec", "last_im", "fall"])),
}

PER = {"train": ("2002-07-01", "2014-12-31"), "valid": ("2015-01-01", END)}
out = []
for name, (W, ev) in variants.items():
    net = lib.backtest_weights(W, R, COST, lag=1)
    Wh = W.reindex(R.index).fillna(0).shift(2).fillna(0)            # weights actually held (lag 1)
    tv = {}
    for p, (s, e) in PER.items():
        mu = R.loc[s:e].mean()
        tim = net.loc[s:e] - (Wh.loc[s:e] * mu).sum(axis=1)
        tv[p] = tim.mean() / tim.std() * np.sqrt(len(tim))
    # register: pos=1 on days with any exposure, ret = net/exposure-free -> use net daily series directly
    held = (Wh.abs().sum(axis=1) > 0).astype(float)
    gross_ret = net.copy()
    row = lib.register(EXP, name, held, gross_ret, cost_bp=0.0,
                       notes=f"net series from backtest_weights lag1 cost {COST}bp/side; timing t tr {tv['train']:+.2f} "
                             f"va {tv['valid']:+.2f}")
    nev = {p: (int(((ev.dec >= s) & (ev.dec <= e)).sum()) if "sign" in ev else int(((ev.dec >= s) & (ev.dec <= e)).sum()))
           for p, (s, e) in PER.items()}
    if "sign" in ev:
        nev = {p: f"{int(((ev.dec>=s)&(ev.dec<=e)&(ev.sign>0)).sum())}L/{int(((ev.dec>=s)&(ev.dec<=e)&(ev.sign<0)).sum())}S"
               for p, (s, e) in PER.items()}
    if "fall" in ev:
        nev = {p: int(((ev.dec >= s) & (ev.dec <= e) & ev.fall).sum()) for p, (s, e) in PER.items()}
    ndec = {p: max(1, int(((ev.dec >= s) & (ev.dec <= e) & (ev.get("sign", ev.get("fall", True)) != 0)).sum()))
            for p, (s, e) in PER.items()}
    bpdec = {p: 1e4 * net.loc[s:e].sum() / ndec[p] for p, (s, e) in PER.items()}
    yr = net.loc["2002-07-01":END].groupby(net.loc["2002-07-01":END].index.year).sum() * 1e4
    out.append({"variant": name, "events_tr": nev["train"], "events_va": nev["valid"],
                "train_t": row["train_t"], "valid_t": row["valid_t"], "valid_sharpe": row["valid_sharpe"],
                "valid_bp_per_trade": row["valid_bp_per_trade"], "train_bp_day": row["train_bp_per_trade"],
                "bp_per_dec_tr": bpdec["train"], "bp_per_dec_va": bpdec["valid"], "timing_t_tr": tv["train"], "timing_t_va": tv["valid"], "gate": lib.passes_gate(row),
                "worst_yr_bp": yr.min(), "best_yr": int(yr.idxmax()), "best_yr_bp": yr.max()})
res = pd.DataFrame(out)
pd.set_option("display.width", 250)
print(res.round(2).to_string(index=False))
res.to_csv(os.path.join(lib.HERE, "results", "368_summary.csv"), index=False)
print("registry tests (this file):", lib.n_tests())
