"""370 - Treasury TIC monthly foreign flows (data source 3, results/data_sources_new.md).
Data (cached data/tic/, not committed; U.S. government, public domain):
  s1_globl.txt  https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/s1_globl.txt
               gross purchases/sales by foreigners, by country & instrument, 1977-01 .. 2023-01 (S form).
               Net = purchases - sales. Treasuries = col[1]-col[7]; US corporate stocks = col[4]-col[10].
  tressect.txt same site; net purchases of Treasury bonds & notes split into FOREIGN OFFICIAL / OTHER FOREIGNERS
               (private) / IROs, 1978-01 .. 2023-01 (S-form based; the legacy file replaced by SLT table 3).
Only the S-form series is used (consistent concept, all of 2002-2022 covered); SLT not used.
Vintage caveat: files hold the latest (revised) S-form figures, not first-release values.

Release-lag assumption: TIC for month m is released around the 15th-18th of month m+2. Month-m data are first used at
the CLOSE OF THE LAST TRADING DAY OF m+2 (>= ~10 days after release), and backtest_weights(lag=1) puts the position on
from the close of the first trading day of m+3; held until the next monthly decision (one month).

Signal construction (common to all variants): x = 3-month sum of the monthly net flow (months m-2..m);
z = (x - mean of the prior 36 values of x) / sd of the prior 36 values (own history only, no look-ahead). Flows grew
by orders of magnitude since 1978, hence the rolling standardisation. Trigger |z| > 1.

Pre-specified variants (fixed before seeing any results; no tuning after):
  A  Foreign OFFICIAL net Treasury bond/note purchases (tressect): z>+1 -> long IEF, z<-1 -> short IEF, next month.
     (reserve-manager buying/selling as a persistent price-pressure flow; complements dead 341 which used weekly
     H.4.1 custody levels, and 348 Japan MOF weekly flows)
  B  Foreign PRIVATE ("other foreigners") net Treasury purchases (tressect): same rule -> IEF.
  C  Caribbean-centre (S1 'Total Caribbean', code 34401) net Treasury purchases, hedge-fund/basis-trade proxy (H3b):
     z>+1 -> SHORT TLT next month (build-up -> unwind risk); else flat.
  D  Foreign net purchases of US corporate stocks (S1 Grand Total, code 99996) (H3a, foreigners as marginal
     trend-chasing buyers): z>+1 -> long SPY / short EFA, z<-1 -> short SPY / long EFA, next month.
Costs: IEF/TLT 1bp/side; SPY 0.5bp/side, EFA 2bp/side (spread D -> 1.25bp/side average per leg, charged via
per-leg turnover). Registered as pos=gross exposure, ret=net/gross, cost_bp=0 (cost inside net).
Timing value = net - W * period-mean daily return of each asset (exposure-matched static holding removed).
ETF data start 2002-07 (TLT/IEF), so effective train = 2002-08..2014. Nothing from 2023+ is printed.
"""
import os, sys, re
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "370_tic_foreign_flows"
D = os.path.join(lib.DATA, "tic")
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 1 and x.std() > 0 else np.nan
num = lambda s: pd.to_numeric(str(s).replace(",", "").replace('"', "").strip(), errors="coerce")

# ---------------- S1 parse
rows = []
with open(os.path.join(D, "s1_globl.txt"), encoding="latin-1") as f:
    for line in f:
        p = line.rstrip("\n").split("\t")
        if len(p) >= 15 and p[1].strip().isdigit() and re.match(r"\d{4}-\d{2}$", p[2].strip()):
            rows.append([p[1].strip(), p[2].strip()] + [num(v) for v in p[3:15]])
s1 = pd.DataFrame(rows, columns=["code", "ym"] + [f"c{i}" for i in range(1, 13)])
s1["ym"] = pd.PeriodIndex(s1.ym, freq="M")
s1["tsy"] = s1.c1 - s1.c7
s1["stk"] = s1.c4 - s1.c10
carib_tsy = s1[s1.code == "34401"].set_index("ym").tsy.sort_index()
world_stk = s1[s1.code == "99996"].set_index("ym").stk.sort_index()

# ---------------- tressect parse
tr = []
with open(os.path.join(D, "tressect.txt"), encoding="latin-1") as f:
    for line in f:
        m = re.match(r"\s*(\d{4}-\d{2})\s+([-\d,]+)\s+([-\d,]+)\s+([-\d,]+)\s+([-\d,]+)\s*$", line)
        if m:
            tr.append([m.group(1)] + [num(m.group(i)) for i in range(2, 6)])
tr = pd.DataFrame(tr, columns=["ym", "tot", "off", "oth", "iro"])
tr = tr.assign(ym=pd.PeriodIndex(tr.ym, freq="M")).set_index("ym").sort_index()

def zsig(s):
    x = s.rolling(3).sum()
    mu, sd = x.shift(1).rolling(36).mean(), x.shift(1).rolling(36).std()
    return (x - mu) / sd

Z = {"A": zsig(tr.off), "B": zsig(tr.oth), "C": zsig(carib_tsy), "D": zsig(world_stk)}
for k, z in Z.items():
    zz = z.loc[:"2022-12"]
    print(f"{k}: months {zz.notna().sum()} from {zz.first_valid_index()}  z>+1 {int((zz>1).sum())}  z<-1 {int((zz<-1).sum())}")

px = {s: lib.get(s) for s in ["SPY", "IEF", "TLT", "EFA"]}
cal = px["SPY"].index
idx = cal[(cal >= "2002-07-31") & (cal <= "2022-12-31")]
dec = pd.Series(cal, index=cal).groupby(cal.to_period("M")).last()     # last trading day per month
R = pd.DataFrame({s: px[s].cc for s in px}).reindex(idx)

def weights(z, rule):
    """rule(z) -> dict asset->weight; month-m data decided at last trading day of m+2"""
    W = pd.DataFrame(np.nan, index=cal, columns=list(px))
    for p, v in z.items():
        if p + 2 in dec.index and np.isfinite(v):
            W.loc[dec[p + 2]] = 0.0
            for a, w in rule(v).items():
                W.loc[dec[p + 2], a] = w
    return W.ffill().fillna(0.0).reindex(idx)

sym = lambda a: (lambda v: {a: 1.0} if v > 1 else ({a: -1.0} if v < -1 else {}))
RULES = {
    "A": (sym("IEF"), "official net UST purchases 3m z>+1 long / <-1 short IEF 1m", 1.0),
    "B": (sym("IEF"), "private net UST purchases 3m z>+1 long / <-1 short IEF 1m", 1.0),
    "C": (lambda v: {"TLT": -1.0} if v > 1 else {}, "Caribbean net UST purchases 3m z>+1 -> short TLT 1m", 1.0),
    "D": (lambda v: {"SPY": 1.0, "EFA": -1.0} if v > 1 else ({"SPY": -1.0, "EFA": 1.0} if v < -1 else {}),
          "foreign net US-stock purchases 3m z>+1 long SPY/short EFA, <-1 reverse, 1m", None),
}
COST = {"SPY": 0.5, "IEF": 1.0, "TLT": 1.0, "EFA": 2.0}
out = []
for k, (rule, desc, _) in RULES.items():
    W = weights(Z[k], rule)
    Wh = W.shift(2).fillna(0.0)                                   # held weights (shift 1 + lag 1)
    net = sum(lib.backtest_weights(W[[a]], R[[a]], COST[a], lag=1) for a in px)
    gr = Wh.abs().sum(axis=1)
    if k == "D":
        gr = gr / 2                                               # one unit of spread = 1 long + 1 short leg
    ret = (net / gr).where(gr > 0, 0.0)
    r = lib.register(EXP, f"370{k} {desc} lag1", gr, ret, 0.0,
                     notes="TIC S-form; month m used at close of last trading day of m+2; lag1; costs IEF/TLT 1bp, SPY 0.5, EFA 2bp/side; ret=net/gross")
    print(f"\n{r['variant']}\n  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    on = gr > 0
    res = {"variant": k, **{x: r[x] for x in ["train_t", "train_sharpe", "valid_t", "valid_sharpe", "valid_bp_per_trade", "valid_traded_%"]},
           "gate": lib.passes_gate(r)}
    for nm, (a, b) in lib.PERIODS.items():
        na, Wa, Ra = net.loc[a:b], Wh.loc[a:b], R.loc[a:b]
        tv = na - (Wa * Ra.mean()).sum(axis=1)
        mo = on.loc[a:b].groupby(na.index.to_period("M")).any()
        mnet = na.groupby(na.index.to_period("M")).sum()[mo]
        print(f"  {nm}: signal months {int(mo.sum())} (long {int((Wa.sum(axis=1).groupby(na.index.to_period('M')).last()>0).sum()) if k!='D' else '-'})  "
              f"net bp/month {1e4*mnet.mean() if len(mnet) else np.nan:+.1f}  monthly t {tt(mnet):+.2f}  "
              f"timing-value daily t {tt(tv[on.loc[a:b]]):+.2f}  hit {100*(mnet>0).mean() if len(mnet) else np.nan:.0f}%")
        res[f"{nm}_months"], res[f"{nm}_bp_month"], res[f"{nm}_tv_t"] = int(mo.sum()), 1e4 * mnet.mean() if len(mnet) else np.nan, tt(tv[on.loc[a:b]])
    yr = net[on].groupby(net[on].index.year).sum() * 1e4
    print("  per-year net bp:", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))
    out.append(res)
pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "370_summary.csv"), index=False)
print("\nregistry size (this agent):", lib.n_tests())
