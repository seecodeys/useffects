"""366 - SPDR ETF primary-market flows (shares outstanding) -> SPY and sector SPDRs (data_sources_new.md item 1, H1a/H1b).
Mechanism: creations/redemptions are AP trades against the underlying basket. If ETF flows are uninformed liquidity
demand they leave temporary price pressure that reverses (Ben-David/Franzoni/Moussawi); if informed, they continue.

Data: State Street public NAV-history XLSX (ssga.com .../navhist-us-en-{ticker}.xlsx; plain public download, no login,
nothing bypassed): Date, NAV, Shares Outstanding, daily from 2006-05-31. Cached in data/ssga/ (git-ignored; State Street
content, never committed). Tickers: SPY and the 9 original Select Sector SPDRs XLK XLF XLE XLV XLY XLP XLI XLB XLU
(XLRE/XLC are too young). Prices: Yahoo adjusted closes (cc returns).

TIMING ASSUMPTION (fixed before results): the shares-outstanding value dated t reflects creation/redemption orders booked
with ~1-day lag, and the file is published with a further lag (fetched Sat 2026-11-21 its last row is Thu 11-19). So the
value dated t is treated as known only by the close of t+2. All signals are shifted 2 sessions and then run through
lib.backtest_weights(..., lag=1): a flow dated t first affects the position from the close of t+3 (one full session after
it is known). Flow f_t = log(S_t / S_{t-1}); |f| > 40% days (splits / data errors) are set to 0 before anything else.

Pre-specified variants (4, fixed before results; no tuning after):
  A  SPY 1-day flow extremes, reversal: e=+1 if f <= expanding q05, -1 if f >= expanding q95 (min 500 obs, thresholds
     through the signal day only); w = mean(e_{t-k}, k=0..4) -> each event held 5 sessions. 0.5bp/side.
  B  Sector 5-day flow cross-sectional reversal (H1a): F5_i = sum of last 5 f_i; long the sector with the lowest F5,
     short the highest (weights +1/-1), 5 overlapping daily tranches (each 1/5). 2bp/side.
  C  Sector 20-day flow cross-sectional reversal: rank on F20_i; long bottom 3 / short top 3 at 1/3 each, 20 overlapping
     tranches (each 1/20). 2bp/side.
  D  Sector idiosyncratic 1-day flow shocks, reversal vs SPY: for each sector, f_i >= its expanding q98 -> short sector,
     long SPY; f_i <= expanding q02 -> long sector, short SPY; each event held 5 sessions (1/5 tranches); sector legs
     2bp/side, SPY leg 0.5bp/side.
Registered as pos = gross exposure held, ret = net pnl / gross (cost inside, cost_bp=0), so bp_per_trade = net bp per
unit-day of gross exposure. Timing value = net - (period-mean weight) x return per leg (exposure-matched static holding).
Nothing from 2023+ is printed; registry scores train/valid only.
"""
import os, sys
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "366_spdr_shares_flow"
URL = "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/navhist-us-en-{t}.xlsx"
DDIR = os.path.join(lib.DATA, "ssga")
SECT = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU"]
PUB = 2          # value dated t known by close of t+2


def shares(t):
    os.makedirs(DDIR, exist_ok=True)
    p = os.path.join(DDIR, f"navhist-{t.lower()}.xlsx")
    if not os.path.exists(p):
        r = requests.get(URL.format(t=t.lower()), headers=lib._HDR, timeout=60, allow_redirects=True)
        r.raise_for_status()
        open(p, "wb").write(r.content)
    d = pd.read_excel(p, skiprows=3)
    d["Date"] = pd.to_datetime(d["Date"].astype(str).str.strip(), format="%d-%b-%Y", errors="coerce")
    s = pd.to_numeric(d["Shares Outstanding"], errors="coerce")
    s = pd.Series(s.values, index=d["Date"]).dropna()
    s = s[s > 0]
    return s[~s.index.duplicated(keep="last")].sort_index()


def tstat(x):
    x = x.dropna()
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


px = {t: lib.get(t) for t in ["SPY"] + SECT}
idx = px["SPY"].index
idx = idx[idx >= "2006-05-31"]
R = pd.DataFrame({t: px[t]["cc"].reindex(idx) for t in px}).fillna(0.0)
S = pd.DataFrame({t: shares(t) for t in px}).reindex(idx).ffill()
F = np.log(S / S.shift(1))
F = F.where(F.abs() <= 0.4, 0.0).fillna(0.0)

# ---- descriptive timing check (<=2022): which day's return lines up with the flow dated t?
print("corr(f_SPY dated t, SPY cc return at t+k), <=2022:",
      " ".join(f"k={k:+d}:{F['SPY'].loc[:'2022'].corr(R['SPY'].shift(-k).loc[:'2022']):+.3f}" for k in range(-3, 3)))
print("first data", S["SPY"].first_valid_index().date(), "| zero-flow share SPY",
      f"{(F['SPY'].loc[:'2022'] == 0).mean():.2f}")

Fk = F.shift(PUB)          # flows as known at each close (published lag)


def exp_q(x, q):
    return x.expanding(500).quantile(q)


# A
f = Fk["SPY"]
e = pd.Series(0.0, index=idx)
e[f <= exp_q(f, 0.05)] = 1.0
e[f >= exp_q(f, 0.95)] = -1.0
e[exp_q(f, 0.95).isna()] = 0.0
wA = sum(e.shift(k).fillna(0.0) for k in range(5)) / 5

# B / C
def xs(Fsum, n):
    rk = Fsum[SECT].rank(axis=1, method="first")
    W = pd.DataFrame(0.0, index=idx, columns=SECT)
    W[rk <= n] = 1.0 / n
    W[rk > len(SECT) - n] = -1.0 / n
    W[Fsum[SECT].isna().any(axis=1)] = 0.0
    return W

F5 = Fk[SECT].rolling(5).sum()
F20 = Fk[SECT].rolling(20).sum()
sB = xs(F5, 1); sB.iloc[:30] = 0.0
sC = xs(F20, 3); sC.iloc[:30] = 0.0
WB = sum(sB.shift(k).fillna(0.0) for k in range(5)) / 5
WC = sum(sC.shift(k).fillna(0.0) for k in range(20)) / 20

# D
eD = pd.DataFrame(0.0, index=idx, columns=SECT)
for t in SECT:
    hi, lo = exp_q(Fk[t], 0.98), exp_q(Fk[t], 0.02)
    eD.loc[Fk[t] >= hi, t] = -1.0
    eD.loc[Fk[t] <= lo, t] = 1.0
    eD.loc[hi.isna(), t] = 0.0
WD = sum(eD.shift(k).fillna(0.0) for k in range(5)) / 5
WD_spy = -WD.sum(axis=1)

VARS = {
    "A": ("A SPY 1d flow q05/q95 reversal 5d lag1", [(wA.to_frame("SPY"), 0.5)], (e != 0), 5),
    "B": ("B sector 5d flow XS reversal top1/bot1 5d lag1", [(WB, 2.0)], None, None),
    "C": ("C sector 20d flow XS reversal top3/bot3 20d lag1", [(WC, 2.0)], None, None),
    "D": ("D sector 1d flow q02/q98 reversal vs SPY 5d lag1", [(WD, 2.0), (WD_spy.to_frame("SPY"), 0.5)],
          (eD != 0).sum(axis=1), 5),
}

rows = []
for k, (name, legs, ev, hold) in VARS.items():
    net = sum(lib.backtest_weights(W, R[W.columns], c, lag=1) for W, c in legs).loc[:"2022"]
    held = [W.reindex(idx).shift(2).fillna(0.0).loc[:"2022"] for W, c in legs]
    g = sum(h.abs().sum(axis=1) for h in held)
    ret = (net / g).where(g > 0, 0.0)
    row = lib.register(EXP, name, g, ret, 0.0, notes=f"SSGA shares out, flow known t+{PUB}, lag1; ret=net/gross")
    tv = pd.Series(0.0, index=net.index)
    for a_, b_ in [lib.TRAIN, lib.VALID]:
        static = sum((h.loc[a_:b_].mean() * R[h.columns].loc[a_:b_]).sum(axis=1) for h in held)
        tv.loc[a_:b_] = net.loc[a_:b_] - static
    yr = net.groupby(net.index.year).sum() * 1e4
    if ev is not None:
        ev = ev.astype(float)
        nev = lambda a_, b_: int(ev.loc[a_:b_].sum())
        nbp = lambda a_, b_: 1e4 * hold * net.loc[a_:b_].sum() / max(nev(a_, b_), 1)
        evs = (f"events tr/va {nev(*lib.TRAIN)}/{nev(*lib.VALID)}  net bp/event tr {nbp(*lib.TRAIN):+.1f} "
               f"va {nbp(*lib.VALID):+.1f}")
        row.update(ev_tr=nev(*lib.TRAIN), ev_va=nev(*lib.VALID), nbp_tr=nbp(*lib.TRAIN), nbp_va=nbp(*lib.VALID))
    else:
        evs = "daily rebalanced (every session is a signal day)"
    print(f"\n{name}\n   {evs}\n   {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    print(f"   timing-value t tr {tstat(tv.loc[:'2014']):+.2f} va {tstat(tv.loc['2015':'2022']):+.2f}")
    print("   yearly net bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items() if 2006 <= y <= 2022))
    row.update(tv_t_tr=tstat(tv.loc[:'2014']), tv_t_va=tstat(tv.loc['2015':'2022']), gate=lib.passes_gate(row))
    rows.append(row)

pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "366_summary.csv"), index=False)
glob = os.path.join(lib.HERE, "results", "registry.csv")
N = len(pd.read_csv(glob)) if os.path.exists(glob) else lib.n_tests()
print(f"\nglobal registry N={N}: luck threshold sqrt(2 ln N) = {np.sqrt(2 * np.log(N)):.2f}")
