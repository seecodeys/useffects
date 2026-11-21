"""359 - GLD bullion-tonnage flows and gold over the following days (idea card X8, results/ideas_crossasset.md).
Mechanism: GLD creations force APs to buy London bullion, redemptions to sell it; fund-flow programmes persist for days,
so a big multi-day change in tonnes held might predict continuation (or, if it is temporary price pressure, reversal).

Data: SPDR Gold Shares public historical archive XLSX (linked from spdrgoldshares.com/usa/gld/ ->
api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en; plain public download, no login,
no restriction bypassed). Column "Tonnes of Gold", daily 2004-11-18+. Cached in data/ (git-ignored; the file says
redistribution is prohibited, so it is never committed). Prices: Yahoo adjusted GLD, GDX (GDX 2006-05+).
Timing: tonnes for day t are published after the close of t, so every weight decided "at t" from T_t is implemented
with lib.backtest_weights(..., lag=1): position from close t+1. Costs 1bp/side GLD, 2bp/side GDX (turnover-based).

Signal: s_t = T_t / T_{t-5} - 1 (5 trading-day % change in tonnes). Deciles from an EXPANDING window of s through t
(min 500 obs, so first signals ~late 2006). d_t = +1 if s_t >= expanding q90, -1 if s_t <= expanding q10, else 0.
Pre-specified variants (4, fixed before results; no tuning after):
  A  GLD continuation: w_t = mean(d_{t-k}, k=0..4)  -> each event held 5 sessions from close t+1 (5 overlapping tranches)
  B  GDX minus GLD on the same w (do miners lag the bullion flow?): +w GDX, -w GLD; GDX leg 2bp, GLD leg 1bp
  C  GLD reversal for sessions 6-20: w_t = -mean(d_{t-k}, k=5..19)  (15 tranches, each 1/15)
  D  1-day: |T_t/T_{t-1}-1| > 1% -> sign of the change, GLD next session (from close t+1 to close t+2)
Registered as pos = gross lagged exposure, ret = net pnl / exposure (cost inside, cost_bp=0), so bp_per_trade = net bp
per unit-day held. Timing value = net - (period-mean lagged weight) * leg return (exposure-matched static holding).
Nothing from 2023+ is printed (descriptive tables stop at 2022-12-31; registry only scores train/valid).
"""
import os, sys
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "359_gld_tonnes_flow"
URL = "https://api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en"
PATH = os.path.join(lib.DATA, "GLD_holdings_archive.xlsx")
CGLD, CGDX = 1.0, 2.0


def holdings():
    if not os.path.exists(PATH):
        r = requests.get(URL, headers=lib._HDR, timeout=60)
        r.raise_for_status()
        open(PATH, "wb").write(r.content)
    h = pd.read_excel(PATH, sheet_name="US GLD Historical Archive")
    h["Date"] = pd.to_datetime(h["Date"].astype(str).str.strip(), format="%d-%b-%Y", errors="coerce")
    h["T"] = pd.to_numeric(h["Tonnes of Gold"], errors="coerce")
    return h.dropna(subset=["Date", "T"]).query("T > 0").set_index("Date")["T"].sort_index()


def tstat(x):
    x = x.dropna()
    return x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan


gld, gdx = lib.get("GLD"), lib.get("GDX")
idx = gld.index
T = holdings()
T = T[~T.index.duplicated(keep="last")].reindex(idx).ffill()
s = T / T.shift(5) - 1
q90 = s.expanding(500).quantile(0.9)
q10 = s.expanding(500).quantile(0.1)
d = pd.Series(0.0, index=idx)
d[s >= q90] = 1.0
d[s <= q10] = -1.0
d[q90.isna() | s.isna()] = 0.0
ch1 = T / T.shift(1) - 1
d1 = np.sign(ch1).where(ch1.abs() > 0.01, 0.0).fillna(0.0)

rG = gld["cc"].reindex(idx).fillna(0.0)
rX = gdx["cc"].reindex(idx)
gdx_live = rX.notna() & (idx >= gdx.index[1])
rX = rX.fillna(0.0)

wA = sum(d.shift(k).fillna(0.0) for k in range(5)) / 5
wC = -sum(d.shift(k).fillna(0.0) for k in range(5, 20)) / 15
wB = wA.where(gdx_live, 0.0)
wD = d1

R1 = rG.to_frame("GLD")
VARS = {
    "A": ("A GLD 5d-tonnes decile cont 5d lag1", {"GLD": (wA, rG, CGLD)}, d, 5),
    "B": ("B GDX-GLD on 5d-tonnes decile 5d lag1", {"GDX": (wB, rX, CGDX), "GLD": (-wB, rG, CGLD)},
          d.where(gdx_live, 0.0), 5),
    "C": ("C GLD reversal sessions 6-20 lag1", {"GLD": (wC, rG, CGLD)}, d, 15),
    "D": ("D GLD 1d |dT|>1% next session lag1", {"GLD": (wD, rG, CGLD)}, d1, 1),
}

print(f"holdings {T.first_valid_index().date()}..; first decile signal {q90.first_valid_index().date()}; "
      f"GDX from {gdx.index[0].date()}")
print(f"desc (<=2022): corr(s_t, GLD 5d return to t) = "
      f"{s.loc[:'2022'].corr((gld['aC'] / gld['aC'].shift(5) - 1).reindex(idx).loc[:'2022']):+.2f}; "
      f"expanding q90/q10 at 2014-12-31: {q90.loc[:'2014'].iloc[-1]*100:+.2f}% / {q10.loc[:'2014'].iloc[-1]*100:+.2f}%")
rows = []
for k, (name, legs, ev, scale) in VARS.items():
    net = sum(lib.backtest_weights(w.to_frame("a"), r.to_frame("a"), c, lag=1) for w, r, c in legs.values()).loc[:"2022"]
    P = {a: w.shift(2).fillna(0.0).loc[:"2022"] for a, (w, r, c) in legs.items()}     # exposure held each day
    g = list(P.values())[0].abs() if k != "B" else P["GDX"].abs() + P["GLD"].abs()
    ret = (net / g).where(g > 0, 0.0)
    row = lib.register(EXP, name, g, ret, 0.0,
                       notes=f"SPDR tonnes, lag1, costs GLD {CGLD}bp GDX {CGDX}bp/side; ret=net/gross")
    tv = pd.Series(0.0, index=net.index)
    for a_, b_ in [lib.TRAIN, lib.VALID]:
        static = sum(P[a].loc[a_:b_].mean() * legs[a][1].loc[a_:b_] for a in legs)
        tv.loc[a_:b_] = net.loc[a_:b_] - static
    nev = lambda a_, b_: int((ev.loc[a_:b_] != 0).sum())
    nlong = lambda a_, b_: int((ev.loc[a_:b_] > 0).sum())
    nbp = lambda a_, b_: 1e4 * scale * net.loc[a_:b_].sum() / max(nev(a_, b_), 1)
    yr = (net.groupby(net.index.year).sum() * 1e4)
    print(f"\n{name}\n   events tr/va {nev(*lib.TRAIN)}/{nev(*lib.VALID)} (long {nlong(*lib.TRAIN)}/{nlong(*lib.VALID)})"
          f"  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    print(f"   net bp per event (1 unit, full hold) tr {nbp(*lib.TRAIN):+.1f} va {nbp(*lib.VALID):+.1f}"
          f" | timing-value t tr {tstat(tv.loc[:'2014']):+.2f} va {tstat(tv.loc['2015':'2022']):+.2f}"
          f" | mean exposure tr {sum(P[a].loc[:'2014'].mean() for a in P if a != 'GLD' or k != 'B'):+.3f}")
    print("   yearly net bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items() if 2006 <= y <= 2022))
    row.update(ev_tr=nev(*lib.TRAIN), ev_va=nev(*lib.VALID), nbp_tr=nbp(*lib.TRAIN), nbp_va=nbp(*lib.VALID),
               tv_t_tr=tstat(tv.loc[:'2014']), tv_t_va=tstat(tv.loc['2015':'2022']), gate=lib.passes_gate(row))
    rows.append(row)

pd.DataFrame(rows).to_csv(os.path.join(lib.HERE, "results", "359_summary.csv"), index=False)
glob = os.path.join(lib.HERE, "results", "registry.csv")
N = len(pd.read_csv(glob)) if os.path.exists(glob) else lib.n_tests()
print(f"\nglobal registry N={N}: luck threshold sqrt(2 ln N) = {np.sqrt(2 * np.log(N)):.2f}")
