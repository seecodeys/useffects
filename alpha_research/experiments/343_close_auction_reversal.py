"""343 - Closing-auction price pressure reverses after the close (idea card P2; Bogousslavsky & Muravyev, JFM 2023).
Passive close-benchmarked flow pushes the 16:00 print; the push should be given back after the close in index
futures/CFDs, which keep trading (ES: until 16:15 ET, then a 16:15-16:30 halt before mid-2021; overnight session after).

Data: HistData SPXUSD / NSXUSD 1-min CFD (NY local time, futures-session pricing), 2010-2022 only (no 2023+ loaded).
Days: SPY trading days. Early-close days dropped by a pre-specified calendar rule (Jul 3, Dec 24, Friday after
Thanksgiving), since 15:50-16:00 is after their 13:00 close.
Signal s = level(16:00 bar) / level(15:50 bar) - 1  (NYSE imbalance publication starts 15:50).
Entry at the 16:01 bar (one bar after the signal bar, to avoid trading on the same quote that defines the signal).
Position = -sign(s). Cost 0.5bp/side (≈ one ES tick), charged on every traded day.
PRE-SPECIFIED VARIANTS (4, no others):
  A: SPX, -sign(s), held 16:01 -> 16:14.
  B: SPX, -sign(s), held 16:01 -> next trading day 09:29.
  C: A, only when |s| > TRAIN (2010-14) 80th percentile of |s|.
  D: A on NSX (NDX CFD).
Implementability: the CFD is a proxy for ES/NQ futures; A/C/D need an ES/NQ trade between 16:00 and 16:15 ET, which
is possible (ES trades through 16:15 ET; SPY after-hours is the less liquid alternative). B is an ES overnight hold.
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
EXP = "343_close_auction_reversal"
YEARS = range(2010, 2023)          # never load 2023+

spy = lib.get("SPY")
D = spy.index[(spy.index >= "2010-01-01") & (spy.index <= "2022-12-31")]
md = D.strftime("%m-%d")
tg = (D.month == 11) & (D.dayofweek == 4) & (D.day >= 23) & (D.day <= 29)
D = D[~((md == "07-03") | (md == "12-24") | tg)]
NXT = pd.DatetimeIndex(spy.index[spy.index.searchsorted(D, side="right")])


def legs(sym):
    px = fx.ny_minute_close(sym, YEARS)
    L = lambda dates, h, m: pd.Series(fx.level_at(px, dates, h, m, tol_min=2).values, index=D)
    df = pd.DataFrame({"p1550": L(D, 15, 50), "p1600": L(D, 16, 0), "p1601": L(D, 16, 1),
                       "p1614": L(D, 16, 14), "pnext": L(NXT, 9, 29)})
    df["s"] = df.p1600 / df.p1550 - 1
    df["r_a"] = df.p1614 / df.p1601 - 1
    df["r_b"] = df.pnext / df.p1601 - 1
    return df


out = []
def reg(name, pos, ret, df):
    ok = pos.notna() & ret.notna()
    pos, ret = pos[ok], ret[ok]
    r = lib.register(EXP, name, pos, ret, 0.5, notes="CFD 2010-22; entry 16:01 bar; 0.5bp/side")
    g = (pos * ret) * 1e4
    tr = pos != 0
    yr = (g - pos.abs() * 1.0)[tr]
    yearly = yr.groupby(yr.index.year).mean().round(2).to_dict()
    gross = {p: g[tr].loc[a:b].mean() for p, (a, b) in lib.PERIODS.items()}
    drift = {p: 1e4 * ret.loc[a:b].mean() for p, (a, b) in lib.PERIODS.items()}
    print(f"{name}\n   {lib.fmt(r)}  train_bp={r['train_bp_per_trade']:+.2f}  gate={lib.passes_gate(r)}")
    print(f"   gross bp/trade train {gross['train']:+.2f} valid {gross['valid']:+.2f} | always-long window drift "
          f"train {drift['train']:+.2f} valid {drift['valid']:+.2f} | n trades {int(tr.sum())}")
    print(f"   net bp/trade by year: {yearly}")
    out.append({"variant": name, **{k: r[k] for k in ["train_t", "train_sharpe", "train_bp_per_trade", "valid_t",
               "valid_sharpe", "valid_bp_per_trade", "valid_traded_%"]}, "gross_tr": gross["train"],
               "gross_va": gross["valid"], "n_trades": int(tr.sum()), "gate": lib.passes_gate(r)})


spx = legs("SPXUSD")
spx = spx[spx.s.abs() < 0.05]
print("SPX days:", len(spx), " corr(s, r_a) train/valid:",
      round(spx.loc[:"2014"].s.corr(spx.loc[:"2014"].r_a), 3), round(spx.loc["2015":].s.corr(spx.loc["2015":].r_a), 3))
pos = -np.sign(spx.s)
reg("A SPX fade 15:50->16:00, hold 16:01->16:14", pos, spx.r_a, spx)
reg("B SPX fade 15:50->16:00, hold 16:01->next 09:29", pos, spx.r_b, spx)
thr = spx.s.abs().loc[:"2014-12-31"].quantile(0.8)
print(f"C threshold (train 80th pct |s|) = {thr*1e4:.2f}bp")
reg("C SPX fade only |s|>train p80, hold 16:01->16:14", pos.where(spx.s.abs() > thr, 0.0), spx.r_a, spx)
nsx = legs("NSXUSD")
nsx = nsx[nsx.s.abs() < 0.05]
reg("D NSX fade 15:50->16:00, hold 16:01->16:14", -np.sign(nsx.s), nsx.r_a, nsx)
pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "343_close_auction_reversal.csv"), index=False)
print("registry size (this agent):", lib.n_tests())
