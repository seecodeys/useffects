"""344 - P3 Heston-Korajczyk-Sadka (2010) same-half-hour persistence across days, cost-aware portfolio.
Data: HistData SPXUSD / NSXUSD 1-min CFD, NY local clock, 2010-2022 only (no 2023+ loaded). 13 regular-session
half-hour bins (09:30-10:00 ... 15:30-16:00); bin return = level(end)/level(start)-1, levels = last 1-min close at or
before the bin boundary (tol 5 min). Half-days/missing bins are skipped.

Signal for bin k on day t uses ONLY completed earlier sessions' bin-k returns (known before bin k opens):
  agg_k(t) = sign(r_k(t-1)) + sign(r_k(t-5))   (HKS: persistence strongest at lag 1 and at weekly multiples)
  position in bin k = +1 if agg=+2, -1 if agg=-2, else flat (only trade when lag-1 and lag-5 agree).
Cost: 0.5bp/side per bin traded (1bp round trip per bin trade). Same capital reused bin by bin; daily P&L = sum of
bin P&L. Registered as pos = #bins traded that day, ret = mean gross bin return, so lib bp_per_trade = net bp per
bin-trade and the t-stat is on the daily sum.

PRE-SPECIFIED VARIANTS (fixed before seeing any result):
  V1 SPX  all-13-bins agg signal.
  V2 SPX  agg signal traded only in the 3 bins with the highest TRAIN (2010-14) gross P&L of the agg signal
          (selection uses train only; valid 2015-22 is out of sample for the selection).
  V3 NSX  all-13-bins agg signal.
  V4 NSX  top-3 TRAIN bins (selected on NSX train), as V2.
Timing value: compare against exposure-matched long-only in the same bins on the same traded days.
"""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")

EXP = "344_same_halfhour_persistence"
spy = lib.get("SPY")
D = spy.index[(spy.index >= "2010-01-04") & (spy.index <= "2022-12-30")]
BOUNDS = [(9 + (30 + 30 * i) // 60, (30 + 30 * i) % 60) for i in range(14)]  # 09:30 ... 16:00


def bin_returns(sym):
    px = fx.ny_minute_close(sym, range(2010, 2023))
    lv = pd.DataFrame({i: fx.level_at(px, D, h, m, tol_min=5) for i, (h, m) in enumerate(BOUNDS)})
    R = pd.DataFrame({k: lv[k + 1] / lv[k] - 1 for k in range(13)})
    R[R.abs() > 0.05] = np.nan
    return R


def run(sym, R, top3):
    S = np.sign(R)
    agg = S.shift(1) + S.shift(5)
    P = pd.DataFrame(np.where(agg == 2, 1.0, np.where(agg == -2, -1.0, 0.0)), index=R.index, columns=R.columns)
    P[R.isna()] = 0.0
    G = (P * R.fillna(0))
    tr = G.loc[:lib.TRAIN[1]]
    bins = list(range(13))
    out = []
    for tag, sel in [("all13", bins), ("top3train", None)]:
        if sel is None:
            sel = list(tr.sum().sort_values(ascending=False).index[:3])
        n = P[sel].abs().sum(axis=1)
        g = G[sel].sum(axis=1)
        ret = (g / n.replace(0, np.nan)).fillna(0.0)
        name = f"{sym} agg sign(lag1)+sign(lag5) same-slot, bins={tag} {sel if tag != 'all13' else ''}".strip()
        row = lib.register(EXP, name, n, ret, cost_bp=0.5,
                           notes="pos=#bins traded, ret=mean gross bin ret; 0.5bp/side per bin")
        # exposure-matched long-only in same bins/days
        L = (P[sel].abs() * R[sel].fillna(0)).sum(axis=1)
        stats = {}
        for per, (a, b) in {"train": lib.TRAIN, "valid": ("2015-01-01", "2022-12-31")}.items():
            nn, gg, ll = n.loc[a:b], g.loc[a:b], L.loc[a:b]
            stats[per] = (1e4 * gg.sum() / nn.sum(), 1e4 * (gg.sum() - nn.sum() * 1e-4) / nn.sum(),
                          1e4 * ll.sum() / nn.sum(), nn.mean())
        yearly = ((g - n * 1e-4).groupby(g.index.year).sum() * 1e4).round(0)
        print(f"\n{name}\n  {lib.fmt(row)}  passes_gate={lib.passes_gate(row)}")
        for per, (gb, nb, lb, nd) in stats.items():
            print(f"  {per}: gross {gb:+.3f} bp/bin-trade, net {nb:+.3f}, long-only same bins {lb:+.3f}, bins/day {nd:.2f}")
        print("  yearly net bp (sum):", dict(yearly))
        out.append(row)
    print(f"  {sym} train per-bin gross bp/trade:", (1e4 * tr.sum() / P.loc[:lib.TRAIN[1]].abs().sum()).round(2).to_dict())
    return out


if __name__ == "__main__":
    for sym in ["SPXUSD", "NSXUSD"]:
        R = bin_returns(sym)
        print(f"{sym}: {R.notna().all(axis=1).sum()} full sessions")
        run(sym, R, None)
    print("registry size:", lib.n_tests(), " luck t ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
