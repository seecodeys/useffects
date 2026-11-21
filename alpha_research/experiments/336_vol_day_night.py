"""336 - Idea card P4 (Muravyev & Ni 2020): volatility is expensive overnight and cheap intraday, so long-vol
positions should bleed close->open and earn (or bleed less) open->close. Expressed with VIXY (long 1-month VIX
futures ETP, Yahoo daily OHLC, 2011+). TRAIN = 2011-01 .. 2014-12 (VIXY data start), VALID = 2015-22. No 2023+.
Returns: overnight = aO_t / aC_{t-1} - 1 (lib 'gap', split/reverse-split adjusted); intraday = Close/Open - 1 ('oc').
Cost 2bp/side (VIXY spreads), charged on every leg: a strategy trading twice a day pays 4 sides/day.

PRE-SPECIFIED VARIANTS (fixed before looking at results; no tuning):
  A  short VIXY close->next open every night, flat intraday                       (2 sides/day)
  B  long VIXY open->close every day, flat overnight                              (2 sides/day)
  C  A + B combined book: short overnight, long intraday                          (4 sides/day)
  D  A only when VIX9D/VIX < 1 at the PRIOR session's close (lag-1, contango/normal regime); flat otherwise

Each variant is registered twice: (raw) the net daily P&L, and (timing value) net P&L minus an exposure-matched
always-short VIXY close->close position: TV_t = net_t - mean_signed_pos(period) * VIXY cc_t, the 082 convention
(mean over the period, per period). The always-short benchmark is charged no cost (one-off position), so TV is
conservative. For C the mean signed position is 0, so its TV equals its raw P&L.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

COST = 2.0
v = lib.get("VIXY").loc[:"2022-12-31"]
idx = v.index
night, day, cc = v["gap"].fillna(0), v["oc"].fillna(0), v["cc"].fillna(0)
vix9d = lib.asof(lib.get("^VIX9D")["Close"], idx)
vix = lib.asof(lib.get("^VIX")["Close"], idx)
# night t (close t-1 -> open t) is entered at close t-1; signal must be known at close t-2 (lag-1)
contango_lag1 = (vix9d / vix < 1).shift(2).fillna(False).astype(float)

one = pd.Series(1.0, index=idx)
legs = {  # variant -> net daily pnl (each leg costed separately, 2 sides per leg-day traded)
    "A short night":            lib.daily_pnl(-one, night, COST),
    "B long day":               lib.daily_pnl(one, day, COST),
    "C short night + long day": lib.daily_pnl(-one, night, COST) + lib.daily_pnl(one, day, COST),
    "D short night if VIX9D/VIX<1 (lag-1)": lib.daily_pnl(-contango_lag1, night, COST),
}
signed = {"A short night": -one, "B long day": one, "C short night + long day": one * 0,
          "D short night if VIX9D/VIX<1 (lag-1)": -contango_lag1}

rows = []
for k, pnl in legs.items():
    act = (signed[k] != 0) | k.startswith("C")
    # raw: pass pnl as 'ret' with unit position on active days, cost 0 (already netted)
    raw = lib.register("336_vol_day_night", k + " (raw, 2bp/side)", act.astype(float), pnl.where(act, 0.0), 0.0,
                       notes="VIXY; pnl net of 2bp/side per leg")
    tv = pd.Series(0.0, index=idx)
    for a, b in [lib.TRAIN, lib.VALID]:
        tv.loc[a:b] = pnl.loc[a:b] - signed[k].loc[a:b].mean() * cc.loc[a:b]
    tvr = lib.register("336_vol_day_night", k + " (timing value vs always-short)", one, tv, 0.0,
                       notes="net minus exposure-matched always-short VIXY cc (no cost on benchmark)")
    for lab, r in [("raw", raw), ("TV", tvr)]:
        rows.append({"variant": k, "metric": lab, "train_t": r["train_t"], "valid_t": r["valid_t"],
                     "train_bp_day": 1e4 * r["train_ann_ret_%"] / 100 / 252,
                     "valid_bp_day": 1e4 * r["valid_ann_ret_%"] / 100 / 252,
                     "valid_sharpe": r["valid_sharpe"], "valid_bp_per_trade": r["valid_bp_per_trade"],
                     "gate": lib.passes_gate(r)})

out = pd.DataFrame(rows)
pd.set_option("display.width", 200)
print(out.round(2).to_string(index=False))

# descriptive context (registered variants only, <=2022): gross mean bp/day by leg & yearly P&L of A and C
print("\nGross mean bp/day  night:", {p: round(1e4 * night.loc[a:b].mean(), 1) for p, (a, b) in lib.PERIODS.items()},
      " day:", {p: round(1e4 * day.loc[a:b].mean(), 1) for p, (a, b) in lib.PERIODS.items()},
      " cc:", {p: round(1e4 * cc.loc[a:b].mean(), 1) for p, (a, b) in lib.PERIODS.items()})
print("D active share:", round(contango_lag1.mean(), 2))
yr = pd.DataFrame({k: 1e4 * p.groupby(p.index.year).mean() for k, p in legs.items()}).round(1)
print("\nNet bp/day by year:\n", yr.to_string())
print("\nregistry size:", lib.n_tests(), " luck t ~", round(np.sqrt(2 * np.log(max(lib.n_tests(), 2))), 2))
out.to_csv(os.path.join(lib.HERE, "results", "336_vol_day_night.csv"), index=False)
