"""357 - Dealer gamma regime from the monthly OPEX roll (card P5; Baltussen, Da, Lammers & Martens JFE 2021;
Barbon & Buraschi 'Gamma Fragility'). At monthly OPEX end users roll puts (dealers short) and overwritten calls
(dealers long) to strikes near spot. Regime R(t) = SPX close(t-1) / SPX close(last monthly OPEX <= t-1) - 1, known
before the open of t. R << 0 -> dealer-short puts near the money -> negative gamma -> intraday momentum into the close.
R >> 0 -> dealer-long calls dominate -> positive gamma -> intraday reversal.
Data: clock-corrected SPX CFD 1-min (HistData SPXUSD, NY local time), 2010-2022 only. Closes = 16:00 CFD level.
OPEX = last trading day on/before the calendar 3rd Friday (Good Friday -> Thursday).
Pre-specified variants (written BEFORE looking; 0.5bp/side, trade held 15:30->16:00 only on regime days):
  A  R < -2%: pos = sign(prev close -> 15:30), hold 15:30->16:00 (momentum)
  B  R > +2%: pos = -sign(09:30 -> 15:30), hold 15:30->16:00 (reversal)
  C  A + B combined
  D  R < -4%: as A
Gross numbers reported for the same variants at cost 0 (not separate registry entries). No tuning after."""
import os, sys, importlib
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
fx = importlib.import_module("003_overnight_fx")
EXP = "357_gamma_regime_opex"

px = fx.ny_minute_close("SPXUSD", range(2010, 2023))
spy = lib.get("SPY"); idx = spy.index[(spy.index >= "2010-01-01") & (spy.index <= "2022-12-31")]
o, a, c = (fx.level_at(px, idx, h, m, tol_min=5) for h, m in [(9, 30), (15, 30), (16, 0)])
prev_c = c.shift(1)                                   # previous trading day's 16:00 level

# OPEX calendar: last trading day on/before each calendar third Friday
months = pd.period_range("2009-12", "2022-12", freq="M")
third_fri = [p.start_time + pd.offsets.WeekOfMonth(week=2, weekday=4) for p in months]
allidx = spy.index
opex = sorted({allidx[allidx.searchsorted(f, side="right") - 1] for f in third_fri if f >= allidx[0]})
assert all(15 <= f.day <= 21 and f.dayofweek == 4 for f in third_fri), "third Friday calc"
opex = pd.DatetimeIndex(opex)
c_all = fx.level_at(px, allidx[(allidx >= "2009-12-01") & (allidx <= "2022-12-31")], 16, 0, tol_min=5)
# last OPEX on/before t-1 for each t
tm1 = pd.Series(idx, index=idx).shift(1)
last_opex = pd.Series(opex[np.clip(opex.searchsorted(tm1.fillna(idx[0]), side="right") - 1, 0, None)], index=idx)
R = prev_c / c_all.reindex(last_opex.values).values - 1

ret = (c / a - 1)                                     # 15:30->16:00
mom = np.sign(a / prev_c - 1); rev = -np.sign(a / o - 1)
ok = ret.notna() & R.notna()
A = mom.where((R < -0.02) & ok, 0.0).fillna(0)
B = rev.where((R > 0.02) & ok, 0.0).fillna(0)
D = mom.where((R < -0.04) & ok, 0.0).fillna(0)
variants = {"A R<-2% momentum": A, "B R>+2% reversal": B, "C A+B combined": A + B, "D R<-4% momentum": D}
retf = ret.fillna(0)
print(f"days {ok.sum()}, R<-2%: {(R<-0.02).sum()}, R>+2%: {(R>0.02).sum()}, R<-4%: {(R<-0.04).sum()}")
print(f"luck t for best of N~{lib.n_tests()} registered: {np.sqrt(2*np.log(max(lib.n_tests(),2))):.2f}")
for name, pos in variants.items():
    r = lib.register(EXP, f"357{name}", pos, retf, 0.5, notes="SPX CFD 15:30->16:00, 2010-22")
    g = lib.split_eval(pos, retf, 0.0)
    print(f"\n{name}: NET {lib.fmt(r)} gate={lib.passes_gate(r)}")
    print(f"   n train/valid events: {int((pos.loc[:'2014']!=0).sum())}/{int((pos.loc['2015':]!=0).sum())}  "
          f"GROSS train_t={g['train_t']:+.2f} valid_t={g['valid_t']:+.2f} valid bp/trade={g['valid_bp_per_trade']:+.2f} "
          f"train bp/trade={g['train_bp_per_trade']:+.2f}; NET train bp/trade={r['train_bp_per_trade']:+.2f}")
    pnl = (pos * retf * 1e4)[pos != 0]
    print("   gross bp sum by year:", pnl.groupby(pnl.index.year).sum().round(0).astype(int).to_dict())
