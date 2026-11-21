"""335 - European-open overnight drift after closing sell pressure (idea card P1, results/ideas_papers.md).
Source: Boyarchenko, Larsen & Whelan, "The Overnight Drift" (RFS 2023). Dealers absorb end-of-day customer selling and
unload inventory when European liquidity arrives (02:00-03:00 ET); the premium is larger after negative closing imbalance.

Data: HistData SPXUSD / NSXUSD 1-min CFD, timestamps are NY LOCAL time (003.ny_minute_close, clock bug fixed - no
re-conversion). Years 2010-2022 only are loaded (train 2010-14, valid 2015-22); 2023+ never loaded.
Event day D = SPY trading day. P = previous SPY trading day.
  ret(D)  = px(D 03:00) / px(D 02:00) - 1           (traded window, entry 02:00, exit 03:00)
  sig(D)  = px(P 16:00) / px(P 15:00) - 1           (prior last-hour return, proxy for closing imbalance; known at entry)
Pre-specified variants (written before any result was seen; no tuning afterwards):
  A: long SPX 02:00->03:00 every day.
  B: long SPX 02:00->03:00 only when sig < 0.
  C: -sign(sig) on SPX 02:00->03:00 (long/short).
  D: B on NSX (Nasdaq-100 CFD), signal from NSX's own last hour.
Cost 0.5bp/side (registered); 1bp/side reported as sensitivity on the same variants (not new variants).
Descriptive: mean hourly return of every overnight hour 17:00(P)->09:00(D) vs 02-03, train/valid only.
"""
import os, sys, importlib.util
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import lib

spec = importlib.util.spec_from_file_location("o003", os.path.join(HERE, "experiments", "003_overnight_fx.py"))
o003 = importlib.util.module_from_spec(spec); spec.loader.exec_module(o003)

EXP = "335_european_open_drift"
YEARS = range(2010, 2023)
END = "2022-12-31"

D = lib.get("SPY").loc["2010-01-05":END].index
P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1)).fillna(D[0])


def build(sym):
    px = o003.ny_minute_close(sym, YEARS)
    px = px.loc[:END + " 23:59"]
    ret = o003.level_at(px, D, 3, 0, 10) / o003.level_at(px, D, 2, 0, 10) - 1
    sig = pd.Series((o003.level_at(px, P, 16, 0, 5) / o003.level_at(px, P, 15, 0, 5) - 1).values, index=D)
    ok = ret.notna() & sig.notna() & (ret.abs() < 0.05)
    return px, ret[ok], sig[ok]


def hourly_table(px):
    rows = {}
    for h in range(17, 33):                  # 17:00 P -> 09:00 D (hours >=24 are on D)
        a, b = h, h + 1
        pa = o003.level_at(px, P if a < 24 else D, a % 24, 0, 10).values
        pb = o003.level_at(px, P if b < 24 else D, b % 24, 0, 10).values
        r = pd.Series(pb / pa - 1, index=D)
        r = r[r.abs() < 0.05]
        rows[f"{a%24:02d}-{b%24:02d}"] = {per: 1e4 * r.loc[s:e].mean() for per, (s, e) in lib.PERIODS.items()}
    return pd.DataFrame(rows).T


def main():
    out = []
    px_s, ret_s, sig_s = build("SPXUSD")
    _, ret_n, sig_n = build("NSXUSD")
    variants = {
        "A_long_0203_spx": (pd.Series(1.0, index=ret_s.index), ret_s),
        "B_long_0203_after_neg_lasthour_spx": ((sig_s < 0).astype(float), ret_s),
        "C_neg_sign_lasthour_0203_spx": (-np.sign(sig_s), ret_s),
        "D_long_0203_after_neg_lasthour_nsx": ((sig_n < 0).astype(float), ret_n),
    }
    for name, (pos, ret) in variants.items():
        row = lib.register(EXP, name, pos, ret, 0.5, notes="P1 BLW overnight drift; 02:00-03:00 NY local")
        gross = lib.split_eval(pos, ret, 0.0)
        c1 = lib.split_eval(pos, ret, 1.0)
        out.append({"variant": name, "train_n": int((pos.loc[:lib.TRAIN[1]] != 0).sum()),
                    "valid_n": int((pos.loc[lib.VALID[0]:END] != 0).sum()),
                    "train_t": row["train_t"], "valid_t": row["valid_t"], "valid_sharpe": row["valid_sharpe"],
                    "train_bp_gross": gross["train_bp_per_trade"], "train_bp_net": row["train_bp_per_trade"],
                    "valid_bp_gross": gross["valid_bp_per_trade"], "valid_bp_net": row["valid_bp_per_trade"],
                    "valid_bp_net_1bp": c1["valid_bp_per_trade"], "valid_t_1bp": c1["valid_t"],
                    "gate": lib.passes_gate(row)})
        pnl = lib.daily_pnl(pos, ret, 0.5).loc[:END]
        print(name, "yearly net bp sum:", (1e4 * pnl.groupby(pnl.index.year).sum()).round(0).to_dict())
    res = pd.DataFrame(out)
    pd.set_option("display.width", 250)
    print(res.round(2).to_string(index=False))
    print("\nMean hourly return (bp), SPX CFD, overnight hours:")
    print(hourly_table(px_s).round(2).to_string())
    print("N tests in this registry:", lib.n_tests())
    res.to_csv(os.path.join(HERE, "results", "335_european_open_drift.csv"), index=False)


if __name__ == "__main__":
    main()
