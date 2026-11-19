"""003 - Overnight FX (HistData 1-min) as a signal for SPY/QQQ open->close.
fx_ret   = level at 09:29 NY on D / level at 15:59 NY on previous US day P - 1
fx_late  = level at 09:29 / level at 08:29 NY on D - 1  (last hour before the open)
Hypotheses (pre-specified):
  H1 follow overnight risk-on/off: pos = sign(fx_ret) [USDJPY up = yen weaker = risk-on]
  H2 catch-up: gap_hat = a + b*fx_ret (TRAIN); pos = sign(gap_hat - spy_gap)
  H3 follow the last hour's FX move: pos = sign(fx_late)
Usage: python experiments/003_overnight_fx.py USDJPY [AUDUSD ...]  (sign flipped for pairs quoted
so that 'up' = risk-off, none here). Years 2001-2025 (2026 not downloaded yet).
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "tools"))
import histdata

EXP = "003_overnight_fx"


def ny_minute_close(sym, years):
    cached = [y for y in years if os.path.exists(os.path.join(histdata.CACHE, f"{sym.upper()}_{y}.zip"))]
    df = pd.concat([histdata.load(sym, y) for y in (cached or years)])
    df.index = df.index.tz_localize("Etc/GMT+5").tz_convert("America/New_York").tz_localize(None)
    return df["Close"].sort_index()


def level_at(px, dates, hh, mm, tol_min=10):
    """Last 1-min close at or before hh:mm NY on each date, within tol_min minutes."""
    targets = pd.DatetimeIndex(dates) + pd.Timedelta(hours=hh, minutes=mm)
    idx = px.index.searchsorted(targets, side="right") - 1
    ok = (idx >= 0)
    vals = np.full(len(targets), np.nan)
    t_found = px.index[np.clip(idx, 0, None)]
    good = ok & ((targets - t_found) <= pd.Timedelta(minutes=tol_min))
    vals[good] = px.values[idx[good]]
    return pd.Series(vals, index=dates)


if __name__ == "__main__":
    pairs = sys.argv[1:] or ["USDJPY"]
    for pair in pairs:
        px = ny_minute_close(pair, range(2001, 2026))
        for sym in ["SPY", "QQQ"]:
            d = lib.get(sym).loc["2001-01-03":"2025-12-31"]
            D = d.index
            P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1))
            at_open = level_at(px, D, 9, 29)
            at_close_prev = pd.Series(level_at(px, P.fillna(D[0]), 15, 59).values, index=D)
            at_0829 = level_at(px, D, 8, 29)
            df = pd.DataFrame({"y": d.oc, "gap": d.gap, "fx": at_open / at_close_prev - 1,
                               "fx_late": at_open / at_0829 - 1}).dropna()
            df = df[df.fx.abs() < 0.1]
            print(f"{pair}/{sym}: {len(df)} days, corr(fx, gap)={df.fx.corr(df.gap):.2f}, "
                  f"corr(fx, y)={df.fx.corr(df.y):+.3f}, corr(fx_late, y)={df.fx_late.corr(df.y):+.3f}")
            tr = df.loc[lib.TRAIN[0]:lib.TRAIN[1]]
            m = sm.OLS(tr.gap, sm.add_constant(tr.fx)).fit()
            df["surprise"] = m.predict(sm.add_constant(df.fx)) - df.gap
            S = {"H1 follow overnight FX": np.sign(df.fx),
                 "H2 catch-up to FX-implied gap": np.sign(df.surprise),
                 "H3 follow last-hour FX move": np.sign(df.fx_late)}
            for name, pos in S.items():
                r = lib.register(EXP, f"{pair}->{sym} {name}", pos, df.y, 0.5)
                print(f"   {name:<32} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    print("registry size:", lib.n_tests())
