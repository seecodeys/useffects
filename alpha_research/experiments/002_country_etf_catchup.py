"""002 - Asian country ETFs: does each ETF's open->close depend on how its home
market moved since the last US close, relative to the ETF's own opening gap?

Home markets close before 9:30 ET, so home_ret (all home sessions since the
previous US close) is fully known. implied_gap = a + b*home_ret + c*spy_prev
fitted on TRAIN only. surprise = implied_gap - etf_gap:
  > 0 -> ETF opened lower than home market implies -> 'catch-up' says go long.
FX moves are omitted (Yahoo FX daily timestamps are ambiguous) -> noise, not bias.
Entry uses the ETF's own opening print, same standard as SPY's gap.
Costs: 2bp/side (country ETFs are less liquid than SPY).
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "002_country_etf_catchup"
COST = 2.0
PAIRS = {"EWJ": "^N225", "EWH": "^HSI", "FXI": "^HSI", "EWY": "^KS11", "EWT": "^TWII",
         "EWA": "^AXJO", "EWS": "^STI", "INDA": "^BSESN"}

def build():
  spy = lib.get("SPY")
  surprises, rets, fits = {}, {}, {}
  for etf, home in PAIRS.items():
      e = lib.get(etf).loc["2000":]
      h = lib.get(home)["Close"]
      D = e.index
      P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1))
      home_ret = pd.Series(lib.asof(h, D).values / lib.asof(h, P).values - 1, index=D)
      df = pd.DataFrame({"y": e.oc, "gap": e.gap, "home": home_ret,
                         "spy_prev": spy.cc.shift(1).reindex(D)}).dropna()
      df = df[df.gap.abs() < 0.2]
      tr = df.loc[lib.TRAIN[0]:lib.TRAIN[1]]
      if len(tr) < 500:      # INDA starts 2012: fit on what train it has
          tr = df.loc[:"2016-12-31"]
      m = sm.OLS(tr.gap, sm.add_constant(tr[["home", "spy_prev"]])).fit()
      df["surprise"] = m.predict(sm.add_constant(df[["home", "spy_prev"]])) - df.gap
      surprises[etf], rets[etf] = df.surprise, df.y


      fits[etf] = m
  return pd.DataFrame(surprises).loc["2000":], pd.DataFrame(rets), fits


def xs_weights(S, top=2):
    """Long the `top` ETFs with the highest surprise (opened cheapest vs home
    market = catch-up), short the `top` lowest. Only days with >=5 ETFs."""
    rk, k = S.rank(axis=1), S.notna().sum(axis=1)
    w = pd.DataFrame(0.0, index=S.index, columns=S.columns)
    w[rk.ge(k.values[:, None] - top + 1)] = 0.5 / top
    w[rk.le(top)] = -0.5 / top
    return w[k >= 5].reindex(S.index).fillna(0.0)


if __name__ == "__main__":
    S, Y, fits = build()
    Y = Y.reindex(S.index)
    out = []
    for etf in S:
        s_ = S[etf].dropna()
        r = lib.register(EXP, f"{etf} catch-up sign(surprise)", np.sign(s_), Y[etf].reindex(s_.index), COST,
                         notes=f"gap~home beta={fits[etf].params['home']:.2f}")
        out.append(r); print(f"{etf:<5} {lib.fmt(r)}")
    pos = np.sign(S); n = pos.notna().sum(axis=1); pw = pos.div(n, axis=0)
    net = (pw * Y).sum(axis=1) - pw.abs().sum(axis=1) * 2 * COST / 1e4
    r = lib.register(EXP, "pooled equal-weight catch-up (all ETFs)", pd.Series(1.0, index=net.index), net, 0.0,
                     notes="costs 2bp/side included in return series")
    out.append(r); print("pooled catch-up     ", lib.fmt(r))
    w = xs_weights(S)
    gross = (w * Y.fillna(0)).sum(axis=1)
    net = gross - w.abs().sum(axis=1) * 2 * COST / 1e4
    r = lib.register(EXP, "cross-sectional catch-up L/S top2-bottom2 (corrected sign)", pd.Series(1.0, index=net.index),
                     net, 0.0, notes="costs 2bp/side included; earlier row with t=-35.9 was this with sign flipped")
    out.append(r); print("XS catch-up L/S     ", lib.fmt(r))
    for per, (a, b) in {"train": lib.TRAIN, "valid": lib.VALID}.items():
        print(f"  {per}: gross {1e4*gross.loc[a:b].mean():+.2f} bp/day, cost {1e4*(gross-net).loc[a:b].mean():.2f} bp/day")
    print("  gross bp/day by year:", (gross.loc[:"2022"].groupby(gross.loc[:"2022"].index.year).mean() * 1e4).round(1).to_dict())
    pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "002_country_etf_catchup.csv"), index=False)
    print("registry size:", lib.n_tests())
