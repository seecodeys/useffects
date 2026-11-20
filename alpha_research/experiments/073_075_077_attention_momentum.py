"""075 - Record-high attention: the session after SPY closes at an all-time high (adjusted), retail buys at the open,
      price fades -> short SPY open->close. Variant: first record high in > 126 sessions (bigger headline). 0.5bp.
077 - Under-reaction / conservatism: 12-1 month momentum, monthly rebalance (overlapping 21d), lag-1, close->close.
      a) 20 single-country ETFs: long top 4 / short bottom 4 (2bp/side)   b) 9 sector SPDRs: long top 3 / short bottom 3 (1bp)
073 - Round-number anchoring (S&P 500 index ^GSPC): prior close within 0.5% BELOW a multiple of 100 -> short SPY next
      open->close (resistance); within 0.5% ABOVE -> long (breakout). 0.5bp.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy = lib.get("SPY")
y = spy.oc.loc["2000":]
ath = (spy.aC >= spy.aC.cummax()).shift(1).reindex(y.index).fillna(False)
gap_since = (~(spy.aC >= spy.aC.cummax())).astype(int)
runs = gap_since.groupby((gap_since == 0).cumsum()).cumsum()            # sessions since last ATH
first_ath = ((spy.aC >= spy.aC.cummax()) & (runs.shift(1) > 126)).shift(1).reindex(y.index).fillna(False)
for name, m in {"075a short SPY session after an all-time-high close": ath,
                "075b short SPY after first ATH in >126 sessions": first_ath}.items():
    r = lib.register("075_record_high", name, -m.astype(float), y, 0.5)
    print(f"{name:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}  n={int(m.sum())}")

def mom(syms, k, cost, label):
    R = pd.DataFrame({s: lib.get(s).cc for s in syms}).loc["1998":]
    m = R.rolling(231).sum().shift(21)                                 # months t-12..t-1
    rk = m.rank(axis=1); n = m.notna().sum(axis=1)
    W = (rk.ge(n.values[:, None] - k + 1).astype(float) - rk.le(k).astype(float)) / (2 * k)
    W = W.where(n >= 2 * k + 1, 0).fillna(0).rolling(21, min_periods=1).mean()
    net = lib.backtest_weights(W, R, cost, lag=1).loc["2000":]
    r = lib.register("077_momentum", label, pd.Series(1.0, index=net.index), net, 0.0)
    g = lib.backtest_weights(W, R, 0, lag=1).loc["2000":]
    print(f"{label:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}  gross by 5y:", (g.groupby((g.index.year//5)*5).mean()*1e4).round(1).to_dict())
C = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL",
     "EWN", "EWD", "EWK", "EWO", "EWM"]
mom(C, 4, 2.0, "077a country 12-1 momentum, top4/bottom4")
mom(["XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY"], 3, 1.0, "077b sector 12-1 momentum, top3/bottom3")

g = lib.get("^GSPC")["Close"]
pc = g.shift(1).reindex(y.index)
dist = (np.ceil(pc / 100) * 100 - pc) / pc                              # distance below next multiple of 100
dist_above = (pc - np.floor(pc / 100) * 100) / pc
pos = pd.Series(np.where(dist <= 0.005, -1.0, np.where(dist_above <= 0.005, 1.0, 0.0)), index=y.index)
r = lib.register("073_round_numbers", "073 S&P near round 100s: short just below / long just above", pos, y, 0.5)
print(f"{'073 round-number anchoring':<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}  active%={100*(pos!=0).mean():.0f}")
print("registry size:", lib.n_tests())
