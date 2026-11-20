"""117 - LETF rebalancing reversal, sharper: LETF activity share = 20d dollar volume of SSO, SDS, UPRO, SPXU, QLD, QID,
      TQQQ, SQQQ relative to SPY+QQQ dollar volume. Fade |SPY day| > 2% from the close to the next open ONLY when the
      LETF share is above its trailing 252d median. 2009+ (LETF era); TRAIN effectively 2010-14. 0.5bp/side.
122 - [arXiv 2609.07946] vol-controlled 60/40 SPY/TLT vs static 60/40: monthly, scale to 10% target vol (63d est.,
      leverage cap 1.5), rest in cash. Risk overlay -> raw Sharpe comparison by period (registered as excess vs static).
123 - [arXiv 2504.20116/2506.19200] LETF trend filter: hold 2x (SSO) when SPY trailing 63d return > 0, else SPY 1x;
      close->close lag-1, 2bp/side; excess vs always-SSO. 2007+.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
spy, qqq = lib.get("SPY"), lib.get("QQQ")
dv = lambda d: (d.Close * d.Volume)
letf = sum(dv(lib.get(s)).reindex(spy.index).fillna(0) for s in ["SSO", "SDS", "UPRO", "SPXU", "QLD", "QID", "TQQQ", "SQQQ"])
share = (letf.rolling(20).mean() / (dv(spy) + dv(qqq).reindex(spy.index)).rolling(20).mean())
hi_share = share > share.rolling(252).median()
r = spy.cc
co = spy["Open"] / spy["Close"].shift(1) - 1
sig = pd.Series(np.where(r > 0.02, -1.0, np.where(r < -0.02, 1.0, 0.0)), index=spy.index).where(hi_share, 0)
ser = (sig * co.shift(-1) - sig.abs() * 2 * 0.5e-4).shift(1).loc["2009":]
res = lib.register("112_letf_rebalance", "117 fade |day|>2% close->next open when LETF activity share high (2009+)",
                   pd.Series(1.0, index=ser.index), ser.fillna(0), 0.0)
print(f"117  {lib.fmt(res)}  gate={lib.passes_gate(res)}  events={int(sig.abs().loc['2009':'2022'].sum())}")

tlt = lib.get("TLT")
R2 = pd.concat({"SPY": spy.cc, "TLT": tlt.cc}, axis=1).dropna()
mix = 0.6 * R2.SPY + 0.4 * R2.TLT
vol = mix.rolling(63).std() * np.sqrt(252)
me = pd.Series(R2.index.to_period("M"), index=R2.index)
scale = (0.10 / vol).clip(upper=1.5).where(me != me.shift(-1)).ffill()       # set at month-end close
Wv = pd.DataFrame({"SPY": 0.6 * scale, "TLT": 0.4 * scale})
Ws = pd.DataFrame({"SPY": 0.6, "TLT": 0.4}, index=R2.index)
nv = lib.backtest_weights(Wv, R2, 1.0, lag=1); ns_ = lib.backtest_weights(Ws, R2, 1.0, lag=1)
for per, (a, b) in {"train": ("2003", "2014"), "valid": lib.VALID}.items():
    sv = lib.evaluate(pd.Series(1.0, index=nv.loc[a:b].index), nv.loc[a:b], 0); ss = lib.evaluate(pd.Series(1.0, index=ns_.loc[a:b].index), ns_.loc[a:b], 0)
    print(f"122 {per}: vol-controlled Sharpe {sv['sharpe']:.2f} maxDD {sv['max_dd_%']:.1f}%  |  static Sharpe {ss['sharpe']:.2f} maxDD {ss['max_dd_%']:.1f}%")
res = lib.register("122_vol_control", "122 vol-controlled 60/40 minus static 60/40 (excess)", pd.Series(1.0, index=nv.index), (nv - ns_).loc["2003":], 0.0)
print(f"122 excess  {lib.fmt(res)}")

sso = lib.get("SSO")
R3 = pd.concat({"SSO": sso.cc, "SPY": spy.cc}, axis=1).dropna()
trend = (spy.aC / spy.aC.shift(63) - 1).reindex(R3.index) > 0
Wt = pd.DataFrame({"SSO": trend.astype(float), "SPY": (~trend).astype(float)})
nt = lib.backtest_weights(Wt, R3, 2.0, lag=1)
na = lib.backtest_weights(pd.DataFrame({"SSO": 1.0, "SPY": 0.0}, index=R3.index), R3, 2.0, lag=1)
res = lib.register("123_letf_trend", "123 SSO when SPY 63d trend>0 else SPY, minus always-SSO", pd.Series(1.0, index=nt.index), (nt - na), 0.0)
print(f"123 excess  {lib.fmt(res)}")
for per, (a, b) in {"train": ("2007", "2014"), "valid": lib.VALID}.items():
    st = lib.evaluate(pd.Series(1.0, index=nt.loc[a:b].index), nt.loc[a:b], 0); sa = lib.evaluate(pd.Series(1.0, index=na.loc[a:b].index), na.loc[a:b], 0)
    print(f"123 {per}: trend-filtered Sharpe {st['sharpe']:.2f} maxDD {st['max_dd_%']:.1f}%  |  always-SSO Sharpe {sa['sharpe']:.2f} maxDD {sa['max_dd_%']:.1f}%")
print("registry size:", lib.n_tests())
