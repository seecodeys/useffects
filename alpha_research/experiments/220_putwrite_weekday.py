"""220 - When do put-writers earn the weekend decay? CBOE WPUT (weekly ATM SPX put-write, real prices, 2006+) and PUT.
Descriptive (2006-2022): mean excess return by weekday. Then one pre-specified timing test: hold WPUT exposure only over
the Friday session (close Thu -> close Fri) + Monday session vs not (timing value vs exposure-matched always-on), 10bp/switch.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
irx = lib.get("^IRX")["Close"] / 100 / 252
for name in ["WPUT", "PUT"]:
    p = pd.read_csv(os.path.join(lib.DATA, "cboe", f"{name}.csv")); p["DATE"] = pd.to_datetime(p.DATE, format="%m/%d/%Y")
    p = p.set_index("DATE").iloc[:, 0].astype(float).sort_index()
    x = (p.pct_change() - irx.reindex(p.index).ffill().fillna(0)).loc["2006":"2022"]
    x = x[p.index.to_series().diff().dt.days.reindex(x.index) <= 4]          # drop gaps in CBOE file
    print(f"220 {name} mean excess bp by weekday 2006-22:", (x.groupby(x.index.dayofweek).mean() * 1e4).round(1).to_dict())
p = pd.read_csv(os.path.join(lib.DATA, "cboe", "WPUT.csv")); p["DATE"] = pd.to_datetime(p.DATE, format="%m/%d/%Y")
p = p.set_index("DATE").WPUT.astype(float).sort_index()
x = (p.pct_change() - irx.reindex(p.index).ffill().fillna(0)).fillna(0)
idx = x.index
want = pd.Series(np.isin(idx.dayofweek, [0, 4]).astype(float), index=idx)
pos = want.shift(-1).fillna(0)
strat = pos * x - pos.diff().abs().fillna(0) * 10e-4 * 0.05        # switching cost ~10bp of premium (premium ~5% of notional... approx)
out = pd.Series(0.0, index=idx)
for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
    out.loc[a:b] = strat.loc[a:b] - pos.loc[a:b].mean() * x.loc[a:b]
r = None if set(sys.argv) & {"--exfri","--holdout"} else lib.register("220_putwrite_weekday", "220 WPUT exposure only Fri+Mon sessions (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
r is not None and print(f"220 timing  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
print("registry size:", lib.n_tests())

# ---- 220b (--exfri / --holdout): POST-HOC rule from the weekday table: WPUT exposure every day EXCEPT the Friday session
if set(sys.argv) & {"--exfri", "--holdout"}:
    want = pd.Series((idx.dayofweek != 4).astype(float), index=idx)
    pos = want.shift(-1).fillna(0)
    strat = pos * x - pos.diff().abs().fillna(0) * 10e-4 * 0.05
    out = pd.Series(0.0, index=idx)
    for a, b in [lib.TRAIN, lib.VALID, lib.HOLDOUT]:
        out.loc[a:b] = strat.loc[a:b] - pos.loc[a:b].mean() * x.loc[a:b]
    if "--exfri" in sys.argv:
        r = lib.register("220_putwrite_weekday", "220b WPUT exposure except Friday sessions (timing value; post-hoc from weekday table)",
                         pd.Series(1.0, index=out.index), out, 0.0, notes="post-hoc")
        print(f"220b  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
        for per, (a, b) in {"train": ("2006", "2014"), "valid": lib.VALID}.items():
            s1 = lib.evaluate(pd.Series(1.0, index=x.loc[a:b].index), (pos * x).loc[a:b], 0); s0 = lib.evaluate(pd.Series(1.0, index=x.loc[a:b].index), x.loc[a:b], 0)
            print(f"   {per}: ex-Friday WPUT Sharpe {s1['sharpe']:.2f} vs always-on {s0['sharpe']:.2f}")
    if "--holdout" in sys.argv:
        h = lib.holdout_eval("220_putwrite_weekday", "220b WPUT except Fridays (timing value)", pd.Series(1.0, index=out.index), out, 0.0)
        print("HOLDOUT", {k: round(float(v), 2) for k, v in h.items()})
        a, b = lib.HOLDOUT
        s1 = lib.evaluate(pd.Series(1.0, index=x.loc[a:b].index), (pos * x).loc[a:b], 0); s0 = lib.evaluate(pd.Series(1.0, index=x.loc[a:b].index), x.loc[a:b], 0)
        print(f"   holdout: ex-Friday WPUT Sharpe {s1['sharpe']:.2f} vs always-on {s0['sharpe']:.2f}")
