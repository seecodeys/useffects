"""365 - Corporate-issuance windows and issuer rate-lock hedging (idea card S8, results/ideas_settlement.md).
Mechanism: IG issuance is heaviest in the 1-2 weeks after Labor Day and after New Year. Issuers/underwriters pre-hedge
(sell Treasuries / rate locks), pressuring 7-30y Treasuries; new-issue concessions cheapen secondary IG credit.
Calendar is fixed and known in advance -> weights decided from the calendar alone, so lag=0 is allowed (no price
information used; the position at close t is known years ahead). Stated: lag 0 used for all rows, by design.

Pre-specified variants (fixed before results; no tuning after):
  A Short TLT from close of the last session before Labor Day to close of the 7th session after it (7 days held).
  B Short TLT from close of the 1st January session to close of the 8th January session (7 days held).
  C Short LQD - beta*IEF over the A and B windows (short LQD, long beta IEF, gross 1; beta = trailing 250d OLS slope
    of LQD cc on IEF cc through the decision close).
  D CONTROL (card d): long TLT over the 5 sessions right after each A/B window (sessions 8-12 after LD / Jan 9-13),
    the unwind/recovery leg; the idea predicts positive returns here.
Costs 1bp/side (bond ETFs) on turnover. Registered as pos = gross exposure, ret = net pnl / exposure (cost inside,
cost_bp=0) so bp_per_trade = net bp per unit-gross day. Timing value = net - W * period-mean daily return (removes
the static drift a same-exposure holding would earn). FOMC check for A: count scheduled FOMC days inside A windows.
Nothing from 2023+ printed; all tables stop at 2022-12-31.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "365_corp_issuance_windows"; C = 1.0
tt = lambda x: x.mean() / x.std() * np.sqrt(len(x)) if x.std() > 0 else np.nan

tlt, lqd, ief = lib.get("TLT"), lib.get("LQD"), lib.get("IEF")
idx = tlt.index[tlt.index <= "2022-12-31"]
cal = tlt.index                      # trading calendar (future dates only used as a known calendar)

def labor_day(y):
    d = pd.Timestamp(y, 9, 1)
    return d + pd.Timedelta(days=(0 - d.weekday()) % 7)

def windows():
    """returns dict kind -> list of (year, decision_dates for main window, decision_dates for control)"""
    out = {"A": [], "B": []}
    for y in range(int(idx[0].year), 2023):
        ld = labor_day(y)
        pre = cal[cal < ld]
        post = cal[cal > ld]
        if len(pre) and len(post) >= 12 and pre[-1] >= idx[0]:
            main = [pre[-1]] + list(post[:6])                 # returns earned on sessions 1..7 after LD
            ctrl = list(post[6:11])                           # returns earned on sessions 8..12
            out["A"].append((y, main, ctrl))
        jan = cal[(cal.year == y) & (cal.month == 1)]
        if len(jan) >= 13 and jan[0] > idx[0]:
            main = list(jan[0:7])                             # returns on Jan sessions 2..8
            ctrl = list(jan[7:12])                            # returns on Jan sessions 9..13
            out["B"].append((y, main, ctrl))
    return out
WIN = windows()

def build(kind):
    if kind in ("A", "B"):
        W = pd.DataFrame(0.0, index=idx, columns=["TLT"]); R = pd.DataFrame({"TLT": tlt.cc.reindex(idx)})
        for _, m, _ in WIN[kind]: W.loc[m, "TLT"] = -1.0
        return W, R
    if kind == "C":
        i2 = idx.intersection(lqd.index).intersection(ief.index)
        rl, ri = lqd.cc.reindex(i2), ief.cc.reindex(i2)
        beta = rl.rolling(250, min_periods=250).cov(ri) / ri.rolling(250, min_periods=250).var()
        W = pd.DataFrame(0.0, index=i2, columns=["LQD", "IEF"]); R = pd.DataFrame({"LQD": rl, "IEF": ri})
        for k in "AB":
            for _, m, _ in WIN[k]:
                m = [d for d in m if d in i2 and np.isfinite(beta.get(d, np.nan))]
                b = beta.loc[m]
                W.loc[m, "LQD"] = -1 / (1 + b.abs()); W.loc[m, "IEF"] = b / (1 + b.abs())
        return W, R
    if kind == "D":
        W = pd.DataFrame(0.0, index=idx, columns=["TLT"]); R = pd.DataFrame({"TLT": tlt.cc.reindex(idx)})
        for k in "AB":
            for _, _, c in WIN[k]: W.loc[c, "TLT"] = 1.0
        return W, R

DESC = {"A": "short TLT Fri-before-LaborDay -> LD+7", "B": "short TLT Jan s1 -> s8",
        "C": "short LQD-beta*IEF over A+B windows", "D": "CONTROL long TLT sessions 8-12 after A/B windows"}
fomc = set(pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"]).query("type=='scheduled'").date)

for k in "ABCD":
    W, R = build(k)
    net = lib.backtest_weights(W, R, C, lag=0).loc[:"2022"]
    P = W.shift(1).fillna(0.0).loc[:"2022"]
    g = P.abs().sum(axis=1)
    ret = (net / g).where(g > 0, 0.0)
    r = lib.register(EXP, f"365{k} {DESC[k]} lag0(calendar)", g, ret, 0.0,
                     notes="calendar-known windows; lag0 allowed; 1bp/side; ret=net/gross")
    Rr = R.reindex(P.index).fillna(0.0)
    tv = net - (P * Rr.loc[:"2022"].mean()).sum(axis=1)       # remove static drift of the held exposure
    on = g > 0
    yr = net[on].groupby(net[on].index.year).sum() * 1e4
    nev = lambda a, b: int(((g.loc[a:b] > 0) & (g.shift(1).fillna(0).loc[a:b] == 0)).sum())
    print(f"\n{r['variant']}\n  {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    for nm, (a, b) in lib.PERIODS.items():
        n = nev(a, b)
        print(f"  {nm}: events {n}  net bp/event {1e4*net.loc[a:b].sum()/max(n,1):+.1f}  "
              f"timing-value t {tt(tv.loc[a:b][on.loc[a:b]]):+.2f}  hit(events>0) "
              f"{(yr.loc[int(a[:4]):int(b[:4])] > 0).mean()*100:.0f}% of years")
    print("  per-year net bp (window sum):", " ".join(f"{y}:{v:+.0f}" for y, v in yr.items()))
    if k == "A":
        held = P.index[on]
        print("  scheduled FOMC days inside A windows (return days):", sorted(d.date() for d in held if d in fomc))
print("\nregistry size (this agent):", lib.n_tests())
