"""375 - Idea card E8: implementing the month-end Treasury bid (finding 2, LOG 275-319) with Treasury futures, and the
cash-vs-futures split. Implementation/robustness study: variants registered, NO holdout look (this script never calls
lib.holdout_eval and never prints anything dated 2023+).

Window W3 = the last 3 NYSE sessions of the month (close of the 4th-last session -> close of the last), as in 275a.
Calendar signal, known in advance, so lag=0 like 275 (no close-print feedback is possible).
Data: Yahoo continuous ZN=F / ZB=F / ZT=F (daily close = settlement-ish ~15:00 ET) and IEF / TLT / SHY (16:00 ET close).
Costs: 0.25bp/side futures, 1bp/side ETFs.

ROLL HANDLING (pre-specified, before seeing returns). The Yahoo continuous series jumps when it switches contracts.
  (a) Exclude Feb/May/Aug/Nov month-ends for every instrument (card E8: first-notice/roll month).
  (b) Also exclude any month whose W3 return days contain a detected switch day for that future, where a switch day is
      a session whose volume is >5x the previous session's AND above its 20-session median. (Inspection of volumes only,
      before any return was computed: Yahoo switches ZN/ZB around the 19th-22nd of Mar/Jun/Sep/Dec, i.e. at expiry of
      the delivery-month contract, and ZT at the last business day of Mar/Jun/Sep/Dec, which falls inside W3.)
  The SAME month set is used for each future and its ETF comparator, so comparisons are like-for-like.

PRE-SPECIFIED VARIANTS (4, registered):
  V1  long ZN over W3, non-roll months, 0.25bp/side.
  V2  long ZB over W3, non-roll months, 0.25bp/side.
  V3  long ZT over W3, non-roll months (after (b) ZT is effectively Jan/Mar.. only non-quarter months), 0.25bp/side.
  V4  basis proxy: long IEF minus h*ZN over W3 on V1's months, minus the T-bill rate (^IRX of the prior day /252) on
      the IEF leg (futures are unfunded, so the hedge earns the carry). h = OLS slope of IEF on ZN using non-overlapping
      5-session returns in TRAIN (5-day to soften the 15:00 vs 16:00 close mismatch). Cost 1bp IEF + 0.25bp*h ZN.
      Prediction: > 0 (cash richens vs futures into month-end if the bid is a cash-index flow).
  Benchmarks (registered and labelled 'benchmark', not candidates): long IEF / TLT / SHY over W3 on the same months as
  V1 / V2 / V3. Per-unit-of-rate-risk comparison: net bp per event divided by the instrument's TRAIN beta to IEF
  (non-overlapping 5-session returns), i.e. bp per IEF-equivalent of duration.
Card E8's entry-timing variant (5th-last vs 4th-last) is dropped to keep <= 4 variants (309/310 covered timing).
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "375_bond_bid_futures"
END = "2022-12-31"
cal = lib.get("SPY").index
cal = cal[cal >= "2000-01-01"]
ym = pd.Series(cal.to_period("M"), index=cal)
bwd = pd.Series(1, index=cal)[::-1].groupby(ym[::-1]).cumsum()[::-1]
inW = (bwd <= 3) & (ym != ym.iloc[-1])                     # return days of W3

FUT = {"ZN": "ZN=F", "ZB": "ZB=F", "ZT": "ZT=F"}
ETF = {"ZN": "IEF", "ZB": "TLT", "ZT": "SHY"}
raw = {k: lib.get(s) for k, s in {**FUT, **{v: v for v in ETF.values()}}.items()}
close = pd.DataFrame({k: (d["Close"] if k in FUT else d["aC"]) for k, d in raw.items()}).reindex(cal)
R = close / close.shift(1) - 1                              # close->close on the NYSE calendar
irx = lib.get("^IRX")["Close"].reindex(cal).ffill().shift(1) / 100 / 252   # prior-day bill rate, per day


def switch_days(k):
    v = raw[k]["Volume"].reindex(cal)
    return set(cal[((v / v.shift(1) > 5) & (v > v.rolling(20, min_periods=10).median())).fillna(False)])


def months_ok(k):
    bad = {m for m in ym.unique() if m.month in (2, 5, 8, 11)}
    sw = switch_days(k)
    bad |= {ym[d] for d in cal[inW] if d in sw}
    start = R[k].first_valid_index()
    ok = inW & ~ym.isin(bad) & (cal >= start) & R[k].notna() & R[ETF[k]].notna()
    # require all 3 return days present
    cnt = ok.groupby(ym).transform("sum")
    return ok & (cnt == 3)


def run(w, Rcols, cost):
    """w: DataFrame of weights on return days; returns daily net (costs on turnover)."""
    pos = w.shift(-1).fillna(0.0)                           # decided at prior close, earns on the window day
    return lib.backtest_weights(pos, Rcols, cost, lag=0)


def beta5(y, x):
    a, b = (1 + y.loc[:"2014"]).cumprod(), (1 + x.loc[:"2014"]).cumprod()
    j = pd.concat([a, b], axis=1).dropna().iloc[::5].pct_change().dropna()
    return float(np.polyfit(j.iloc[:, 1], j.iloc[:, 0], 1)[0])


def events(net, ok):
    exitd = ok.shift(1, fill_value=False) & ~ok             # exit-cost day belongs to the previous month's event
    key = ym.where(~exitd, ym.shift(1))
    ev = net.groupby(key).sum()[ok.groupby(ym).any()]
    return ev[ev.index <= pd.Period(END, "M")]


def reg(name, net, ok, note):
    held = (ok | ok.shift(1, fill_value=False)).astype(float)  # window days + exit-cost day
    r = lib.register(EXP, name, held, net, 0.0, note)
    return r


out = []
okZN = months_ok("ZN")
rows = {}
for k, tag in [("ZN", "V1"), ("ZB", "V2"), ("ZT", "V3")]:
    ok = months_ok(k)
    e = ETF[k]
    bF, bE = beta5(R[k], R["IEF"]), beta5(R[e], R["IEF"])
    nF = run(ok.astype(float).to_frame(k), R[[k]], 0.25)
    nE = run(ok.astype(float).to_frame(e), R[[e]], 1.0)
    nEx = nE - ok.astype(float) * irx.fillna(0)             # ETF excess over bills (futures are excess returns)
    rF = reg(f"{tag} long {k}=F last-3 sessions, non-roll months", nF, ok, "0.25bp/side; roll months excluded")
    rE = reg(f"benchmark long {e} last-3, same months as {tag}", nE, ok, "1bp/side; benchmark, not a candidate")
    evF, evE, evEx = events(nF, ok), events(nE, ok), events(nEx, ok)
    rows[tag] = (k, e, bF, bE, rF, rE, evF, evE, evEx)

# V4 basis: IEF - h*ZN, minus bills on IEF leg
h = beta5(R["IEF"], R["ZN"])
okb = okZN.astype(float)
nI = run(okb.to_frame("IEF"), R[["IEF"]], 1.0)
nZ = run((-h * okb).to_frame("ZN"), R[["ZN"]], 0.25)
nB = nI + nZ - okb * irx.fillna(0)
rB = reg(f"V4 basis IEF - {h:.2f}*ZN last-3, minus bills", nB, okZN, "1bp IEF + 0.25bp ZN per side")
evB = events(nB, okZN)
# control: same basis on all other (non-window) sessions, daily, gross
oth = (~inW) & R["IEF"].notna() & R["ZN"].notna() & (cal <= END)
ctrl = (R["IEF"] - h * R["ZN"] - irx)[oth] * 1e4


def tstat(x):
    return x.mean() / x.std() * np.sqrt(len(x))


def split(ev):
    a, b = ev[ev.index.year <= 2014], ev[(ev.index.year >= 2015)]
    return a, b


lines = [f"# 375 - month-end bond bid on Treasury futures (card E8)\n",
         f"Registry: {lib.REGISTRY} (n={lib.n_tests()}); data through {END} only.\n",
         "| variant | n tr/va | train bp/ev (t) | valid bp/ev (t) | beta to IEF | valid bp per IEF-equiv | gate |",
         "|---|---|---|---|---|---|---|"]
for tag, (k, e, bF, bE, rF, rE, evF, evE, evEx) in rows.items():
    for nm, ev, b, r in [(f"{tag} {k}=F", evF * 1e4, bF, rF), (f"bench {e}", evE * 1e4, bE, rE),
                         (f"bench {e} excess of bills", evEx * 1e4, bE, None)]:
        a, v = split(ev)
        g = "" if r is None else str(lib.passes_gate(r))
        lines.append(f"| {nm} | {len(a)}/{len(v)} | {a.mean():+.1f} ({tstat(a):.1f}) | {v.mean():+.1f} ({tstat(v):.1f}) "
                     f"| {b:.2f} | {v.mean() / b:+.1f} | {g} |")
a, v = split(evB * 1e4)
lines.append(f"| V4 basis IEF-{h:.2f}ZN-bills | {len(a)}/{len(v)} | {a.mean():+.1f} ({tstat(a):.1f}) | {v.mean():+.1f} "
             f"({tstat(v):.1f}) | - | - | {lib.passes_gate(rB)} |")
lines.append(f"\nBasis control, non-window sessions, gross bp/day x3: train {ctrl.loc[:'2014'].mean() * 3:+.2f}, "
             f"valid {ctrl.loc['2015':].mean() * 3:+.2f}")
lines.append("\nRegistry rows (lib.fmt):")
for tag, (k, e, bF, bE, rF, rE, *_ ) in rows.items():
    lines.append(f"- {rF['variant']}: {lib.fmt(rF)}")
    lines.append(f"- {rE['variant']}: {lib.fmt(rE)}")
lines.append(f"- {rB['variant']}: {lib.fmt(rB)}")
# yearly check of the futures legs (<=2022)
for tag, (k, e, *_r) in rows.items():
    evF = _r[4]
    y = (evF.groupby(evF.index.year).sum() * 1e4).round(0)
    lines.append(f"\n{tag} {k}=F yearly net bp: {y.astype(int).to_dict()}  positive {(y > 0).sum()}/{len(y)}")
# months used
for tag, (k, *_ ) in rows.items():
    mu = sorted({p.month for p in rows[tag][6].index})
    lines.append(f"{tag} months-of-year used: {mu}")
zt_dec = sorted(str(p) for p in rows["V3"][6].index if p.month == 12)
lines.append(f"ZT Dec months kept (switch not detected inside W3): {zt_dec}; their net bp: "
             f"{(rows['V3'][6][[p for p in rows['V3'][6].index if p.month == 12]] * 1e4).round(1).tolist()}")
print("\n".join(lines))
