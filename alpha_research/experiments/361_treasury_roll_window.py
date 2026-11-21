"""361 — Treasury-futures quarterly roll window (idea card S1, results/ideas_settlement.md).

Mechanism: asset managers and basis traders roll TY/US futures in the days before First Notice Day (FND) of the
Mar/Jun/Sep/Dec contracts. CBOT rule: FND = last business day of the month before delivery, i.e. the last business
day of Feb/May/Aug/Nov. The roll is a dated transfer of duration exposure; cash-futures basis trades are re-set
against the deferred contract, which may move cash Treasuries in a known window.

Calendar (business days = IEF trading days, a proxy for CBOT/SIFMA business days): day 0 = last business day of the
month. Roll window = returns of days -8..-4 (enter at the close of day -9, exit at the close of day -4). The window
deliberately stops before day -3 so it never overlaps the last-3-session month-end bond bid (LOG 275/276). Control:
the same days in the other 8 months, which also nets out any generic intra-month seasonal.
Delivery window = returns of business days +1..+5 of Mar/Jun/Sep/Dec (enter at the close of the last business day of
the prior month), vs the same days in other months (this nets out the generic early-month reversal of LOG 281/319).

Trade (difference-in-means as a strategy, duration-neutral over the year): weight s*(+1) in each event-month window
and s*(-0.5) in each control-month window (4 x 1 = 8 x 0.5). s = sign of the TRAIN difference in means (two-sided
test; the direction is chosen in train only and fixed for valid). The calendar is known in advance, so weights are
set ahead of time (lag 0 in backtest_weights is not a same-close artefact). Costs 1bp/side (bond ETFs).

Pre-specified variants (fixed before seeing results):
  A: IEF roll window (days -8..-4 of Feb/May/Aug/Nov) vs the same days in other months.
  B: TLT, same windows as A (US/WN roll).
  C: IEF delivery window (days +1..+5 of Mar/Jun/Sep/Dec) vs the same days in other months.
  D: A only in quarters where the previous roll was "rich": the previous roll's jump in Yahoo's ZN=F continuous series
     (ZN=F cc return minus beta*IEF cc on the switch day; switch day = day with the largest volume ratio vs the prior
     day in the second half of the delivery month; beta = trailing 250-day OLS) is above the expanding median of all
     earlier jumps (>=4 prior). The previous roll's jump is observed ~2 months before the next roll window. Control
     legs (-0.5 each) = the two non-roll months preceding each active roll month (e.g. Dec/Jan for Feb).
Registered series: daily net P&L (pos=1 on held days), so bp_per_trade in the registry is per held DAY; per-window
bp is reported from the difference-in-means tables.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib

END = "2022-12-31"          # never look past VALID
EXP = "361_treasury_roll_window"
COST = 1.0
ROLL_M = {2, 5, 8, 11}
DELIV_M = {3, 6, 9, 12}

ief = lib.get("IEF")
tlt = lib.get("TLT")
R = pd.concat({"IEF": ief["cc"], "TLT": tlt["cc"]}, axis=1).loc[:END].dropna()
days = R.index
ym = days.to_period("M")
pos_in_m = pd.Series(np.arange(len(days)), index=days).groupby(ym).transform(lambda x: x - x.iloc[-1])  # 0 = last
pos_fwd = pd.Series(np.arange(len(days)), index=days).groupby(ym).transform(lambda x: x - x.iloc[0] + 1)  # 1 = first

# ---- event windows: list of (month period, list of return dates)
def roll_windows():
    out = []
    for p, g in pos_in_m.groupby(ym):
        if len(g) < 15:
            continue
        rd = g.index[(g.values >= -8) & (g.values <= -4)]
        out.append((p, rd))
    return out


def deliv_windows():
    out = []
    for p, g in pos_fwd.groupby(ym):
        rd = g.index[(g.values >= 1) & (g.values <= 5)]
        if len(rd) == 5:
            out.append((p, rd))
    return out


def window_ret(sym, rd):
    return (1 + R[sym].loc[rd]).prod() - 1


def build_events(wins, sym, event_months):
    rows = []
    for p, rd in wins:
        rows.append(dict(month=str(p), m=p.month, year=p.year, event=p.month in event_months,
                         start=rd[0].date(), end=rd[-1].date(), ret_bp=1e4 * window_ret(sym, rd)))
    return pd.DataFrame(rows)


def welch(a, b):
    d = a.mean() - b.mean()
    se = np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))
    return d, d / se


def diff_table(ev, label):
    out = {}
    for name, (a, b) in lib.PERIODS.items():
        e = ev[(ev.year >= int(a[:4])) & (ev.year <= int(b[:4]))]
        x, y = e[e.event].ret_bp, e[~e.event].ret_bp
        d, t = welch(x, y)
        out[name] = dict(n_ev=len(x), n_ctl=len(y), ev_bp=x.mean(), ctl_bp=y.mean(), diff_bp=d, welch_t=t)
    print(f"\n{label}: difference in means (bp per window)")
    print(pd.DataFrame(out).T.round(2).to_string())
    return out


def weights(wins, sym, event_set, s):
    """wins: list of (period, return dates); event_set: set of periods treated as event (+1), others in wins -0.5."""
    W = pd.DataFrame(0.0, index=days, columns=R.columns)
    for p, rd in wins:
        i0 = days.get_loc(rd[0])
        if i0 == 0:
            continue
        hold = days[i0 - 1: days.get_loc(rd[-1])]          # decided at close of day before first return date
        W.loc[hold, sym] = s * (1.0 if p in event_set else -0.5)
    return W


def run(variant, wins, sym, event_set, notes):
    ev = pd.DataFrame([dict(month=str(p), m=p.month, year=p.year, event=p in event_set,
                            start=rd[0].date(), end=rd[-1].date(), ret_bp=1e4 * window_ret(sym, rd))
                       for p, rd in wins])
    tab = diff_table(ev, f"{variant} ({sym})")
    s = np.sign(tab["train"]["diff_bp"]) or 1.0
    W = weights(wins, sym, event_set, s)
    pnl = lib.backtest_weights(W, R, COST, lag=0)
    pos = (pnl != 0).astype(float)                         # register a unit-exposure P&L series as pos*ret
    row = lib.register(EXP, variant, pos, pnl, cost_bp=0.0, notes=f"{notes}; sign s={s:+.0f}; net of 1bp/side")
    # per-trade net bp (per unit of event exposure): mean window P&L
    yr = pnl.groupby(pnl.index.year).sum() * 1e4
    print(f"  sign s={s:+.0f}  {lib.fmt(row)}  gate={lib.passes_gate(row)}")
    print("  yearly net bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items()))
    ev["variant"] = variant; ev["sym"] = sym; ev["sign"] = s
    return row, ev, tab


rw = roll_windows()
dw = deliv_windows()
roll_set = {p for p, _ in rw if p.month in ROLL_M}
deliv_set = {p for p, _ in dw if p.month in DELIV_M}

results, evs = {}, []
for v, wins, sym, es, note in [("A_IEF_roll", rw, "IEF", roll_set, "days -8..-4 of Feb/May/Aug/Nov vs other months"),
                               ("B_TLT_roll", rw, "TLT", roll_set, "days -8..-4 of Feb/May/Aug/Nov vs other months"),
                               ("C_IEF_delivery", dw, "IEF", deliv_set, "days +1..+5 of Mar/Jun/Sep/Dec vs other")]:
    row, ev, tab = run(v, wins, sym, es, note)
    results[v] = (row, tab); evs.append(ev)

# ---- D: roll richness from ZN=F continuous series
zn = lib.get("ZN=F")
zr = pd.concat([zn["cc"], ief["cc"]], axis=1, keys=["z", "i"]).dropna()
beta = (zr["z"].rolling(250).cov(zr["i"]) / zr["i"].rolling(250).var()).shift(1)
vr = (zn["Volume"] / zn["Volume"].shift(1)).reindex(zr.index)
jumps = []
for p, g in zr.groupby(zr.index.to_period("M")):
    if p.month not in DELIV_M or p.year > 2022:
        continue
    cand = g.index[g.index.day >= 10]
    v = vr.loc[cand].dropna()
    if v.empty or not np.isfinite(beta.get(v.idxmax(), np.nan)):
        continue
    d = v.idxmax()
    jumps.append(dict(deliv=str(p), switch=d.date(), vol_ratio=v.max(),
                      jump_bp=1e4 * (zr.at[d, "z"] - beta[d] * zr.at[d, "i"])))
J = pd.DataFrame(jumps)
J["med_prior"] = J["jump_bp"].expanding().median().shift(1)
J["n_prior"] = np.arange(len(J))
J["rich"] = (J["jump_bp"] > J["med_prior"]) & (J["n_prior"] >= 4)
print("\nZN=F switch-day detection (first rows):")
print(J.head(8).to_string(index=False))
print(f"median vol ratio on detected switch days: {J.vol_ratio.median():.1f}; rich share {J.rich.mean():.2f}")

# next roll month after each delivery month p (e.g. Mar -> May); D active for that roll month + its 2 preceding months
rich_next = {}
for _, r in J.iterrows():
    p = pd.Period(r["deliv"], "M")
    rich_next[p + 2] = bool(r["rich"])
d_event = {p for p in roll_set if rich_next.get(p, False)}
d_ctl = {q for p in d_event for q in (p - 1, p - 2)}
wins_d = [(p, rd) for p, rd in rw if p in d_event or p in d_ctl]
row, ev, tab = run("D_IEF_roll_rich", wins_d, "IEF", d_event, "A restricted to quarters after a rich ZN=F roll jump")
results["D_IEF_roll_rich"] = (row, tab); evs.append(ev)

out = pd.concat(evs, ignore_index=True)
out = out[out.year <= 2022]
out.to_csv(os.path.join(lib.HERE, "results", "361_events.csv"), index=False)
J.to_csv(os.path.join(lib.HERE, "results", "361_zn_jumps.csv"), index=False)
print("\nN registered (this agent):", lib.n_tests())
