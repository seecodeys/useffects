"""342 - Mutual-fund October-31 fiscal year-end tax-loss selling (idea card B9; Gibson, Safieddine & Titman JFQA).
Most US mutual funds have an Oct-31 fiscal year end; they realise losses in YTD losers before Oct 31, and the selling
pressure ends at Oct 31 -> YTD losers should rebound vs YTD winners in early November.
Universe (card ii, survivorship-free ETFs with long history): 9 sector SPDRs (XLK XLF XLE XLV XLY XLP XLI XLB XLU),
IWM, and the 20 iShares single-country ETFs from 062. Names enter once they have a full YTD window.
Signal: YTD return = adjusted close on the last trading day <= Oct 20 divided by the last close of the prior year
(known 7+ trading days before any entry). Quintiles: bottom/top floor(N/5) names (N = available names), equal weight,
dollar-neutral (+1 losers / -1 winners gross 2). Close->close with lib.backtest_weights(lag=1): the target weights are
stamped one day before the intended holding start so the realised hold is exactly the windows below. 2bp/side.
Not a duplicate of 046 (Dec/Jan IWM-SPY).
Pre-specified variants (max 4, no tuning after):
  A rebound: long bottom - top YTD quintile from last-October close to last-November close.
  B pressure leg: long TOP - bottom (i.e. short losers) from Oct-20-signal close (first close after signal day) to
    last-October close.
  C as A but held only through the 10th trading day of November.
  D placebo: YTD to Jun 20, long bottom - top from last-June close to last-July close (expected ~0).
Train 2000-2014 / valid 2015-2022; nothing after 2022-12-31 is read. Year-by-year event P&L reported.
Timing benchmark: the same YTD-loser-minus-winner basket (Oct-20 ranking) held all year (exposure-matched average)."""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

END = "2022-12-31"
SECT = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "IWM"]
CTRY = ["EWJ", "EWG", "EWU", "EWH", "EWA", "EWS", "EWC", "EWW", "EWZ", "EWY", "EWT", "EWP", "EWI", "EWQ", "EWL",
        "EWN", "EWD", "EWK", "EWO", "EWM"]
U = SECT + CTRY
P = pd.DataFrame({s: lib.get(s).aC for s in U}).loc["1998-01-01":END]
R = P.pct_change()
days = P.index
EXP = "342_oct31_fund_taxloss"


def last_le(d):
    i = days.searchsorted(pd.Timestamp(d), side="right") - 1
    return days[i]


def month_last(y, m):
    x = days[(days.year == y) & (days.month == m)]
    return x[-1] if len(x) else None


def ranks(y, cutoff):            # cutoff 'MM-DD'
    d0 = last_le(f"{y-1}-12-31"); d1 = last_le(f"{y}-{cutoff}")
    ytd = (P.loc[d1] / P.loc[d0] - 1).dropna()
    k = max(1, len(ytd) // 5)
    s = ytd.sort_values()
    return list(s.index[:k]), list(s.index[-k:]), d1, len(ytd)


def build(windows):
    """windows: list of (start_close, end_close, losers, winners, sign). Position held over (start, end]."""
    H = pd.DataFrame(0.0, index=days, columns=U)
    for st, en, lo, wi, sg in windows:
        m = (days >= st) & (days < en)
        H.loc[m, lo] += sg / len(lo); H.loc[m, wi] -= sg / len(wi)
    W = H.shift(-1).fillna(0.0)            # lag=1 adds one extra day of delay; stamp one day earlier
    net = lib.backtest_weights(W, R, 2.0, lag=1)
    gross = lib.backtest_weights(W, R, 0.0, lag=1)
    held = H.shift(1).abs().sum(axis=1) > 0                       # days whose return the position earns
    return net, gross, held


def nth_after(d, n):
    i = days.get_loc(d); return days[min(i + n, len(days) - 1)]


rows = []; ev = {}
for tag, desc in [("A", "rebound Oct-end->Nov-end"), ("B", "pressure Oct20->Oct-end (short losers)"),
                  ("C", "rebound Oct-end->Nov day10"), ("D", "placebo Jun-end->Jul-end")]:
    win = []
    for y in range(2000, 2023):
        if tag == "D":
            lo, wi, d1, n = ranks(y, "06-20"); st, en, sg = month_last(y, 6), month_last(y, 7), 1
        else:
            lo, wi, d1, n = ranks(y, "10-20")
            if tag == "A": st, en, sg = month_last(y, 10), month_last(y, 11), 1
            if tag == "B": st, en, sg = nth_after(d1, 1), month_last(y, 10), -1
            if tag == "C": st = month_last(y, 10); en, sg = nth_after(st, 10), 1
        assert d1 < st
        win.append((st, en, lo, wi, sg))
    net, gross, held = build(win)
    net, held = net.loc["2000":END], held.loc["2000":END]
    r = lib.register(EXP, f"342{tag} {desc} (lag1, 2bp/side, L/S quintiles)", held.astype(float), net, 0.0,
                     notes="close->close, cost already in ret; univ 9 sectors+IWM+20 countries")
    r["gate"] = lib.passes_gate(r); r["tag"] = tag
    # per-event P&L (compounded net over window)
    e = {}
    for st, en, lo, wi, sg in win:
        m = (net.index > st) & (net.index <= en)
        e[st.year] = 1e4 * net[m].sum()
    ev[tag] = pd.Series(e)
    rows.append(r)

# timing benchmark: Oct-20 loser-minus-winner basket held every day of the following 12 months (Nov..Oct)
win = []
for y in range(2000, 2022):
    lo, wi, d1, n = ranks(y, "10-20"); win.append((month_last(y, 10), month_last(y + 1, 10), lo, wi, 1))
lo, wi, d1, n = ranks(2022, "10-20"); win.append((month_last(2022, 10), days[-1], lo, wi, 1))
_, gb, hb = build(win)
gb = gb.loc["2000":END]
for p, (a, b) in lib.PERIODS.items():
    print(f"benchmark (same basket held all year, gross) {p}: {1e4*gb.loc[a:b][hb.loc[a:b]].mean():+.2f} bp/day")

print("\nregistered (net):")
for r in rows:
    print(r["tag"], lib.fmt(r), f"train_bp={r['train_bp_per_trade']:+.2f}  gate={r['gate']}")
E = pd.DataFrame(ev)
print("\nper-event net bp (year):"); print(E.round(0).to_string())
for p, (a, b) in [("train", (2000, 2014)), ("valid", (2015, 2022))]:
    s = E.loc[a:b]
    print(p, "mean bp/event", s.mean().round(1).to_dict(), " t", (s.mean() / s.std() * np.sqrt(len(s))).round(2).to_dict(),
          " %pos", (100 * (s > 0).mean()).round(0).to_dict())
print("N registered in this file:", lib.n_tests())
E.to_csv(os.path.join(lib.HERE, "results", "342_events.csv"))
