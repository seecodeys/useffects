"""Forward paper-trading ledger for the two findings — a true out-of-sample record, committed to git BEFORE each event.

  python tools/paper_ledger.py plan     # add next month-end's planned trades (state as of the latest close)
  python tools/paper_ledger.py settle   # fill realized returns for planned trades whose dates have passed

Trades (as specified in README playbook):
  EQ  : SPY open->close on the last trading day, side = -sign(SPY MTD - TLT MTD) at the prior close; size 1.0 if SPY 20d
        vol > 2002-14 median else 0.5 (168 tilt). NOTE: the sign is provisional until the prior close; `plan` re-run
        on the day before the last trading day finalizes it (the row is updated, with a new timestamp).
  BOND: IEF long from the close before the 3rd-last session through the last session's close.
Costs: 0.5bp/side SPY, 1bp/side IEF."""
import os, sys, datetime as dt
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
import lib, signals
LEDGER = os.path.join(HERE, "results", "paper_trades.csv")
COLS = ["trade", "month", "entry", "exit", "side", "size", "gap_at_plan", "planned_at", "data_through", "final", "ret_bp", "settled_at"]

def load():
    return pd.read_csv(LEDGER, dtype=str) if os.path.exists(LEDGER) else pd.DataFrame(columns=COLS)

def sessions_back(ltd, n):
    d, out = ltd, [ltd]
    while len(out) < n + 1:
        d -= pd.Timedelta(days=1)
        if d.weekday() < 5 and not signals.nyse_closed(d): out.append(d)
    return out          # [ltd, ltd-1, ..., ltd-n]

def plan():
    spy, tlt = lib.get("SPY", refresh=True), lib.get("TLT", refresh=True)
    last = spy.index[-1]
    y, m = last.year, last.month
    ltd = signals.last_trading_day(y, m)
    if ltd <= last:
        y, m = (y + (m == 12), m % 12 + 1); ltd = signals.last_trading_day(y, m)
    first = pd.Timestamp(y, m, 1)
    base = lambda d: d.aC[d.index < first].iloc[-1]
    gap = (spy.aC.iloc[-1] / base(spy) - 1) - (tlt.aC.iloc[-1] / base(tlt) - 1)
    vol = spy.cc.iloc[-20:].std(); med = spy.cc.loc["2002":"2014"].rolling(20).std().median()
    s = sessions_back(ltd, 3)
    final = sessions_back(ltd, 1)[1] == last          # data through the session before the last trading day
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [{"trade": "EQ", "month": f"{y}-{m:02d}", "entry": f"{ltd.date()} open", "exit": f"{ltd.date()} close",
             "side": "LONG" if gap < 0 else "SHORT", "size": "1.0" if vol > med else "0.5", "gap_at_plan": f"{gap:+.4f}",
             "planned_at": now, "data_through": str(last.date()), "final": str(final), "ret_bp": "", "settled_at": ""},
            {"trade": "BOND", "month": f"{y}-{m:02d}", "entry": f"{s[3].date()} close", "exit": f"{ltd.date()} close", "side": "LONG",
             "size": "1.0", "gap_at_plan": "", "planned_at": now, "data_through": str(last.date()), "final": str(last <= s[3]),
             "ret_bp": "", "settled_at": ""}]
    L = load()
    for r in rows:
        k = (L.trade == r["trade"]) & (L.month == r["month"])
        if k.any() and L.loc[k, "ret_bp"].fillna("").iloc[0] != "": continue        # already settled
        if k.any() and r["trade"] == "BOND" and L.loc[k, "final"].iloc[0] == "True": continue
        L = pd.concat([L[~k], pd.DataFrame([r])], ignore_index=True)
    L[COLS].to_csv(LEDGER, index=False); print(L[COLS].tail(6).to_string(index=False))

def settle():
    L = load(); spy, ief = lib.get("SPY", refresh=True), lib.get("IEF", refresh=True)
    for i, r in L.iterrows():
        if str(r.ret_bp) not in ("", "nan"): continue
        e = pd.Timestamp(r.exit.split()[0])
        if e not in spy.index: continue
        sign = 1 if r.side == "LONG" else -1
        if r.trade == "EQ":
            ret = sign * spy.loc[e, "oc"] - 2 * 0.5e-4
        else:
            en = pd.Timestamp(r.entry.split()[0]); ret = ief.aC.loc[e] / ief.aC.loc[en] - 1 - 2 * 1e-4
        L.loc[i, "ret_bp"] = f"{1e4 * ret * float(r['size']):+.1f}"
        L.loc[i, "settled_at"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    L[COLS].to_csv(LEDGER, index=False); print(L[COLS].to_string(index=False))

if __name__ == "__main__":
    {"plan": plan, "settle": settle}[sys.argv[1]]()
