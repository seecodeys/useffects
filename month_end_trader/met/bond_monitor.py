"""Bond-leg decay monitor (leg B). Same SPRT machinery as leg A (monitor.sprt / size_multiplier):

* paper ledger: IEF adjusted close of the entry session E -> adjusted close of the month-end session X, minus
  2bp round trip, for every month since 2002-08
* SPRT from monitoring.start: H1 edge = bond.edge_h1_bp vs H0 = 0, sd = bond.sd_bp (both bp of IEF notional)
* execution gap: live ZN fill result (bp of ZN notional x hedge ratio) vs the paper IEF result
Writes state/bond_monitor.json (read by the bond preflight) and reports/bond_decay_<date>.md.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pandas as pd

from . import calendar as cal
from .bond import entry_session
from .config import Config
from .monitor import size_multiplier, sprt
from .signal import _as_date_index, drop_partial_today, yahoo_daily
from .state import all_trades

COST_BP = 2.0
PROXY = "IEF"


def bond_ledger(ief_adj: pd.Series, hold: int = 3, start="2002-08-01", until: dt.date | None = None) -> pd.DataFrame:
    s = _as_date_index(ief_adj.dropna())
    rows, skipped = [], []
    d = pd.Timestamp(start).date()
    last = until or max(s.index)
    while True:
        X = cal.last_session_of_month(d.year, d.month)
        if X > last:
            break
        E = entry_session(X, hold)
        if E in s.index and X in s.index:
            r = float(s.loc[X] / s.loc[E] - 1)
            rows.append(dict(date=X, entry=E, ief_bp=1e4 * r, paper_bp=1e4 * r - COST_BP))
        else:
            skipped.append((X, "IEF close missing"))
        d = (pd.Timestamp(d.replace(day=1)) + pd.offsets.MonthBegin(1)).date()
    out = pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame(columns=["entry", "ief_bp", "paper_bp"])
    out.attrs["skipped"] = skipped
    return out


def live_rows(cfg: Config) -> pd.DataFrame:
    rows = []
    for t in all_trades(cfg.monitoring.state_dir, prefix="bond"):
        if t.get("phase") == "FLAT" and t.get("trade_bp") is not None:
            p = t.get("plan") or {}
            X = dt.date.fromisoformat(t["trade_date"])
            E = dt.date.fromisoformat(p["entry_date"]) if p.get("entry_date") else X
            # comparable with the paper window only if held for the whole window at the planned times
            comparable = (t.get("exit_reason") == "scheduled close" and not t.get("adopted")
                          and not t.get("pnl_estimated"))
            rows.append(dict(date=X, live_bp=float(t["trade_bp"]) * float(p.get("hedge_ratio", 1.0)),
                             pnl_pct=t.get("pnl_pct_equity"), days=(X - E).days, comparable=comparable,
                             exit_reason=t.get("exit_reason")))
    cols = ["live_bp", "pnl_pct", "days", "comparable", "exit_reason"]
    return pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame(columns=cols)


def run_bond_report(cfg: Config, asof: dt.date | None = None, ief: pd.Series | None = None, write=True) -> dict:
    m, b = cfg.monitoring, cfg.bond
    if ief is None:
        ief = drop_partial_today(yahoo_daily(PROXY, days=365 * 26), dt.datetime.now(cal.ET))["adjclose"]
    asof = asof or max(_as_date_index(ief).index)
    led = bond_ledger(ief, b.hold_sessions, until=asof)
    start = pd.Timestamp(m.start + "-01").date()
    since = led[led.index >= start]
    L, path, A, B, decided = sprt(since["paper_bp"].values, b.edge_h1_bp, b.sd_bp, m.sprt_alpha, m.sprt_beta)
    prev = Path(m.state_dir) / "bond_monitor.json"
    was_dead = False
    if prev.exists():
        try:
            pj = json.loads(prev.read_text())
            was_dead = bool(pj.get("edge_dead", pj.get("size_multiplier", 1.0) == 0.0))
        except Exception:
            was_dead = True
    mult, status = size_multiplier(L, A, B, decided, was_dead)
    L23, *_ = sprt(led.loc[led.index >= dt.date(2023, 1, 1), "paper_bp"].values, b.edge_h1_bp, b.sd_bp,
                   m.sprt_alpha, m.sprt_beta)
    live = live_rows(cfg)
    exec_gap = None
    if len(live):
        j = live[live["comparable"].astype(bool)].join(led[["paper_bp"]], how="inner")
        if len(j):
            # futures earn the return in EXCESS of financing; IEF's adjusted close is a total return
            carry = 1e4 * b.financing_rate * j["days"].astype(float) / 365
            exec_gap = float((j["live_bp"] - (j["paper_bp"] + COST_BP - carry)).mean())
    warnings = []
    if len(led) >= 36 and led["paper_bp"].tail(36).mean() < 0:
        warnings.append("bond bid: last 36 months average below zero")
    if exec_gap is not None and exec_gap < -5:
        warnings.append(f"live ZN fills trail the IEF paper result by {exec_gap:.1f}bp per trade")
    res = dict(asof=str(asof), status=status, size_multiplier=mult if b.auto_derisk else 1.0, edge_dead=(mult == 0.0),
               sprt=dict(score=L, upper=A, lower=B, decided=decided, trades=len(since), since=m.start, score_since_2023=L23),
               paper=dict(trades=len(led), mean_bp=float(led["paper_bp"].mean()) if len(led) else float("nan"),
                          win_pct=float(100 * (led["paper_bp"] > 0).mean()) if len(led) else float("nan"),
                          last12_mean_bp=float(led["paper_bp"].tail(12).mean()) if len(led) else float("nan"),
                          last36_mean_bp=float(led["paper_bp"].tail(36).mean()) if len(led) else float("nan")),
               live_trades=len(live), execution_gap_bp=exec_gap, warnings=warnings,
               skipped_months=[str(d) for d, _ in led.attrs.get("skipped", [])])
    res["summary"] = (f"SPRT {L:+.2f} (bounds {B:+.2f}/{A:+.2f}, {len(since)} trades since {m.start}) -> size "
                      f"x{res['size_multiplier']:g}. Paper: {res['paper']['trades']} months, mean "
                      f"{res['paper']['mean_bp']:+.1f}bp, last 12 {res['paper']['last12_mean_bp']:+.1f}bp"
                      + ("; " + "; ".join(warnings) if warnings else ""))
    if write:
        Path(m.state_dir).mkdir(parents=True, exist_ok=True)
        tmp = Path(m.state_dir) / ".bond_monitor.json.tmp"
        tmp.write_text(json.dumps(res, indent=2, default=str))
        tmp.replace(Path(m.state_dir) / "bond_monitor.json")
        rd = Path(m.report_dir); rd.mkdir(parents=True, exist_ok=True)
        lines = [f"# Bond leg decay report {asof}", "", f"**Status:** {status} (size x{res['size_multiplier']:g})", "",
                 res["summary"], "", "| month-end | entry | IEF bp | paper bp |", "|---|---|---|---|"]
        for d, r in led.tail(24).iterrows():
            lines.append(f"| {d} | {r['entry']} | {r['ief_bp']:+.1f} | {r['paper_bp']:+.1f} |")
        if len(live):
            lines += ["", "## Live trades", "", "| month-end | live bp (IEF-equiv) | P&L % equity |", "|---|---|---|"]
            for d, r in live.iterrows():
                lines.append(f"| {d} | {r['live_bp']:+.1f} | {r['pnl_pct'] if r['pnl_pct'] is not None else ''} |")
        (rd / f"bond_decay_{asof}.md").write_text("\n".join(lines) + "\n")
    return res
