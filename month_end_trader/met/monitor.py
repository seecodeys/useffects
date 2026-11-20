"""Decay monitor. Run after each trade (and on demand).

* paper ledger: the rule's result from official SPY open/close prints for every month-end (2002-08 onward)
* SPRT from monitoring.start: H1 edge = edge_h1_bp vs H0 edge = 0, per-trade sd = sd_bp
      score >= ln((1-b)/a)  -> 'edge alive'
      score <= ln(b/(1-a))  -> 'edge dead'      -> size x0  (if auto_derisk)
      score <= half of that -> 'weakening'      -> size x0.5
* response slope: last-day SPY open->close regressed on the gap (expanding window, plus last 36 trades)
* leading indicators (last 24 trades): signed overnight gap into T, signed T-1 open->close, signed TLT on T
* execution gap: live fill result vs paper result, from the trade state files
Outputs reports/decay_<date>.md + .png and state/monitor.json (read by preflight for auto de-risking).
"""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from . import calendar as cal
from .config import Config
from .signal import compute_signal, yahoo_daily, DataError
from .state import all_trades

COST_BP = 1.0   # round-trip cost assumed for the paper ledger (MES ~1bp)


def paper_ledger(spy: pd.DataFrame, tlt: pd.DataFrame, start="2002-08-01", until: dt.date | None = None) -> pd.DataFrame:
    """spy/tlt: DataFrames with open, close, adjclose indexed by date."""
    rows = []
    s0 = pd.Timestamp(start).date()
    last = until or max(spy.index)
    d = s0
    while True:
        T = cal.last_session_of_month(d.year, d.month)
        if T > last:
            break
        if T in spy.index:
            try:
                sig = compute_signal(T, spy["adjclose"], tlt["adjclose"], "yahoo")
                o, c = spy.at[T, "open"], spy.at[T, "close"]
                t1 = dt.date.fromisoformat(sig.signal_through)
                row = dict(date=T, gap=sig.gap, direction=sig.direction,
                           oc_bp=1e4 * (c / o - 1),
                           paper_bp=sig.direction * 1e4 * (c / o - 1) - COST_BP,
                           gap_in_bp=sig.direction * 1e4 * (o / spy.at[t1, "close"] - 1) if t1 in spy.index else np.nan,
                           t1_oc_bp=sig.direction * 1e4 * (spy.at[t1, "close"] / spy.at[t1, "open"] - 1) if t1 in spy.index else np.nan,
                           tlt_bp=-sig.direction * 1e4 * (tlt.at[T, "close"] / tlt.at[T, "open"] - 1) if T in tlt.index else np.nan)
                rows.append(row)
            except DataError:
                pass
        d = (pd.Timestamp(d.replace(day=1)) + pd.offsets.MonthBegin(1)).date()
    return pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame()


def sprt(x_bp, h1_bp, sd_bp, alpha, beta):
    A, B = math.log((1 - beta) / alpha), math.log(beta / (1 - alpha))
    path, L, decided = [], 0.0, None
    for v in x_bp:
        L += (v * h1_bp - h1_bp ** 2 / 2) / sd_bp ** 2
        path.append(L)
        if decided is None and (L >= A or L <= B):
            decided = "edge alive" if L >= A else "edge dead"
    return L, path, A, B, decided


def size_multiplier(L, A, B, decided):
    if decided == "edge dead" or L <= B:
        return 0.0, "edge dead (SPRT lower boundary): stop"
    if L <= B / 2:
        return 0.5, "weakening (SPRT past half the lower boundary): half size"
    if decided == "edge alive" or L >= A:
        return 1.0, "edge alive (SPRT upper boundary)"
    return 1.0, "undecided (keep normal size)"


def slope(df):
    x, y = df["gap"].values, df["oc_bp"].values
    if len(x) < 6:
        return float("nan"), float("nan")
    X = np.column_stack([np.ones_like(x), x * 100])
    b, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ b
    se = math.sqrt(resid @ resid / (len(x) - 2) / ((x * 100 - (x * 100).mean()) ** 2).sum())
    return float(b[1]), float(b[1] / se)      # bp of SPY move per 1% of gap; t-stat


def live_rows(cfg: Config) -> pd.DataFrame:
    rows = []
    for t in all_trades(cfg.monitoring.state_dir):
        if t.get("phase") == "FLAT" and t.get("trade_bp") is not None:
            rows.append(dict(date=dt.date.fromisoformat(t["trade_date"]), live_bp=t["trade_bp"],
                             net_pnl=t.get("net_pnl"), pnl_pct_equity=t.get("pnl_pct_equity"),
                             exit_reason=t.get("exit_reason")))
    return pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame(columns=["live_bp"])


def run_report(cfg: Config, asof: dt.date | None = None, spy=None, tlt=None, write=True) -> dict:
    m = cfg.monitoring
    if spy is None:
        spy = yahoo_daily(cfg.signal.equity, days=365 * 26)
        tlt = yahoo_daily(cfg.signal.bond, days=365 * 26)
    asof = asof or max(spy.index)
    led = paper_ledger(spy, tlt, until=asof)
    start = pd.Timestamp(m.start + "-01").date()
    live_start = led[led.index >= start]
    L, path, A, B, decided = sprt(live_start["paper_bp"].values, m.edge_h1_bp, m.sd_bp, m.sprt_alpha, m.sprt_beta)
    mult, status = size_multiplier(L, A, B, decided)
    L23, *_ = sprt(led.loc[led.index >= dt.date(2023, 1, 1), "paper_bp"].values, m.edge_h1_bp, m.sd_bp, m.sprt_alpha, m.sprt_beta)
    b_all, t_all = slope(led)
    b_36, t_36 = slope(led.tail(36))
    last24 = led.tail(24)
    lead = {k: (float(last24[k].mean()), float(last24[k].mean() / last24[k].std() * math.sqrt(len(last24))) if last24[k].std() > 0 else float("nan"))
            for k in ("paper_bp", "gap_in_bp", "t1_oc_bp", "tlt_bp")}
    live = live_rows(cfg)
    exec_gap = None
    if len(live):
        j = live.join(led[["paper_bp"]], how="inner")
        if len(j):
            exec_gap = float((j["live_bp"] - (j["paper_bp"] + COST_BP)).mean())
    warnings = []
    if lead["gap_in_bp"][1] > 2: warnings.append("overnight gap into T now moves in the signal direction (pre-positioning)")
    if lead["t1_oc_bp"][1] > 2: warnings.append("day before month-end now moves in the signal direction (front-running)")
    if not math.isnan(t_36) and b_36 > -2 and b_all < -2: warnings.append("response slope over the last 36 trades has collapsed")
    if exec_gap is not None and exec_gap < -3: warnings.append(f"live fills trail paper by {exec_gap:.1f}bp per trade")
    res = dict(asof=str(asof), status=status, size_multiplier=mult if cfg.guardrails.auto_derisk else 1.0,
               sprt=dict(score=L, upper=A, lower=B, decided=decided, trades=len(live_start), since=m.start,
                         score_since_2023=L23),
               paper=dict(trades=len(led), mean_bp=float(led["paper_bp"].mean()),
                          last12_mean_bp=float(led["paper_bp"].tail(12).mean()),
                          last36_mean_bp=float(led["paper_bp"].tail(36).mean())),
               slope=dict(all=b_all, all_t=t_all, last36=b_36, last36_t=t_36),
               leading_last24={k: dict(mean_bp=v[0], t=v[1]) for k, v in lead.items()},
               execution_gap_bp=exec_gap, warnings=warnings)
    res["summary"] = (f"SPRT {L:+.2f} (bounds {B:+.2f}/{A:+.2f}, {len(live_start)} trades since {m.start}) -> size x{res['size_multiplier']:g}. "
                      f"Paper mean last 36: {res['paper']['last36_mean_bp']:+.1f}bp. Slope last 36: {b_36:+.1f}bp per 1% gap (t {t_36:+.1f}). "
                      + (" WARN: " + "; ".join(warnings) if warnings else ""))
    if write:
        Path(m.state_dir).mkdir(parents=True, exist_ok=True)
        (Path(m.state_dir) / "monitor.json").write_text(json.dumps(res, indent=2, default=str))
        led.to_csv(Path(m.report_dir) / "paper_ledger.csv")
        _write_md(cfg, res, led, live, path, A, B, asof)
    return res


def _write_md(cfg, res, led, live, path, A, B, asof):
    rd = Path(cfg.monitoring.report_dir)
    lines = [f"# Decay monitor, {asof}", "", f"**Status:** {res['status']}  ", f"**Size multiplier:** x{res['size_multiplier']:g}", "",
             "| Metric | Value |", "|---|---|",
             f"| SPRT score since {res['sprt']['since']} | {res['sprt']['score']:+.2f} (dead <= {B:+.2f}, alive >= {A:+.2f}), {res['sprt']['trades']} trades |",
             f"| SPRT score since 2023 (info) | {res['sprt']['score_since_2023']:+.2f} |",
             f"| Paper mean, all {res['paper']['trades']} trades | {res['paper']['mean_bp']:+.1f}bp |",
             f"| Paper mean, last 12 / 36 | {res['paper']['last12_mean_bp']:+.1f} / {res['paper']['last36_mean_bp']:+.1f}bp |",
             f"| Slope (bp per 1% gap), all / last 36 | {res['slope']['all']:+.1f} (t {res['slope']['all_t']:+.1f}) / {res['slope']['last36']:+.1f} (t {res['slope']['last36_t']:+.1f}) |",
             ]
    for k, lab in [("gap_in_bp", "Overnight gap into T (signed)"), ("t1_oc_bp", "T-1 open->close (signed)"), ("tlt_bp", "TLT on T, rebalancing direction")]:
        v = res["leading_last24"][k]
        lines.append(f"| {lab}, last 24 | {v['mean_bp']:+.1f}bp (t {v['t']:+.1f}) |")
    eg = res["execution_gap_bp"]
    lines.append("| Live minus paper (execution) | " + ("n/a" if eg is None else f"{eg:+.1f}bp") + " |")
    if res["warnings"]:
        lines += ["", "**Warnings:**"] + [f"- {w}" for w in res["warnings"]]
    if len(live):
        lines += ["", "## Live trades", "", "```", live.to_string(), "```"]
    (rd / f"decay_{asof}.md").write_text("\n".join(lines) + "\n")
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4))
        c = led["paper_bp"].cumsum()
        a1.step(pd.to_datetime(c.index), c.values, where="post", color="#2a78d6", lw=1.6)
        a1.set_title("Paper result, cumulative net bp (1x)", loc="left", fontsize=9); a1.grid(axis="y", color="#e6e5e0")
        a2.plot(range(1, len(path) + 1), path, color="#eb6834", lw=1.6, marker="o", ms=3)
        for y, lab in [(A, "alive"), (B, "dead"), (B / 2, "half size")]:
            a2.axhline(y, color="#8a8a85", lw=0.8, ls="--"); a2.text(0.5, y, lab, fontsize=7, color="#6b6a64", va="bottom")
        a2.set_title(f"SPRT since {cfg.monitoring.start}", loc="left", fontsize=9); a2.set_xlabel("trades")
        for a in (a1, a2):
            a.spines[["top", "right"]].set_visible(False)
        fig.tight_layout(); fig.savefig(rd / f"decay_{asof}.png", dpi=150); plt.close(fig)
    except Exception:
        pass
