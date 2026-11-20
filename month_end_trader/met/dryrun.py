"""End-to-end dry run: the real daemon loop against the simulated broker on historical data.
The fake clock starts some months back and the daemon runs through several month-ends exactly as it would
live (announce, preflight, entry, hold guards, exit, reconcile, report). Results are compared with the
paper ledger computed independently from the same prices."""
from __future__ import annotations

import datetime as dt
import logging
import tempfile
from pathlib import Path

import pandas as pd

from . import calendar as cal
from .alerts import Alerter
from .config import Config
from .executor import Executor
from .fake_broker import FakeBroker, FakeClock
from .monitor import run_report, paper_ledger

FIX = Path(__file__).resolve().parent.parent / "tests" / "fixtures"


def load_fixture():
    spy = pd.read_csv(FIX / "spy.csv", parse_dates=["date"]).set_index("date")
    tlt = pd.read_csv(FIX / "tlt.csv", parse_dates=["date"]).set_index("date")
    spy.index = spy.index.date; tlt.index = tlt.index.date
    return spy, tlt


def make_env(start: dt.datetime, state_dir: str, spy, tlt, cash=20_000.0, core_shares=None, **kw):
    cfg = Config()
    cfg.ib.account = "DU1234567"
    cfg.monitoring.state_dir = state_dir
    cfg.monitoring.report_dir = state_dir
    cfg.monitoring.log_dir = state_dir
    cfg.monitoring.start = f"{start.year}-{start.month:02d}"
    cfg.signal.sources = ["ibkr", "yahoo"]
    cfg.validate()
    clock = FakeClock(start)
    first_px = float(spy.loc[[d for d in spy.index if d <= start.date()][-1], "close"])
    if core_shares is None:
        core_shares = int(100_000 / first_px)
    b = FakeBroker(clock, cash=cash, core_shares=core_shares, spy_price=first_px, fut_price=first_px * 10,
                   closes={"SPY": pd.Series(spy["adjclose"]), "TLT": pd.Series(tlt["adjclose"])},
                   raw_closes={"SPY": pd.Series(spy["close"]), "TLT": pd.Series(tlt["close"])}, **kw)
    for d in spy.index:
        if d >= start.date():
            b.day_path[d] = (float(spy.at[d, "open"]) * 10, float(spy.at[d, "close"]) * 10, [])
    sink = []
    al = Alerter("paper", sink=sink)

    def yahoo(sym):
        return b.daily_adjusted_closes(sym)       # same data, independent call path

    fetchers = {"ibkr": lambda s: b.daily_adjusted_closes(s), "yahoo": yahoo}
    asof_spy = lambda: spy[[d <= clock.now().date() for d in spy.index]]
    asof_tlt = lambda: tlt[[d <= clock.now().date() for d in tlt.index]]
    rep = lambda d: run_report(cfg, asof=d, spy=asof_spy(), tlt=asof_tlt())
    ex = Executor(cfg, b, al, fetchers, reporter=rep)
    return cfg, clock, b, ex, sink


def dry_run(months=3, start=dt.datetime(2025, 8, 20, 8, 0), verbose=True):
    logging.basicConfig(level=logging.WARNING)
    spy, tlt = load_fixture()
    with tempfile.TemporaryDirectory() as tmp:
        cfg, clock, b, ex, sink = make_env(start.replace(tzinfo=cal.ET), tmp, spy, tlt)
        end_month = (pd.Timestamp(start) + pd.offsets.MonthBegin(months)).date()
        stop_at = dt.datetime.combine(end_month, dt.time(8, 0), tzinfo=cal.ET)
        ex.run_forever(stop=lambda: clock.now() >= stop_at)
        from .state import all_trades
        trades = all_trades(tmp)
        led = paper_ledger(spy, tlt, start=str(start.date()), until=stop_at.date())
        if verbose:
            print(f"dry run {start.date()} -> {stop_at.date()}: {len(b.orders)} orders")
            for t in trades:
                print(f"  {t['trade_date']} {t['phase']:<8} {t.get('plan', {}).get('action', '-'):<4} "
                      f"qty {t.get('entry_filled', 0):>3} entry {t.get('entry_price', 0) or 0:9.2f} "
                      f"exit {t.get('exit_price', 0) or 0:9.2f} bp {t.get('trade_bp', float('nan')):+7.1f} "
                      f"pnl {t.get('net_pnl', 0) or 0:+10,.0f}")
            print("  paper ledger:", ", ".join(f"{d}:{v:+.1f}bp" for d, v in led["paper_bp"].items()))
            print("  alerts:")
            for lvl, title, _ in sink:
                print(f"   [{lvl}] {title}")
            print(f"  final net liq {b.account_snapshot().net_liquidation:,.0f}; open futures {b.fut_pos}")
        return trades, led, sink, b
