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


def ief_fixture() -> pd.Series:
    x = pd.read_csv(FIX / "ief.csv")
    return pd.Series(x["adjclose"].values, index=pd.to_datetime(x["date"]).dt.date)


def zn_path_from_ief(ief: pd.Series, hedge: float, p0: float = 110.0, anchor: dt.date | None = None):
    """ZN price that moves 1/hedge of IEF's adjusted return, stepping at each NYSE close (visible from 15:50 ET).
    Equal to p0 at `anchor` (default: the first date); relative moves only, so the level stays plausible."""
    dates = list(ief.index)
    base = float(ief.loc[max(d for d in dates if d <= anchor)]) if anchor else float(ief.iloc[0])

    def px(now):
        t = now.astimezone(cal.ET)
        i = 0
        lo, hi = 0, len(dates) - 1
        while lo <= hi:                                   # last session whose close-10m is <= t
            mid = (lo + hi) // 2
            d = dates[mid]
            c = cal.session_open_close(d)[1] if cal.is_session(d) else dt.datetime.combine(d, dt.time(16), tzinfo=cal.ET)
            if c - dt.timedelta(minutes=10) <= t:
                i, lo = mid, mid + 1
            else:
                hi = mid - 1
        return p0 * (1 + (float(ief.iloc[i]) / base - 1) / hedge)
    return px


def add_bond_leg(cfg, b, ex, ief: pd.Series | None = None):
    """Enable leg B on a dry-run env: ZN path from IEF, bond monitor on the same data. Returns the Daemon."""
    from .bond import BondExecutor
    from .bond_monitor import run_bond_report
    from .daemon import Daemon
    cfg.bond.enabled = True
    ief = ief_fixture() if ief is None else ief
    clock = b.clock
    b.zn_price_fn = zn_path_from_ief(ief, cfg.bond.hedge_ratio, anchor=clock.now().date())
    rep = lambda d: run_bond_report(cfg, asof=d, ief=ief[[x <= clock.now().date() for x in ief.index]])
    bx = BondExecutor(cfg, b, ex.alert, ex.fetchers, reporter=rep)
    return Daemon([ex, bx]), bx


def dry_run(months=3, start=dt.datetime(2025, 8, 20, 8, 0), verbose=True, bond=True):
    logging.basicConfig(level=logging.WARNING)
    spy, tlt = load_fixture()
    with tempfile.TemporaryDirectory() as tmp:
        cfg, clock, b, ex, sink = make_env(start.replace(tzinfo=cal.ET), tmp, spy, tlt)
        runner = ex
        if bond:
            runner, _ = add_bond_leg(cfg, b, ex)
        end_month = (pd.Timestamp(start) + pd.offsets.MonthBegin(months)).date()
        stop_at = dt.datetime.combine(end_month, dt.time(8, 0), tzinfo=cal.ET)
        runner.run_forever(stop=lambda: clock.now() >= stop_at, lock=False)
        from .state import all_trades
        trades = all_trades(tmp)
        btrades = all_trades(tmp, prefix="bond") if bond else []
        led = paper_ledger(spy, tlt, start=str(start.date()), until=stop_at.date())
        if verbose:
            print(f"dry run {start.date()} -> {stop_at.date()}: {len(b.orders)} orders")
            print(" leg A (MES):")
            for t in trades:
                print(f"  {t['trade_date']} {t['phase']:<8} {t.get('plan', {}).get('action', '-'):<4} "
                      f"qty {t.get('entry_filled', 0):>3} entry {t.get('entry_price', 0) or 0:9.2f} "
                      f"exit {t.get('exit_price', 0) or 0:9.2f} bp {t.get('trade_bp', float('nan')):+7.1f} "
                      f"pnl {t.get('net_pnl', 0) or 0:+10,.0f}  size x{(t.get('plan') or {}).get('vol_scale', 1):g}")
            print("  paper ledger:", ", ".join(f"{d}:{v:+.1f}bp" for d, v in led["paper_bp"].items()))
            if bond:
                from .bond_monitor import bond_ledger
                bl = bond_ledger(ief_fixture(), cfg.bond.hold_sessions, start=str(start.date()), until=stop_at.date())
                print(" leg B (ZN):")
                for t in btrades:
                    p = t.get("plan") or {}
                    print(f"  {p.get('entry_date', '?')} -> {t['trade_date']} {t['phase']:<8} {p.get('contract_local_symbol', '-'):<6} "
                          f"qty {t.get('entry_filled', 0):>3} IEF-equiv bp {(t.get('trade_bp') or float('nan')) * p.get('hedge_ratio', 1):+7.1f} "
                          f"pnl {t.get('net_pnl', 0) or 0:+10,.0f}")
                print("  IEF ledger:", ", ".join(f"{d}:{v:+.1f}bp" for d, v in bl["ief_bp"].items()))
            print("  alerts:")
            for lvl, title, _ in sink:
                print(f"   [{lvl}] {title}")
            print(f"  final net liq {b.account_snapshot().net_liquidation:,.0f}; open futures MES {b.fut_pos} ZN {b.zn_pos}")
        return trades, led, sink, b
