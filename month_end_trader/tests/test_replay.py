"""Historical replay: run the real daemon through past month-ends on the simulated broker and check every
trade against the independently computed paper ledger (direction and per-trade bp within slippage).
Default: every 6th month since 2003 (~47 months). MET_SLOW=1 replays all 291 TLT-era month-ends."""
import datetime as dt
import os

import pytest

from met import calendar as cal
from met.monitor import paper_ledger
from met.state import TradeState
from met.dryrun import make_env
from conftest import run_until

ALL = []
d = dt.date(2002, 8, 1)
while d <= dt.date(2026, 10, 1):
    ALL.append(cal.last_session_of_month(d.year, d.month))
    d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
CASES = ALL if os.environ.get("MET_SLOW") == "1" else ALL[6::6]


@pytest.fixture(scope="module")
def ledger(data):
    spy, tlt = data
    return paper_ledger(spy, tlt, start="2002-08-01", until=dt.date(2026, 10, 31))


@pytest.mark.parametrize("T", CASES, ids=str)
def test_replay_month(T, tmp_path, data, ledger):
    spy, tlt = data
    prev = cal.previous_session(T)
    cfg, clock, b, ex, alerts = make_env(dt.datetime.combine(prev, dt.time(15, 0), tzinfo=cal.ET), str(tmp_path), spy, tlt)
    ex.reporter = None
    _, close = cal.session_open_close(T)
    run_until(ex, clock, close + dt.timedelta(minutes=30))
    s = TradeState(str(tmp_path), str(T))
    assert s.phase == "FLAT", (T, s.phase, s.get("blockers"))
    row = ledger.loc[T]
    assert s.get("direction") == row["direction"]
    # bot bp (MES fills with 1 tick slippage each side) vs paper bp (SPY prints, 1bp cost)
    slip_bp = 2 * 0.25 / (10 * float(spy.at[T, "open"])) * 1e4
    assert s.get("trade_bp") == pytest.approx(row["paper_bp"] + 1.0 - slip_bp, abs=0.6), T
    assert b.fut_pos == 0
    assert not [a for a in alerts if a[0] == "CRITICAL"]
