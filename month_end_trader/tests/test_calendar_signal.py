import datetime as dt

import numpy as np
import pandas as pd
import pytest

from met import calendar as cal
from met.signal import compute_signal, signals_from_sources, DataError, drop_partial_today
from pathlib import Path

FIX = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize("y,m,expected", [
    (2026, 11, dt.date(2026, 11, 30)),
    (2026, 12, dt.date(2026, 12, 31)),
    (2025, 11, dt.date(2025, 11, 28)),    # Black Friday half day is the last session
    (2024, 11, dt.date(2024, 11, 29)),
    (2027, 5, dt.date(2027, 5, 28)),      # 31 May 2027 is Memorial Day
    (2026, 1, dt.date(2026, 1, 30)),      # 31 Jan 2026 is Saturday
    (2023, 3, dt.date(2023, 3, 31)),
    (2012, 10, dt.date(2012, 10, 31)),    # Hurricane Sandy closed 29-30 Oct, reopened 31st
])
def test_last_session(y, m, expected):
    assert cal.last_session_of_month(y, m) == expected


def test_early_close_and_times():
    assert cal.is_early_close(dt.date(2025, 11, 28))
    o, c = cal.session_open_close(dt.date(2025, 11, 28))
    assert (o.hour, o.minute, c.hour, c.minute) == (9, 30, 13, 0)
    assert not cal.is_early_close(dt.date(2026, 11, 30))
    o, c = cal.session_open_close(dt.date(2026, 7, 31))       # summer time handled
    assert (o.hour, c.hour) == (9, 16) and o.utcoffset() == dt.timedelta(hours=-4)


def test_next_trade_date():
    assert cal.next_trade_date(dt.date(2026, 11, 20)) == dt.date(2026, 11, 30)
    assert cal.next_trade_date(dt.date(2026, 11, 30)) == dt.date(2026, 11, 30)
    assert cal.next_trade_date(dt.date(2026, 12, 1)) == dt.date(2026, 12, 31)
    assert cal.prior_month_last_session(dt.date(2026, 11, 30)) == dt.date(2026, 10, 30)
    assert cal.previous_session(dt.date(2025, 11, 28)) == dt.date(2025, 11, 26)   # Thanksgiving skipped


def test_signal_matches_backtest_every_month(data):
    """The live signal code must reproduce the research backtest on every TLT-era month (2002-08..2026-10)."""
    spy, tlt = data
    bt = pd.read_csv(FIX / "backtest_signals.csv", parse_dates=["date"])
    bt = bt[bt.bond_source == "TLT"]
    assert len(bt) >= 290
    mism = []
    for _, r in bt.iterrows():
        s = compute_signal(r.date.date(), spy["adjclose"], tlt["adjclose"], "fixture")
        if s.direction != int(r.dir) or abs(s.gap - r.gap) > 2e-5:
            mism.append((r.date.date(), s.direction, r.dir, s.gap, r.gap))
    assert not mism, mism[:5]


def test_signal_requires_exact_data(data):
    spy, tlt = data
    d = dt.date(2026, 10, 30)
    with pytest.raises(DataError):
        compute_signal(d, spy["adjclose"].drop(index=dt.date(2026, 10, 29)), tlt["adjclose"])
    with pytest.raises(DataError):
        compute_signal(d, spy["adjclose"], tlt["adjclose"].drop(index=dt.date(2026, 9, 30)))


def test_current_signal_is_long(data):
    """As of the 19 Nov 2026 close the study said LONG; with data through 25 Nov unknown we check the
    computation path on an available date instead: Oct 2026 was LONG (gap -4.3%)."""
    spy, tlt = data
    s = compute_signal(dt.date(2026, 10, 30), spy["adjclose"], tlt["adjclose"])
    assert s.side == "LONG" and s.gap == pytest.approx(-0.0431, abs=5e-4)


def test_sources_disagree_blocks(data):
    spy, tlt = data
    d = dt.date(2026, 10, 30)
    bad_tlt = tlt["adjclose"].copy(); bad_tlt.loc[dt.date(2026, 10, 29)] *= 0.9   # force opposite sign
    f = {"ibkr": lambda s: spy["adjclose"] if s == "SPY" else tlt["adjclose"],
         "yahoo": lambda s: spy["adjclose"] if s == "SPY" else bad_tlt}
    sig, problems, sigs = signals_from_sources(d, f, "SPY", "TLT", require_agreement=True)
    assert sig is None and any("disagree" in p for p in problems)
    sig, problems, _ = signals_from_sources(d, f, "SPY", "TLT", require_agreement=False)
    assert sig is not None


def test_one_source_down_still_works(data):
    spy, tlt = data
    def boom(s): raise RuntimeError("down")
    f = {"ibkr": boom, "yahoo": lambda s: spy["adjclose"] if s == "SPY" else tlt["adjclose"]}
    sig, problems, _ = signals_from_sources(dt.date(2026, 10, 30), f, "SPY", "TLT")
    assert sig.source == "yahoo" and any("ibkr failed" in p for p in problems)


def test_drop_partial_today():
    idx = [dt.date(2026, 11, 18), dt.date(2026, 11, 19)]
    df = pd.DataFrame({"adjclose": [1.0, 2.0]}, index=idx)
    during = dt.datetime(2026, 11, 19, 12, 0, tzinfo=cal.ET)
    after = dt.datetime(2026, 11, 19, 17, 0, tzinfo=cal.ET)
    assert list(drop_partial_today(df, during).index) == idx[:1]
    assert list(drop_partial_today(df, after).index) == idx
