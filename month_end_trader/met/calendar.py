"""NYSE session calendar (holidays and early closes) via exchange_calendars."""
from __future__ import annotations

import datetime as dt
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

ET = ZoneInfo("America/New_York")


@lru_cache(maxsize=1)
def _cal():
    return xc.get_calendar("XNYS", start="1990-01-01")


def is_session(d: dt.date) -> bool:
    return _cal().is_session(pd.Timestamp(d))


def sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    return [t.date() for t in _cal().sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))]


def previous_session(d: dt.date) -> dt.date:
    c = _cal()
    t = pd.Timestamp(d)
    if c.is_session(t):
        return c.previous_session(t).date()
    return c.date_to_session(t, direction="previous").date()


def next_session(d: dt.date) -> dt.date:
    c = _cal()
    t = pd.Timestamp(d)
    if c.is_session(t):
        return c.next_session(t).date()
    return c.date_to_session(t, direction="next").date()


def last_session_of_month(year: int, month: int) -> dt.date:
    first = dt.date(year, month, 1)
    last_day = (pd.Timestamp(first) + pd.offsets.MonthEnd(0)).date()
    return _cal().date_to_session(pd.Timestamp(last_day), direction="previous").date()


def is_last_session_of_month(d: dt.date) -> bool:
    return is_session(d) and last_session_of_month(d.year, d.month) == d


def prior_month_last_session(d: dt.date) -> dt.date:
    """Last session of the month before d's month (the MTD base)."""
    prev = d.replace(day=1) - dt.timedelta(days=1)
    return last_session_of_month(prev.year, prev.month)


def session_open_close(d: dt.date) -> tuple[dt.datetime, dt.datetime]:
    c = _cal()
    t = pd.Timestamp(d)
    o = c.session_open(t).tz_convert(ET).to_pydatetime()
    cl = c.session_close(t).tz_convert(ET).to_pydatetime()
    return o, cl


def is_early_close(d: dt.date) -> bool:
    _, cl = session_open_close(d)
    return (cl.hour, cl.minute) != (16, 0)


def next_trade_date(today: dt.date) -> dt.date:
    """Next last-session-of-month on or after today."""
    cand = last_session_of_month(today.year, today.month)
    if cand >= today:
        return cand
    nxt = (pd.Timestamp(today.replace(day=1)) + pd.offsets.MonthBegin(1)).date()
    return last_session_of_month(nxt.year, nxt.month)
