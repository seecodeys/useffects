"""NYSE session calendar (holidays and early closes) via exchange_calendars."""
from __future__ import annotations

import datetime as dt
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

ET = ZoneInfo("America/New_York")


_CAL = {"day": None, "cal": None}


def _cal():
    """Rebuilt once per day; horizon extends 5 years ahead. Ad-hoc closures (e.g. national days of mourning)
    need an exchange_calendars upgrade, or an entry in MET_EXTRA_CLOSURES=YYYY-MM-DD,YYYY-MM-DD."""
    today = dt.date.today()
    if _CAL["day"] != today:
        import os
        extra = [d for d in os.environ.get("MET_EXTRA_CLOSURES", "").split(",") if d.strip()]
        c = xc.get_calendar("XNYS", start="1990-01-01", end=str(today.replace(year=today.year + 5)))
        if extra:
            c = _with_closures(c, extra)
        _CAL.update(day=today, cal=c)
        for f in (session_open_close, last_session_of_month, previous_session, next_session, is_session):
            f.cache_clear()
    return _CAL["cal"]


def _with_closures(c, extra):
    class Patched:
        def __init__(self, base, closed):
            self.b, self.closed = base, {pd.Timestamp(x) for x in closed}
        def is_session(self, t):
            return pd.Timestamp(t) not in self.closed and self.b.is_session(t)
        def sessions_in_range(self, a, z):
            return self.b.sessions_in_range(a, z).difference(pd.DatetimeIndex(sorted(self.closed)))
        def previous_session(self, t):
            p = self.b.previous_session(t)
            while p in self.closed:
                p = self.b.previous_session(p)
            return p
        def next_session(self, t):
            n = self.b.next_session(t)
            while n in self.closed:
                n = self.b.next_session(n)
            return n
        def date_to_session(self, t, direction):
            s = self.b.date_to_session(t, direction=direction)
            while s in self.closed:
                s = self.b.previous_session(s) if direction == "previous" else self.b.next_session(s)
            return s
        def session_open(self, t): return self.b.session_open(t)
        def session_close(self, t): return self.b.session_close(t)
    return Patched(c, extra)


@lru_cache(maxsize=4096)
def is_session(d: dt.date) -> bool:
    return _cal().is_session(pd.Timestamp(d))


def sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    return [t.date() for t in _cal().sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))]


@lru_cache(maxsize=4096)
def previous_session(d: dt.date) -> dt.date:
    c = _cal()
    t = pd.Timestamp(d)
    if c.is_session(t):
        return c.previous_session(t).date()
    return c.date_to_session(t, direction="previous").date()


@lru_cache(maxsize=4096)
def next_session(d: dt.date) -> dt.date:
    c = _cal()
    t = pd.Timestamp(d)
    if c.is_session(t):
        return c.next_session(t).date()
    return c.date_to_session(t, direction="next").date()


@lru_cache(maxsize=4096)
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


@lru_cache(maxsize=4096)
def session_open_close(d: dt.date) -> tuple[dt.datetime, dt.datetime]:
    c = _cal()
    t = pd.Timestamp(d)
    o = c.session_open(t).tz_convert(ET).to_pydatetime()
    cl = c.session_close(t).tz_convert(ET).to_pydatetime()
    return o, cl


def next_trade_date_after(now: dt.datetime) -> dt.date:
    """Like next_trade_date, but rolls to next month once today's trade day has closed (for display)."""
    T = next_trade_date(now.date())
    if T == now.date() and now >= session_open_close(T)[1]:
        T = next_trade_date(T + dt.timedelta(days=1))
    return T


def is_early_close(d: dt.date) -> bool:
    _, cl = session_open_close(d)
    return (cl.hour, cl.minute) != (16, 0)


def next_trade_date(today: dt.date) -> dt.date:
    """Next last-session-of-month on or after today."""
    _cal()                                   # daily calendar refresh hook (clears the lru caches)
    cand = last_session_of_month(today.year, today.month)
    if cand >= today:
        return cand
    nxt = (pd.Timestamp(today.replace(day=1)) + pd.offsets.MonthBegin(1)).date()
    return last_session_of_month(nxt.year, nxt.month)
