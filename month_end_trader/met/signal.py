"""The 013 signal: direction = -sign(SPY MTD - TLT MTD), MTD from the prior month's last close to the T-1 close.

Pure computation in `compute_signal`; data providers return a pd.Series of dividend-adjusted closes
indexed by date. Two independent sources (IBKR ADJUSTED_LAST and Yahoo adjclose) are cross-checked.
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass, asdict

import pandas as pd
import requests

from . import calendar as cal

log = logging.getLogger(__name__)


class DataError(RuntimeError):
    pass


@dataclass
class Signal:
    trade_date: str
    signal_through: str
    base_date: str
    equity_mtd: float
    bond_mtd: float
    gap: float
    direction: int          # +1 long, -1 short, 0 none
    source: str

    @property
    def side(self) -> str:
        return {1: "LONG", -1: "SHORT", 0: "NONE"}[self.direction]

    def to_dict(self):
        d = asdict(self); d["side"] = self.side; return d


def compute_signal(trade_date: dt.date, equity: pd.Series, bond: pd.Series, source: str = "",
                   provisional: bool = False) -> Signal:
    """equity/bond: adjusted closes indexed by datetime.date (or Timestamp). Requires exact base and T-1 closes.
    provisional=True (CLI checks before T-1 has closed): use the latest common close after the base instead."""
    if not cal.is_last_session_of_month(trade_date):
        raise DataError(f"{trade_date} is not the last session of its month")
    t1 = cal.previous_session(trade_date)
    base = cal.prior_month_last_session(trade_date)
    if t1 <= base:
        raise DataError(f"{trade_date}: no month-to-date sessions before the trade date")
    e = _as_date_index(equity); b = _as_date_index(bond)
    if provisional and (t1 not in e.index or t1 not in b.index):
        common = sorted(set(e.index) & set(b.index))
        later = [d for d in common if base < d < t1]
        if later:
            t1 = later[-1]
            source = f"{source} (provisional, through {t1})"
    for name, s in (("equity", e), ("bond", b)):
        for d in (base, t1):
            if d not in s.index or pd.isna(s.loc[d]):
                raise DataError(f"{source}: {name} close missing for {d}")
    e_mtd = float(e.loc[t1] / e.loc[base] - 1)
    b_mtd = float(b.loc[t1] / b.loc[base] - 1)
    gap = e_mtd - b_mtd
    direction = 0 if gap == 0 else (-1 if gap > 0 else 1)
    return Signal(str(trade_date), str(t1), str(base), e_mtd, b_mtd, gap, direction, source)


def _as_date_index(s: pd.Series) -> pd.Series:
    if len(s) and type(s.index[0]) is dt.date and s.index.is_monotonic_increasing and s.index.is_unique:
        return s
    s = s.copy()
    s.index = [pd.Timestamp(i).date() for i in s.index]
    return s[~pd.Index(s.index).duplicated(keep="last")].sort_index()


# ----------------------------------------------------------------------------------------------- providers
_YH = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}


def yahoo_daily(symbol: str, days: int = 120, session=None) -> pd.DataFrame:
    """Daily bars from Yahoo's chart API: columns open, close, adjclose; index datetime.date (exchange-local)."""
    sess = session or requests
    end = int(time.time()); start = end - days * 86400
    last_err = None
    for attempt in range(4):
        try:
            r = sess.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}", headers=_YH, timeout=20,
                         params={"period1": start, "period2": end, "interval": "1d", "events": "div,split"})
            if r.status_code == 200:
                res = r.json()["chart"]["result"][0]
                q = res["indicators"]["quote"][0]
                adj = (res["indicators"].get("adjclose") or [{}])[0].get("adjclose")
                if not adj:     # never fall back to raw closes: missing TLT dividends flip ~5% of signals
                    raise DataError(f"yahoo {symbol}: adjclose missing")
                ts = pd.to_datetime(res["timestamp"], unit="s", utc=True).tz_convert(res["meta"]["exchangeTimezoneName"])
                df = pd.DataFrame({"open": q["open"], "close": q["close"], "adjclose": adj},
                                  index=[t.date() for t in ts]).dropna()
                return df[~df.index.duplicated(keep="last")]
            last_err = f"HTTP {r.status_code}"
        except DataError:
            raise
        except Exception as e:  # network / parse
            last_err = repr(e)
        time.sleep(1.5 * (attempt + 1))
    raise DataError(f"yahoo {symbol}: {last_err}")


def drop_partial_today(df: pd.DataFrame, now_et: dt.datetime) -> pd.DataFrame:
    """Yahoo includes today's live bar during the session; drop it unless the session is closed."""
    today = now_et.date()
    if today in df.index and cal.is_session(today):
        _, close = cal.session_open_close(today)
        if now_et < close + dt.timedelta(minutes=20):
            df = df.drop(index=today)
    return df


def signals_from_sources(trade_date: dt.date, fetchers: dict, equity: str, bond: str,
                         require_agreement: bool = True, max_gap_diff: float = 0.0015, min_sources: int = 1,
                         provisional: bool = False) -> tuple[Signal | None, list[str], list[Signal]]:
    """fetchers: {name: callable(symbol) -> pd.Series of adjusted closes}. Returns (signal, problems, all_signals).
    signal is None when no usable/consistent signal could be produced."""
    sigs, problems = [], []
    for name, fetch in fetchers.items():
        try:
            sigs.append(compute_signal(trade_date, fetch(equity), fetch(bond), source=name, provisional=provisional))
        except Exception as e:
            problems.append(f"signal source {name} failed: {e}")
    if not sigs:
        return None, problems + ["no signal source available"], sigs
    if len(sigs) < min_sources:
        return None, problems + [f"only {len(sigs)} of the required {min_sources} signal sources available"], sigs
    if len(sigs) > 1:
        spread = max(s.gap for s in sigs) - min(s.gap for s in sigs)
        if spread > max_gap_diff:
            return None, problems + ["signal sources differ by %.3f%% in gap (> %.3f%%): data error? " % (
                100 * spread, 100 * max_gap_diff) + ", ".join(
                f"{s.source}: {s.equity_mtd:+.4%}/{s.bond_mtd:+.4%}" for s in sigs)], sigs
    dirs = {s.direction for s in sigs}
    if len(dirs) > 1:
        msg = "signal sources disagree: " + ", ".join(f"{s.source}={s.side} gap={s.gap:+.4%}" for s in sigs)
        if require_agreement:
            return None, problems + [msg], sigs
        problems.append(msg)
    return sigs[0], problems, sigs
