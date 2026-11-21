"""Historical replay of leg B: the real daemon (both legs) through past bond windows on the simulated broker, with a
ZN price path built from IEF adjusted closes (ZN return = IEF return / hedge ratio, stepping at each 16:00 close
and visible from 15:50 so the 15:58 entry / 15:55 exit trade at that session's close). Every trade is checked against
the independently computed bond ledger. Default: every 12th month since 2003 (24); MET_SLOW=1 replays all 286."""
import datetime as dt
import os
from pathlib import Path

import pandas as pd
import pytest

from met import calendar as cal
from met.bond import BondExecutor, entry_session
from met.bond_monitor import bond_ledger
from met.daemon import Daemon
from met.dryrun import make_env
from met.state import TradeState

FIX = Path(__file__).resolve().parent / "fixtures"
ALL = []
d = dt.date(2003, 1, 1)
while d <= dt.date(2026, 10, 1):
    ALL.append(cal.last_session_of_month(d.year, d.month))
    d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
CASES = ALL if os.environ.get("MET_SLOW") == "1" else ALL[3::12]


@pytest.fixture(scope="module")
def ief():
    x = pd.read_csv(FIX / "ief.csv")
    return pd.Series(x["adjclose"].values, index=pd.to_datetime(x["date"]).dt.date)


@pytest.fixture(scope="module")
def led(ief):
    return bond_ledger(ief)


def zn_path(ief, E, hedge, p0=110.0):
    base = float(ief.loc[E])
    dates = sorted(d for d in ief.index if E <= d <= E + dt.timedelta(days=14))

    def px(now):
        t = now.astimezone(cal.ET)
        last = None
        for d in dates:
            c = cal.session_open_close(d)[1] if cal.is_session(d) else None
            if c is not None and t >= c - dt.timedelta(minutes=10):
                last = d
        if last is None:
            return p0
        return p0 * (1 + (float(ief.loc[last]) / base - 1) / hedge)
    return px


@pytest.mark.parametrize("X", CASES, ids=str)
def test_bond_replay_month(X, tmp_path, data, ief, led):
    spy, tlt = data
    E = entry_session(X, 3)
    start = dt.datetime.combine(cal.previous_session(E), dt.time(15, 0), tzinfo=cal.ET)
    cfg, clock, b, ex, alerts = make_env(start, str(tmp_path), spy, tlt)
    ex.reporter = None
    cfg.bond.enabled = True
    b.zn_price_fn = zn_path(ief, E, cfg.bond.hedge_ratio)
    bx = BondExecutor(cfg, b, ex.alert, ex.fetchers)
    d = Daemon([ex, bx])
    _, cX = cal.session_open_close(X)
    d.run_forever(stop=lambda: clock.now() >= cX + dt.timedelta(minutes=30), lock=False)
    s = TradeState(str(tmp_path), str(X), prefix="bond")
    assert s.phase == "FLAT", (X, s.phase, s.get("blockers"), s.data["events"][-3:])
    assert b.zn_pos == 0
    # bot: ZN fills (half a 1/64 tick of slippage per side) x hedge ratio -> IEF bp; ledger: IEF close E -> close X
    slip_bp = 1e4 * 2 * (1 / 128) / 110.0 * cfg.bond.hedge_ratio
    ief_bp = led.loc[X, "ief_bp"]
    bot_bp = s.get("trade_bp") * cfg.bond.hedge_ratio
    assert bot_bp == pytest.approx(ief_bp - slip_bp, abs=0.6 + 0.01 * abs(ief_bp)), (X, bot_bp, ief_bp)
    assert not [a for a in alerts if a[0] == "CRITICAL"], [a[1] for a in alerts if a[0] == "CRITICAL"]
