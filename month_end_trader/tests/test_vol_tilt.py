"""Vol tilt (research 168): full size when SPY's 20-day vol through T-1 is above the fixed 2002-14 median."""
import datetime as dt

import numpy as np
import pandas as pd
import pytest

from met import calendar as cal
from met.config import Config
from met.guardrails import trailing_vol, vol_scale
from met.state import TradeState
from conftest import run_until


def research_vol(spy, T):
    """The research definition: adjusted close-to-close returns, rolling(20).std().shift(1) evaluated at T."""
    s = pd.Series(spy["adjclose"].values, index=pd.to_datetime(list(spy.index)))
    v = s.pct_change().rolling(20).std().shift(1)
    return float(v.loc[pd.Timestamp(T)])


def test_trailing_vol_matches_research_definition(data):
    spy, _ = data
    for y, m in [(2008, 10), (2017, 6), (2020, 3), (2024, 7), (2025, 9)]:
        T = cal.last_session_of_month(y, m)
        assert trailing_vol(spy["adjclose"], T, 20) == pytest.approx(research_vol(spy, T), rel=1e-9)


def test_fixed_threshold_is_the_2002_14_median(data):
    spy, tlt = data
    s = pd.Series(spy["adjclose"].values, index=pd.to_datetime(list(spy.index)))
    s = s[s.index >= pd.Timestamp(tlt.index[0])]           # research index starts with TLT (2002-07/08)
    v = s.pct_change().rolling(20).std().shift(1)
    med = float(v.loc["2002-08":"2014"].median())
    assert med == pytest.approx(Config().strategy.vol_threshold, rel=0.01)


def test_regimes_known_months(data):
    spy, _ = data
    cfg = Config(); cfg.strategy.vol_tilt = True
    f = {"x": lambda sym: spy["adjclose"]}
    assert vol_scale(cfg, dt.date(2008, 10, 31), f)[0] == 1.0       # crash: high vol
    assert vol_scale(cfg, dt.date(2017, 6, 30), f)[0] == 0.5        # 2017: very calm


def test_missing_data_and_disagreement_give_low_size(data):
    spy, _ = data
    cfg = Config(); cfg.strategy.vol_tilt = True
    T = dt.date(2008, 10, 31)
    gap = spy["adjclose"].drop(index=cal.previous_session(cal.previous_session(T)))
    sc, v, notes = vol_scale(cfg, T, {"a": lambda s: gap})
    assert sc == 0.5 and v is None and any("no vol" in n for n in notes)
    calm = spy["adjclose"].copy()
    calm.loc[[d for d in calm.index if d < T][-30:]] = np.linspace(100, 101, 30)
    sc, v, notes = vol_scale(cfg, T, {"a": lambda s: spy["adjclose"], "b": lambda s: calm})
    assert sc == 0.5 and any("disagree" in n for n in notes)


def test_tilt_off_is_full_size(data):
    spy, _ = data
    assert vol_scale(Config(), dt.date(2017, 6, 30), {"x": lambda s: spy["adjclose"]}) == (1.0, None, [])


def test_daemon_trades_half_size_in_calm_month(env):
    """2025-09-30: calm September (vol below threshold) -> half of the 2x target."""
    cfg, clock, b, ex, alerts = env()
    cfg.strategy.vol_tilt = True
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    p = s.get("plan")
    assert p["vol_scale"] == 0.5 and p["vol_20d"] < cfg.strategy.vol_threshold
    assert 0.6 <= p["effective_leverage"] <= 1.0
    assert s.phase == "FLAT" and b.fut_pos == 0
    assert any("reduced size" in body for l, t, body in alerts if "Tomorrow" in t)
