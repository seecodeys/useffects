import datetime as dt
import math

import numpy as np
import pytest

from met.config import Config
from met.monitor import sprt, size_multiplier, paper_ledger, run_report


def test_sprt_bounds_and_decisions():
    L, path, A, B, d = sprt([], 20, 78, 0.1, 0.1)
    assert A == pytest.approx(math.log(9)) and B == pytest.approx(-math.log(9)) and d is None
    L, _, A, B, d = sprt([20.0] * 200, 20, 78, 0.1, 0.1)
    assert d == "edge alive"
    L, _, A, B, d = sprt([0.0] * 200, 20, 78, 0.1, 0.1)
    assert d == "edge dead" and size_multiplier(L, A, B, d)[0] == 0.0
    L, _, A, B, d = sprt([-30.0] * 10, 20, 78, 0.1, 0.1)       # weakening, not yet dead
    assert B < L <= B / 2 and size_multiplier(L, A, B, d)[0] == 0.5


def test_paper_ledger_matches_backtest(data):
    spy, tlt = data
    led = paper_ledger(spy, tlt, start="2002-08-01", until=dt.date(2026, 10, 31))
    assert len(led) == 291
    # research numbers (2002-08..2026-10): ~+21-23bp/trade gross, ~62% hit
    assert 19 <= led["paper_bp"].mean() <= 24
    assert 0.58 <= (led["paper_bp"] > 0).mean() <= 0.66


def test_run_report_writes_outputs(tmp_path, data):
    spy, tlt = data
    cfg = Config(); cfg.ib.account = "DU1"
    cfg.monitoring.state_dir = cfg.monitoring.report_dir = str(tmp_path); cfg.monitoring.start = "2023-01"
    res = run_report(cfg, asof=dt.date(2026, 10, 31), spy=spy, tlt=tlt)
    assert res["sprt"]["trades"] == 46 and res["sprt"]["decided"] == "edge alive"
    assert res["size_multiplier"] == 1.0
    assert (tmp_path / "monitor.json").exists() and (tmp_path / "decay_2026-10-31.md").exists()
