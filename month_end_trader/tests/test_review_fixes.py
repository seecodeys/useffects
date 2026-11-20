"""Regression tests for the issues found in the safety / IBKR / signal reviews."""
import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from met import calendar as cal
from met.executor import Executor
from met.monitor import sprt, size_multiplier, paper_ledger
from met.signal import signals_from_sources, yahoo_daily, DataError, compute_signal
from met.state import TradeState, DaemonLock, unfinished
from conftest import run_until

T = dt.date(2025, 9, 30)


def st(cfg, d=T):
    return TradeState(cfg.monitoring.state_dir, str(d))


def crit(alerts):
    return [t for l, t, _ in alerts if l == "CRITICAL"]


def test_exit_timeout_that_fills_does_not_reverse(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.late_fill_next = 1                       # first exit reports Timeout/0 but actually fills
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0, "position reversed"
    assert st(cfg).phase == "FLAT"
    assert sum(o["action"] == "SELL" for o in b.orders) == 1


def test_entry_timeout_that_fills_is_adopted_and_exited(env):
    cfg, clock, b, ex, alerts = env()
    b.late_fill_next = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("entry_status").startswith("Resolved") and b.fut_pos == 0


def test_position_survives_midnight(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 10_000                      # every exit rejected until after midnight
    run_until(ex, clock, dt.datetime(2025, 10, 1, 0, 30))
    assert b.fut_pos != 0
    b.reject_next = 0
    run_until(ex, clock, dt.datetime(2025, 10, 1, 1, 0))
    assert b.fut_pos == 0 and st(cfg).phase == "FLAT" and st(cfg).get("reconciled")
    assert any("unfinished" in t for t in crit(alerts))


def test_slow_preflight_no_late_entry(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 31, 0))
    cfg.strategy.entry_grace_s = 90
    slow = ex.fetchers["yahoo"]
    def slow_fetch(sym):
        clock.advance(60)                        # 2 symbols -> preflight finishes at ~09:33
        return slow(sym)
    ex.fetchers["yahoo"] = slow_fetch
    # make preflight start inside the window: entry_deadline 09:31:30
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.orders == [] and st(cfg).phase == "SKIPPED"


def test_stale_zero_position_read_not_mistaken_for_external_close(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    b.pos_glitch_reads = 1                      # one empty positions() read (stale cache)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("exit_reason") == "scheduled close" and b.fut_pos == 0


def test_contract_lookup_not_needed_for_exit(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    def boom(*a, **k): raise RuntimeError("contract details unavailable")
    b.front_future = boom
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, ex.reporter)
    run_until(ex2, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "FLAT" and b.fut_pos == 0


def test_unreadable_monitor_json_blocks(env):
    cfg, clock, b, ex, alerts = env()
    Path(cfg.monitoring.state_dir, "monitor.json").write_text("{not json")
    ex.reporter = None
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_monitor_multiplier_above_one_is_clamped(env):
    cfg, clock, b, ex, alerts = env()
    Path(cfg.monitoring.state_dir, "monitor.json").write_text(json.dumps({"size_multiplier": 3.0}))
    ex.reporter = None
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).get("plan")["effective_leverage"] <= 2.0


def test_reported_account_mismatch_blocks(env):
    cfg, clock, b, ex, alerts = env()
    b.account_reported = "DU7777777"
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED"


def test_corrupt_state_uses_backup(tmp_path):
    s = TradeState(str(tmp_path), "2025-09-30")
    s.update("t", "PLANNED", note="a"); s.update("t", "ENTERED", note="b", entry_filled=3, direction=1)
    s.path.write_text("{truncated")
    s2 = TradeState(str(tmp_path), "2025-09-30")
    assert s2.phase in ("PLANNED", "ENTERED")
    assert "2025-09-30" in unfinished(str(tmp_path)) or s2.phase == "PLANNED"


def test_daemon_lock_exclusive(tmp_path):
    a = DaemonLock(str(tmp_path)).acquire()
    with pytest.raises(RuntimeError):
        DaemonLock(str(tmp_path)).acquire()
    a.release()
    DaemonLock(str(tmp_path)).acquire().release()


def test_source_gap_tolerance(data):
    spy, tlt = data
    d = dt.date(2026, 10, 30)
    off = tlt["adjclose"].copy(); off.loc[dt.date(2026, 10, 29)] *= 1.003    # same sign, 30bp different
    f = {"ibkr": lambda s: spy["adjclose"] if s == "SPY" else tlt["adjclose"],
         "yahoo": lambda s: spy["adjclose"] if s == "SPY" else off}
    sig, problems, _ = signals_from_sources(d, f, "SPY", "TLT", True, 0.0015)
    assert sig is None and any("data error" in p for p in problems)


def test_compute_signal_rejects_non_month_end(data):
    spy, tlt = data
    with pytest.raises(DataError):
        compute_signal(dt.date(2026, 10, 29), spy["adjclose"], tlt["adjclose"])


def test_yahoo_missing_adjclose_raises():
    class R:
        status_code = 200
        def json(self):
            return {"chart": {"result": [{"meta": {"exchangeTimezoneName": "America/New_York"},
                                          "timestamp": [1763560800],
                                          "indicators": {"quote": [{"open": [1.0], "close": [1.0]}]}}]}}
    class S:
        def get(self, *a, **k): return R()
    with pytest.raises(DataError):
        yahoo_daily("TLT", session=S())


def test_sprt_clamped_detects_decline_after_alive():
    good = [40.0] * 46
    L, *_ = sprt(good, 20, 78, 0.1, 0.1)
    assert L == pytest.approx(2.197, abs=1e-3)              # clamped at A
    L, path, A, B, d = sprt(good + [-40.0] * 25, 20, 78, 0.1, 0.1)
    assert L <= B / 2, "decline should register quickly after clamping"
    L, path, A, B, d = sprt([-150.0] * 20 + [60.0] * 30, 20, 78, 0.1, 0.1)
    assert d == "edge alive" and size_multiplier(L, A, B, d)[0] == 1.0     # re-arms after recovery


def test_half_day_partial_bar():
    from met.signal import drop_partial_today
    df = pd.DataFrame({"adjclose": [1.0, 2.0]}, index=[dt.date(2026, 11, 25), dt.date(2026, 11, 27)])
    assert list(drop_partial_today(df, dt.datetime(2026, 11, 27, 13, 5, tzinfo=cal.ET)).index) == [dt.date(2026, 11, 25)]
    assert len(drop_partial_today(df, dt.datetime(2026, 11, 27, 13, 25, tzinfo=cal.ET))) == 2


def test_calendar_horizon():
    assert cal.last_session_of_month(2029, 11) == dt.date(2029, 11, 30)
    assert cal.next_trade_date_after(dt.datetime(2026, 11, 30, 17, 0, tzinfo=cal.ET)) == dt.date(2026, 12, 31)
    assert cal.next_trade_date_after(dt.datetime(2026, 11, 30, 10, 0, tzinfo=cal.ET)) == dt.date(2026, 11, 30)


def test_paper_ledger_accepts_timestamp_index(data):
    spy, tlt = data
    s2, t2 = spy.copy(), tlt.copy()
    s2.index = pd.to_datetime(s2.index); t2.index = pd.to_datetime(t2.index)
    led = paper_ledger(s2, t2, start="2025-01-01", until=dt.date(2025, 12, 31))
    assert len(led) == 12
