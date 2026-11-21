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
from met.config import Config
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
    assert s.phase == "FLAT" and s.get("entry_filled") == s.get("qty_planned") and b.fut_pos == 0


def test_position_survives_midnight(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 10_000                      # every exit rejected until after midnight
    run_until(ex, clock, dt.datetime(2025, 10, 1, 0, 30))
    assert b.fut_pos != 0
    b.reject_next = 0
    run_until(ex, clock, dt.datetime(2025, 10, 1, 2, 30))         # hourly retries after the round cap
    assert b.fut_pos == 0 and st(cfg).phase == "FLAT" and st(cfg).get("reconciled")
    assert any("still open after its trade day" in t for t in crit(alerts))


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


def test_weekly_status_once_per_monday(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 22, 7, 0))      # Monday
    run_until(ex, clock, dt.datetime(2025, 9, 23, 7, 0))
    weekly = [t for l, t, body in alerts if t.startswith("Weekly status")]
    assert len(weekly) == 1
    body = [body for l, t, body in alerts if t.startswith("Weekly status")][0]
    assert "next trade date 2025-09-30" in body and "provisional signal through 2025-09-19" in body



# ---------------------------------------------------------------- review round 2 (executor/state)
def test_stale_position_after_exit_fill_does_not_reverse(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.stale_after_fill = 5
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0 and [o["action"] for o in b.orders] == ["BUY", "SELL"]


def test_rejected_exits_are_rate_limited(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 10_000
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 59))
    sells = sum(o["action"] == "SELL" for o in b.orders)
    assert sells <= 70, sells                       # ~4h at <=1 per 5 min after backoff, not thousands
    assert sum("MANUAL INTERVENTION" in t for t in crit(alerts)) == 1


def test_no_orders_while_futures_closed(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 30, 8, 0))     # T = Fri 31 Oct 2025
    run_until(ex, clock, dt.datetime(2025, 10, 31, 15, 0))
    b.reject_next = 10_000
    run_until(ex, clock, dt.datetime(2025, 11, 1, 12, 0))               # Saturday
    n = len(b.orders)
    run_until(ex, clock, dt.datetime(2025, 11, 2, 17, 0))               # still Sunday before 18:00
    assert len(b.orders) == n
    b.reject_next = 0
    run_until(ex, clock, dt.datetime(2025, 11, 2, 19, 0))               # CME reopens 18:00 Sunday
    assert b.fut_pos == 0


def test_reconcile_leaves_user_position_alone_after_flat(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 2))
    assert st(cfg).phase == "FLAT"
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "BUY", 2, "user", 5)                               # the user's own trade
    run_until(ex, clock, dt.datetime(2025, 10, 2, 12, 0))
    assert b.fut_pos == 2 and st(cfg).get("reconciled")


def test_error_state_does_not_flatten_user_position(env):
    cfg, clock, b, ex, alerts = env()
    b.reject_next = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    assert st(cfg).phase == "ERROR"
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "SELL", 3, "user", 5)
    run_until(ex, clock, dt.datetime(2025, 10, 1, 12, 0))
    assert b.fut_pos == -3


def test_stuck_working_orders_cancelled_then_exit(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.stuck_working = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0 and st(cfg).phase == "FLAT"


def test_entry_order_still_working_stays_submitted(env):
    cfg, clock, b, ex, alerts = env()
    b.late_fill_next = 1; b.stuck_working = 1; b.cancel_clears_stuck = False
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 33))
    assert st(cfg).phase in ("ENTRY_SUBMITTED", "ENTERED")       # executions show the fill
    b.stuck_working = 0
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "FLAT" and b.fut_pos == 0


def test_feb29_calendar():
    import met.calendar as c
    assert c.last_session_of_month(2028, 2) == dt.date(2028, 2, 29)
    assert c.next_trade_date(dt.date(2028, 2, 29)) == dt.date(2028, 2, 29)


def test_config_strict_types(tmp_path):
    from met.config import load_config, ConfigError
    p = tmp_path / "c.yaml"
    p.write_text("ib: {account: DU1}\nguardrails: {auto_derisk: 'false'}\n")
    with pytest.raises(ConfigError):
        load_config(p)
    p.write_text("ib: {account: DU1}\nguardrails: {max_contracts: 40.5}\n")
    with pytest.raises(ConfigError):
        load_config(p)
    p.write_text("ib: {account: DU1}\nmonitoring: {state_dir: ~/met_state_test}\n")
    cfg = load_config(p)
    assert "~" not in cfg.monitoring.state_dir


def test_dead_is_sticky():
    L, _, A, B, d = sprt([0.0] * 100 + [15.0], 20, 78, 0.1, 0.1)
    assert L > B and size_multiplier(L, A, B, d, was_dead=True)[0] == 0.0
    assert size_multiplier(L, A, B, d, was_dead=False)[0] == 0.5


# ---------------------------------------------------------------- review round 3 (whole package)
FRI = dt.date(2025, 10, 31)


def test_stale_zero_after_entry_fill_not_abandoned(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 30, 8, 0))
    b.stale_after_fill = 4                         # entry fill, then several stale 0 reads
    run_until(ex, clock, dt.datetime(2025, 10, 31, 19, 0))
    s = st(cfg, FRI)
    assert b.fut_pos == 0 and s.phase == "FLAT" and s.get("exit_reason") == "scheduled close"


def test_late_entry_fill_with_stale_read_is_adopted(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 30, 8, 0))
    b.late_fill_next = 1                           # entry reported Timeout/0 but fills
    b.pos_glitch_reads = 2                         # and the next two position reads miss it
    run_until(ex, clock, dt.datetime(2025, 10, 31, 19, 0))
    assert b.fut_pos == 0, "late fill held overnight"


def test_crash_in_entry_submitted_resolved_next_week(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 31, 9, 29))
    run_until(ex, clock, dt.datetime(2025, 10, 31, 9, 29, 30))
    s = st(cfg, FRI); assert s.phase == "PLANNED"
    clock.advance(35)
    c = b.front_future("MES", "CME", FRI + dt.timedelta(days=3))
    b.connect(); b.market_order(c, s.get("plan")["action"], s.get("qty_planned"), f"MET-{FRI}-entry", 5)
    s.update(clock.now().isoformat(), "ENTRY_SUBMITTED", note="crash")
    clock.t = dt.datetime(2025, 11, 3, 9, 0, tzinfo=cal.ET)              # daemon back on Monday
    b.disconnect()
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, None)
    run_until(ex2, clock, dt.datetime(2025, 11, 3, 10, 0))
    # Monday's IB no longer reports Friday's executions: the bot must not guess; it says so precisely
    assert st(cfg, FRI).phase == "ERROR" and st(cfg, FRI).get("executions_expired")
    assert any("UNKNOWN" in t for t in crit(alerts))
    assert b.fut_pos != 0            # left for the user (met resolve / manual close with MET ref)


def test_daemon_starts_while_ib_down_and_alerts(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 20, 8, 0))
    b.connect_fail = 10_000
    run_until(ex, clock, dt.datetime(2025, 10, 20, 9, 0))
    assert any("unreachable" in t for t in crit(alerts))


def test_preflight_retries_transient_blockers(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 0))
    b.price_unavailable = True
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 15))
    assert st(cfg).phase is None and st(cfg).get("preflight_attempts", 0) >= 1
    b.price_unavailable = False
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "FLAT"


def test_require_all_sources(env):
    cfg, clock, b, ex, alerts = env()
    cfg.signal.require_all_sources = True
    def down(sym): raise RuntimeError("HTTP 429")
    ex.fetchers["yahoo"] = down
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_futures_open_half_day_and_holiday():
    from met.executor import futures_open
    E = cal.ET
    assert futures_open(dt.datetime(2025, 11, 28, 12, 59, tzinfo=E))
    assert not futures_open(dt.datetime(2025, 11, 28, 13, 20, tzinfo=E))
    assert not futures_open(dt.datetime(2025, 11, 27, 14, 0, tzinfo=E))     # Thanksgiving afternoon
    assert futures_open(dt.datetime(2025, 11, 27, 19, 0, tzinfo=E))
    assert not futures_open(dt.datetime(2025, 11, 1, 12, 0, tzinfo=E))      # Saturday


def test_cli_flatten_refuses_while_daemon_runs(tmp_path, monkeypatch):
    from met import cli
    (tmp_path / "c.yaml").write_text(f"ib: {{account: DU1}}\nmonitoring: {{state_dir: {tmp_path}/s}}\n")
    lk = DaemonLock(str(tmp_path / "s")).acquire()
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not connect")))
    try:
        assert cli.main(["flatten", "--config", str(tmp_path / "c.yaml"), "--yes"]) == 2
    finally:
        lk.release()


def test_config_validation_round3():
    from met.config import Config, ConfigError
    for f in [lambda c: setattr(c.guardrails, "margin_buffer_pct", 0.0),
              lambda c: setattr(c.guardrails, "min_net_liq", 0),
              lambda c: setattr(c.monitoring, "start", "2026-11-01")]:
        c = Config(); c.ib.account = "DU1"; f(c)
        with pytest.raises(ConfigError):
            c.validate()


def test_rehearsal_next_trade():
    from met.rehearsal import rehearse
    s, b, stamped = rehearse(dt.date(2026, 11, 30), verbose=False)
    titles = [t for _, _, t, _ in stamped]
    mes = [o["action"] for o in b.orders if o["ref"].startswith("MET-")]
    assert s.phase == "FLAT" and mes in (["BUY", "SELL"], ["SELL", "BUY"])
    from met.state import TradeState
    zn = [o for o in b.orders if o["ref"].startswith("MEB-")]
    assert [o["action"] for o in zn] == ["BUY", "SELL"] and b.zn_pos == 0
    assert zn[0]["t"].date() == dt.date(2026, 11, 24) and zn[1]["t"].date() == dt.date(2026, 11, 30)
    assert any("Bond window 2026-11-24 -> 2026-11-30" in t for t in titles)
    assert any("Tomorrow (2026-11-30)" in t for t in titles)
    ann = next(ts for ts, _, t, _ in stamped if "Tomorrow" in t)
    assert ann.date() == dt.date(2026, 11, 27) and ann.time() == dt.time(13, 25)     # half-day T-1


# ---------------------------------------------------------------- review round 4 (adoption)
def test_missed_entry_fill_adopted_intraday_and_guarded(env):
    cfg, clock, b, ex, alerts = env()
    o = b.day_path[T][0]
    b.day_path[T] = (o, o * 0.95, [(7200, o * 0.95)])          # -5% two hours in
    b.late_fill_next = 1; b.pos_glitch_reads = 4               # fill hidden from the bot's first reads
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert b.fut_pos == 0 and s.phase == "FLAT"
    assert s.get("exit_reason") == "disaster stop", s.get("exit_reason")   # guards applied after adoption
    exit_t = [x["t"] for x in b.orders if x["action"] == "SELL"][0]
    assert exit_t.time() < dt.time(11, 45)
    assert not s.get("pnl_estimated") and s.get("net_pnl") < 0        # exact from executions


def test_rejected_entry_never_adopts_user_position(env):
    cfg, clock, b, ex, alerts = env()
    b.reject_next = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 10, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "BUY", 3, "user", 5)                     # same direction as the bot's plan
    run_until(ex, clock, dt.datetime(2025, 10, 1, 12, 0))
    assert b.fut_pos == 3


def test_adoption_window_closes_next_session(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 29, 30))
    s = st(cfg)
    s.update(clock.now().isoformat(), "ERROR", note="test", uncertain=True, entry_status="Timeout")
    b.disconnect()
    clock.t = dt.datetime(2025, 10, 6, 10, 0, tzinfo=cal.ET)
    b.connect()
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "BUY", 2, "user", 5)
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, None)
    run_until(ex2, clock, dt.datetime(2025, 10, 6, 12, 0))
    assert b.fut_pos == 2 and st(cfg).get("reconciled")


def test_preflight_blocked_by_unfinished_older_trade(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 31, 9, 0))
    old = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    old.update("t", "ERROR", note="x", uncertain=True)          # unreconciled, no contract id
    ex._unfinished_last["2025-09-30"] = clock.now() + dt.timedelta(days=1)   # keep drive_unfinished away
    run_until(ex, clock, dt.datetime(2025, 10, 31, 12, 0))
    s = st(cfg, dt.date(2025, 10, 31))
    assert s.phase == "SKIPPED" and any("unfinished older" in x for x in s.get("blockers")) and b.orders == []


def test_no_false_critical_during_preflight_retry(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 0))
    b.price_unavailable = True
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 26))
    assert not [t for t in crit(alerts) if "cannot connect" in t]


def test_permanent_blocker_classification():
    from met.executor import PERMANENT_PREFIXES
    perm = ["existing MES positions before entry: MESZ5 +1", "kill switch active", "decay monitor says stop (x)"]
    trans = ["signal source ibkr failed: An existing connection was forcibly closed",
             "contract/price unavailable: score 3", "decay monitor state unreadable (x); run `met report`"]
    assert all(p.startswith(PERMANENT_PREFIXES) for p in perm)
    assert not any(t.startswith(PERMANENT_PREFIXES) for t in trans)


def test_exit_submitted_with_nothing_owned_is_finalised(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    s.update(clock.now().isoformat(), "EXIT_SUBMITTED", note="crash after exit fill", reconciled=False)
    clock.t = dt.datetime(2025, 10, 1, 10, 0, tzinfo=cal.ET)
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, None)
    run_until(ex2, clock, dt.datetime(2025, 10, 1, 10, 10))
    assert st(cfg).phase == "FLAT" and st(cfg).get("reconciled")


def test_cme_full_closures():
    from met.executor import futures_open
    E = cal.ET
    assert not futures_open(dt.datetime(2025, 12, 25, 10, 0, tzinfo=E))
    assert not futures_open(dt.datetime(2025, 12, 24, 19, 0, tzinfo=E))     # evening before Christmas
    assert not futures_open(dt.datetime(2026, 4, 3, 10, 0, tzinfo=E))       # Good Friday 2026
    assert futures_open(dt.datetime(2025, 12, 26, 10, 0, tzinfo=E))


# ---------------------------------------------------------------- review round 5 (adversarial) -> execution ownership
def _sells(b):
    return sum(o.get("filled", 0) or 0 for o in b.orders if o["action"] == "SELL")


def test_positions_hidden_at_exit_still_exits(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59, 57))
    b.pos_glitch_reads = 5
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 30))
    assert b.fut_pos == 0 and st(cfg).phase == "FLAT"


def test_late_exit_fill_plus_stale_read_no_reversal(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59))
    b.late_fill_next = 1; b.stale_after_fill = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0 and _sells(b) == 7


def test_exit_fill_arriving_30min_later_is_caught(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59))
    b.fill_delay_next = (1800, False)          # exit1 'dies' but fills at ~16:30
    run_until(ex, clock, dt.datetime(2025, 9, 30, 18, 30))
    assert b.fut_pos == 0, b.fut_pos
    assert any("LATE FILL" in t or "REVERSED" in t for t in crit(alerts))


def test_entry_reported_dead_then_fills_is_managed(env):
    cfg, clock, b, ex, alerts = env()
    b.fill_delay_next = (20, False)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert b.fut_pos == 0 and s.phase == "FLAT"
    exit_t = [o["t"] for o in b.orders if o["action"] == "SELL"][0]
    assert dt.time(15, 59, 58) <= exit_t.time() < dt.time(16, 0, 5)


def test_user_position_after_failed_entry_untouched(env):
    cfg, clock, b, ex, alerts = env()
    b.fill_delay_next = (10**7, False)         # entry never fills
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b._pending = []
    b.market_order(c, "BUY", 7, "user", 5)
    run_until(ex, clock, dt.datetime(2025, 10, 1, 12, 0))
    assert b.fut_pos == 7


def test_rejections_for_two_days_bounded(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 10**6
    run_until(ex, clock, dt.datetime(2025, 10, 2, 16, 0))
    assert len(b.orders) <= 64, len(b.orders)          # 10 fast rounds + 4 at 15 min + hourly while CME trades
    assert sum("MANUAL INTERVENTION" in t for t in crit(alerts)) == 1


def test_executions_unavailable_means_no_orders(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59))
    b.executions_fail = 30
    n = len(b.orders)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 0, 30))
    assert len(b.orders) == n                  # never trades blind
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0



# ---------------------------------------------------------------- review round 6 (execution semantics)
def test_manual_close_with_met_ref_resolves(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "SELL", 7, f"MET-{T}-manual", 5)          # user closes in TWS with the bot's ref
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and b.fut_pos == 0 and sum(o["action"] == "SELL" for o in b.orders) == 1


def test_manual_close_without_ref_asks_for_resolve(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "SELL", 7, "TWS-manual", 5)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert b.fut_pos == 0 and sum(o["action"] == "SELL" for o in b.orders) == 1    # no extra orders
    assert any("RESOLVE NEEDED" in t for t in crit(alerts))


def test_execution_correction_replaces_price(tmp_path):
    s = TradeState(str(tmp_path), "2025-09-30")
    s.update("t", "ENTERED", direction=1)
    s.merge_fills([dict(exec_id="0001.aa.01", rev=1, ref="MET-2025-09-30-entry", side="BUY", qty=7.0, price=6600.0, time="t")])
    s.merge_fills([dict(exec_id="0001.aa.01", rev=2, ref="MET-2025-09-30-entry", side="BUY", qty=7.0, price=6601.0, time="t")])
    s.merge_fills([dict(exec_id="0001.aa.01", rev=1, ref="MET-2025-09-30-entry", side="BUY", qty=7.0, price=6600.0, time="t")])
    assert s.get("entry_price") == 6601.0 and s.owned == 7


def test_fa_master_account_rejected():
    from met.config import Config, ConfigError
    c = Config(); c.ib.account = "F1234567"
    with pytest.raises(ConfigError):
        c.validate()


def test_cli_resolve_records_manual_close(env, monkeypatch, tmp_path):
    from met import cli
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "SELL", 7, "TWS-manual", 5)              # closed outside the bot, no MET ref
    (tmp_path / "c.yaml").write_text(f"ib: {{account: DU1234567}}\nmonitoring: {{state_dir: {cfg.monitoring.state_dir}, start: '2025-09'}}\n")
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    rc = cli.main(["resolve", str(T), "--config", str(tmp_path / "c.yaml"), "--yes", "--price", "6640"])
    s = st(cfg)
    assert rc == 0 and s.phase == "FLAT" and s.get("reconciled") and s.get("pnl_estimated") and s.owned == 0
    from met.state import unfinished
    assert str(T) not in unfinished(cfg.monitoring.state_dir)


# ---------------------------------------------------------------- review round 7
def test_reversal_after_flat_closed_immediately(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    Path(cfg.monitoring.state_dir, "KILL").write_text("x")
    b.fill_delay_next = (120, False)            # exit1 reported dead, fills 2 min later
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 1))
    Path(cfg.monitoring.state_dir, "KILL").unlink()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 15))
    assert b.fut_pos == 0, b.fut_pos            # reversed -7 closed within minutes, not at 15:59:58
    s = st(cfg)
    assert s.get("trade_bp") is None            # reversal legs: not comparable with the paper result


def test_single_stale_read_does_not_delay_exit_past_close(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59, 57))
    b.pos_glitch_reads = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 5))
    sell_t = [o["t"] for o in b.orders if o["action"] == "SELL"][0]
    assert sell_t.time() < dt.time(16, 0, 0), sell_t


def test_resolve_refuses_without_price_or_sync(env, monkeypatch, tmp_path):
    from met import cli
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "SELL", 7, "TWS-manual", 5)
    (tmp_path / "c.yaml").write_text(f"ib: {{account: DU1234567}}\nmonitoring: {{state_dir: {cfg.monitoring.state_dir}, start: '2025-09'}}\n")
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    b.price_unavailable = True
    assert cli.main(["resolve", str(T), "--config", str(tmp_path / "c.yaml"), "--yes"]) == 2
    b.price_unavailable = False
    b.executions_fail = 10
    assert cli.main(["resolve", str(T), "--config", str(tmp_path / "c.yaml"), "--yes", "--price", "6640"]) == 2
    assert st(cfg).phase != "FLAT"


def test_cli_flatten_syncs_before_ownership(env, monkeypatch, tmp_path):
    from met import cli
    cfg, clock, b, ex, alerts = env()
    b.fill_delay_next = (20, False)            # entry reported dead, fills while the daemon is stopped
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 30, 10))
    assert st(cfg).phase in ("ERROR", "ENTRY_SUBMITTED")     # Timeout waits for executions before failing
    clock.advance(30)
    (tmp_path / "c.yaml").write_text(f"ib: {{account: DU1234567}}\nmonitoring: {{state_dir: {cfg.monitoring.state_dir}, start: '2025-09'}}\n")
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    cli.main(["flatten", "--config", str(tmp_path / "c.yaml"), "--yes"])
    assert b.fut_pos == 0


def test_reversal_fills_excluded_from_exit_vwap(tmp_path):
    s = TradeState(str(tmp_path), "2025-09-30")
    s.update("t", "ENTERED", direction=1)
    s.merge_fills([dict(exec_id="1", rev=0, ref="MET-2025-09-30-entry", side="BUY", qty=7.0, price=100.0, time="t"),
                   dict(exec_id="2", rev=0, ref="MET-2025-09-30-exit1", side="SELL", qty=14.0, price=101.0, time="t"),
                   dict(exec_id="3", rev=0, ref="MET-2025-09-30-exit2", side="BUY", qty=7.0, price=99.0, time="t")])
    assert s.get("exit_price") == 101.0 and s.get("exit_filled") == 14 and s.get("reversal_fills") == 7 and s.owned == 0


# ---------------------------------------------------------------- review round 8
def test_bot_waits_for_users_manual_close(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59))
    b.manual_open = 1                              # user's MET-…-manual limit order working
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 0, 30))
    assert not [o for o in b.orders if o["action"] == "SELL"]
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.manual_open = 0
    b.market_order(c, "SELL", 7, f"MET-{T}-manual", 5)        # it fills
    run_until(ex, clock, dt.datetime(2025, 9, 30, 17, 0))
    assert b.fut_pos == 0 and st(cfg).phase == "FLAT"
    assert sum(o["action"] == "SELL" for o in b.orders) == 1


def test_disagreement_flag_resets_when_sources_agree(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 59, 57))
    b.pos_glitch_reads = 4                         # one disagreeing round
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 5))
    assert b.fut_pos == 0 and not st(cfg).get("disagree_rounds")


# ---------------------------------------------------------------- review round 9
def test_provisional_signal_before_t1_close(data):
    spy, tlt = data
    cut = dt.date(2026, 10, 20)                       # mid-month: T-1 (29 Oct) not available yet
    e = spy["adjclose"][[d <= cut for d in spy.index]]
    b = tlt["adjclose"][[d <= cut for d in tlt.index]]
    with pytest.raises(DataError):
        compute_signal(dt.date(2026, 10, 30), e, b)
    s = compute_signal(dt.date(2026, 10, 30), e, b, provisional=True)
    assert "provisional" in s.source and s.signal_through == str(cut)


def test_alert_failure_releases_dedupe(monkeypatch):
    from met import alerts as A
    al = A.Alerter("paper")
    calls = []
    monkeypatch.setattr(al, "_telegram", lambda text: calls.append(text) or False)
    monkeypatch.setattr(A.time, "sleep", lambda s: None)
    monkeypatch.setenv("MET_TELEGRAM_TOKEN", "x"); monkeypatch.setenv("MET_TELEGRAM_CHAT_ID", "y")
    al._recent["t"] = A.time.time()
    assert al._deliver("CRITICAL", "t", "body") is False and "t" not in al._recent and len(calls) == 3


def test_edge_dead_persists_without_autoderisk(tmp_path, data):
    import json
    spy, tlt = data
    cfg = Config(); cfg.ib.account = "DU1"
    cfg.monitoring.state_dir = cfg.monitoring.report_dir = str(tmp_path); cfg.monitoring.start = "2023-01"
    cfg.guardrails.auto_derisk = False
    (tmp_path / "monitor.json").write_text(json.dumps({"size_multiplier": 1.0, "edge_dead": True}))
    from met.monitor import size_multiplier as sm
    assert sm(-1.5, 2.2, -2.2, None, was_dead=True)[0] == 0.0
    assert sm(-0.5, 2.2, -2.2, None, was_dead=True)[0] == 0.5
    assert sm(0.5, 2.2, -2.2, None, was_dead=True)[0] == 1.0
