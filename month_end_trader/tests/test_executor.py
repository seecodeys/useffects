"""Daemon / executor behaviour against the simulated broker, including failure modes and restarts.
Trade date used: 2025-09-30 (signal LONG, SPY rose open->close) unless noted."""
import datetime as dt
import json
from pathlib import Path

import pytest

from met import calendar as cal
from met.config import Config, ConfigError
from met.executor import Executor
from met.guardrails import size_contracts
from met.state import TradeState
from conftest import run_until

T = dt.date(2025, 9, 30)


def st(cfg, d=T):
    return TradeState(cfg.monitoring.state_dir, str(d))


def levels(alerts, lvl):
    return [t for l, t, _ in alerts if l == lvl]


def test_happy_path_long(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("reconciled") and s.get("reported")
    assert s.get("plan")["action"] == "BUY"
    assert [o["action"] for o in b.orders] == ["BUY", "SELL"]
    entry_t, exit_t = b.orders[0]["t"], b.orders[1]["t"]
    assert entry_t.time() >= dt.time(9, 30, 1) and entry_t.time() < dt.time(9, 30, 5)
    assert dt.time(15, 59, 58) <= exit_t.time() < dt.time(16, 0, 0)
    assert b.fut_pos == 0 and s.get("net_pnl") > 0
    assert 1.5 <= s.get("plan")["effective_leverage"] <= 2.0
    assert not levels(alerts, "CRITICAL") and not levels(alerts, "WARN")
    assert any("Tomorrow (2025-09-30)" in t for t in levels(alerts, "INFO"))


def test_no_double_entry_after_flat(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    n = len(b.orders)
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, ex.reporter)       # restart
    run_until(ex2, clock, dt.datetime(2025, 10, 1, 10, 0))
    assert len(b.orders) == n


def test_short_trade(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 10, 30, 8, 0))
    run_until(ex, clock, dt.datetime(2025, 10, 31, 19, 0))
    s = st(cfg, dt.date(2025, 10, 31))
    assert s.phase == "FLAT" and s.get("plan")["action"] == "SELL"
    assert [o["action"] for o in b.orders] == ["SELL", "BUY"]


def test_early_close_day_exits_before_1pm(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 11, 26, 8, 0))
    run_until(ex, clock, dt.datetime(2025, 11, 28, 16, 0))
    s = st(cfg, dt.date(2025, 11, 28))
    assert s.phase == "FLAT"
    assert dt.time(12, 59, 58) <= b.orders[-1]["t"].time() < dt.time(13, 0)


def test_december_uses_next_quarter_contract(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 12, 30, 8, 0))
    run_until(ex, clock, dt.datetime(2025, 12, 31, 19, 0))
    s = st(cfg, dt.date(2025, 12, 31))
    assert s.get("contract_local_symbol") == "MESH6"     # Dec 2025 contract expired 19 Dec


def test_kill_switch_before_entry(env):
    cfg, clock, b, ex, alerts = env()
    Path(cfg.monitoring.state_dir, "KILL").write_text("x")
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_kill_switch_while_holding_flattens(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    assert st(cfg).phase == "ENTERED"
    Path(cfg.monitoring.state_dir, "KILL").write_text("x")
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 2))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("exit_reason") == "kill switch" and b.fut_pos == 0


def test_entry_rejected_no_exit_orders(env):
    cfg, clock, b, ex, alerts = env()
    b.reject_next = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "ERROR" and len(b.orders) == 1 and b.fut_pos == 0
    assert any("ENTRY FAILED" in t for t in levels(alerts, "CRITICAL"))


def test_partial_fill_exits_only_filled(env):
    cfg, clock, b, ex, alerts = env()
    b.partial_next = 3
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("entry_filled") == 3
    assert b.orders[1]["qty"] == 3 and b.fut_pos == 0


def test_exit_rejections_are_retried(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 2
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and b.fut_pos == 0
    assert sum(o["status"] == "Rejected" for o in b.orders) == 2
    assert len(levels(alerts, "CRITICAL")) == 2


def test_exit_keeps_retrying_until_flat(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 7          # more than one exit round of 5 attempts
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "FLAT" and b.fut_pos == 0
    assert any("STILL OPEN" in t for t in levels(alerts, "CRITICAL"))


def test_cannot_connect_means_no_late_entry(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 0))
    b.connect_fail = 10_000
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    b.connect_fail = 0
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_started_after_entry_window_skips(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 45))
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_restart_mid_trade_resumes_and_exits(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    assert st(cfg).phase == "ENTERED"
    b.disconnect()
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, ex.reporter)       # new process, same state dir
    run_until(ex2, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and b.fut_pos == 0 and len(b.orders) == 2
    assert dt.time(15, 59, 58) <= b.orders[1]["t"].time() < dt.time(16, 0)


def test_disconnect_during_hold_reconnects_and_exits(env):
    cfg, clock, b, ex, alerts = env()
    b.disconnect_at = dt.datetime(2025, 9, 30, 13, 0, tzinfo=cal.ET)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "FLAT" and b.fut_pos == 0


def test_recover_from_crash_after_entry_submit(env):
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 29))
    s = st(cfg); assert s.phase == "PLANNED"
    # simulate: order went out and filled, process died before recording it
    clock.advance(65)
    c = b.front_future("MES", "CME", T + dt.timedelta(days=3))
    b.market_order(c, "BUY", s.get("qty_planned"), "manual", 5)
    s.update(clock.now().isoformat(), "ENTRY_SUBMITTED", note="crash")
    ex2 = Executor(cfg, b, ex.alert, ex.fetchers, ex.reporter)
    run_until(ex2, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("entry_status") == "Recovered" and b.fut_pos == 0


def test_disaster_stop(env):
    cfg, clock, b, ex, alerts = env()
    o = b.day_path[T][0]
    b.day_path[T] = (o, o * 0.95, [(3600, o * 0.96)])        # -4% one hour in: 2x overlay ~ -8% of equity
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.get("exit_reason") == "disaster stop"
    assert b.orders[1]["t"].time() < dt.time(10, 40)
    assert any("DISASTER STOP" in t for t in levels(alerts, "CRITICAL"))


def test_existing_futures_position_blocks(env):
    cfg, clock, b, ex, alerts = env()
    b.fut_pos = 1; b.fut_cost = 6600.0
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "SKIPPED" and any("existing MES" in x for x in s.get("blockers"))
    assert b.orders == []


def test_margin_buffer_blocks(env):
    cfg, clock, b, ex, alerts = env()
    b.whatif_excess = 1_000.0
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_core_policy_block_and_warn(env):
    cfg, clock, b, ex, alerts = env(core_shares=0)
    cfg.guardrails.core_policy = "block"
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED"


def test_core_policy_warn_still_trades(env):
    cfg, clock, b, ex, alerts = env(core_shares=0, cash=120_000.0)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and any("core SPY weight" in w for w in s.get("warnings"))


def test_signal_source_disagreement_skips(env):
    cfg, clock, b, ex, alerts = env()
    bad = b.closes["TLT"].copy(); bad.loc[dt.date(2025, 9, 29)] *= 0.8
    ex.fetchers["yahoo"] = lambda s: b.daily_adjusted_closes(s) if s == "SPY" else bad
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "SKIPPED" and any("disagree" in x for x in s.get("blockers"))


def test_price_unavailable_skips(env):
    cfg, clock, b, ex, alerts = env()
    b.price_unavailable = True
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


@pytest.mark.parametrize("mult,expect", [(0.5, "half"), (0.0, "skip")])
def test_auto_derisk(env, mult, expect):
    cfg, clock, b, ex, alerts = env()
    Path(cfg.monitoring.state_dir, "monitor.json").write_text(json.dumps({"size_multiplier": mult, "status": "test"}))
    ex.reporter = None
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    if expect == "skip":
        assert s.phase == "SKIPPED"
    else:
        assert s.phase == "FLAT" and 0.7 <= s.get("plan")["effective_leverage"] <= 1.0


def test_live_mode_requires_confirmation(env, monkeypatch):
    cfg, clock, b, ex, alerts = env()
    cfg.mode = "live"; cfg.ib.account = "U7654321"; cfg.ib.port = 4001; b.account = "U7654321"
    monkeypatch.delenv("MET_LIVE_CONFIRM", raising=False)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "SKIPPED" and b.orders == [] and any("MET_LIVE_CONFIRM" in x for x in s.get("blockers"))


def test_wrong_account_blocks(env):
    cfg, clock, b, ex, alerts = env()
    b.account = "DU9999999"
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    assert st(cfg).phase == "SKIPPED" and b.orders == []


def test_overnight_guard_alerts_on_foreign_position(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 15, 8, 0))
    b.connect()
    b.fut_pos = 2; b.fut_cost = 2 * 6600.0
    run_until(ex, clock, dt.datetime(2025, 9, 15, 17, 0))
    assert any("open after the close" in t for t in levels(alerts, "CRITICAL"))
    assert b.orders == []          # never touches positions it does not own


# ------------------------------------------------------------------ config and sizing
def test_config_validation():
    c = Config(); c.ib.account = "DU1"
    c.validate()
    for mutate in [lambda c: setattr(c.ib, "account", "U1"),                     # paper mode, live account
                   lambda c: setattr(c.ib, "port", 4001),                          # paper mode, live port
                   lambda c: setattr(c.strategy, "leverage", 3.5),                 # > max_leverage 3
                   lambda c: setattr(c.guardrails, "max_leverage", 5.0),          # > hard cap
                   lambda c: setattr(c.strategy, "exit_lead_s", 0),
                   lambda c: setattr(c.strategy, "instrument", "SPX")]:
        c2 = Config(); c2.ib.account = "DU1"; mutate(c2)
        with pytest.raises(ConfigError):
            c2.validate()


def test_sizing():
    assert size_contracts(120_000, 7340, 5, 2.0, 1.0, 3.0, 40) == (6, pytest.approx(6 * 7340 * 5 / 120_000))
    assert size_contracts(120_000, 7340, 5, 2.0, 0.5, 3.0, 40)[0] == 3
    assert size_contracts(10_000_000, 7340, 5, 2.0, 1.0, 3.0, 40)[0] == 40           # max_contracts
    assert size_contracts(20_000, 7340, 5, 2.0, 1.0, 3.0, 40)[0] == 1
    assert size_contracts(15_000, 7340, 5, 2.0, 1.0, 2.0, 40)[0] == 0                # 1 contract would be 2.4x > cap
    assert size_contracts(0, 7340, 5, 2.0, 1.0, 3.0, 40)[0] == 0
