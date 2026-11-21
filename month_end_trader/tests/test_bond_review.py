"""Regression tests for review round B1 (bond leg). Each test reproduces the reviewer's failing scenario and asserts
the fixed behaviour."""
import datetime as dt
import json
from pathlib import Path

import pytest

from met import calendar as cal
from met import cli
from met.bond import BondExecutor
from met.broker import OrderResult
from met.daemon import Daemon
from met.state import TradeState, unfinished
from test_bond import benv, bst, drift, lv, run, zn_orders  # noqa: F401  (fixture + helpers)


class Crash(BaseException):
    """Simulated process death (not an Exception: the daemon's error handling must not catch it)."""


def crash_after_zn_send(b):
    orig = b.market_order

    def mo(c, action, qty, ref, timeout_s):
        r = orig(c, action, qty, ref, timeout_s)
        if c.symbol == "ZN":
            raise Crash()
        return r
    b.market_order = mo
    return orig


def restart(cfg, b, d, bx):
    bx2 = BondExecutor(cfg, b, bx.alert._inner, bx.fetchers)
    return Daemon([d.legs[0], bx2]), bx2


def test_r1_crash_after_entry_send_is_adopted_and_exited(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    orig = crash_after_zn_send(b)
    with pytest.raises(Crash):
        run(d, clock, dt.datetime(2025, 9, 25, 16, 30))
    b.market_order = orig
    assert b.zn_pos == 3
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)          # back after midnight: executions expired
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 10, 1, 12, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.get("adopted") and s.get("pnl_estimated") and s.phase == "FLAT" and b.zn_pos == 0
    sells = [o for o in zn_orders(b) if o["action"] == "SELL"]
    assert len(sells) == 1 and sells[0]["t"].date() == dt.date(2025, 9, 30)      # held to the scheduled exit
    assert any("ADOPTED" in t for t in lv(alerts, "CRITICAL"))


def test_r1_no_adoption_when_quantity_differs(benv):
    """Broker shows something other than the planned quantity: never adopt, leave it to the user."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    orig = crash_after_zn_send(b)
    with pytest.raises(Crash):
        run(d, clock, dt.datetime(2025, 9, 25, 16, 30))
    b.market_order = orig
    c = b.front_future("ZN", "CBOT", dt.date(2025, 12, 1))
    b.market_order(c, "BUY", 1, "USER-x", 10)                          # the user added one
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 26, 12, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert not s.get("adopted") and b.zn_pos == 4


def test_r1_adoption_can_be_disabled(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    cfg.bond.adopt_unknown_entry = False
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    orig = crash_after_zn_send(b)
    with pytest.raises(Crash):
        run(d, clock, dt.datetime(2025, 9, 25, 16, 30))
    b.market_order = orig
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 26, 12, 0))
    assert not bst(cfg, dt.date(2025, 9, 30)).get("adopted") and b.zn_pos == 3


def test_r2_resolve_bond_leg(benv, monkeypatch):
    """Exit fill lost on X (crash before merge), restart X+1: `met resolve X --leg bond` resolves the BOND state and the
    next month's window is not blocked."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 15, 54, 50))
    orig = crash_after_zn_send(b)
    with pytest.raises(Crash):
        run(d, clock, dt.datetime(2025, 9, 30, 16, 30))
    b.market_order = orig
    clock.t = dt.datetime(2025, 10, 1, 9, 0, tzinfo=cal.ET)
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 10, 1, 14, 0))
    assert any("--leg bond" in body for l, t, body in alerts if l == "CRITICAL" and "RESOLVE" in t)
    monkeypatch.setattr(cli, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    assert cli.main(["resolve", "2025-09-30", "--leg", "bond", "--yes", "--price", "110"]) == 0
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.owned == 0 and s.get("reconciled") and "2025-09-30" not in unfinished(cfg.monitoring.state_dir, prefix="bond")
    a = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    assert not any(f["ref"].startswith("MET-") and "manual" in f["ref"] for f in (a.get("fills") or {}).values())
    d3, _ = restart(cfg, b, d, bx)
    run(d3, clock, dt.datetime(2025, 10, 31, 20, 0))
    assert bst(cfg, dt.date(2025, 10, 31)).phase == "FLAT"


def test_r3_bond_exit_timeouts_do_not_delay_leg_a(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 15, 54, 0))
    orig = b.market_order
    left = {"n": 3}

    def mo(c, action, qty, ref, timeout_s):
        if c.symbol == "ZN" and left["n"] > 0:
            left["n"] -= 1
            clock.advance(timeout_s + 15)                    # IBKRBroker: wait, cancel, wait 15 s
            b.orders.append(dict(id="x", ref=ref, action=action, qty=qty, status="Timeout", t=clock.now()))
            return OrderResult("x", "Timeout", qty, 0, 0.0, "timed out")
        return orig(c, action, qty, ref, timeout_s)
    b.market_order = mo
    run(d, clock, dt.datetime(2025, 9, 30, 17, 0))
    mes = [o for o in b.orders if o["ref"].startswith("MET-2025-09-30-exit")]
    assert mes and mes[0]["t"].time() < dt.time(16, 0)
    assert bst(cfg, dt.date(2025, 9, 30)).phase == "FLAT" and b.zn_pos == 0


def test_r4_late_entry_fill_while_down_is_adopted_not_mislabelled(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    b.fill_delay_next = (900, False)
    run(d, clock, dt.datetime(2025, 9, 25, 16, 5))
    clock.t = dt.datetime(2025, 9, 25, 16, 20, tzinfo=cal.ET)
    b._process_pending()
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 30, 20, 0))
    assert not any("not from the bot's orders" in t for t in lv(alerts, "CRITICAL"))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.get("adopted") and s.phase == "FLAT" and b.zn_pos == 0


def test_r5_bond_monitor_refreshed_when_missing(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    calls = []
    bx.reporter = lambda day: calls.append(day) or None
    run(d, clock, dt.datetime(2025, 9, 25, 16, 30))
    assert calls                                         # refreshed (eve-of-E announce and/or preflight)


def test_r7_monitor_file_is_not_a_trade_state(tmp_path):
    (tmp_path / "bond_monitor.json").write_text("{broken")
    assert unfinished(str(tmp_path), prefix="bond") == []


def test_r10_bond_leverage_above_leg_a_hard_cap(benv):
    """bond.max_leverage up to the bond hard limit (6x) is honoured, not silently capped at leg A's 4x."""
    from met.guardrails import size_contracts
    q, eff = size_contracts(100_000, 100.0, 1000.0, 5.0, 1.0, 5.5, 20, "floor", hard_max_leverage=6.0, hard_max_contracts=50)
    assert q == 5 and eff == pytest.approx(5.0)


@pytest.mark.parametrize("inst,sym,mult", [("ZN", "ZNZ5", 1000.0), ("MTN", "MTNZ5", 100.0)])
def test_cli_test_order_bond_leg(benv, monkeypatch, capsys, inst, sym, mult):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 16, 11, 0), zn_fn=drift(108.0))
    cfg.bond.instrument = inst; cfg.bond.hedge_ratio = 0.0; cfg.bond.max_contracts = 0; cfg.validate()
    b.connect()
    monkeypatch.setattr(cli, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    assert cli.main(["test-order", "--leg", "bond"]) == 0
    out = capsys.readouterr().out
    assert sym in out and f"multiplier {mult:g}" in out and b.zn_pos == 0
    tests = [o for o in b.orders if o["ref"].startswith("TEST-")]
    assert [o["action"] for o in tests] == ["BUY", "SELL"]
    assert (b.limits["TEST-buy"] is not None) == (inst == "MTN")
