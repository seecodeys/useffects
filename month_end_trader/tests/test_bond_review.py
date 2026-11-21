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


# ---------------------------------------------------------------- review round B3
def test_b3_test_order_unfilled_buy_sells_nothing(benv, monkeypatch, capsys):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 16, 11, 0), zn_fn=drift(108.0))
    cfg.bond.instrument = "MTN"; cfg.bond.hedge_ratio = 0.0; cfg.bond.max_contracts = 0; cfg.validate()
    b.connect()
    b.reject_next = 1
    monkeypatch.setattr(cli, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    assert cli.main(["test-order", "--leg", "bond"]) == 2
    assert b.zn_pos == 0 and not [o for o in b.orders if o["ref"] == "TEST-sell"]


def test_b3_unknown_futures_value_blocks_instead_of_zero(benv):
    from met.broker import Position
    from met.guardrails import total_leverage_blocker
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 15, 0), zn_fn=drift())
    b.connect()
    orig = b.positions
    b.positions = lambda: orig() + [Position("ZN", "FUT", 30, "ZNZ5", float("nan"))]
    msg = total_leverage_blocker(cfg, b, "MES", 100_000, 120_000)
    assert msg and "failed" in msg


def test_b3_instrument_switch_mid_hold_keeps_old_instrument(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 26, 12, 0))
    assert bst(cfg, dt.date(2025, 9, 30)).phase == "ENTERED"
    cfg.bond.instrument = "MTN"; cfg.bond.hedge_ratio = 0.0; cfg.bond.max_contracts = 0; cfg.validate()
    d2, bx2 = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0 and s.get("est_costs") == pytest.approx(6 * 1.70)
    crit = lv(alerts, "CRITICAL")
    assert not any("differs from the bot" in t for t in crit), crit
    assert sum("unfinished trade is in ZN" in t for t in crit) >= 1


def test_b3_provisional_vol(data):
    from met.config import Config
    from met.guardrails import vol_scale
    spy, _ = data
    cfg = Config(); cfg.strategy.vol_tilt = True
    T = dt.date(2008, 10, 31)
    early = spy["adjclose"][[x <= dt.date(2008, 10, 27) for x in spy.index]]
    sc, v, notes = vol_scale(cfg, T, {"x": lambda s: early}, provisional=True)
    assert v is not None and sc == 1.0
    assert vol_scale(cfg, T, {"x": lambda s: early})[1] is None            # live: never provisional


def test_b3_bond_exec_gap_excludes_stops_and_nets_carry(tmp_path):
    from met.config import Config
    cfg = Config(); cfg.ib.account = "DU1"; cfg.monitoring.state_dir = cfg.monitoring.report_dir = str(tmp_path)
    cfg.monitoring.start = "2025-01"
    led = bond_ledger_fixture()
    X = dt.date(2025, 9, 30)
    paper = float(led.loc[X, "paper_bp"]) + 2.0
    carry = 1e4 * 0.04 * 5 / 365
    for x, reason, bp in ((X, "scheduled close", paper - carry), (dt.date(2025, 8, 29), "disaster stop", -400.0)):
        st = TradeState(str(tmp_path), str(x), prefix="bond")
        E = {X: "2025-09-25", dt.date(2025, 8, 29): "2025-08-26"}[x]
        st.update("t", "FLAT", plan=dict(entry_date=E, hedge_ratio=1.0), trade_bp=bp, exit_reason=reason)
    res = run_bond_report(cfg, ief=ief_series())
    assert res["execution_gap_bp"] == pytest.approx(0.0, abs=0.05)


def ief_series():
    import pandas as pd
    from pathlib import Path
    x = pd.read_csv(Path(__file__).resolve().parent / "fixtures" / "ief.csv")
    return pd.Series(x["adjclose"].values, index=pd.to_datetime(x["date"]).dt.date)


def bond_ledger_fixture():
    from met.bond_monitor import bond_ledger
    return bond_ledger(ief_series())


from met.bond_monitor import run_bond_report  # noqa: E402


# ---------------------------------------------------------------- review round B4
def test_b4_preflight_before_entry_on_sifma_entry_day(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    tm = bx.times(dt.date(2027, 3, 31))
    assert tm["E"] == dt.date(2027, 3, 25) and tm["entry"].time() == dt.time(12, 50)     # Holy Thursday
    assert tm["preflight"] < tm["entry"] and tm["preflight"].time() == dt.time(12, 22)
    tm = bx.times(dt.date(2025, 9, 30))
    assert tm["preflight"].time() == dt.time(15, 30)                                       # normal days unchanged


def test_b4_announce_survives_gateway_restart_at_1630(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 24, 16, 29))
    down_until = dt.datetime(2025, 9, 24, 16, 36, tzinfo=cal.ET)
    orig = b.connect

    def connect():
        if clock.now() < down_until:
            raise __import__("met.broker", fromlist=["BrokerError"]).BrokerError("gateway restarting")
        orig()
    b.connect = connect
    b.disconnect_at = dt.datetime(2025, 9, 24, 16, 29, 30, tzinfo=cal.ET)
    run(d, clock, dt.datetime(2025, 9, 24, 18, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.get("announced")
    assert not any("not connected the evening before the bond entry" in t for t in lv(alerts, "CRITICAL"))


# ---------------------------------------------------------------- review round B6 (data robustness)
from conftest import run_until  # noqa: E402
from met.signal import DataError  # noqa: E402


def _yahoo_with(b, tlt_factor, fail_after=None):
    n = {"calls": 0}

    def yahoo(sym):
        n["calls"] += 1
        if fail_after is not None and n["calls"] > fail_after:
            raise DataError(f"yahoo {sym}: HTTP 429")
        s = b.daily_adjusted_closes(sym).copy()
        if sym == "TLT":
            s[s.index <= dt.date(2025, 9, 1)] *= tlt_factor
        return s
    return yahoo


def test_b6_disagreement_not_cleared_by_a_429(env):
    """Sources disagree badly (5% TLT error), then Yahoo rate-limits: the bot must not trade on IBKR alone."""
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 0))
    ex.fetchers["yahoo"] = _yahoo_with(b, 0.95, fail_after=6)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 35))
    s = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    assert s.phase == "SKIPPED" and s.get("required_sources") == ["ibkr", "yahoo"]
    assert not [o for o in b.orders if o["ref"].startswith("MET-")]


def test_b6_missing_dividend_same_direction_trades_with_warning(env):
    """One source missing a ~0.35% TLT dividend while the gap is far larger: trade, with a warning."""
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 9, 0))
    b.connect()
    gap = abs(__import__("met.signal", fromlist=["compute_signal"]).compute_signal(
        dt.date(2025, 9, 30), b.daily_adjusted_closes("SPY"), b.daily_adjusted_closes("TLT")).gap)
    ex.fetchers["yahoo"] = _yahoo_with(b, 1 - 0.0035)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 35))
    s = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    if gap > 0.004:
        assert s.phase == "ENTERED" and any("cannot flip" in w for w in s.get("warnings"))
    else:
        assert s.phase == "SKIPPED"


def test_b6_missing_or_stale_monitor_means_reduced_size(tmp_path):
    from met.config import Config
    from met.guardrails import read_derisk
    from met.bond import read_bond_derisk
    cfg = Config(); cfg.ib.account = "DU1"; cfg.monitoring.state_dir = str(tmp_path); cfg.monitoring.start = "2026-11"
    T = dt.date(2026, 12, 31)                       # 30 Nov 2026 (tracked) has completed
    assert read_derisk(cfg, T)[0] == 0.5 and read_bond_derisk(cfg, T)[0] == 0.5
    assert read_derisk(cfg, dt.date(2026, 11, 30))[0] == 1.0       # first tracked trade: nothing to be stale yet
    (tmp_path / "monitor.json").write_text(json.dumps({"asof": "2026-11-27", "size_multiplier": 1.0}))
    assert read_derisk(cfg, T)[0] == 0.5                               # before the 30 Nov trade
    (tmp_path / "monitor.json").write_text(json.dumps({"asof": "2026-11-30", "size_multiplier": 1.0}))
    assert read_derisk(cfg, T)[0] == 1.0
    (tmp_path / "monitor.json").write_text(json.dumps({"asof": "2026-11-27", "size_multiplier": 0.0}))
    assert read_derisk(cfg, T)[0] == 0.0                               # a stop signal is never relaxed


def test_b6_failed_report_is_retried(env):
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 30, 8, 0))
    calls = []

    def rep(d):
        calls.append(clock.now())
        if len(calls) < 3:
            raise DataError("yahoo: HTTP 429")
        return {"status": "ok", "summary": ""}
    ex.reporter = rep
    run_until(ex, clock, dt.datetime(2025, 9, 30, 23, 0))
    s = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    assert s.get("reported") and len([c for c in calls if c.date() == dt.date(2025, 9, 30) and c.hour >= 18]) == 3


def test_b6_yahoo_cache_and_deadline(monkeypatch):
    import time
    from met import signal as sg
    from met.cli import make_fetchers
    from met.config import Config
    n = {"get": 0}

    class R:
        status_code = 429
    monkeypatch.setattr(sg.requests, "get", lambda *a, **k: (n.__setitem__("get", n["get"] + 1), R())[1])
    monkeypatch.setattr(sg.time, "sleep", lambda s: None)
    t = [0.0]
    monkeypatch.setattr(sg.time, "monotonic", lambda: (t.__setitem__(0, t[0] + 20), t[0])[1])
    with pytest.raises(DataError):
        sg.yahoo_daily("SPY")
    assert n["get"] <= 2                                               # 30 s budget, not 4 attempts x 20 s
