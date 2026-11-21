"""Leg B (month-end Treasury bid, ZN futures) against the simulated broker, together with leg A in one daemon."""
import datetime as dt
import json
from pathlib import Path

import pandas as pd
import pytest

from met import calendar as cal
from met.bond import (BondExecutor, bond_preflight, contract_month_for, entry_session, first_notice_day,
                      window_for)
from met.bond_monitor import bond_ledger, run_bond_report
from met.config import Config, ConfigError
from met.daemon import Daemon
from met.state import TradeState

FIX = Path(__file__).resolve().parent / "fixtures"


def bst(cfg, X):
    return TradeState(cfg.monitoring.state_dir, str(X), prefix="bond")


def zn_orders(b):
    return [o for o in b.orders if o["ref"].startswith("MEB-")]


def lv(alerts, level):
    return [t for l, t, _ in alerts if l == level]


@pytest.fixture
def benv(env):
    """env + bond leg enabled: returns (cfg, clock, broker, daemon, alerts, bond_executor)."""
    def make(start, zn_fn=None, **kw):
        cfg, clock, b, ex, alerts = env(start, **kw)
        cfg.bond.enabled = True
        if zn_fn:
            b.zn_price_fn = zn_fn
        bx = BondExecutor(cfg, b, ex.alert, ex.fetchers, reporter=None)
        return cfg, clock, b, Daemon([ex, bx]), alerts, bx
    return make


def run(d, clock, until):
    until = until.replace(tzinfo=cal.ET) if until.tzinfo is None else until
    d.run_forever(stop=lambda: clock.now() >= until, lock=False)


def drift(start_px=110.0, per_day=0.05):
    """Price rising per_day points per calendar day from 2025-01-01."""
    t0 = dt.datetime(2025, 1, 1, tzinfo=cal.ET)
    return lambda now: start_px + per_day * (now - t0).total_seconds() / 86400


# ------------------------------------------------------------------------------------------ calendar / contract
def test_window_and_first_notice():
    assert entry_session(dt.date(2026, 11, 30), 3) == dt.date(2026, 11, 24)      # skips Thanksgiving
    assert window_for(dt.date(2026, 11, 21), 3) == (dt.date(2026, 11, 24), dt.date(2026, 11, 30))
    assert first_notice_day(2026, 12) == dt.date(2026, 11, 30)
    assert first_notice_day(2027, 3) == dt.date(2027, 2, 26)


@pytest.mark.parametrize("X,ym", [
    (dt.date(2026, 11, 30), (2027, 3)),     # Dec contract's first notice day IS the exit day
    (dt.date(2026, 2, 27), (2026, 6)),
    (dt.date(2026, 5, 29), (2026, 9)),
    (dt.date(2026, 8, 31), (2026, 12)),
    (dt.date(2025, 12, 31), (2026, 3)),
    (dt.date(2026, 1, 30), (2026, 3)),
    (dt.date(2026, 10, 30), (2026, 12)),
])
def test_contract_month_avoids_first_notice(X, ym):
    y, m, fnd = contract_month_for(X, 5)
    assert (y, m) == ym and fnd > X


def test_every_month_2003_2030_has_buffer():
    for y in range(2003, 2031):
        for m in range(1, 13):
            X = cal.last_session_of_month(y, m)
            _, _, fnd = contract_month_for(X, 5)
            assert len(cal.sessions(X + dt.timedelta(days=1), fnd)) > 5


def test_config_validation():
    c = Config(); c.ib.account = "DU1"
    c.bond.leverage = 5.0
    with pytest.raises(ConfigError, match="bond.leverage"):
        c.validate()
    c = Config(); c.ib.account = "DU1"; c.bond.entry_grace_s = 200
    with pytest.raises(ConfigError, match="entry_grace_s"):
        c.validate()
    c = Config(); c.ib.account = "DU1"; c.strategy.vol_threshold = 0.13      # annualised by mistake
    with pytest.raises(ConfigError, match="DAILY"):
        c.validate()


# ------------------------------------------------------------------------------------------ happy paths
def test_happy_path_both_legs(benv):
    """Sep 2025: E = Thu 25 Sep, X = Tue 30 Sep (weekend inside the hold). Leg A trades on the 30th as usual."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    X = dt.date(2025, 9, 30)
    s = bst(cfg, X)
    assert s.phase == "FLAT" and s.get("reconciled"), s.data["events"][-5:]
    zo = zn_orders(b)
    assert [o["action"] for o in zo] == ["BUY", "SELL"]
    assert zo[0]["t"].date() == dt.date(2025, 9, 25) and dt.time(15, 58) <= zo[0]["t"].time() < dt.time(15, 59)
    assert zo[1]["t"].date() == X and dt.time(15, 55) <= zo[1]["t"].time() < dt.time(15, 56)     # clear of leg A
    p = s.get("plan")
    assert p["contract_local_symbol"] == "ZNZ5" and p["entry_date"] == "2025-09-25"
    assert 2.5 <= p["effective_leverage"] <= 3.3 and p["qty"] == zo[0]["qty"]
    assert s.get("net_pnl") > 0 and b.zn_pos == 0
    a = TradeState(cfg.monitoring.state_dir, str(X))
    assert a.phase == "FLAT" and a.get("reconciled")
    assert b.fut_pos == 0
    assert not lv(alerts, "CRITICAL"), lv(alerts, "CRITICAL")
    assert any(t.startswith("BOND") and "PLANNED BUY" in t for t in lv(alerts, "INFO"))


def test_november_window_uses_march_contract_and_half_day(benv):
    """Nov 2025: X = Fri 28 Nov (1 pm close), E = Mon 24 Nov; Thanksgiving inside the hold. Dec contract's first
    notice day is 28 Nov -> March contract; exit at 12:50 (CBOT rates halt 13:00)."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 11, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 11, 28, 18, 0))
    s = bst(cfg, dt.date(2025, 11, 28))
    assert s.phase == "FLAT" and s.get("plan")["contract_local_symbol"] == "ZNH6"
    zo = zn_orders(b)
    assert zo[0]["t"].date() == dt.date(2025, 11, 24)
    assert zo[1]["t"].date() == dt.date(2025, 11, 28) and dt.time(12, 50) <= zo[1]["t"].time() < dt.time(12, 51)


def test_restart_mid_hold_keeps_ownership(benv):
    """Executions of the entry day are gone after midnight (IB Gateway): ownership must come from saved fills."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 26, 11, 0))
    assert bst(cfg, dt.date(2025, 9, 30)).phase == "ENTERED"
    assert b.executions("MEB-") == []                       # yesterday's fills no longer reported
    bx2 = BondExecutor(cfg, b, bx.alert._inner, bx.fetchers)            # restart: fresh in-memory state
    d2 = Daemon([d.legs[0], bx2])
    run(d2, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0
    assert [o["action"] for o in zn_orders(b)] == ["BUY", "SELL"]
    assert not any("stopped reporting" in t for t in lv(alerts, "WARN"))


# ------------------------------------------------------------------------------------------ guards
def test_overnight_disaster_stop(benv):
    """ZN collapses Sunday evening (futures reopen 18:00): the bot exits early, once."""
    def fn(now):
        return 110.0 if now < dt.datetime(2025, 9, 28, 19, 0, tzinfo=cal.ET) else 104.0
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=fn)
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and s.get("exit_reason") == "disaster stop"
    zo = zn_orders(b)
    assert [o["action"] for o in zo] == ["BUY", "SELL"]
    assert zo[1]["t"].date() == dt.date(2025, 9, 28) and zo[1]["t"].time() >= dt.time(19, 0)
    assert any("DISASTER STOP" in t for t in lv(alerts, "CRITICAL"))


def test_no_price_check_while_market_closed(benv):
    """Weekend: CBOT closed, no price -> no 'disaster stop is blind' alarm."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 26, 17, 30))
    b.price_unavailable = True
    run(d, clock, dt.datetime(2025, 9, 28, 17, 0))
    assert not any("blind" in t for t in lv(alerts, "CRITICAL"))
    b.price_unavailable = False


def test_kill_switch_on_weekend_flattens_at_reopen(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 27, 12, 0))                 # Saturday
    (Path(cfg.monitoring.state_dir) / "KILL").write_text("x")
    run(d, clock, dt.datetime(2025, 9, 28, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and s.get("exit_reason") == "kill switch"
    sell = zn_orders(b)[-1]
    assert sell["action"] == "SELL" and sell["t"] >= dt.datetime(2025, 9, 28, 18, 0, tzinfo=cal.ET)
    assert b.zn_pos == 0


def test_missed_entry_day_skips_month(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 26, 9, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "SKIPPED" and not zn_orders(b)


def test_foreign_zn_position_blocks_and_is_untouched(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    b.connect()
    c = b.front_future("ZN", "CBOT", dt.date(2025, 12, 1))
    b.market_order(c, "BUY", 1, "USER-1", 10)
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "SKIPPED" and any("existing ZN" in x for x in s.get("blockers"))
    assert b.zn_pos == 1 and not zn_orders(b)


def test_small_account_rounds_to_zero(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift(), cash=0.0, core_shares=40)
    run(d, clock, dt.datetime(2025, 9, 25, 17, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "SKIPPED" and any("rounds to 0" in x for x in s.get("blockers"))


def test_margin_block_retries_then_skips(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    b.whatif_excess = 100.0
    run(d, clock, dt.datetime(2025, 9, 25, 17, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "SKIPPED" and s.get("preflight_attempts", 0) >= 10 and not zn_orders(b)


def test_entry_rejected_is_error_and_reconciled(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 50))
    b.reject_next = 1
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "ERROR" and s.get("reconciled")
    assert [o["action"] for o in zn_orders(b)] == ["BUY"] and b.zn_pos == 0


def test_late_entry_fill_becomes_normal_hold(benv):
    """Entry reports Timeout but fills 40 s later: the bot sees its own execution and holds to month-end."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 50))
    b.fill_delay_next = (40, False)
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0
    assert s.get("exit_reason") == "scheduled close"
    sells = [o for o in zn_orders(b) if o["action"] == "SELL"]
    assert len(sells) == 1 and sells[0]["t"].date() == dt.date(2025, 9, 30)


def test_exit_rejections_retry_until_flat(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 15, 50))
    b.reject_next = 3
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0
    assert sum(1 for o in zn_orders(b) if o["action"] == "SELL" and o["status"] == "Rejected") == 3


def test_legs_do_not_touch_each_other(benv):
    """Leg A cancels/reads only MET- orders and MES positions; leg B only MEB- and ZN."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    a_refs = {o["ref"].split("-")[0] for o in b.orders if not o["ref"].startswith("MEB-")}
    assert a_refs == {"MET"}
    assert all(o["ref"].startswith("MEB-2025-09-30-") for o in zn_orders(b))
    assert TradeState(cfg.monitoring.state_dir, "2025-09-30").get("contract_local_symbol").startswith("MES")


def test_unfinished_bond_trade_after_window_is_driven_flat(benv):
    """Daemon dies before the exit and comes back the next day: the leftover ZN is closed (overnight path)."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 15, 0))
    clock.t = dt.datetime(2025, 10, 1, 9, 0, tzinfo=cal.ET)             # down over the exit
    run(d, clock, dt.datetime(2025, 10, 1, 12, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0
    assert any("still open after its trade day" in t for t in lv(alerts, "CRITICAL"))


# ------------------------------------------------------------------------------------------ monitor
def ief_fixture():
    d = pd.read_csv(FIX / "ief.csv")
    return pd.Series(d["adjclose"].values, index=pd.to_datetime(d["date"]).dt.date)


def test_bond_ledger_matches_research():
    led = bond_ledger(ief_fixture())
    assert len(led) >= 285
    assert led["paper_bp"].mean() == pytest.approx(20.3, abs=1.0)
    assert 0.6 <= (led["paper_bp"] > 0).mean() <= 0.65


def test_bond_report_and_derisk(tmp_path):
    cfg = Config(); cfg.ib.account = "DU1"
    cfg.monitoring.state_dir = cfg.monitoring.report_dir = str(tmp_path)
    cfg.monitoring.start = "2023-01"
    res = run_bond_report(cfg, ief=ief_fixture())
    j = json.loads((tmp_path / "bond_monitor.json").read_text())
    assert j["size_multiplier"] in (0.5, 1.0) and res["sprt"]["trades"] >= 40
    assert list(tmp_path.glob("bond_decay_*.md"))


def test_derisk_zero_or_unreadable_blocks(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 15, 0), zn_fn=drift())
    b.connect()
    p = Path(cfg.monitoring.state_dir) / "bond_monitor.json"
    p.write_text(json.dumps({"size_multiplier": 0.0, "status": "edge dead"}))
    plan, blockers, _ = bond_preflight(cfg, b, dt.date(2025, 9, 30), today=dt.date(2025, 9, 25))
    assert plan is None and any("says stop" in x for x in blockers)
    p.write_text("{broken")
    plan, blockers, _ = bond_preflight(cfg, b, dt.date(2025, 9, 30), today=dt.date(2025, 9, 25))
    assert plan is None and any("unreadable" in x for x in blockers)
    p.write_text(json.dumps({"size_multiplier": 0.5, "status": "weakening"}))
    plan, blockers, _ = bond_preflight(cfg, b, dt.date(2025, 9, 30), today=dt.date(2025, 9, 25))
    assert plan is not None and plan.derisk_multiplier == 0.5 and plan.effective_leverage < 2.0


# ------------------------------------------------------------------------------------------ combined exposure
def test_total_exposure_cap_blocks_leg_a_while_bond_holds(benv):
    """On month-end day leg B already holds ~3.2x ZN; with a 4x total cap leg A's ~1x+ order is refused."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    cfg.guardrails.max_total_leverage = 4.0
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    a = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    assert a.phase == "SKIPPED" and any("total futures exposure" in x for x in a.get("blockers"))
    assert bst(cfg, dt.date(2025, 9, 30)).phase == "FLAT"
    assert not [o for o in b.orders if o["ref"].startswith("MET-")]


def test_total_exposure_cap_blocks_bond_alone(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    cfg.guardrails.max_total_leverage = 2.0
    run(d, clock, dt.datetime(2025, 9, 25, 17, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "SKIPPED" and any("total futures exposure" in x for x in s.get("blockers"))
    assert s.get("preflight_attempts") is None              # permanent: no retry loop


def test_default_total_cap_allows_both_legs(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    assert TradeState(cfg.monitoring.state_dir, "2025-09-30").phase == "FLAT"
    assert bst(cfg, dt.date(2025, 9, 30)).phase == "FLAT"


def test_announce_evening_before_entry(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 24, 17, 0))
    msgs = [(t, body) for l, t, body in alerts if "Bond window" in t]
    assert len(msgs) == 1 and "2025-09-25 -> 2025-09-30" in msgs[0][0] and "BUY ~3 ZNZ5" in msgs[0][1]
    run(d, clock, dt.datetime(2025, 9, 24, 23, 0))
    assert len([1 for l, t, _ in alerts if "Bond window" in t]) == 1            # once


def test_weekly_status_includes_bond(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 29, 7, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 29, 8, 30))
    body = next(body for l, t, body in alerts if "Weekly status" in t)
    assert "bond leg: next window" in body and "ZN positions" in body


def test_rates_hours_half_day_and_holiday():
    from met.executor import futures_open, rates_futures_open
    ET = cal.ET
    hd = dt.datetime(2025, 11, 28, 13, 5, tzinfo=ET)            # Black Friday 13:05: CME equity open, CBOT rates shut
    assert futures_open(hd) and not rates_futures_open(hd)
    assert rates_futures_open(dt.datetime(2025, 11, 28, 12, 55, tzinfo=ET))
    assert rates_futures_open(dt.datetime(2025, 11, 30, 18, 30, tzinfo=ET))      # Sunday reopen
    assert not rates_futures_open(dt.datetime(2025, 11, 27, 14, 0, tzinfo=ET))   # Thanksgiving afternoon


def test_half_day_exit_retries_stay_before_rates_close(benv):
    """Three rejected exit rounds on the 1 pm close still flatten before 13:00 ET (no weekend carry)."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 11, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 11, 28, 12, 45))
    b.reject_next = 3
    run(d, clock, dt.datetime(2025, 11, 28, 18, 0))
    s = bst(cfg, dt.date(2025, 11, 28))
    sells = [o for o in zn_orders(b) if o["action"] == "SELL" and o["status"] != "Rejected"]
    assert s.phase == "FLAT" and b.zn_pos == 0 and sells[0]["t"].time() < dt.time(13, 0)


def test_roll_buffer_capped():
    c = Config(); c.ib.account = "DU1"; c.bond.roll_buffer_days = 19
    with pytest.raises(ConfigError, match="roll_buffer_days"):
        c.validate()


# ------------------------------------------------------------------------------------------ MTN (micro, small accounts)
def test_mtn_last_safe_day_and_contract_choice():
    from met.bond import last_safe_day
    assert last_safe_day(2026, 12, "MTN") == dt.date(2026, 11, 27)      # 2 sessions before Tue 1 Dec: Mon 30, Fri 27
    assert contract_month_for(dt.date(2026, 11, 30), 5, "MTN")[:2] == (2027, 3)
    assert contract_month_for(dt.date(2026, 10, 30), 5, "MTN")[:2] == (2026, 12)
    assert contract_month_for(dt.date(2026, 11, 30), 5, "ZN")[:2] == (2027, 3)
    for y in range(2025, 2031):
        for m in range(1, 13):
            X = cal.last_session_of_month(y, m)
            _, _, safe = contract_month_for(X, 5, "MTN")
            assert len(cal.sessions(X + dt.timedelta(days=1), safe)) > 5


def test_mtn_small_account_happy_path(benv):
    """~$18k account: leg B in 4 MTN (~2.4x IEF-equivalent); leg A's half-size month rounds to 0 MES and is skipped."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift(108.0), cash=3_000.0, core_shares=23)
    cfg.bond.instrument = "MTN"; cfg.bond.hedge_ratio = 0.0
    cfg.validate()
    assert cfg.bond.hedge_ratio == 0.885
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    p = s.get("plan")
    assert s.phase == "FLAT" and p["contract_local_symbol"] == "MTNZ5" and p["multiplier"] == 100.0
    assert 3 <= p["qty"] <= 5 and 2.0 <= p["ief_equiv_leverage"] <= 3.0
    assert s.get("net_pnl") > 0 and b.zn_pos == 0
    a = TradeState(cfg.monitoring.state_dir, "2025-09-30")
    assert a.phase in ("FLAT", "SKIPPED")
    assert not lv(alerts, "CRITICAL"), lv(alerts, "CRITICAL")


def test_mtn_validation():
    c = Config(); c.ib.account = "DU1"; c.bond.instrument = "MTN"; c.bond.max_contracts = 300
    c.validate()
    assert c.bond.hedge_ratio == 0.885 and c.bond.max_contracts == 300
    c = Config(); c.ib.account = "DU1"; c.bond.instrument = "MTN"; c.validate()
    assert c.bond.max_contracts == 200
    c = Config(); c.ib.account = "DU1"; c.validate()
    assert c.bond.max_contracts == 20
    c = Config(); c.ib.account = "DU1"; c.bond.instrument = "XYZ"
    with pytest.raises(ConfigError, match="bond.instrument"):
        c.validate()


def test_mtn_uses_marketable_limits_and_reprices(benv):
    """MTN (thin): every order is a limit 3/32 through the last price; a limit the market ran through is cancelled
    and the next exit round is re-priced and fills."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift(108.0), cash=3_000.0, core_shares=23)
    cfg.bond.instrument = "MTN"; cfg.bond.hedge_ratio = 0.0; cfg.validate()
    run(d, clock, dt.datetime(2025, 9, 30, 15, 54, 0))
    buy = zn_orders(b)[0]
    assert b.limits[buy["ref"]] is not None and buy["status"] == "Filled"
    assert b.limits[buy["ref"]] > 108.0                           # BUY limit above the market
    base = b.zn_price_fn
    shift = {"v": 0.0, "first": True}
    b.zn_price_fn = lambda now: base(now) + shift["v"]
    orig = b.market_order

    def mo(c, action, qty, ref, timeout_s, limit_price=None):
        if c.symbol == "MTN" and action == "SELL":
            assert limit_price is not None
            if shift["first"]:                       # market gaps down after the limit was priced
                shift["first"] = False
                shift["v"] = -0.5
        return orig(c, action, qty, ref, timeout_s, limit_price)
    b.market_order = mo
    run(d, clock, dt.datetime(2025, 9, 30, 15, 55, 1))
    run(d, clock, dt.datetime(2025, 9, 30, 20, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "FLAT" and b.zn_pos == 0
    sells = [o for o in zn_orders(b) if o["action"] == "SELL"]
    assert sells[0]["status"] == "LimitUnfilled" and sells[-1]["status"] == "Filled"
    assert b.limits[sells[-1]["ref"]] < b.limits[sells[0]["ref"]]          # re-priced lower


def test_zn_auto_uses_market_orders(benv):
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 17, 0))
    assert zn_orders(b) and all(b.limits[o["ref"]] is None for o in zn_orders(b))


def test_sifma_early_close_days():
    from met.bond import sifma_early_close
    assert sifma_early_close(dt.date(2026, 12, 31)) and sifma_early_close(dt.date(2025, 12, 31))
    assert sifma_early_close(dt.date(2024, 3, 28))                 # Thursday before Good Friday 2024
    assert sifma_early_close(dt.date(2027, 5, 28))                 # Friday before Memorial Day (31 May 2027)
    assert not sifma_early_close(dt.date(2026, 5, 22)) or cal.last_session_of_month(2026, 5) != dt.date(2026, 5, 22)
    assert not sifma_early_close(dt.date(2026, 11, 27))            # NYSE half day: handled as a half day
    assert not sifma_early_close(dt.date(2026, 11, 30))


def test_december_exit_at_1250(benv):
    """31 Dec 2025 (full NYSE session, SIFMA early close): bond exit at 12:50 ET; leg A still 15:59:58."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 12, 23, 8, 0), zn_fn=drift())
    tm = bx.times(dt.date(2025, 12, 31))
    assert tm["exit"].time() == dt.time(12, 50) and tm["E"] == dt.date(2025, 12, 26)
    run(d, clock, dt.datetime(2025, 12, 31, 18, 0))
    s = bst(cfg, dt.date(2025, 12, 31))
    sell = [o for o in zn_orders(b) if o["action"] == "SELL"][0]
    assert s.phase == "FLAT" and sell["t"].date() == dt.date(2025, 12, 31) and dt.time(12, 50) <= sell["t"].time() < dt.time(12, 51)
