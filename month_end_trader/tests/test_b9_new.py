"""Review round B9: tests that kill mutants which survived the existing suite (targeted mutation testing, round 2:
calendar, signal, monitor, bond_monitor, ibkr helpers, executor market hours, cli). Each test names its mutant."""
import datetime as dt
import json
import math
from pathlib import Path
from types import SimpleNamespace as NS

import pandas as pd
import pytest

from met import calendar as cal
from met.state import TradeState
from conftest import run_until
from test_bond import benv, drift, ief_fixture  # noqa: F401  (fixture + helpers)

ET = cal.ET
T = dt.date(2025, 9, 30)


# ------------------------------------------------------------------------------------------ calendar
def test_b9_previous_next_session_from_weekend():
    """K4/K5 (previous_session / next_session on a non-session date use date_to_session in the right direction)."""
    sat = dt.date(2026, 11, 21)
    assert cal.previous_session(sat) == dt.date(2026, 11, 20)
    assert cal.next_session(sat) == dt.date(2026, 11, 23)
    assert cal.previous_session(dt.date(2026, 11, 26)) == dt.date(2026, 11, 25)    # Thanksgiving
    assert cal.next_session(dt.date(2026, 11, 26)) == dt.date(2026, 11, 27)


@pytest.fixture
def closures(monkeypatch):
    def set_(v):
        monkeypatch.setenv("MET_EXTRA_CLOSURES", v)
        cal._CAL["day"] = None
        cal._cal()
    yield set_
    monkeypatch.delenv("MET_EXTRA_CLOSURES", raising=False)
    cal._CAL["day"] = None
    cal._cal()


def test_b9_extra_closure_month_end(closures):
    """K9/K10 (MET_EXTRA_CLOSURES: the closed day is not a session, and a closed month-end rolls BACK)."""
    closures("2026-11-30")
    assert not cal.is_session(dt.date(2026, 11, 30))
    assert cal.last_session_of_month(2026, 11) == dt.date(2026, 11, 27)
    assert cal.previous_session(dt.date(2026, 12, 1)) == dt.date(2026, 11, 27)
    assert cal.next_session(dt.date(2026, 11, 27)) == dt.date(2026, 12, 1)
    assert cal.next_trade_date(dt.date(2026, 11, 21)) == dt.date(2026, 11, 27)


def test_b9_next_trade_date_after_rolls_only_after_close():
    """K11 (next_trade_date_after rolls to next month once the trade day has CLOSED, not once it opened)."""
    T0 = dt.date(2026, 11, 30)
    assert cal.next_trade_date_after(dt.datetime(2026, 11, 30, 12, 0, tzinfo=ET)) == T0
    assert cal.next_trade_date_after(dt.datetime(2026, 11, 30, 16, 1, tzinfo=ET)) == dt.date(2026, 12, 31)


# ------------------------------------------------------------------------------------------ signal
def test_b9_drop_partial_today_keeps_dropping_until_close_plus_20m():
    """G1 (drop_partial_today: today's bar is dropped until close + 20 min, Yahoo finalises late)."""
    from met.signal import drop_partial_today
    idx = [dt.date(2026, 11, 18), dt.date(2026, 11, 19)]
    df = pd.DataFrame({"adjclose": [1.0, 2.0]}, index=idx)
    assert list(drop_partial_today(df, dt.datetime(2026, 11, 19, 16, 10, tzinfo=ET)).index) == idx[:1]
    assert list(drop_partial_today(df, dt.datetime(2026, 11, 19, 16, 21, tzinfo=ET)).index) == idx


def test_b9_as_date_index_dedupes_last_and_sorts():
    """G3/G4 (_as_date_index: duplicate dates keep the LAST value; an unsorted date index is sorted)."""
    from met.signal import _as_date_index
    s = pd.Series([1.0, 2.0, 3.0], index=pd.to_datetime(["2026-11-18", "2026-11-19", "2026-11-19"]))
    r = _as_date_index(s)
    assert list(r.index) == [dt.date(2026, 11, 18), dt.date(2026, 11, 19)] and r.iloc[-1] == 3.0
    u = pd.Series([2.0, 1.0], index=[dt.date(2026, 11, 19), dt.date(2026, 11, 18)])
    r = _as_date_index(u)
    assert list(r.index) == [dt.date(2026, 11, 18), dt.date(2026, 11, 19)] and list(r.values) == [1.0, 2.0]


def test_b9_provisional_when_only_one_series_lacks_t1(data):
    """G6 (provisional: one source missing T-1 is enough to fall back to the latest COMMON close)."""
    from met.signal import compute_signal
    spy, tlt = data
    cut = dt.date(2026, 10, 20)
    e = spy["adjclose"][[d <= cut for d in spy.index]]
    s = compute_signal(dt.date(2026, 10, 30), e, tlt["adjclose"], provisional=True)
    assert "provisional" in s.source and s.signal_through == str(cut)


def test_b9_provisional_without_month_to_date_data_raises(data):
    """G7 (provisional: only dates strictly after the base qualify; with none, the T-1 close is still required)."""
    from met.signal import compute_signal, DataError
    spy, tlt = data
    base = dt.date(2026, 9, 30)
    e = spy["adjclose"][[d <= base for d in spy.index]]
    b = tlt["adjclose"][[d <= base for d in tlt.index]]
    with pytest.raises(DataError):
        compute_signal(dt.date(2026, 10, 30), e, b, provisional=True)


# ------------------------------------------------------------------------------------------ monitor
def test_b9_paper_ledger_row_formulas(data):
    """M1/M2 (paper_bp = direction x open->close - COST_BP; gap_in_bp = direction x (open_T / close_T-1 - 1))."""
    from met.monitor import paper_ledger, COST_BP
    spy, tlt = data
    led = paper_ledger(spy, tlt, start="2026-09-01", until=dt.date(2026, 10, 31))
    T0 = dt.date(2026, 10, 30)
    r = led.loc[T0]
    o, c = spy.at[T0, "open"], spy.at[T0, "close"]
    assert r["paper_bp"] == pytest.approx(r["direction"] * 1e4 * (c / o - 1) - 1.0) and COST_BP == 1.0
    t1 = cal.previous_session(T0)
    assert r["gap_in_bp"] == pytest.approx(r["direction"] * 1e4 * (o / spy.at[t1, "close"] - 1))


def test_b9_sprt_bounds_and_winsorisation():
    """M4/M5 (sprt: A = ln((1-beta)/alpha); inputs winsorised on BOTH sides)."""
    from met.monitor import sprt
    _, _, A, B, _ = sprt([], 20, 78, 0.05, 0.2)
    assert A == pytest.approx(math.log(0.8 / 0.05)) and B == pytest.approx(math.log(0.2 / 0.95))
    L1, *_ = sprt([-1e6], 20, 78, 0.1, 0.1)
    L2, *_ = sprt([-2 * 78], 20, 78, 0.1, 0.1)
    assert L1 == pytest.approx(L2)


def test_b9_size_multiplier_dead_recovering_undecided():
    """M6/M7 (size_multiplier: once dead, stays 0 until above B/2; 'recovering' half size applies only after dead)."""
    from met.monitor import size_multiplier
    A, B = math.log(9), -math.log(9)
    assert size_multiplier(0.75 * B, A, B, None, was_dead=True)[0] == 0.0
    assert size_multiplier(0.75 * B, A, B, None, was_dead=False)[0] == 0.5
    assert size_multiplier(-0.1, A, B, None, was_dead=True)[0] == 0.5
    assert size_multiplier(-0.1, A, B, None, was_dead=False)[0] == 1.0


def _mon_cfg(tmp_path):
    from met.config import Config
    cfg = Config(); cfg.ib.account = "DU1"
    cfg.monitoring.state_dir = cfg.monitoring.report_dir = str(tmp_path); cfg.monitoring.start = "2023-01"
    return cfg


def _fixed_sprt(monkeypatch, L):
    from met import monitor
    A, B = math.log(9), -math.log(9)
    monkeypatch.setattr(monitor, "sprt", lambda *a, **k: (L, [L], A, B, None))


@pytest.mark.parametrize("prev,mult", [
    ({"edge_dead": True, "size_multiplier": 1.0}, 0.5),     # auto_derisk off wrote x1, but the edge WAS dead
    ("{broken", 0.5),                                       # unreadable previous state: assume dead
    ({"edge_dead": False, "size_multiplier": 1.0}, 1.0),
])
def test_b9_run_report_was_dead_from_previous_state(tmp_path, data, monkeypatch, prev, mult):
    """M8/M9 (run_report: was_dead read from edge_dead (not size_multiplier); unreadable monitor.json -> dead)."""
    from met.monitor import run_report
    spy, tlt = data
    cfg = _mon_cfg(tmp_path)
    (tmp_path / "monitor.json").write_text(prev if isinstance(prev, str) else json.dumps(prev))
    _fixed_sprt(monkeypatch, -0.1)
    res = run_report(cfg, asof=dt.date(2026, 10, 31), spy=spy, tlt=tlt, write=False)
    assert res["size_multiplier"] == mult


def _trade(state_dir, d, **kw):
    t = dict(trade_date=str(d), phase="FLAT", events=[], reconciled=True)
    t.update(kw)
    Path(state_dir, f"trade_{d}.json").write_text(json.dumps(t))


def test_b9_live_rows_and_execution_gap(tmp_path, data):
    """M10/M11/M12 (live_rows drops stop/manual exits; exec gap uses only same-direction trades and adds COST_BP
    back to the paper result)."""
    from met.monitor import paper_ledger, live_rows, run_report
    spy, tlt = data
    cfg = _mon_cfg(tmp_path)
    led = paper_ledger(spy, tlt, start="2026-07-01", until=dt.date(2026, 10, 31))
    d1, d2, d3 = dt.date(2026, 8, 31), dt.date(2026, 9, 30), dt.date(2026, 10, 30)
    _trade(tmp_path, d1, trade_bp=float(led.at[d1, "paper_bp"]) + 1.0, direction=int(led.at[d1, "direction"]),
           exit_reason="scheduled close")
    _trade(tmp_path, d2, trade_bp=-500.0, direction=int(led.at[d2, "direction"]), exit_reason="disaster stop")
    _trade(tmp_path, d3, trade_bp=-700.0, direction=-int(led.at[d3, "direction"]), exit_reason="scheduled close")
    lr = live_rows(cfg)
    assert sorted(lr.index) == [d1, d3]
    res = run_report(cfg, asof=dt.date(2026, 10, 31), spy=spy, tlt=tlt, write=False)
    assert res["execution_gap_bp"] == pytest.approx(0.0, abs=1e-9)


# ------------------------------------------------------------------------------------------ bond monitor
def test_b9_bond_ledger_window_and_cost():
    """BM1/BM2/BM7 (bond ledger: IEF close of E = entry_session(X, hold) -> close of X, minus 2bp)."""
    from met.bond import entry_session
    from met.bond_monitor import bond_ledger
    from met.signal import _as_date_index
    s = _as_date_index(ief_fixture().dropna())
    led = bond_ledger(s, 3, start="2026-08-01", until=dt.date(2026, 10, 31))
    X = dt.date(2026, 9, 30)
    E = entry_session(X, 3)
    assert led.at[X, "entry"] == E
    assert led.at[X, "paper_bp"] == pytest.approx(1e4 * (s.loc[X] / s.loc[E] - 1) - 2.0)


def _bond_trade(state_dir, X, E, **kw):
    t = dict(trade_date=str(X), phase="FLAT", events=[], reconciled=True, exit_reason="scheduled close",
             plan=dict(entry_date=str(E), hedge_ratio=1.25))
    t.update(kw)
    Path(state_dir, f"bond_{X}.json").write_text(json.dumps(t))


def test_b9_bond_execution_gap(tmp_path):
    """BM3/BM4/BM5/BM6 (bond exec gap: only comparable trades (scheduled, not adopted); live bp scaled by the hedge
    ratio; futures carry SUBTRACTED from the IEF total-return paper result)."""
    from met.bond import entry_session
    from met.bond_monitor import bond_ledger, run_bond_report
    cfg = _mon_cfg(tmp_path)
    cfg.bond.financing_rate = 0.04
    ief = ief_fixture()
    led = bond_ledger(ief, cfg.bond.hold_sessions, start="2026-07-01", until=dt.date(2026, 10, 31))
    X1, X2, X3 = dt.date(2026, 8, 31), dt.date(2026, 9, 30), dt.date(2026, 10, 30)
    E1 = entry_session(X1, cfg.bond.hold_sessions)
    carry = 1e4 * 0.04 * (X1 - E1).days / 365
    want = 3.0 + led.at[X1, "paper_bp"] + 2.0 - carry          # live bp in IEF terms: 3bp better than paper
    _bond_trade(tmp_path, X1, E1, trade_bp=want / 1.25)
    _bond_trade(tmp_path, X2, entry_session(X2, 3), trade_bp=-400.0, adopted=True)
    _bond_trade(tmp_path, X3, entry_session(X3, 3), trade_bp=-400.0, exit_reason="manual flatten")
    res = run_bond_report(cfg, asof=dt.date(2026, 10, 31), ief=ief, write=False)
    assert res["execution_gap_bp"] == pytest.approx(3.0, abs=1e-6)


# ------------------------------------------------------------------------------------------ ibkr helpers
def test_b9_f_treats_ib_unset_as_missing():
    """I1 (_f: IB's UNSET_DOUBLE 1.797e308 means 'no value')."""
    from met.ibkr import _f
    assert _f(1.7976931348623157e308, 0.0) == 0.0 and _f("2.5") == 2.5 and math.isnan(_f(None))


def test_b9_pick_front_includes_expiry_on_min_date():
    """I2 (pick_front: a contract expiring ON min_expiry qualifies)."""
    from met.ibkr import pick_front
    assert pick_front([("20261218", "Z6"), ("20270319", "H7")], dt.date(2026, 12, 18))[1] == "Z6"


def _trade_obj(filled, avg, fills):
    return NS(orderStatus=NS(filled=filled, avgFillPrice=avg),
              fills=[NS(execution=NS(shares=q, price=p)) for q, p in fills])


def test_b9_fill_summary_orderstatus_fallback():
    """I6/I7 (fill_summary: orderStatus ahead of executions wins on qty; no executions -> orderStatus avg price)."""
    from met.ibkr import fill_summary
    assert fill_summary(_trade_obj(5, 7000.0, [(3, 7000.0)]))[0] == 5
    assert fill_summary(_trade_obj(2, 7001.5, [])) == (2, 7001.5)


# ------------------------------------------------------------------------------------------ executor hours
def test_b9_futures_halt_and_sunday_reopen():
    """X2/X3 (futures_open: closed Sunday before 18:00 and during the whole 17:00-18:00 daily halt)."""
    from met.executor import futures_open
    assert not futures_open(dt.datetime(2025, 11, 30, 12, 0, tzinfo=ET))      # Sunday midday
    assert futures_open(dt.datetime(2025, 11, 30, 18, 1, tzinfo=ET))
    assert not futures_open(dt.datetime(2025, 12, 2, 17, 30, tzinfo=ET))      # Tuesday halt
    assert futures_open(dt.datetime(2025, 12, 2, 18, 0, tzinfo=ET))


def test_b9_fill_date_is_exchange_local():
    """X8 (_fill_date: a UTC fill time after 00:00Z belongs to the previous ET date)."""
    from met.executor import _fill_date
    assert _fill_date({"time": "2025-10-01T01:30:00+00:00"}) == T
    assert _fill_date({"time": "2025-09-30T15:59:58"}) == T
    assert _fill_date({"time": "garbage"}) is None


# ------------------------------------------------------------------------------------------ cli
def _cli(monkeypatch, cfg, b):
    from met import cli
    monkeypatch.setattr(cli, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: b)
    monkeypatch.setattr(cli, "setup_logging", lambda *a, **k: None)
    return cli


def _tests_sent(b):
    return [o for o in b.orders if o["ref"].startswith("TEST-")]


def test_b9_test_order_refusals(env, monkeypatch, tmp_path):
    """C1/C2/C3 (test-order: paper only; refused on a trade day; refused while a BOND trade is unfinished)."""
    cfg, clock, b, ex, alerts = env()
    b.connect()
    cli = _cli(monkeypatch, cfg, b)
    cfg.mode = "live"
    assert cli.main(["test-order"]) == 2 and not _tests_sent(b)
    cfg.mode = "paper"
    with monkeypatch.context() as m:
        m.setattr(cli.cal, "next_trade_date", lambda d: d)               # today is the trade day
        assert cli.main(["test-order"]) == 2 and not _tests_sent(b)
    p = Path(cfg.monitoring.state_dir, "bond_2025-09-30.json")
    p.write_text(json.dumps(dict(trade_date="2025-09-30", phase="ENTERED", events=[])))
    assert cli.main(["test-order"]) == 2 and not _tests_sent(b)
    p.unlink()
    assert cli.main(["test-order"]) == 0 and [o["action"] for o in _tests_sent(b)] == ["BUY", "SELL"]


def test_b9_flatten_leg_a_market_closed_queues_exit(env, monkeypatch):
    """C4/C5 (flatten: CME closed -> no order, exit queued in the state, rc 3)."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    assert TradeState(cfg.monitoring.state_dir, str(T)).phase == "ENTERED"
    clock.t = dt.datetime(2025, 9, 30, 17, 30, tzinfo=ET)                  # daily CME halt
    cli = _cli(monkeypatch, cfg, b)
    n = len(b.orders)
    assert cli.main(["flatten", "--yes"]) == 3
    assert len(b.orders) == n and b.fut_pos != 0
    assert TradeState(cfg.monitoring.state_dir, str(T)).phase == "EXIT_SUBMITTED"


def test_b9_flatten_bond_market_closed_queues_exit(benv, monkeypatch):
    """C6 (flatten, bond leg: CBOT closed -> no order, exit queued, rc 3)."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    from test_bond import run
    run(d, clock, dt.datetime(2025, 9, 26, 12, 0))
    X = dt.date(2025, 9, 30)
    s = TradeState(cfg.monitoring.state_dir, str(X), prefix="bond")
    assert s.phase == "ENTERED" and s.owned != 0
    clock.t = dt.datetime(2025, 9, 26, 17, 30, tzinfo=ET)                  # Friday after 17:00
    cli = _cli(monkeypatch, cfg, b)
    n = len(b.orders)
    assert cli.main(["flatten", "--yes"]) == 3
    assert len(b.orders) == n and b.zn_pos != 0
    assert TradeState(cfg.monitoring.state_dir, str(X), prefix="bond").phase == "EXIT_SUBMITTED"


def test_b9_flatten_does_not_touch_position_bot_does_not_own(env, monkeypatch):
    """C9 (flatten: broker shows a position but the bot's fills own nothing -> untouched, state unchanged)."""
    cfg, clock, b, ex, alerts = env()
    b.reject_next = 1                                                      # the bot's entry is rejected
    run_until(ex, clock, dt.datetime(2025, 9, 30, 12, 0))
    s = TradeState(cfg.monitoring.state_dir, str(T))
    assert s.owned == 0 and s.get("contract_con_id"), (s.phase, s.data)
    ph = s.phase
    b.market_order(ex.contract_for(s), "BUY", 2, "TWS-y", 5)               # the user's own position
    cli = _cli(monkeypatch, cfg, b)
    n = len(b.orders)
    cli.main(["flatten", "--yes"])
    assert len(b.orders) == n and b.fut_pos == 2
    assert TradeState(cfg.monitoring.state_dir, str(T)).phase == ph


def test_cli_unknown_command_fails_before_connecting(monkeypatch):
    from met import cli
    called = []
    monkeypatch.setattr(cli, "_broker", lambda *a, **k: called.append(1))
    with pytest.raises(SystemExit) as e:
        cli.main(["bond_check", "--config", "nonexistent.yaml"])
    assert e.value.code == 2 and not called


def test_cli_gateway_down_is_one_line_not_traceback(monkeypatch, capsys, tmp_path):
    from met import cli
    from met.broker import BrokerError
    monkeypatch.setattr(cli, "_main", lambda a, ap: (_ for _ in ()).throw(BrokerError("IB connect 127.0.0.1:4002 failed")))
    assert cli.main(["check"]) == 2
    assert "cannot reach IB Gateway" in capsys.readouterr().err
    monkeypatch.setattr(cli, "_main", lambda a, ap: (_ for _ in ()).throw(BrokerError("no price for MES")))
    with pytest.raises(BrokerError):
        cli.main(["check"])
