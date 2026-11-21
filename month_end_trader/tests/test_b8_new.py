"""Review round B8: tests that kill mutants which survived the existing suite (targeted mutation testing).
Each test names the mutant it guards. Style follows tests/test_bond.py / tests/test_bond_review.py."""
import datetime as dt

from met import calendar as cal
from met.state import TradeState
from conftest import run_until
from test_bond import benv, bst, drift, lv, run, zn_orders  # noqa: F401  (fixture + helpers)

T = dt.date(2025, 9, 30)


def st(cfg, d=T):
    return TradeState(cfg.monitoring.state_dir, str(d))


# ------------------------------------------------------------------------------------------ executor
def test_b8_exit_closes_only_bots_qty_when_user_adds_same_direction(env):
    """E1 (flatten qty = min(|owned|, |pos|)): the user adds 1 MES in the bot's direction; the exit must close only
    the bot's own quantity and leave the user's contract alone."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 10, 0))
    s = st(cfg)
    assert s.phase == "ENTERED" and s.owned > 0
    q = s.owned
    b.market_order(ex.contract_for(s), "BUY", 1, "USER-x", 10)
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    sells = [o for o in b.orders if o["ref"].startswith(f"MET-{T}-exit")]
    assert [o["qty"] for o in sells] == [q]
    assert b.fut_pos == 1 and st(cfg).phase == "FLAT"


def test_b8_entry_still_working_after_60s_is_cancelled_again(env):
    """E6 (resolve_entry: an entry order still working 60 s after submission is cancelled): the order shows as
    working for 5 min; the bot must re-send the cancel once it has been working for more than 60 s."""
    cfg, clock, b, ex, alerts = env()
    b.fill_delay_next = (300, True)
    calls = []
    orig = b.cancel_working

    def cw(prefix="MET-"):
        calls.append(clock.now())
        return orig(prefix)
    b.cancel_working = cw
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 40))
    sub = dt.datetime.fromisoformat(st(cfg).get("entry_submitted_at"))
    assert any(60 < (t - sub).total_seconds() < 300 for t in calls), calls


def test_b8_entry_exactly_at_deadline_is_sent(env):
    """E7 (do_entry: `now > deadline`, consistent with tick's `now > entry_deadline` -> SKIPPED): an entry evaluated
    exactly at the deadline is still allowed (boundary)."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 29, 0))
    s = st(cfg)
    assert s.phase == "PLANNED"
    dl = ex.times(T)["entry_deadline"]
    clock.t = dl
    ex.do_entry(T, s, dl)
    assert st(cfg).phase == "ENTERED"


def test_b8_first_exit_retry_after_5s(env):
    """E11 (_backoff: first failed round waits EXIT_BACKOFF_S[0] = 5 s, not the second step)."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 1
    run_until(ex, clock, dt.datetime(2025, 9, 30, 17, 0))
    ex_orders = [o for o in b.orders if o["ref"].startswith(f"MET-{T}-exit")]
    assert len(ex_orders) == 2 and ex_orders[0]["status"] == "Rejected"
    assert (ex_orders[1]["t"] - ex_orders[0]["t"]).total_seconds() < 10
    assert ex_orders[1]["t"].time() < dt.time(16, 0, 10)


def test_b8_after_manual_alert_retries_every_15_min_first(env):
    """E11b (_backoff: after MAX_EXIT_ROUNDS failures, 15-min rounds for the first hour before hourly ones)."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 15, 0))
    b.reject_next = 11
    run_until(ex, clock, dt.datetime(2025, 9, 30, 23, 0))
    rej = [o for o in b.orders if o["ref"].startswith(f"MET-{T}-exit") and o["status"] == "Rejected"]
    assert len(rej) >= 11
    assert (rej[10]["t"] - rej[9]["t"]).total_seconds() < 1000          # 10th failure -> 900 s, not 3600 s


def test_b8_do_entry_rechecks_deadline_on_fresh_clock(env):
    """E7b (do_entry's own deadline check): time can pass between tick's check and do_entry (slow contract/position
    lookups); an entry evaluated after the deadline must be SKIPPED, never sent late."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 9, 29, 0))
    s = st(cfg)
    assert s.phase == "PLANNED"
    dl = ex.times(T)["entry_deadline"]
    clock.t = dl + dt.timedelta(seconds=1)
    ex.do_entry(T, s, dl)
    assert st(cfg).phase == "SKIPPED" and not b.orders


def test_b8_reconcile_still_owned_moves_to_exit(env):
    """E9 (reconcile: bot still owns something -> EXIT_SUBMITTED). Callers usually catch this first (watch_fills /
    drive_unfinished), so this guards the function's own contract directly."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 11, 0))
    s = st(cfg)
    assert s.owned > 0
    s.update(ex.ts(), "ERROR", note="test: forced")
    ex.reconcile(T, s)
    assert s.phase == "EXIT_SUBMITTED" and not s.get("reconciled")


def test_b8_executions_in_another_contract_are_not_owned(env):
    """E12 (sync: executions whose local symbol is not the trade's contract are ignored)."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 10, 0))
    q = st(cfg).owned
    assert q > 0
    b.execs.append(dict(exec_id="FOREIGN1", rev=0, ref=f"MET-{T}-manual", side="BUY", qty=1.0, price=6700.0,
                        time=str(clock.t), local_symbol="MESH6"))
    run_until(ex, clock, dt.datetime(2025, 9, 30, 19, 0))
    s = st(cfg)
    assert s.phase == "FLAT" and s.owned == 0 and b.fut_pos == 0
    assert [o["qty"] for o in b.orders if o["ref"].startswith(f"MET-{T}-exit")] == [q]


def test_b8_broker_opposite_direction_sends_no_order(env):
    """E14 (flatten: broker position on the OTHER side of the bot's fills is a disagreement, not something to close):
    the user sold more than the bot holds; the bot must not sell into the user's short."""
    cfg, clock, b, ex, alerts = env()
    run_until(ex, clock, dt.datetime(2025, 9, 30, 10, 0))
    s = st(cfg)
    q = s.owned
    b.market_order(ex.contract_for(s), "SELL", q + 2, "USER-x", 10)
    assert b.fut_pos == -2
    run_until(ex, clock, dt.datetime(2025, 9, 30, 16, 30))
    assert not [o for o in b.orders if o["ref"].startswith(f"MET-{T}-exit")]
    assert b.fut_pos == -2


# ------------------------------------------------------------------------------------------ bond adoption
def _crash_and_restart(cfg, clock, b, d, bx, before_restart=None):
    from test_bond_review import Crash, crash_after_zn_send, restart
    import pytest
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    orig = crash_after_zn_send(b)
    with pytest.raises(Crash):
        run(d, clock, dt.datetime(2025, 9, 25, 16, 30))
    b.market_order = orig
    assert b.zn_pos == 3
    if before_restart:
        before_restart()
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)
    d2, bx2 = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 26, 12, 0))
    return bst(cfg, dt.date(2025, 9, 30))


def test_b8_no_adoption_when_broker_shows_less_than_planned(benv):
    """B7 (_try_adopt requires pos == planned qty exactly): the user reduced the position; never adopt."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())

    def user_sells_one():
        c = b.front_future("ZN", "CBOT", dt.date(2025, 12, 1))
        b.market_order(c, "SELL", 1, "USER-x", 10)
    s = _crash_and_restart(cfg, clock, b, d, bx, user_sells_one)
    assert not s.get("adopted") and b.zn_pos == 2


def test_b8_no_adoption_while_another_bond_trade_is_unfinished(benv):
    """B8 (_try_adopt: another unfinished bond state -> the position could be that trade's; never adopt). A corrupt
    older state stays unfinished."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    from pathlib import Path

    def corrupt_older():
        Path(cfg.monitoring.state_dir, "bond_2025-08-29.json").write_text("{not json")
    s = _crash_and_restart(cfg, clock, b, d, bx, corrupt_older)
    assert not s.get("adopted") and b.zn_pos == 3


def test_b8_no_adoption_when_other_zn_contracts_are_held(benv):
    """B9 (_try_adopt: all ZN positions must equal the contract's position): the account also holds another ZN
    contract, so the planned-quantity match is not proof of ownership."""
    from met.broker import Position
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())

    def other_contract():
        orig = b.all_future_positions
        b.all_future_positions = lambda sym: orig(sym) + ([Position("ZN", "FUT", 1, "ZNM6", 110_000.0)]
                                                           if sym == "ZN" else [])
    s = _crash_and_restart(cfg, clock, b, d, bx, other_contract)
    assert not s.get("adopted") and b.zn_pos == 3


def test_b8_rejected_bond_entry_never_adopts_user_position(benv):
    """B10 (_try_adopt refuses when the entry order was Rejected): the bot's entry was rejected, the daemon was down
    over the evening, and the user bought exactly the planned quantity in the planned contract."""
    from test_bond_review import restart
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 25, 15, 57, 50))
    b.reject_next = 1
    run(d, clock, dt.datetime(2025, 9, 25, 16, 1))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert s.phase == "ERROR" and s.get("entry_status") == "Rejected"
    qty = s.get("plan")["qty"]
    b.market_order(bx.contract_for(s), "BUY", qty, "USER-x", 10)
    clock.t = dt.datetime(2025, 9, 26, 9, 0, tzinfo=cal.ET)
    d2, _ = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 26, 12, 0))
    s = bst(cfg, dt.date(2025, 9, 30))
    assert not s.get("adopted") and b.zn_pos == qty


# ------------------------------------------------------------------------------------------ bond exits
def test_b8_bond_retry_deferred_in_leg_a_exit_window(benv):
    """B11 / D2 (bond flatten defers from 2 min before leg A's exit until 1 min after; Daemon wires leg A as peer):
    a bond exit retry that falls due at ~15:58:50 must wait until 16:00:58."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 30, 15, 54, 50))
    b.reject_next = 5
    run(d, clock, dt.datetime(2025, 9, 30, 16, 30))
    a_exit = d.legs[0].times(T)["exit"]
    zx = [o for o in zn_orders(b) if "-exit" in o["ref"]]
    assert len(zx) == 6
    assert not [o for o in zx if a_exit - dt.timedelta(seconds=120) <= o["t"] < a_exit + dt.timedelta(seconds=60)], \
        [(o["t"].time(), o["status"]) for o in zx]
    assert bst(cfg, T).phase == "FLAT" and b.zn_pos == 0 and b.fut_pos == 0


def test_b8_held_instrument_released_after_old_trade_closes(benv):
    """B13 (_check_instrument_switch resets the held instrument once no old-instrument trade is unfinished)."""
    from test_bond_review import restart
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 24, 8, 0), zn_fn=drift())
    run(d, clock, dt.datetime(2025, 9, 26, 12, 0))
    cfg.bond.instrument = "MTN"; cfg.bond.hedge_ratio = 0.0; cfg.bond.max_contracts = 0; cfg.validate()
    d2, bx2 = restart(cfg, b, d, bx)
    run(d2, clock, dt.datetime(2025, 9, 26, 13, 0))
    assert bx2.instrument == "ZN"
    run(d2, clock, dt.datetime(2025, 9, 30, 20, 0))
    assert bst(cfg, T).get("reconciled")
    assert bx2.instrument == "MTN"


def test_b8_limit_price_rounds_away_from_market(benv):
    """B14 (limit_price: BUY rounds UP, SELL rounds DOWN to the 1/32 grid, so an off-grid last price never makes
    the limit less marketable than limit_ticks)."""
    cfg, clock, b, d, alerts, bx = benv(dt.datetime(2025, 9, 25, 8, 0))
    cfg.bond.order_type = "limit"
    px, step = 110.01, 1 / 32
    bx._try_price = lambda c: px
    buy, sell = bx.limit_price(None, "BUY"), bx.limit_price(None, "SELL")
    k = cfg.bond.limit_ticks
    assert buy >= px + k * step and sell <= px - k * step
    assert abs(buy / step - round(buy / step)) < 1e-9 and abs(sell / step - round(sell / step)) < 1e-9


# ------------------------------------------------------------------------------------------ guardrails / signal
def test_b8_size_respects_hard_caps():
    """G2 / G3 (size_contracts: hard_max_contracts and hard_max_leverage cap even a looser configured limit)."""
    from met.guardrails import size_contracts
    q, eff = size_contracts(1_000_000, 100.0, 1000.0, leverage=10.0, derisk=1.0, max_leverage=10.0,
                            max_contracts=1000, hard_max_leverage=100.0, hard_max_contracts=5)
    assert q == 5
    q, eff = size_contracts(1_000_000, 100.0, 1000.0, leverage=10.0, derisk=1.0, max_leverage=10.0,
                            max_contracts=1000, hard_max_leverage=2.0, hard_max_contracts=1000)
    assert q == 20 and eff <= 2.0 + 1e-9


def test_b8_vol_exactly_at_threshold_is_low_regime(env):
    """G4 (vol_scale: full size only when vol is strictly ABOVE the threshold)."""
    from met.guardrails import trailing_vol, vol_scale
    cfg, clock, b, ex, alerts = env(dt.datetime(2025, 9, 29, 17, 0))
    b.connect()
    cfg.strategy.vol_tilt = True
    v = trailing_vol(ex.fetchers["ibkr"](cfg.signal.equity), T, cfg.strategy.vol_window)
    cfg.strategy.vol_threshold = v
    scale, vol, _ = vol_scale(cfg, T, ex.fetchers)
    assert vol == v and scale == cfg.strategy.low_vol_scale


def test_b8_source_spread_that_could_flip_the_smaller_gap_blocks(monkeypatch):
    """Sg3 (signals_from_sources: tolerate a source spread only if it is smaller than the SMALLER |gap|)."""
    from met import signal as sigmod
    gaps = {"ibkr": 0.001, "yahoo": 0.004}               # same direction, spread 0.3% > tolerance, > min |gap|
    monkeypatch.setattr(sigmod, "compute_signal", lambda d, e, bd, source="", provisional=False: sigmod.Signal(
        str(d), "x", "y", gaps[source], 0.0, gaps[source], -1, source))
    sig, problems, sigs = sigmod.signals_from_sources(T, {"ibkr": lambda s: None, "yahoo": lambda s: None},
                                                      "SPY", "TLT", max_gap_diff=0.0015)
    assert sig is None and len(sigs) == 2


def test_b8_roll_buffer_lower_bound():
    """C1 (config: bond.roll_buffer_days must be >= 2)."""
    import pytest
    from met.config import Config, ConfigError
    c = Config(); c.ib.account = "DU1"; c.bond.roll_buffer_days = 1
    with pytest.raises(ConfigError, match="roll_buffer_days"):
        c.validate()


def test_b8_daemon_rate_limits_repeated_leg_errors(env):
    """D1 (Daemon.tick: the same exception type from a leg is alerted at most every 15 min, even when its text
    changes, so the Alerter's identical-title dedupe does not help)."""
    from met.daemon import Daemon
    cfg, clock, b, ex, alerts = env()
    n = {"i": 0}

    def boom():
        n["i"] += 1
        raise ValueError(f"bad {n['i']}")
    ex.tick = boom
    d = Daemon([ex])
    for _ in range(3):
        d.tick(); clock.advance(60)
    assert sum("daemon tick error" in t for t in lv(alerts, "CRITICAL")) == 1
    clock.advance(900)
    d.tick()
    assert sum("daemon tick error" in t for t in lv(alerts, "CRITICAL")) == 2
