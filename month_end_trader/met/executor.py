"""Trade-day state machine and the continuously running daemon.

The daemon calls `tick()` repeatedly. Every action is idempotent and driven by persisted TradeState, so the
process can be restarted at any moment (including mid-trade) and resume safely.

  T-1 after close   announce tomorrow's signal
  T open-20m        preflight  -> PLANNED | SKIPPED
  T open+1s         entry      -> ENTRY_SUBMITTED -> ENTERED | ERROR      (never later than open+grace)
  T while holding   guards: disaster stop, kill switch, position reconciliation
  T close-2s        exit       -> EXIT_SUBMITTED -> FLAT                  (retried until flat)
  T close+10m       reconcile: broker must be flat in the bot's contract; otherwise flatten + CRITICAL
  T close+2h        decay-monitor report
  any time          unfinished trades from earlier dates are driven to flat (never forgotten after midnight)
  every session     close+15m overnight guard (alert on any position in the instrument)

Ownership rule: preflight refuses to trade if ANY position exists in the instrument, so once a trade state is
ENTRY_SUBMITTED or later, the broker position in that exact contract is the bot's. Exits are therefore sized
from the broker position (never from the bot's own fill bookkeeping), which is robust to late fills,
timeouts, partial fills and crashes.
"""
from __future__ import annotations

import datetime as dt
import logging
import traceback
from pathlib import Path

from . import calendar as cal
from .alerts import Alerter
from .broker import Broker, BrokerError, FutureContract
from .config import Config
from .guardrails import preflight
from .state import ACTIVE, DaemonLock, StateCorrupt, TradeState, kill_switch_active, unfinished

log = logging.getLogger("met.executor")

PERMANENT_BLOCKERS = ("kill switch", "not the last NYSE session", "MET_LIVE_CONFIRM", "connected account",
                      "does not match mode", "below minimum", "existing ", "core ", "decay monitor says stop",
                      "below min_abs_gap", "exactly zero", "unreadable")
COST_PER_CONTRACT_SIDE = {"MES": 0.62, "ES": 2.25}   # IBKR commission + exchange + regulatory (estimate)
GUARD_QUIET_S = 45          # no guard work this close to the exit time
PRICE_FAIL_ALERT = 4        # consecutive missing prices before a CRITICAL alert
EXIT_BACKOFF_S = (5, 15, 30, 60, 120, 300)   # between failed exit rounds
MANUAL_ALERT_AFTER = 8      # failed exit rounds before the 'manual intervention' alert


def futures_open(now: dt.datetime) -> bool:
    """CME equity futures: Sun 18:00 - Fri 17:00 ET, daily halt 17:00-18:00. On NYSE half days and NYSE
    weekday holidays the CME equity session halts early (13:15 ET half days, ~13:00 holidays) until 18:00."""
    t, wd, d = now.time(), now.weekday(), now.date()
    if wd == 5 or (wd == 4 and t >= dt.time(17)) or (wd == 6 and t < dt.time(18)):
        return False
    if dt.time(17) <= t < dt.time(18):
        return False
    if wd < 5:
        if not cal.is_session(d):
            return not (dt.time(13) <= t < dt.time(18))
        if cal.is_early_close(d):
            return not (dt.time(13, 15) <= t < dt.time(18))
    return True


class Executor:
    def __init__(self, cfg: Config, broker: Broker, alerter: Alerter, fetchers: dict, reporter=None):
        self.cfg, self.broker, self.alert, self.fetchers = cfg, broker, alerter, fetchers
        self.reporter = reporter                  # callable(trade_date) -> dict | None
        self._contracts: dict = {}
        self._overnight_checked: set = set()
        self._last_conn_check: dt.datetime | None = None
        self._price_fail = 0
        self._last_mismatch_alert: dt.datetime | None = None
        self._kill_seen = False
        self._unfinished_last: dict = {}
        self._corrupt_alerted: set = set()
        self._disc_alert: dt.datetime | None = None
        self._today_corrupt_alert: dt.datetime | None = None
        self._disconnected_since: dt.datetime | None = None
        self._disc_any_alert: dt.datetime | None = None

    # ------------------------------------------------------------------ helpers
    def ts(self) -> str:
        return self.broker.now().isoformat(timespec="seconds")

    def times(self, d: dt.date) -> dict:
        o, c = cal.session_open_close(d)
        s = self.cfg.strategy
        return dict(open=o, close=c,
                    preflight=o - dt.timedelta(minutes=20),
                    entry=o + dt.timedelta(seconds=s.entry_delay_s),
                    entry_deadline=o + dt.timedelta(seconds=s.entry_grace_s),
                    exit=c - dt.timedelta(seconds=s.exit_lead_s),
                    reconcile=c + dt.timedelta(minutes=10),
                    report=c + dt.timedelta(hours=2))

    def contract_for(self, st: TradeState) -> FutureContract:
        """Exit path: rebuilt from persisted fields, no broker lookup."""
        key = st.get("contract_con_id")
        if key in self._contracts:
            return self._contracts[key]
        p = st.get("plan") or {}
        if key:
            c = self.broker.contract_from(self.cfg.strategy.instrument, self.cfg.strategy.exchange, key,
                                          st.get("contract_local_symbol"), dt.date.fromisoformat(p["contract_expiry"]),
                                          p["multiplier"])
        else:
            raise BrokerError(f"trade state {st.trade_date} has no contract id; refusing to guess a contract")
        self._contracts[key] = c
        return c

    def ensure_connected(self) -> bool:
        if self.broker.is_connected():
            return True
        sock = getattr(self.broker, "socket_ok", None)
        if sock is not None and sock():
            return False             # socket fine, IB itself unreachable (1100): wait for 1102, don't reconnect
        try:
            self.broker.connect()
            self.alert.info("reconnected to broker")
            return True
        except Exception as e:
            log.warning("connect failed: %r", e)
            return False

    def settle(self, timeout_s=20):
        """Wait until none of the bot's orders are still working."""
        waited = 0
        while self.broker.working_orders() > 0 and waited < timeout_s:
            self.broker.sleep(1); waited += 1
        return self.broker.working_orders() == 0

    def broker_pos(self, c) -> float:
        return self.broker.future_position(c)

    def confirmed_flat(self, c) -> bool:
        """Two zero readings a few seconds apart with no working orders (guards against stale caches)."""
        if not self.settle():
            return False
        if self.broker_pos(c) != 0:
            return False
        self.broker.sleep(2)
        return self.broker.working_orders() == 0 and self.broker_pos(c) == 0

    # ------------------------------------------------------------------ steps
    def announce(self, trade_date: dt.date):
        from .signal import signals_from_sources
        st = TradeState(self.cfg.monitoring.state_dir, str(trade_date))
        if st.get("announced") or st.get("announce_attempts", 0) >= 6:
            return
        if not self.broker.is_connected() and not st.get("announce_attempts"):
            self.alert.critical(f"broker not connected the evening before the trade ({trade_date})",
                                "fix IB Gateway before the 09:10 ET preflight")
        self._refresh_monitor_if_stale()
        sig, problems, sigs = signals_from_sources(trade_date, self.fetchers, self.cfg.signal.equity,
                                                   self.cfg.signal.bond, self.cfg.signal.require_agreement,
                                                   self.cfg.signal.max_source_gap_diff)
        if sig is None:
            n = st.get("announce_attempts", 0) + 1
            if n == 6:
                self.alert.warn(f"Signal for {trade_date} not available", "\n".join(problems))
            st.update(self.ts(), note="announce failed: " + "; ".join(problems), announced=False, announce_attempts=n)
            return
        body = (f"{self.cfg.signal.equity} MTD {sig.equity_mtd:+.2%}, {self.cfg.signal.bond} MTD {sig.bond_mtd:+.2%}, "
                f"gap {sig.gap:+.2%} -> {sig.side} {self.cfg.strategy.instrument} at {self.cfg.strategy.leverage:g}x "
                f"from the open to the close. Preflight runs 20 min before the open.")
        self.alert.info(f"Tomorrow ({trade_date}) month-end trade: {sig.side}",
                        body + ("\n" + "\n".join(problems) if problems else ""))
        st.update(self.ts(), note="announced", announced=True, announced_signal=sig.to_dict())

    def _refresh_monitor_if_stale(self):
        if not (self.reporter and self.cfg.guardrails.auto_derisk):
            return
        mp = Path(self.cfg.monitoring.state_dir) / "monitor.json"
        stale = True
        try:
            import json
            asof = dt.date.fromisoformat(json.loads(mp.read_text())["asof"])
            stale = (self.broker.now().date() - asof).days > 40
        except Exception:
            pass
        if stale:
            try:
                self.reporter(self.broker.now().date())
            except Exception as e:
                self.alert.warn("decay monitor could not be refreshed", repr(e))

    def do_preflight(self, trade_date: dt.date, st: TradeState):
        try:
            plan, blockers, warnings = preflight(self.cfg, self.broker, trade_date, self.fetchers)
        except Exception as e:
            plan, blockers, warnings = None, [f"preflight crashed: {e!r}"], []
            log.error(traceback.format_exc())
        o, _ = cal.session_open_close(trade_date)
        transient = plan is None and blockers and all(not any(k in b for k in PERMANENT_BLOCKERS) for b in blockers)
        if transient and self.broker.now() < o - dt.timedelta(minutes=3):
            n = st.get("preflight_attempts", 0) + 1
            st.update(self.ts(), note="preflight retry: " + "; ".join(blockers), preflight_attempts=n,
                      preflight_retry_at=(self.broker.now() + dt.timedelta(seconds=60)).isoformat())
            if n == 1:
                self.alert.warn(f"{trade_date}: preflight blocked, retrying until 09:27", "\n".join(blockers))
            return
        if plan is None:
            st.update(self.ts(), "SKIPPED", note="; ".join(blockers), blockers=blockers, warnings=warnings)
            self.alert.warn(f"{trade_date}: trade SKIPPED", "\n".join(blockers + warnings))
            return
        st.update(self.ts(), "PLANNED", note="preflight ok", plan=plan.to_dict(), direction=plan.direction,
                  qty_planned=plan.qty, contract_local_symbol=plan.contract_local_symbol,
                  contract_con_id=plan.contract_con_id, warnings=warnings)
        self.alert.info(f"{trade_date}: PLANNED {plan.action} {plan.qty} {plan.contract_local_symbol}",
                        f"gap {plan.gap:+.2%}, ref {plan.ref_price:.2f}, net liq {plan.net_liq:,.0f}, "
                        f"leverage {plan.effective_leverage:.2f}x" + ("\nWarnings: " + "; ".join(warnings) if warnings else ""))

    def do_entry(self, trade_date: dt.date, st: TradeState, deadline: dt.datetime):
        if kill_switch_active(self.cfg.monitoring.state_dir):
            st.update(self.ts(), "SKIPPED", note="kill switch active at entry")
            self.alert.warn(f"{trade_date}: entry cancelled by kill switch")
            return
        if self.cfg.mode == "live" and not self.cfg.live_confirmed():
            st.update(self.ts(), "SKIPPED", note="live not confirmed at entry")
            return
        plan = st.get("plan")
        c = self.contract_for(st)
        if self.broker.all_future_positions(self.cfg.strategy.instrument):
            st.update(self.ts(), "SKIPPED", note="unexpected futures position at entry")
            self.alert.critical(f"{trade_date}: unexpected {self.cfg.strategy.instrument} position at entry; not trading")
            return
        if self.broker.now() > deadline:                       # fresh clock: never a late entry
            st.update(self.ts(), "SKIPPED", note="entry deadline passed before submission")
            self.alert.warn(f"{trade_date}: entry deadline passed; no late entry")
            return
        st.update(self.ts(), "ENTRY_SUBMITTED", note=f"{plan['action']} {plan['qty']}")
        r = self.broker.market_order(c, plan["action"], plan["qty"], ref=f"MET-{trade_date}-entry",
                                     timeout_s=self.cfg.strategy.fill_timeout_s)
        if r.status in ("Filled", "PartiallyFilled") and r.filled > 0 and r.avg_price > 0:
            self._record_entry(trade_date, st, r.filled, r.avg_price, r.status, r.message)
        else:
            self._resolve_entry(trade_date, st, r.avg_price if r.avg_price > 0 else None, r.status, r.message,
                                reported=r.filled)

    def _record_entry(self, trade_date, st, filled, avg, status, msg=""):
        plan = st.get("plan")
        st.update(self.ts(), "ENTERED", note=f"filled {filled}@{avg}", entry_filled=int(filled), entry_price=avg,
                  entry_status=status, entry_time=self.ts(), exit_filled=0)
        lvl = self.alert.info if filled == plan["qty"] else self.alert.warn
        lvl(f"{trade_date}: ENTERED {plan['action']} {filled}/{plan['qty']} {st.get('contract_local_symbol')} @ {avg:.2f}")

    def _resolve_entry(self, trade_date, st, avg, status, msg="", reported=0):
        """Order outcome uncertain (timeout, cancel race, crash): the broker position decides, but only once
        no entry order is still working (otherwise it could fill later and be orphaned)."""
        c = self.contract_for(st)
        if not self.settle(30):
            self.broker.cancel_working()
            if not self.settle(15):
                st.update(self.ts(), note="entry order still working; will re-check")
                self.alert.critical(f"{trade_date}: entry order still working at the broker", "will re-check every tick")
                return
        d = st.get("direction")
        pos = self.broker_pos(c)
        if pos == 0:                                  # one zero read is not enough (stale cache)
            self.broker.sleep(3)
            pos = self.broker_pos(c)
        if pos == 0 and reported > 0:                 # broker reported a fill: trust it over a stale position
            pos = d * reported
        if pos != 0 and (pos > 0) == (d > 0):
            q = int(min(abs(pos), st.get("qty_planned") or abs(pos)))
            if avg is None:
                avg = self._try_price(c) or st.get("plan")["ref_price"]
            self.alert.warn(f"{trade_date}: entry resolved from broker position {pos:+g} ({status})")
            self._record_entry(trade_date, st, q, avg, f"Resolved-{status}", msg)
        elif pos == 0:
            st.update(self.ts(), "ERROR", note=f"entry not filled: {status} {msg}", entry_status=status)
            self.alert.critical(f"{trade_date}: ENTRY FAILED ({status})",
                                msg + "\nThe bot re-checks the broker at reconcile and adopts a late fill if one appears.")
        else:
            st.update(self.ts(), "ERROR", note=f"wrong-side position {pos} after entry", entry_status=status)
            self.alert.critical(f"{trade_date}: unexpected wrong-side position after entry",
                                f"broker {pos:+g}; the bot does not own it and will not touch it")

    def guard_while_holding(self, trade_date, st, exit_time):
        if (exit_time - self.broker.now()).total_seconds() <= GUARD_QUIET_S:
            return
        if kill_switch_active(self.cfg.monitoring.state_dir):
            self.alert.critical(f"{trade_date}: kill switch while holding -> flattening")
            st.update(self.ts(), "EXIT_SUBMITTED", note="kill switch", exit_reason="kill switch")
            self.flatten(trade_date, st, reason="kill switch")
            return
        c = self.contract_for(st)
        owned = st.owned
        pos = self.broker_pos(c)
        if pos != owned:
            # never conclude 'closed outside the bot' while holding (stale reads are common right after fills);
            # the scheduled exit and reconcile settle it
            now = self.broker.now()
            if (not self._last_mismatch_alert or (now - self._last_mismatch_alert).total_seconds() > 600):
                self._last_mismatch_alert = now
                self.alert.critical(f"{trade_date}: broker position differs from the bot's",
                                    f"broker {pos:+g}, bot owns {owned:+d}; the exit closes only the bot's quantity")
        lim = self.cfg.guardrails.disaster_stop_pct_equity
        if lim > 0 and owned != 0:
            px = self._try_price(c)
            entry = st.get("entry_price") or 0
            if px is None or entry <= 0:
                return
            pnl = (px - entry) * c.multiplier * owned
            eq = st.get("plan")["net_liq"]
            if pnl <= -lim * eq:
                self.alert.critical(f"{trade_date}: DISASTER STOP", f"overlay P&L {pnl:,.0f} <= -{lim:.0%} of equity")
                st.update(self.ts(), "EXIT_SUBMITTED", note="disaster stop", exit_reason="disaster stop")
                self.flatten(trade_date, st, reason="disaster stop")

    def _try_price(self, c):
        try:
            px = self.broker.price(c)
            self._price_fail = 0
            return px
        except BrokerError as e:
            self._price_fail += 1
            log.warning("price unavailable: %s", e)
            if self._price_fail == PRICE_FAIL_ALERT:
                self.alert.critical("no market price for the open position: disaster stop is blind", str(e))
            return None

    def _backoff(self, st):
        n = int(st.get("exit_failures", 0)) + 1
        delay = EXIT_BACKOFF_S[min(n - 1, len(EXIT_BACKOFF_S) - 1)]
        st.update(self.ts(), note=f"exit retry in {delay}s", exit_failures=n,
                  next_exit_try=(self.broker.now() + dt.timedelta(seconds=delay)).isoformat())
        if n == MANUAL_ALERT_AFTER:
            self.alert.critical(f"{st.trade_date}: EXIT FAILING - MANUAL INTERVENTION NEEDED",
                                f"{n} failed exit rounds; the bot keeps retrying every {EXIT_BACKOFF_S[-1]}s. "
                                f"Close {st.get('contract_local_symbol')} {st.owned:+d} in TWS and then run `met status`.")

    def flatten(self, trade_date, st, reason="scheduled") -> bool:
        """One exit round: close the quantity the bot OWNS (never more than the broker shows in that direction).
        Rounds are rate-limited with backoff; returns True when the bot owns nothing any more."""
        c = self.contract_for(st)
        now = self.broker.now()
        nxt = st.get("next_exit_try")
        if nxt and now < dt.datetime.fromisoformat(nxt):
            return False
        if not futures_open(now):
            if not st.get("closed_market_alerted"):
                self.alert.critical(f"{trade_date}: futures market closed with an open position",
                                    "will exit when CME reopens")
                st.update(self.ts(), note="market closed", closed_market_alerted=True)
            return False
        if not self.settle(10):
            self.broker.cancel_working()
            if not self.settle(10):
                self.alert.critical(f"{trade_date}: bot orders stuck working at the broker", "cancel requested")
                self._backoff(st)
                return False
        owned = st.owned
        if owned == 0:
            self._finalise(trade_date, st)
            return True
        pos = self.broker_pos(c)
        if pos == 0 or (pos > 0) != (owned > 0):
            if pos == 0 and self.confirmed_flat(c):
                px = self._try_price(c)
                st.update(self.ts(), note="broker flat: exit fill not reported (timeout/cancel race)",
                          exit_filled=st.get("entry_filled"), inferred_flat=True,
                          exit_price=st.get("exit_price") or px)
                self._finalise(trade_date, st)
                return True
            self.alert.critical(f"{trade_date}: broker position disagrees with the bot",
                                f"broker {pos:+g}, bot owns {owned:+d}; not sending an order")
            self._backoff(st)
            return False
        qty = int(min(abs(owned), abs(pos)))
        action = "SELL" if owned > 0 else "BUY"
        n = int(st.get("exit_orders", 0)) + 1
        st.update(self.ts(), "EXIT_SUBMITTED", note=f"{reason}: {action} {qty}", exit_reason=st.get("exit_reason") or reason,
                  exit_orders=n)
        r = self.broker.market_order(c, action, qty, ref=f"MET-{trade_date}-exit{n}",
                                     timeout_s=self.cfg.strategy.fill_timeout_s)
        if r.filled > 0:
            px = r.avg_price if r.avg_price > 0 else (self._try_price(c) or 0.0)
            pf, pp = st.get("exit_filled", 0) or 0, st.get("exit_price") or 0.0
            tot = pf + r.filled
            st.update(self.ts(), note=f"exit fill {r.filled}@{px}", exit_filled=tot,
                      exit_price=(pp * pf + px * r.filled) / tot if px else st.get("exit_price"))
            # let the broker's position catch up before anything else reads it
            target = pos - (r.filled if pos > 0 else -r.filled)
            for _ in range(10):
                if self.broker_pos(c) == target:
                    break
                self.broker.sleep(1)
        if st.owned == 0:
            self._finalise(trade_date, st)
            return True
        self.alert.critical(f"{trade_date}: exit incomplete", f"{r.status}: {r.filled}/{qty} filled. {r.message}")
        self._backoff(st)
        return False

    def _finalise(self, trade_date, st):
        c = self.contract_for(st)
        q, d = st.get("entry_filled") or 0, st.get("direction") or 0
        ep, xp = st.get("entry_price"), st.get("exit_price")
        extra = {}
        if q and ep and xp and ep > 0 and xp > 0:
            gross = d * (xp - ep) * c.multiplier * q
            costs = 2 * q * COST_PER_CONTRACT_SIDE.get(self.cfg.strategy.instrument, 0.62)
            eq = st.get("plan")["net_liq"]
            extra = dict(gross_pnl=round(gross, 2), est_costs=round(costs, 2), net_pnl=round(gross - costs, 2),
                         pnl_pct_equity=100 * (gross - costs) / eq, trade_bp=1e4 * d * (xp / ep - 1))
        st.update(self.ts(), "FLAT", note="flat", next_exit_try=None, **extra)
        if extra:
            self.alert.info(f"{trade_date}: FLAT. Net P&L {extra['net_pnl']:+,.0f} ({extra['pnl_pct_equity']:+.2f}% of equity)",
                            f"entry {ep:.2f} exit {xp:.2f}, {q} contracts, {extra['trade_bp']:+.1f}bp on the underlying, "
                            f"reason: {st.get('exit_reason')}")
        elif q:
            self.alert.warn(f"{trade_date}: FLAT, but P&L could not be computed (missing fill prices)")

    def adoptable(self, st) -> int:
        """Signed quantity in the bot's own contract that can be attributed to the bot: same direction as its
        trade and no larger than what it ordered. Two reads a few seconds apart (both must agree)."""
        if not st.get("contract_con_id") or not st.get("direction"):
            return 0
        c = self.contract_for(st)
        if not self.settle(15):
            return 0
        p1 = self.broker_pos(c)
        self.broker.sleep(3)
        p2 = self.broker_pos(c)
        d, cap = st.get("direction"), int(st.get("entry_filled") or st.get("qty_planned") or 0)
        pos = p1 if p1 == p2 else 0
        if pos != 0 and (pos > 0) == (d > 0) and abs(pos) <= cap:
            return int(pos)
        return 0

    def reconcile(self, trade_date, st):
        """After the close: the bot must own nothing. A position in the bot's contract, in the bot's direction
        and no larger than its order, after an uncertain entry/exit, is adopted and exited. Anything else is
        reported, never touched."""
        if st.get("reconciled"):
            return
        if st.phase == "ENTRY_SUBMITTED":
            self._resolve_entry(trade_date, st, None, "Recovered")
            if st.phase == "ENTRY_SUBMITTED":
                return                                   # order still working: not done
        if st.owned != 0:
            if st.phase != "EXIT_SUBMITTED":
                st.update(self.ts(), "EXIT_SUBMITTED", note="reconcile: still owned", exit_reason=st.get("exit_reason") or "reconcile")
            self.alert.critical(f"{trade_date}: RECONCILE: bot still owns {st.owned:+d}", "exit retries continue")
            return
        uncertain = st.phase == "ERROR" or (st.phase == "FLAT" and st.get("inferred_flat"))
        if uncertain:
            q = self.adoptable(st)
            if q:
                filled = int(st.get("entry_filled") or 0)
                if st.phase == "ERROR" or not filled:
                    fields = dict(entry_filled=abs(q), exit_filled=0,
                                  entry_price=st.get("entry_price") or self._try_price(self.contract_for(st)))
                else:
                    fields = dict(exit_filled=filled - abs(q))
                st.update(self.ts(), "EXIT_SUBMITTED", note=f"adopted {q:+d} after uncertain outcome",
                          exit_reason="adopted", inferred_flat=False, next_exit_try=None, **fields)
                self.alert.critical(f"{trade_date}: ADOPTED {q:+d} {st.get('contract_local_symbol')} after an uncertain "
                                    f"order outcome", "closing it now")
                return
        if st.get("contract_con_id") and st.phase in ("FLAT", "ERROR"):
            c = self.contract_for(st)
            pos = self.broker_pos(c)
            if pos != 0:
                self.alert.critical(f"{trade_date}: {c.local_symbol} {pos:+g} open at the broker, not attributable to the bot",
                                    "left untouched - check it")
        st.update(self.ts(), note="reconciled", reconciled=True)

    def drive_unfinished(self, today: dt.date):
        """Earlier-dated trades the bot may still own are driven to flat (rate-limited; never blocks today)."""
        now = self.broker.now()
        for d in unfinished(self.cfg.monitoring.state_dir):
            if d >= str(today):
                continue
            last = self._unfinished_last.get(d)
            if last and (now - last).total_seconds() < 300:
                continue
            self._unfinished_last[d] = now
            try:
                st = TradeState(self.cfg.monitoring.state_dir, d)
                day = dt.date.fromisoformat(d)
                if st.owned == 0:
                    self.reconcile(day, st)              # resolves ENTRY_SUBMITTED / adopts late fills
                if st.owned != 0:
                    self.alert.critical(f"{d}: trade still open after its trade day", f"bot owns {st.owned:+d}; exiting")
                    if st.phase != "EXIT_SUBMITTED":
                        st.update(self.ts(), "EXIT_SUBMITTED", note="overnight", exit_reason=st.get("exit_reason") or "overnight")
                    self.flatten(day, st, reason="overnight")
            except StateCorrupt as e:
                key = (d, today)
                if key not in self._corrupt_alerted:
                    self._corrupt_alerted.add(key)
                    self.alert.critical(f"state file for {d} is corrupt", f"{e}. Check the broker manually.")
            except Exception as e:
                log.error("drive_unfinished %s: %r", d, e)

    def weekly_status(self, now: dt.datetime, T: dt.date):
        """Monday 08:00 ET: proof of life with the next trade date and the provisional signal."""
        if now.weekday() != 0 or now.time() < dt.time(8, 0):
            return
        mark = Path(self.cfg.monitoring.state_dir) / "weekly_status.txt"
        key = now.date().isoformat()
        try:
            if mark.exists() and mark.read_text().strip() == key:
                return
        except OSError:
            pass
        lines = [f"next trade date {T} (open 09:30 ET, exit {self.times(T)['exit']:%H:%M:%S} ET)",
                 f"broker connected: {self.broker.is_connected()}",
                 f"kill switch: {kill_switch_active(self.cfg.monitoring.state_dir)}"]
        try:
            if self.broker.is_connected():
                fp = self.broker.all_future_positions(self.cfg.strategy.instrument)
                lines.append(f"{self.cfg.strategy.instrument} positions: " + (", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in fp) or "none"))
        except Exception:
            pass
        try:
            base = cal.prior_month_last_session(T)
            fetch = next(iter(self.fetchers.values()))
            e, b = fetch(self.cfg.signal.equity), fetch(self.cfg.signal.bond)
            import pandas as pd
            e.index = [pd.Timestamp(i).date() for i in e.index]; b.index = [pd.Timestamp(i).date() for i in b.index]
            last = min(e.index[-1], b.index[-1])
            if last > base:
                gap = (e.loc[last] / e.loc[base] - 1) - (b.loc[last] / b.loc[base] - 1)
                lines.append(f"provisional signal through {last}: {'SHORT' if gap > 0 else 'LONG'}, gap {gap:+.2%}")
        except Exception as ex:
            lines.append(f"provisional signal unavailable ({ex.__class__.__name__})")
        mp = Path(self.cfg.monitoring.state_dir) / "monitor.json"
        if mp.exists():
            try:
                import json
                lines.append("decay monitor: " + json.loads(mp.read_text()).get("status", "?"))
            except Exception:
                pass
        self.alert.info("Weekly status: bot running", "\n".join(lines))
        try:
            mark.write_text(key)
        except OSError:
            pass

    def overnight_guard(self, d: dt.date):
        if d in self._overnight_checked:
            return
        pos = self.broker.all_future_positions(self.cfg.strategy.instrument)
        if not pos:
            self.broker.sleep(3)
            pos = self.broker.all_future_positions(self.cfg.strategy.instrument)
        if pos:
            self.alert.critical(f"{d}: {self.cfg.strategy.instrument} position open after the close",
                                ", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in pos) +
                                "\nThe bot only flattens positions in contracts it traded (see trade state).")
        self._overnight_checked.add(d)

    # ------------------------------------------------------------------ main tick
    def tick(self) -> dt.datetime:
        """Do whatever is due now; return when the next tick should happen."""
        now = self.broker.now()
        today = now.date()
        T = cal.next_trade_date(today)
        tm = self.times(T)
        nxt = [now + dt.timedelta(seconds=60)]

        if not self._last_conn_check or (now - self._last_conn_check).total_seconds() > (
                15 if today == T else 120):
            self._last_conn_check = now
            ok = self.ensure_connected()
            if ok:
                self._disconnected_since = None
            else:
                self._disconnected_since = self._disconnected_since or now
                down = (now - self._disconnected_since).total_seconds()
                if today == T and tm["preflight"] <= now <= tm["close"] + dt.timedelta(hours=1):
                    self.alert.critical("broker disconnected on trade day")
                elif down > 600 and (not self._disc_any_alert or (now - self._disc_any_alert).total_seconds() > 3 * 3600):
                    self._disc_any_alert = now
                    self.alert.critical(f"broker unreachable for {down / 60:.0f} min",
                                        f"next trade date {T}; check IB Gateway (login / 2FA)")
        self.alert.heartbeat()
        k = kill_switch_active(self.cfg.monitoring.state_dir)
        if k and not self._kill_seen:
            self.alert.warn("kill switch is ON (no new trades; open trades are flattened)")
        self._kill_seen = k

        self.weekly_status(now, T)
        if not self.broker.is_connected() and unfinished(self.cfg.monitoring.state_dir):
            if not self._disc_alert or (now - self._disc_alert).total_seconds() > 1800:
                self._disc_alert = now
                self.alert.critical("disconnected while a trade may still be open",
                                    ", ".join(unfinished(self.cfg.monitoring.state_dir)))

        prev = cal.previous_session(T)
        _, prev_close = cal.session_open_close(prev)
        ann_t = prev_close + dt.timedelta(minutes=25)
        if today == prev and now >= ann_t:
            self.announce(T)
        elif today == prev:
            nxt.append(ann_t)

        if today == T:
            try:
                st = TradeState(self.cfg.monitoring.state_dir, str(T))
            except StateCorrupt as e:
                if not self._today_corrupt_alert or (now - self._today_corrupt_alert).total_seconds() > 3600:
                    self._today_corrupt_alert = now
                    self.alert.critical(f"{T}: today's state file is corrupt", f"{e}. Check the broker manually.")
                return now + dt.timedelta(seconds=60)
            if st.phase is None:
                if now >= tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="started too late for preflight/entry")
                    self.alert.warn(f"{T}: missed the entry window (bot not running in time)")
                elif now >= tm["preflight"]:
                    ra = st.get("preflight_retry_at")
                    if ra and now < dt.datetime.fromisoformat(ra):
                        nxt.append(dt.datetime.fromisoformat(ra))
                    elif self.ensure_connected():
                        self.do_preflight(T, st)
                        return self.broker.now()              # re-evaluate with a fresh clock
                    self.alert.critical(f"{T}: cannot connect for preflight; will retry")
                    nxt.append(now + dt.timedelta(seconds=20))
                else:
                    nxt.append(tm["preflight"])
            if st.phase == "PLANNED":
                if now > tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="entry window missed")
                    self.alert.warn(f"{T}: entry window missed; no late entry")
                elif now >= tm["entry"]:
                    if self.ensure_connected():
                        self.do_entry(T, st, tm["entry_deadline"])
                    else:
                        nxt.append(now + dt.timedelta(seconds=2))
                else:
                    nxt.append(tm["entry"])
            if st.phase == "ENTRY_SUBMITTED" and self.ensure_connected():
                self._resolve_entry(T, st, None, "Recovered")
            if st.phase in ("ENTERED", "EXIT_SUBMITTED"):
                if now >= tm["exit"] or st.phase == "EXIT_SUBMITTED":
                    if self.ensure_connected():
                        self.flatten(T, st, reason=st.get("exit_reason") or "scheduled close")
                    if st.phase != "FLAT":
                        t_next = st.get("next_exit_try")
                        nxt.append(dt.datetime.fromisoformat(t_next) if t_next else now + dt.timedelta(seconds=5))
                else:
                    if self.ensure_connected():
                        self.guard_while_holding(T, st, tm["exit"])
                    nxt += [tm["exit"], now + dt.timedelta(seconds=self.cfg.guardrails.disaster_poll_s)]
            if now >= tm["reconcile"] and st.phase not in (None, "PLANNED", "SKIPPED", "EXIT_SUBMITTED") \
                    and self.ensure_connected():
                self.reconcile(T, st)
            elif now < tm["reconcile"]:
                nxt.append(tm["reconcile"])
            if st.phase not in ("FLAT", "SKIPPED", None, "PLANNED") and now >= tm["reconcile"]:
                nxt.append(now + dt.timedelta(seconds=15))
            if now >= tm["report"] and not st.get("reported") and self.reporter:
                try:
                    res = self.reporter(T)
                    st.update(self.ts(), note="report done", reported=True)
                    if res:
                        self.alert.info(f"Decay monitor after {T}: {res.get('status')}", res.get("summary", ""))
                except Exception as e:
                    self.alert.warn(f"decay report failed: {e!r}")
                    st.update(self.ts(), note=f"report failed {e!r}", reported=True)
            elif now < tm["report"]:
                nxt.append(tm["report"])

        if self.broker.is_connected():
            self.drive_unfinished(today)           # after today's work, rate-limited, never raises

        if cal.is_session(today):
            _, c = cal.session_open_close(today)
            g = c + dt.timedelta(minutes=15)
            if now >= g and self.broker.is_connected():
                self.overnight_guard(today)
            elif now < g:
                nxt.append(g)
        return max(min(t for t in nxt if t > now - dt.timedelta(seconds=1)), now)

    def run_forever(self, stop=lambda: False, lock=True):
        lk = DaemonLock(self.cfg.monitoring.state_dir).acquire() if lock else None
        try:
            self.alert.info(f"met daemon started (mode={self.cfg.mode}, account={self.cfg.ib.account}, "
                            f"leverage={self.cfg.strategy.leverage:g}x {self.cfg.strategy.instrument})",
                            f"next trade date: {cal.next_trade_date(self.broker.now().date())}; "
                            f"state dir {Path(self.cfg.monitoring.state_dir).resolve()}")
            while not stop():
                try:
                    wake = self.tick()
                except Exception as e:
                    log.error(traceback.format_exc())
                    self.alert.critical(f"daemon tick error: {e!r}")
                    wake = self.broker.now() + dt.timedelta(seconds=15)
                wait = (wake - self.broker.now()).total_seconds()
                self.broker.sleep(max(0.2, min(wait, 60)))
        finally:
            if lk:
                lk.release()
