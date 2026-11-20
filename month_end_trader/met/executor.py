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

COST_PER_CONTRACT_SIDE = {"MES": 0.62, "ES": 2.25}   # IBKR commission + exchange + regulatory (estimate)
GUARD_QUIET_S = 20          # no guard work this close to the exit time
PRICE_FAIL_ALERT = 4        # consecutive missing prices before a CRITICAL alert


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
        else:   # legacy state without con_id
            d = dt.date.fromisoformat(st.trade_date)
            c = self.broker.front_future(self.cfg.strategy.instrument, self.cfg.strategy.exchange,
                                         d + dt.timedelta(days=self.cfg.strategy.roll_min_days))
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

    def do_preflight(self, trade_date: dt.date, st: TradeState):
        try:
            plan, blockers, warnings = preflight(self.cfg, self.broker, trade_date, self.fetchers)
        except Exception as e:
            plan, blockers, warnings = None, [f"preflight crashed: {e!r}"], []
            log.error(traceback.format_exc())
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
            self._resolve_entry(trade_date, st, r.avg_price if r.avg_price > 0 else None, r.status, r.message)

    def _record_entry(self, trade_date, st, filled, avg, status, msg=""):
        plan = st.get("plan")
        st.update(self.ts(), "ENTERED", note=f"filled {filled}@{avg}", entry_filled=int(filled), entry_price=avg,
                  entry_status=status, entry_time=self.ts(), exit_filled=0)
        lvl = self.alert.info if filled == plan["qty"] else self.alert.warn
        lvl(f"{trade_date}: ENTERED {plan['action']} {filled}/{plan['qty']} {st.get('contract_local_symbol')} @ {avg:.2f}")

    def _resolve_entry(self, trade_date, st, avg, status, msg=""):
        """Order outcome uncertain (timeout, cancel race, crash): the broker position decides."""
        c = self.contract_for(st)
        self.settle(30)
        pos = self.broker_pos(c)
        d = st.get("direction")
        if pos != 0 and (pos > 0) == (d > 0):
            if avg is None:
                try:
                    avg = self.broker.price(c)
                except BrokerError:
                    avg = st.get("plan")["ref_price"]
            self.alert.warn(f"{trade_date}: entry resolved from broker position {pos:+g} ({status})")
            self._record_entry(trade_date, st, int(abs(pos)), avg, f"Resolved-{status}", msg)
        elif pos == 0:
            st.update(self.ts(), "ERROR", note=f"entry not filled: {status} {msg}", entry_status=status)
            self.alert.critical(f"{trade_date}: ENTRY FAILED ({status})", msg)
        else:
            st.update(self.ts(), "ERROR", note=f"wrong-side position {pos} after entry", entry_status=status)
            self.alert.critical(f"{trade_date}: position {pos:+g} has the wrong sign after entry; flattening")
            self.flatten(trade_date, st, reason="wrong-side entry")

    def guard_while_holding(self, trade_date, st, exit_time):
        if (exit_time - self.broker.now()).total_seconds() <= GUARD_QUIET_S:
            return
        if kill_switch_active(self.cfg.monitoring.state_dir):
            self.alert.critical(f"{trade_date}: kill switch while holding -> flattening")
            self.flatten(trade_date, st, reason="kill switch")
            return
        c = self.contract_for(st)
        expected = st.open_quantity
        pos = self.broker_pos(c)
        if pos != expected:
            if pos == 0 and self.confirmed_flat(c):
                px = self._try_price(c)
                st.update(self.ts(), "FLAT", note="position closed outside the bot", exit_reason="external",
                          exit_filled=st.get("entry_filled"), exit_price=px)
                self.alert.critical(f"{trade_date}: position was closed outside the bot")
                return
            now = self.broker.now()
            if pos != 0 and (not self._last_mismatch_alert or (now - self._last_mismatch_alert).total_seconds() > 600):
                self._last_mismatch_alert = now
                self.alert.critical(f"{trade_date}: broker position {pos:+g} differs from expected {expected:+d}; "
                                    f"the exit will flatten the broker position")
        lim = self.cfg.guardrails.disaster_stop_pct_equity
        if lim > 0 and pos != 0:
            px = self._try_price(c)
            if px is None:
                return
            entry = st.get("entry_price") or 0
            if entry <= 0:
                return
            pnl = (px - entry) * c.multiplier * pos
            eq = st.get("plan")["net_liq"]
            if pnl <= -lim * eq:
                self.alert.critical(f"{trade_date}: DISASTER STOP: overlay P&L {pnl:,.0f} <= -{lim:.0%} of equity")
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

    def flatten(self, trade_date, st, reason="scheduled"):
        """Bring the broker position in the bot's contract to zero. Sized from the broker each attempt."""
        c = self.contract_for(st)
        for attempt in range(1, 6):
            if not self.settle():
                self.alert.critical(f"{trade_date}: earlier orders still working; waiting before exit attempt {attempt}")
                continue
            pos = self.broker_pos(c)
            if pos == 0:
                if self.confirmed_flat(c):
                    break
                continue
            action = "SELL" if pos > 0 else "BUY"
            qty = int(abs(pos))
            st.update(self.ts(), "EXIT_SUBMITTED", note=f"{reason}: {action} {qty} (attempt {attempt})",
                      exit_reason=st.get("exit_reason") or reason)
            r = self.broker.market_order(c, action, qty, ref=f"MET-{trade_date}-exit{attempt}",
                                         timeout_s=self.cfg.strategy.fill_timeout_s)
            if r.filled > 0 and r.avg_price > 0:
                pf, pp = st.get("exit_filled", 0) or 0, st.get("exit_price") or 0.0
                tot = pf + r.filled
                st.update(self.ts(), note=f"exit fill {r.filled}@{r.avg_price}", exit_filled=tot,
                          exit_price=(pp * pf + r.avg_price * r.filled) / tot)
            if r.status != "Filled":
                self.alert.critical(f"{trade_date}: exit attempt {attempt} {r.status} ({r.filled}/{qty})", r.message)
                self.broker.sleep(3)
        else:
            pos = self.broker_pos(c) if self.broker.is_connected() else None
            self.alert.critical(f"{trade_date}: STILL OPEN after 5 exit attempts (broker {pos}). Will keep retrying.")
            return False
        self._finalise(trade_date, st)
        return True

    def _finalise(self, trade_date, st):
        c = self.contract_for(st)
        q, d = st.get("entry_filled") or 0, st.get("direction") or 0
        ep, xp = st.get("entry_price"), st.get("exit_price")
        if q and ep and xp and ep > 0 and xp > 0:
            gross = d * (xp - ep) * c.multiplier * q
            costs = 2 * q * COST_PER_CONTRACT_SIDE.get(self.cfg.strategy.instrument, 0.62)
            eq = st.get("plan")["net_liq"]
            st.update(self.ts(), "FLAT", note="flat", gross_pnl=round(gross, 2), est_costs=round(costs, 2),
                      net_pnl=round(gross - costs, 2), pnl_pct_equity=100 * (gross - costs) / eq,
                      trade_bp=1e4 * d * (xp / ep - 1))
            self.alert.info(f"{trade_date}: FLAT. Net P&L {gross - costs:+,.0f} ({100 * (gross - costs) / eq:+.2f}% of equity)",
                            f"entry {ep:.2f} exit {xp:.2f}, {q} contracts, {st.get('trade_bp'):+.1f}bp on the underlying, "
                            f"reason: {st.get('exit_reason')}")
        else:
            st.update(self.ts(), "FLAT", note="flat (P&L unknown: missing fill prices)")
            self.alert.warn(f"{trade_date}: FLAT, but P&L could not be computed (missing fill prices)")

    def reconcile(self, trade_date, st):
        if st.get("reconciled"):
            return
        if st.phase in ACTIVE or st.phase == "FLAT":
            if st.get("contract_con_id") or st.get("plan"):
                c = self.contract_for(st)
                pos = self.broker_pos(c)
                if pos != 0 or self.broker.working_orders() > 0:
                    self.alert.critical(f"{trade_date}: RECONCILE: broker shows {pos:+g} {c.local_symbol} "
                                        f"(state {st.phase}); flattening")
                    if not self.flatten(trade_date, st, reason=st.get("exit_reason") or "reconcile"):
                        return
        st.update(self.ts(), note="reconciled", reconciled=True)

    def drive_unfinished(self, today: dt.date):
        """Any trade state from an earlier date that may still own a position is flattened now."""
        for d in unfinished(self.cfg.monitoring.state_dir):
            if d >= str(today):
                continue
            try:
                st = TradeState(self.cfg.monitoring.state_dir, d)
            except StateCorrupt as e:
                self.alert.critical(f"state file for {d} is corrupt", f"{e}. Check the broker manually; "
                                    f"MES positions: {self.broker.all_future_positions(self.cfg.strategy.instrument)}")
                continue
            if st.phase in ACTIVE:
                self.alert.critical(f"{d}: trade still unfinished on {today}; flattening now")
            self.reconcile(dt.date.fromisoformat(d), st)

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
                15 if today == T else 300):
            self._last_conn_check = now
            if not self.ensure_connected() and today == T and tm["preflight"] <= now <= tm["close"] + dt.timedelta(hours=1):
                self.alert.critical("broker disconnected on trade day")
        self.alert.heartbeat()
        k = kill_switch_active(self.cfg.monitoring.state_dir)
        if k and not self._kill_seen:
            self.alert.warn("kill switch is ON (no new trades; open trades are flattened)")
        self._kill_seen = k

        if self.broker.is_connected():
            self.drive_unfinished(today)
        self.weekly_status(now, T)

        prev = cal.previous_session(T)
        _, prev_close = cal.session_open_close(prev)
        ann_t = prev_close + dt.timedelta(minutes=25)
        if today == prev and now >= ann_t:
            self.announce(T)
        elif today == prev:
            nxt.append(ann_t)

        if today == T:
            st = TradeState(self.cfg.monitoring.state_dir, str(T))
            if st.phase is None:
                if now >= tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="started too late for preflight/entry")
                    self.alert.warn(f"{T}: missed the entry window (bot not running in time)")
                elif now >= tm["preflight"]:
                    if self.ensure_connected():
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
                        nxt.append(now + dt.timedelta(seconds=10))
                else:
                    if self.ensure_connected():
                        self.guard_while_holding(T, st, tm["exit"])
                    nxt += [tm["exit"], now + dt.timedelta(seconds=self.cfg.guardrails.disaster_poll_s)]
            if now >= tm["reconcile"] and st.phase not in (None, "PLANNED", "SKIPPED") and self.ensure_connected():
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
