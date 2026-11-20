"""Trade-day state machine and the continuously running daemon.

The daemon calls `tick()` repeatedly. Every action is idempotent and driven by the persisted TradeState,
so the process can be restarted at any moment (including mid-trade) and resume safely:

  T-1 after close   announce tomorrow's signal
  T open-20m        preflight  -> PLANNED | SKIPPED
  T open+1s         entry      -> ENTRY_SUBMITTED -> ENTERED | ERROR      (never later than open+grace)
  T while holding   guards: disaster stop, kill switch, position reconciliation
  T close-2s        exit       -> EXIT_SUBMITTED -> FLAT                  (retried until flat)
  T close+10m       reconcile: broker must show no position; otherwise flatten + CRITICAL
  T close+2h        decay-monitor report
  every session     close+15m overnight guard (no bot-owned futures left open)
"""
from __future__ import annotations

import datetime as dt
import logging
import traceback

from . import calendar as cal
from .alerts import Alerter
from .broker import Broker, BrokerError, FutureContract
from .config import Config
from .guardrails import preflight
from .state import TradeState, kill_switch_active

log = logging.getLogger("met.executor")

COST_PER_CONTRACT_SIDE = {"MES": 0.62, "ES": 2.25}   # IBKR commission + exchange + regulatory (estimate)


class Executor:
    def __init__(self, cfg: Config, broker: Broker, alerter: Alerter, fetchers: dict, reporter=None):
        self.cfg, self.broker, self.alert, self.fetchers = cfg, broker, alerter, fetchers
        self.reporter = reporter                  # callable(trade_date) -> dict | None
        self._contract: FutureContract | None = None
        self._last_guard_check: dt.datetime | None = None
        self._overnight_checked: set = set()
        self._last_conn_check: dt.datetime | None = None

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
                    report=c + dt.timedelta(hours=2),
                    hard_flat=c + dt.timedelta(minutes=55))   # CME equity futures halt at 17:00 ET

    def contract_for(self, st: TradeState) -> FutureContract:
        if self._contract is None or self._contract.local_symbol != st.get("contract_local_symbol"):
            s = self.cfg.strategy
            d = dt.date.fromisoformat(st.data["trade_date"])
            c = self.broker.front_future(s.instrument, s.exchange, d + dt.timedelta(days=s.roll_min_days))
            if st.get("contract_local_symbol") and c.local_symbol != st.get("contract_local_symbol"):
                raise BrokerError(f"contract changed {st.get('contract_local_symbol')} -> {c.local_symbol}")
            self._contract = c
        return self._contract

    def ensure_connected(self) -> bool:
        if self.broker.is_connected():
            return True
        try:
            self.broker.connect()
            self.alert.info("reconnected to broker")
            return True
        except Exception as e:
            log.warning("connect failed: %r", e)
            return False

    # ------------------------------------------------------------------ steps
    def announce(self, trade_date: dt.date):
        from .signal import signals_from_sources
        st = TradeState(self.cfg.monitoring.state_dir, str(trade_date))
        if st.get("announced"):
            return
        sig, problems, sigs = signals_from_sources(trade_date, self.fetchers, self.cfg.signal.equity,
                                                   self.cfg.signal.bond, self.cfg.signal.require_agreement)
        if sig is None:
            self.alert.warn(f"Signal for {trade_date} not available yet", "\n".join(problems))
            st.update(self.ts(), note="announce failed: " + "; ".join(problems), announced=False)
            return
        body = (f"{self.cfg.signal.equity} MTD {sig.equity_mtd:+.2%}, {self.cfg.signal.bond} MTD {sig.bond_mtd:+.2%}, "
                f"gap {sig.gap:+.2%} -> {sig.side} {self.cfg.strategy.instrument} at {self.cfg.strategy.leverage:g}x "
                f"from the open to the close. Preflight runs 20 min before the open.")
        self.alert.info(f"Tomorrow ({trade_date}) month-end trade: {sig.side}", body + ("\n" + "\n".join(problems) if problems else ""))
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
                  qty_planned=plan.qty, contract_local_symbol=plan.contract_local_symbol, warnings=warnings)
        self.alert.info(f"{trade_date}: PLANNED {plan.action} {plan.qty} {plan.contract_local_symbol}",
                        f"gap {plan.gap:+.2%}, ref {plan.ref_price:.2f}, net liq {plan.net_liq:,.0f}, "
                        f"leverage {plan.effective_leverage:.2f}x" + ("\nWarnings: " + "; ".join(warnings) if warnings else ""))

    def do_entry(self, trade_date: dt.date, st: TradeState):
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
        st.update(self.ts(), "ENTRY_SUBMITTED", note=f"{plan['action']} {plan['qty']}")
        r = self.broker.market_order(c, plan["action"], plan["qty"], ref=f"MET-{trade_date}-entry",
                                     timeout_s=self.cfg.strategy.fill_timeout_s)
        self._record_entry(trade_date, st, r.filled, r.avg_price, r.status, r.message)

    def _record_entry(self, trade_date, st, filled, avg, status, msg=""):
        plan = st.get("plan")
        if filled <= 0:
            st.update(self.ts(), "ERROR", note=f"entry not filled: {status} {msg}", entry_status=status)
            self.alert.critical(f"{trade_date}: ENTRY FAILED ({status})", msg)
            return
        st.update(self.ts(), "ENTERED", note=f"filled {filled}@{avg}", entry_filled=int(filled), entry_price=avg,
                  entry_status=status, entry_time=self.ts(), exit_filled=0)
        lvl = self.alert.info if filled == plan["qty"] else self.alert.warn
        lvl(f"{trade_date}: ENTERED {plan['action']} {filled}/{plan['qty']} {st.get('contract_local_symbol')} @ {avg:.2f}")

    def recover_entry_submitted(self, trade_date, st):
        """Crash between submit and confirmation: trust the broker's position."""
        c = self.contract_for(st)
        pos = self.broker.future_position(c)
        d = st.get("direction")
        if pos != 0 and (pos > 0) == (d > 0):
            px = self.broker.price(c)
            self.alert.warn(f"{trade_date}: recovered entry from broker position {pos:+g} (price unknown, using {px:.2f})")
            self._record_entry(trade_date, st, int(abs(pos)), px, "Recovered")
        else:
            st.update(self.ts(), "ERROR", note=f"entry state unknown; broker position {pos}")
            self.alert.critical(f"{trade_date}: entry outcome unknown after restart; broker position {pos:+g}")

    def guard_while_holding(self, trade_date, st):
        c = self.contract_for(st)
        expected = st.open_quantity
        pos = self.broker.future_position(c)
        if pos != expected:
            self.alert.critical(f"{trade_date}: position mismatch broker {pos:+g} vs expected {expected:+d}")
            if pos == 0:   # closed externally
                st.update(self.ts(), "FLAT", note="position closed outside the bot", exit_filled=st.get("entry_filled"))
                return
        if kill_switch_active(self.cfg.monitoring.state_dir):
            self.alert.critical(f"{trade_date}: kill switch while holding -> flattening")
            self.do_exit(trade_date, st, reason="kill switch")
            return
        lim = self.cfg.guardrails.disaster_stop_pct_equity
        if lim > 0:
            try:
                px = self.broker.price(c)
                pnl = st.get("direction") * (px - st.get("entry_price")) * c.multiplier * abs(expected)
                eq = st.get("plan")["net_liq"]
                if pnl <= -lim * eq:
                    self.alert.critical(f"{trade_date}: DISASTER STOP: overlay P&L {pnl:,.0f} <= -{lim:.0%} of equity")
                    self.do_exit(trade_date, st, reason="disaster stop")
            except BrokerError as e:
                log.warning("price unavailable for disaster check: %s", e)

    def do_exit(self, trade_date, st, reason="scheduled"):
        c = self.contract_for(st)
        for attempt in range(1, 6):
            qty = abs(st.open_quantity)
            if qty == 0:
                break
            action = "SELL" if st.get("direction") > 0 else "BUY"
            st.update(self.ts(), "EXIT_SUBMITTED", note=f"{reason}: {action} {qty} (attempt {attempt})", exit_reason=reason)
            r = self.broker.market_order(c, action, qty, ref=f"MET-{trade_date}-exit{attempt}",
                                         timeout_s=self.cfg.strategy.fill_timeout_s)
            if r.filled > 0:
                prev_f, prev_px = st.get("exit_filled", 0), st.get("exit_price") or 0.0
                tot = prev_f + r.filled
                st.update(self.ts(), note=f"exit fill {r.filled}@{r.avg_price}", exit_filled=tot,
                          exit_price=(prev_px * prev_f + r.avg_price * r.filled) / tot)
            if st.open_quantity == 0:
                break
            self.alert.critical(f"{trade_date}: exit attempt {attempt} incomplete ({r.status}) {r.message}")
            self.broker.sleep(3)
        if st.open_quantity != 0:
            self.alert.critical(f"{trade_date}: STILL OPEN after 5 exit attempts: {st.open_quantity:+d}. Will keep retrying.")
            return
        self._finalise(trade_date, st)

    def _finalise(self, trade_date, st):
        c = self.contract_for(st)
        q = st.get("entry_filled"); d = st.get("direction")
        gross = d * (st.get("exit_price") - st.get("entry_price")) * c.multiplier * q
        costs = 2 * q * COST_PER_CONTRACT_SIDE.get(self.cfg.strategy.instrument, 0.62)
        eq = st.get("plan")["net_liq"]
        st.update(self.ts(), "FLAT", note="flat", gross_pnl=round(gross, 2), est_costs=round(costs, 2),
                  net_pnl=round(gross - costs, 2), pnl_pct_equity=100 * (gross - costs) / eq,
                  trade_bp=1e4 * d * (st.get("exit_price") / st.get("entry_price") - 1))
        self.alert.info(f"{trade_date}: FLAT. Net P&L {gross - costs:+,.0f} ({100 * (gross - costs) / eq:+.2f}% of equity)",
                        f"entry {st.get('entry_price'):.2f} exit {st.get('exit_price'):.2f}, {q} contracts, "
                        f"{st.get('trade_bp'):+.1f}bp on the underlying, reason: {st.get('exit_reason')}")

    def reconcile(self, trade_date, st):
        if st.get("reconciled"):
            return
        problems = []
        if st.phase in ("ENTERED", "EXIT_SUBMITTED"):
            problems.append(f"state still {st.phase} after the close")
        if st.phase in ("ENTERED", "EXIT_SUBMITTED", "FLAT", "ERROR", "ENTRY_SUBMITTED"):
            c = self.contract_for(st)
            pos = self.broker.future_position(c)
            if pos != 0:
                problems.append(f"broker still shows {pos:+g} {c.local_symbol}")
        if problems:
            self.alert.critical(f"{trade_date}: RECONCILE FAILED", "\n".join(problems))
            if st.open_quantity != 0:
                self.do_exit(trade_date, st, reason="reconcile")
            return
        st.update(self.ts(), note="reconciled", reconciled=True)

    def overnight_guard(self, d: dt.date):
        """After every session: no futures in the instrument may remain unless the user holds them on purpose."""
        if d in self._overnight_checked:
            return
        pos = self.broker.all_future_positions(self.cfg.strategy.instrument)
        if pos:
            self.alert.critical(f"{d}: {self.cfg.strategy.instrument} position open after the close",
                                ", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in pos) +
                                "\nThe bot only flattens positions it opened (see trade state).")
        self._overnight_checked.add(d)

    # ------------------------------------------------------------------ main tick
    def tick(self) -> dt.datetime:
        """Do whatever is due now; return when the next tick should happen."""
        now = self.broker.now()
        today = now.date()
        T = cal.next_trade_date(today)
        tm = self.times(T)
        nxt = [now + dt.timedelta(seconds=60)]

        if not self._last_conn_check or (now - self._last_conn_check).total_seconds() > 300:
            self._last_conn_check = now
            if not self.ensure_connected() and today == T and tm["preflight"] <= now <= tm["hard_flat"]:
                self.alert.critical("broker disconnected on trade day")
        self.alert.heartbeat()

        # announce the evening before
        prev = cal.previous_session(T)
        _, prev_close = cal.session_open_close(prev)
        ann_t = prev_close + dt.timedelta(minutes=25)
        if today == prev and now >= ann_t:
            self.announce(T)
        elif today == prev:
            nxt.append(ann_t)

        if today == T:
            st = TradeState(self.cfg.monitoring.state_dir, str(T))
            ph = st.phase
            if ph is None:
                if now >= tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="started too late for preflight/entry")
                    self.alert.warn(f"{T}: missed the entry window (bot not running in time)")
                elif now >= tm["preflight"]:
                    if self.ensure_connected():
                        self.do_preflight(T, st)
                    else:
                        self.alert.critical(f"{T}: cannot connect for preflight; will retry")
                        nxt.append(now + dt.timedelta(seconds=20))
                else:
                    nxt.append(tm["preflight"])
            ph = st.phase
            if ph == "PLANNED":
                if now > tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="entry window missed")
                    self.alert.warn(f"{T}: entry window missed; no late entry")
                elif now >= tm["entry"]:
                    if self.ensure_connected():
                        self.do_entry(T, st)
                    else:
                        nxt.append(now + dt.timedelta(seconds=2))
                else:
                    nxt.append(tm["entry"])
            ph = st.phase
            if ph == "ENTRY_SUBMITTED" and self.ensure_connected():
                self.recover_entry_submitted(T, st)
            ph = st.phase
            if ph in ("ENTERED", "EXIT_SUBMITTED"):
                if now >= tm["exit"] or ph == "EXIT_SUBMITTED":
                    if self.ensure_connected():
                        self.do_exit(T, st, reason=st.get("exit_reason") or "scheduled close")
                    if st.open_quantity != 0:
                        nxt.append(now + dt.timedelta(seconds=10))
                else:
                    if self.ensure_connected():
                        self.guard_while_holding(T, st)
                    nxt += [tm["exit"], now + dt.timedelta(seconds=self.cfg.guardrails.disaster_poll_s)]
            if now >= tm["reconcile"] and st.phase not in (None, "PLANNED") and self.ensure_connected():
                self.reconcile(T, st)
            elif now < tm["reconcile"]:
                nxt.append(tm["reconcile"])
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

    def run_forever(self, stop=lambda: False):
        self.alert.info(f"met daemon started (mode={self.cfg.mode}, account={self.cfg.ib.account}, "
                        f"leverage={self.cfg.strategy.leverage:g}x {self.cfg.strategy.instrument})",
                        f"next trade date: {cal.next_trade_date(self.broker.now().date())}")
        while not stop():
            try:
                wake = self.tick()
            except Exception as e:
                log.error(traceback.format_exc())
                self.alert.critical(f"daemon tick error: {e!r}")
                wake = self.broker.now() + dt.timedelta(seconds=15)
            wait = (wake - self.broker.now()).total_seconds()
            self.broker.sleep(max(0.2, min(wait, 60)))
