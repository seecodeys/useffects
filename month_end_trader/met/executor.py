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

Ownership rule: the bot owns exactly the signed sum of ITS OWN executions (orderRef MET-<date>-...), read from
the broker and persisted in the state. Position snapshots are only a cross-check: an exit order is sent only when
both agree there is something to close, sized min(|owned|, |broker position|). Positions opened by anyone else
never appear in the bot's executions and are never touched.
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

PERMANENT_PREFIXES = ("kill switch", "live mode requires", "connected account", "account ", "net liquidation ",
                      "existing ", "decay monitor says stop", "|gap|", "signal gap is exactly zero",
                      "unfinished older trade", "contract multiplier", "position size rounds",
                      "total futures exposure")
COST_PER_CONTRACT_SIDE = {"MES": 0.62, "ES": 2.25, "ZN": 1.70, "ZF": 1.70}   # IBKR commission + exchange + regulatory (estimate)
BOT_REF = "MET-"            # bot orders: MET-<date>-entry / -exitN (user '-manual' orders are excluded by the broker)
RESET_EXIT = dict(next_exit_try=None, exit_failures=0, disagree_rounds=0, manual_alerted=False,
                  closed_market_alerted=False, reversal_alerted=False)
GUARD_QUIET_S = 45          # no guard work this close to the exit time
PRICE_FAIL_ALERT = 4        # consecutive missing prices before a CRITICAL alert
EXIT_BACKOFF_S = (5, 15, 30, 60, 120, 300)   # between failed exit rounds
MAX_EXIT_ROUNDS = 10        # failed exit rounds before the 'manual intervention' alert; then 15-min rounds for an hour, then hourly
SLOW_EXIT_RETRY_S = 900     # first hour after that: a reopen after a 'market closed' rejection is not missed by ~1 h


def _fill_date(f) -> dt.date | None:
    """Exchange-local (ET) date of a persisted fill's time string; None if unparseable."""
    try:
        t = dt.datetime.fromisoformat(str(f.get("time", "")).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (t.astimezone(cal.ET) if t.tzinfo else t).date()


def _cme_full_closure(d: dt.date) -> bool:
    """Days CME equity futures do not trade at all: 25 Dec, 1 Jan, Good Friday (NYSE holiday in April/March
    that falls on a Friday two days before Easter)."""
    if (d.month, d.day) in ((12, 25), (1, 1)):
        return True
    from dateutil.easter import easter
    return d == easter(d.year) - dt.timedelta(days=2)


def futures_open(now: dt.datetime) -> bool:
    """CME equity futures: Sun 18:00 - Fri 17:00 ET, daily halt 17:00-18:00. On NYSE half days and NYSE
    weekday holidays the CME equity session halts early (13:15 ET half days, ~13:00 holidays) until 18:00."""
    t, wd, d = now.time(), now.weekday(), now.date()
    if _cme_full_closure(d) or (t >= dt.time(17) and _cme_full_closure(d + dt.timedelta(days=1))):
        return False
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


def rates_futures_open(now: dt.datetime) -> bool:
    """CBOT Treasury futures: same weekly/daily schedule as CME equity futures, but on NYSE half days and NYSE
    weekday holidays the rates session halts at 12:00 CT = 13:00 ET (not 13:15)."""
    t, d = now.time(), now.date()
    if now.weekday() < 5 and (cal.is_session(d) and cal.is_early_close(d) or not cal.is_session(d)) \
            and dt.time(13) <= t < dt.time(18):
        return False
    return futures_open(now)


class Executor:
    """Leg A (013 month-end, MES/ES open->close). The bond leg (bond.py) subclasses this and overrides the
    leg-specific hooks below; ownership, exits, reconcile and late-fill handling are shared."""
    REF = BOT_REF                 # orderRef prefix of this leg's orders
    CLI_LEG = ""                  # appended to CLI hints in alerts (bond leg: " --leg bond")
    STATE_PREFIX = "trade"        # state files <prefix>_<date>.json
    LEG = "A"

    @property
    def instrument(self) -> str:
        return self.cfg.strategy.instrument

    @property
    def exchange(self) -> str:
        return self.cfg.strategy.exchange

    @property
    def fill_timeout_s(self) -> int:
        return self.cfg.strategy.fill_timeout_s

    @property
    def disaster_pct(self) -> float:
        return self.cfg.guardrails.disaster_stop_pct_equity

    def market_open(self, now: dt.datetime) -> bool:
        return futures_open(now)

    def State(self, d) -> TradeState:
        return TradeState(self.cfg.monitoring.state_dir, str(d), prefix=self.STATE_PREFIX)

    def unfinished(self) -> list:
        return unfinished(self.cfg.monitoring.state_dir, prefix=self.STATE_PREFIX)

    def entry_date(self, st) -> dt.date:
        """Session on which the entry order was sent (executions for it expire after that day's midnight)."""
        return dt.date.fromisoformat(st.trade_date)

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
        self._sync_fail = 0
        self._last_reconnect: dt.datetime | None = None
        self._evening_last: dict = {}
        self._unfinished_alert: dict = {}

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
                    reconcile=c + dt.timedelta(minutes=2 if cal.is_early_close(d) else 10),
                    report=c + dt.timedelta(hours=2))

    def contract_for(self, st: TradeState) -> FutureContract:
        """Exit path: rebuilt from persisted fields, no broker lookup."""
        key = st.get("contract_con_id")
        if key in self._contracts:
            return self._contracts[key]
        p = st.get("plan") or {}
        if key:
            c = self.broker.contract_from(self.instrument, self.exchange, key,
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
            self._last_reconnect = self.broker.now()
            self.alert.info("reconnected to broker")
            return True
        except Exception as e:
            log.warning("connect failed: %r", e)
            return False

    def settle(self, timeout_s=20):
        """Wait until none of the bot's orders are still working."""
        waited = 0
        while self.broker.working_orders(self.REF) > 0 and waited < timeout_s:
            self.broker.sleep(1); waited += 1
        return self.broker.working_orders(self.REF) == 0

    def broker_pos(self, c) -> float:
        return self.broker.future_position(c)

    # ------------------------------------------------------------------ steps
    def announce(self, trade_date: dt.date):
        from .signal import signals_from_sources
        st = self.State(trade_date)
        if st.get("announced") or st.get("announce_attempts", 0) >= 6:
            return
        if not self.broker.is_connected() and not st.get("announce_attempts"):
            self.alert.critical(f"broker not connected the evening before the trade ({trade_date})",
                                "fix IB Gateway before the 09:10 ET preflight")
        self._refresh_monitor_if_stale()
        sig, problems, sigs = signals_from_sources(trade_date, self.fetchers, self.cfg.signal.equity,
                                                   self.cfg.signal.bond, self.cfg.signal.require_agreement,
                                                   self.cfg.signal.max_source_gap_diff,
                                                   len(self.fetchers) if self.cfg.signal.require_all_sources else 1)
        if sig is None:
            n = st.get("announce_attempts", 0) + 1
            if n == 6:
                self.alert.warn(f"Signal for {trade_date} not available", "\n".join(problems))
            st.update(self.ts(), note="announce failed: " + "; ".join(problems), announced=False, announce_attempts=n)
            return
        from .guardrails import vol_scale
        try:
            vs, vol, _ = vol_scale(self.cfg, trade_date, self.fetchers)
        except Exception:
            vs, vol = 1.0, None
        lev = self.cfg.strategy.leverage * vs
        tilt = (f" (vol {vol:.2%}/day {'>' if vs == 1.0 else '<='} {self.cfg.strategy.vol_threshold:.2%}: "
                f"{'full' if vs == 1.0 else 'reduced'} size)") if self.cfg.strategy.vol_tilt and vol is not None else ""
        body = (f"{self.cfg.signal.equity} MTD {sig.equity_mtd:+.2%}, {self.cfg.signal.bond} MTD {sig.bond_mtd:+.2%}, "
                f"gap {sig.gap:+.2%} -> {sig.side} {self.instrument} at {lev:g}x{tilt} "
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
        older = [d for d in self.unfinished() if d < str(trade_date)]
        try:
            plan, blockers, warnings = preflight(self.cfg, self.broker, trade_date, self.fetchers)
        except Exception as e:
            plan, blockers, warnings = None, [f"preflight crashed: {e!r}"], []
            log.error(traceback.format_exc())
        if older:
            plan = None
            blockers = blockers + [f"unfinished older trade state(s) {', '.join(older)}: resolve them first (met status)"]
        o, _ = cal.session_open_close(trade_date)
        transient = plan is None and blockers and not any(b.startswith(PERMANENT_PREFIXES) or "not the last NYSE session" in b
                                                          for b in blockers)
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
                        f"leverage {plan.effective_leverage:.2f}x" +
                        (f", vol {plan.vol_20d:.2%}/day -> size x{plan.vol_scale:g}" if plan.vol_20d is not None
                         else (f", size x{plan.vol_scale:g} (vol unavailable)" if plan.vol_scale != 1.0 else "")) + ("\nWarnings: " + "; ".join(warnings) if warnings else ""))

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
        if self.broker.all_future_positions(self.instrument):
            st.update(self.ts(), "SKIPPED", note="unexpected futures position at entry")
            self.alert.critical(f"{trade_date}: unexpected {self.instrument} position at entry; not trading")
            return
        if self.broker.now() > deadline:                       # fresh clock: never a late entry
            st.update(self.ts(), "SKIPPED", note="entry deadline passed before submission")
            self.alert.warn(f"{trade_date}: entry deadline passed; no late entry")
            return
        st.update(self.ts(), "ENTRY_SUBMITTED", note=f"{plan['action']} {plan['qty']}", fills={},
                  entry_submitted_at=self.ts())
        r = self.broker.market_order(c, plan["action"], plan["qty"], ref=f"{self.REF}{trade_date}-entry",
                                     timeout_s=self.fill_timeout_s)
        if r.fills:
            st.merge_fills(r.fills)
        st.update(self.ts(), note=f"entry order result {r.status} {r.filled}", entry_status=r.status)
        if r.status not in ("Filled", "PartiallyFilled", "Rejected"):
            self.broker.cancel_working(self.REF)                 # uncertain: make sure nothing is left working
        self.resolve_entry(trade_date, st, final_status=r.status, msg=r.message)

    def sync(self, st, tries=3) -> bool:
        """Pull this trade's executions (orderRef MET-<date>-...) into the state. Ownership comes only from here."""
        for i in range(tries):
            try:
                ex = self.broker.executions(f"{self.REF}{st.trade_date}-")
                sym = st.get("contract_local_symbol")
                ex = [e for e in ex if not sym or not e.get("local_symbol") or e["local_symbol"] == sym]
                today = self.broker.now().date()
                if st.get("fills"):
                    seen = {e["exec_id"] for e in ex}
                    missing = [k for k, f in st.get("fills").items() if k not in seen and not k.startswith(("manual", "adopted"))
                               and _fill_date(f) == today]       # earlier days' fills have legitimately expired
                    if missing and not st.get("missing_exec_warned"):
                        st.update(self.ts(), note=f"executions missing from IB: {missing}", missing_exec_warned=True)
                        self.alert.warn(f"{st.trade_date}: IB stopped reporting some of today's bot executions",
                                        "likely the Gateway's day rollover (set its timezone to America/New_York); "
                                        "the bot keeps using its saved fills")
                if st.merge_fills(ex):
                    st.update(self.ts(), note=f"fills synced: bot owns {st.owned:+d}")
                self._sync_fail = 0
                return True
            except BrokerError as e:
                log.warning("executions unavailable: %s", e)
                if i < tries - 1:
                    self.broker.sleep(2)
        self._sync_fail += 1
        if self._sync_fail in (3, 30):
            self.alert.critical("cannot read executions from the broker", "the bot will not send orders until it can")
        return False

    def resolve_entry(self, trade_date, st, final_status=None, msg=""):
        """Decide the entry outcome from executions. Stays ENTRY_SUBMITTED while undecided."""
        self.broker.sleep(1)
        final_status = final_status or st.get("entry_status")
        if st.owned == 0 and self._last_reconnect and (self.broker.now() - self._last_reconnect).total_seconds() < 30:
            return                                        # executions may be incomplete right after a reconnect
        if not self.sync(st):
            return
        d, plan = st.get("direction"), st.get("plan")
        expired = self.broker.now().date() > self.entry_date(st)
        owned = st.owned
        if owned != 0:
            if (owned > 0) != (d > 0) or abs(owned) > plan["qty"]:
                self.alert.critical(f"{trade_date}: entry executions inconsistent with the plan",
                                    f"fills {owned:+d} vs plan {plan['action']} {plan['qty']}; the exit will close {owned:+d}")
            st.update(self.ts(), "ENTERED", note=f"entered {owned:+d} @ {st.get('entry_price')}", entry_time=self.ts())
            lvl = self.alert.info if abs(owned) == plan["qty"] else self.alert.warn
            lvl(f"{trade_date}: ENTERED {plan['action']} {abs(owned)}/{plan['qty']} {st.get('contract_local_symbol')} "
                f"@ {st.get('entry_price'):.2f}")
            return
        if self.broker.working_orders(self.REF) > 0:
            sub = st.get("entry_submitted_at")
            if sub and (self.broker.now() - dt.datetime.fromisoformat(sub)).total_seconds() > 60:
                self.broker.cancel_working(self.REF)
            return                                        # still working: decide later
        if expired and st.owned == 0:
            st.update(self.ts(), "ERROR", note="entry outcome unknown: executions expired", executions_expired=True)
            self.alert.critical(f"{trade_date}: entry outcome UNKNOWN (IB no longer reports that day's executions)",
                                f"Check {st.get('contract_local_symbol')} in TWS. If the bot's entry filled, close it "
                                f"yourself in TWS: the bot will not touch a position it cannot prove is its own.")
            return
        if final_status in (None, "Filled", "PartiallyFilled", "Unknown", "Timeout"):
            # broker said filled (or we restarted) but no executions yet: give the reports time
            n = int(st.get("entry_resolve_tries", 0)) + 1
            st.update(self.ts(), note="no entry executions yet", entry_resolve_tries=n)
            if n < 20:
                return
        st.update(self.ts(), "ERROR", note=f"entry not filled: {final_status} {msg}")
        self.alert.critical(f"{trade_date}: ENTRY FAILED ({final_status})",
                            (msg or "") + "\nUntil midnight ET the bot keeps reading its own executions and will manage a late fill.")

    def watch_fills(self, trade_date, st, exit_time):
        """ERROR / FLAT states on the trade day: a late fill of the bot's own order shows up in executions."""
        if not self.sync(st):
            return
        if st.owned != 0:
            exited_before = bool(st.get("exit_orders")) or bool(st.get("exit_reason"))
            reversed_ = (st.owned > 0) != (int(st.get("direction") or 0) > 0)
            close_now = self.broker.now() >= exit_time or exited_before or reversed_
            st.update(self.ts(), "EXIT_SUBMITTED" if close_now else "ENTERED",
                      note=f"late fill detected in executions: bot owns {st.owned:+d}", reconciled=False,
                      exit_reason=("late fill" if close_now else st.get("exit_reason")), **RESET_EXIT)
            self.alert.critical(f"{trade_date}: LATE FILL of the bot's own order: owns {st.owned:+d}",
                                "closing it now" if close_now else "guards and the scheduled exit now apply")

    def guard_while_holding(self, trade_date, st, exit_time):
        if (exit_time - self.broker.now()).total_seconds() <= GUARD_QUIET_S:
            return
        if kill_switch_active(self.cfg.monitoring.state_dir):
            self.alert.critical(f"{trade_date}: kill switch while holding -> flattening")
            st.update(self.ts(), "EXIT_SUBMITTED", note="kill switch", exit_reason="kill switch")
            self.flatten(trade_date, st, reason="kill switch")
            return
        if not self.sync(st):
            return
        c = self.contract_for(st)
        owned = st.owned
        if owned == 0:
            self._finalise(trade_date, st)               # closed by fills we can see (e.g. manual in TWS with our ref)
            return
        pos = self.broker_pos(c)
        if pos != owned and pos != st.get("last_mismatch_pos"):
            self.broker.sleep(2)
            pos = self.broker_pos(c)                      # a single stale read right after a fill is common
        if pos == owned and st.get("mismatch_alerted"):
            st.update(self.ts(), note="broker agrees with fills again", mismatch_alerted=False, last_mismatch_pos=None)
        if pos != owned:
            now = self.broker.now()
            gap = 3600 if st.get("mismatch_alerted") else 0
            if not self._last_mismatch_alert or (now - self._last_mismatch_alert).total_seconds() > max(gap, 600):
                st.update(self.ts(), note=f"mismatch broker {pos:+g} vs fills {owned:+d}", mismatch_alerted=True,
                          last_mismatch_pos=pos)
                self._last_mismatch_alert = now
                self.alert.critical(f"{trade_date}: broker position differs from the bot's fills",
                                    f"broker {pos:+g}, bot's executions {owned:+d}; the exit closes only the bot's quantity")
        lim = self.disaster_pct
        if lim > 0:
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

    def _backoff(self, st, quick=False):
        n = int(st.get("exit_failures", 0)) + 1
        delay = 5 if quick else EXIT_BACKOFF_S[min(n - 1, len(EXIT_BACKOFF_S) - 1)]
        if n >= MAX_EXIT_ROUNDS:              # 15-min rounds for the first hour (a reopen is not missed), then hourly
            delay = SLOW_EXIT_RETRY_S if n < MAX_EXIT_ROUNDS + 4 else 3600
        st.update(self.ts(), note=f"exit retry in {delay}s", exit_failures=n,
                  next_exit_try=(self.broker.now() + dt.timedelta(seconds=delay)).isoformat())
        if n == MAX_EXIT_ROUNDS and not st.get("manual_alerted"):
            st.update(self.ts(), note="manual alert", manual_alerted=True)
            if st.get("disagree_rounds"):
                self.alert.critical(f"{st.trade_date}: RESOLVE NEEDED - the bot's fills and the broker disagree",
                                    f"bot's fills {st.owned:+d}, broker shows {st.get('last_broker_pos')}. Do NOT open new "
                                    f"orders. If you closed it yourself, stop the daemon and run `met resolve {st.trade_date}{self.CLI_LEG} "
                                    f"--price <your fill price>`.")
            else:
                self.alert.critical(f"{st.trade_date}: EXIT FAILING - MANUAL INTERVENTION NEEDED",
                                    f"{n} failed exit rounds; retrying every 15 min for an hour, then hourly. Close "
                                    f"{st.get('contract_local_symbol')} {st.owned:+d} in TWS with Order Ref "
                                    f"{self.REF}{st.trade_date}-manual, or stop the daemon and run `met flatten`.")

    def flatten(self, trade_date, st, reason="scheduled") -> bool:
        """One exit round. Closes the quantity the bot's OWN EXECUTIONS say it holds, and only if the broker's
        position agrees there is at least that much in that direction (both stale at once is needed to err)."""
        c = self.contract_for(st)
        now = self.broker.now()
        nxt = st.get("next_exit_try")
        if nxt and now < dt.datetime.fromisoformat(nxt):
            return False
        if not self.market_open(now):
            if not st.get("closed_market_alerted"):
                what = "an unresolved trade (fills and broker disagree)" if st.get("disagree_rounds") else "an open position"
                self.alert.critical(f"{trade_date}: futures market closed with {what}", "will retry when CME reopens")
                st.update(self.ts(), note="market closed", closed_market_alerted=True)
            return False
        if not self.settle(10):
            self.broker.cancel_working(self.REF)
            if not self.settle(10):
                self.alert.critical(f"{trade_date}: bot orders stuck working at the broker", "cancel requested")
                self._backoff(st)
                return False
        if not self.sync(st):
            self._backoff(st)
            return False
        owned = st.owned
        if owned == 0:
            self._finalise(trade_date, st)
            return True
        if self.broker.manual_working(f"{self.REF}{trade_date}-manual") > 0:
            if not st.get("manual_wait_alerted"):
                st.update(self.ts(), note="user's manual close working", manual_wait_alerted=True)
                self.alert.warn(f"{trade_date}: your {self.REF}{trade_date}-manual order is working; the bot waits for it")
            self._backoff(st, quick=True)
            return False
        if (owned > 0) != (st.get("direction", 1) > 0) and not st.get("reversal_alerted"):
            st.update(self.ts(), note="reversed", reversal_alerted=True)
            self.alert.critical(f"{trade_date}: bot position REVERSED to {owned:+d} by a late fill", "closing it")
        pos = self.broker_pos(c)
        if pos != owned:                                  # disagreement: need two agreeing fresh reads
            self.broker.sleep(1)
            pos2 = self.broker_pos(c)
            if pos2 == owned:
                pos = pos2                                # first read was stale
            elif pos2 != pos:
                self._backoff(st, quick=True)
                return False
        if pos == 0 or (pos > 0) != (owned > 0):
            n_dis = int(st.get("disagree_rounds", 0)) + 1
            st.update(self.ts(), note=f"fills {owned:+d} vs broker {pos:+g}", disagree_rounds=n_dis,
                      last_broker_pos=pos)
            if n_dis == 3:
                self.alert.critical(f"{trade_date}: RESOLVE NEEDED - bot's fills say {owned:+d} but the broker shows {pos:+g}",
                                    f"If you closed it yourself (or IB liquidated it), run `met resolve {trade_date}{self.CLI_LEG}` "
                                    f"(daemon stopped). Next time put Order Ref {self.REF}{trade_date}-manual on manual closes.")
            elif n_dis == 1:
                self.alert.critical(f"{trade_date}: executions and broker position disagree",
                                    f"bot's fills {owned:+d}, broker {pos:+g}; not sending an order")
            self._backoff(st)
            return False
        if st.get("disagree_rounds"):
            st.update(self.ts(), note="fills and broker agree again", disagree_rounds=0, last_broker_pos=None)
        qty = int(min(abs(owned), abs(pos)))
        action = "SELL" if owned > 0 else "BUY"
        n = int(st.get("exit_orders", 0)) + 1
        st.update(self.ts(), "EXIT_SUBMITTED", note=f"{reason}: {action} {qty}", exit_reason=st.get("exit_reason") or reason,
                  exit_orders=n)
        r = self.broker.market_order(c, action, qty, ref=f"{self.REF}{trade_date}-exit{n}",
                                     timeout_s=self.fill_timeout_s)
        if r.fills:
            st.merge_fills(r.fills)
            st.update(self.ts(), note=f"exit fills from order result: owns {st.owned:+d}")
        if r.status not in ("Filled", "PartiallyFilled", "Rejected"):
            self.broker.cancel_working(self.REF)
        self.broker.sleep(2)
        self.sync(st)
        if st.owned == 0:
            self._finalise(trade_date, st)
            return True
        rounds = int(st.get("exit_failures", 0))
        if rounds in (0, 2) or rounds % 4 == 3:
            self.alert.critical(f"{trade_date}: exit incomplete", f"{r.status}: {r.filled}/{qty}; bot still owns {st.owned:+d}. {r.message}")
        self._backoff(st, quick=r.status in ("Timeout", "Unknown"))
        return False

    def _finalise(self, trade_date, st):
        """FLAT with P&L from the bot's own executions (exact prices)."""
        c = self.contract_for(st)
        fills = (st.get("fills") or {}).values()
        q, d = st.get("entry_filled") or 0, st.get("direction") or 0
        extra = {}
        if fills and q:
            cash = -sum((f["qty"] if f["side"] == "BUY" else -f["qty"]) * f["price"] for f in fills)
            gross = cash * c.multiplier
            n_contracts = sum(f["qty"] for f in fills)
            costs = n_contracts * COST_PER_CONTRACT_SIDE.get(self.instrument, 0.62)
            eq = st.get("plan")["net_liq"]
            ep, xp = st.get("entry_price"), st.get("exit_price")
            extra = dict(gross_pnl=round(gross, 2), est_costs=round(costs, 2), net_pnl=round(gross - costs, 2),
                         pnl_pct_equity=100 * (gross - costs) / eq,
                         trade_bp=(1e4 * d * (xp / ep - 1) if ep and xp and not st.get("reversal_fills")
                                   and int(st.get("exit_filled") or 0) == int(q) else None))
        st.update(self.ts(), "FLAT", note="flat", next_exit_try=None, **extra)
        if extra:
            self.alert.info(f"{trade_date}: FLAT. Net P&L {extra['net_pnl']:+,.0f} ({extra['pnl_pct_equity']:+.2f}% of equity)",
                            f"entry {st.get('entry_price'):.2f} exit {st.get('exit_price'):.2f}, {q} contracts, "
                            f"from executions; reason: {st.get('exit_reason')}")

    def reconcile(self, trade_date, st):
        """After the close: the bot's executions must net to zero. Positions that are not the bot's executions
        are reported, never touched."""
        if st.get("reconciled"):
            return
        if not self.sync(st):
            return
        if st.phase == "ENTRY_SUBMITTED":
            self.resolve_entry(trade_date, st)
            if st.phase == "ENTRY_SUBMITTED":
                return
        if st.owned != 0:
            if st.phase != "EXIT_SUBMITTED":
                st.update(self.ts(), "EXIT_SUBMITTED", note="reconcile: still owned",
                          exit_reason=st.get("exit_reason") or "reconcile")
            self.alert.critical(f"{trade_date}: RECONCILE: bot still owns {st.owned:+d}", "exit retries continue")
            return
        if st.phase == "EXIT_SUBMITTED":
            self._finalise(trade_date, st)
        if st.get("contract_con_id") and st.phase in ("FLAT", "ERROR"):
            c = self.contract_for(st)
            pos = self.broker_pos(c)
            if pos != 0:
                if st.get("executions_expired") or (st.phase == "ERROR" and self.broker.now().date() > self.entry_date(st)):
                    self.alert.critical(f"{trade_date}: {c.local_symbol} {pos:+g} open; the bot cannot tell whether it is its own",
                                        f"IB no longer reports that day's executions. Check TWS; if it is the bot's, close it "
                                        f"yourself in TWS (the bot will not touch it).")
                else:
                    self.alert.critical(f"{trade_date}: {c.local_symbol} {pos:+g} open at the broker, not from the bot's orders",
                                        "left untouched - check it")
        st.update(self.ts(), note="reconciled", reconciled=True)

    def evening_watch(self, T, st, now):
        """After reconcile, keep checking the bot's executions every 15 min until midnight: an order IB reported
        as dead can still fill (it never has to be a stale-position guess)."""
        if st.phase not in ("FLAT", "ERROR") or not st.get("reconciled"):
            return
        last = self._evening_last.get(st.trade_date)
        if last and (now - last).total_seconds() < 900:
            return
        self._evening_last[st.trade_date] = now
        if self.sync(st) and st.owned != 0:
            st.update(self.ts(), "EXIT_SUBMITTED", note=f"late fill after reconcile: owns {st.owned:+d}",
                      reconciled=False, exit_reason="late fill", **RESET_EXIT)
            self.alert.critical(f"{T}: LATE FILL after the close: bot owns {st.owned:+d}", "closing it")

    def drive_unfinished(self, today: dt.date):
        """Earlier-dated trades the bot still owns (by its executions) are driven to flat; rate-limited, quiet
        while CME is closed, never blocks today."""
        now = self.broker.now()
        for d in self.unfinished():
            if d >= str(today):
                continue
            last = self._unfinished_last.get(d)
            if last and (now - last).total_seconds() < 300:
                continue
            self._unfinished_last[d] = now
            try:
                st = self.State(d)
                day = dt.date.fromisoformat(d)
                if st.owned == 0:
                    self.reconcile(day, st)
                if st.owned != 0 and self.market_open(now):
                    la = self._unfinished_alert.get(d)
                    if not la or (now - la).total_seconds() > 3600:
                        self._unfinished_alert[d] = now
                        if st.get("disagree_rounds"):
                            self.alert.critical(f"{d}: unresolved trade (fills {st.owned:+d}, broker {st.get('last_broker_pos')})",
                                                f"run `met resolve {d}{self.CLI_LEG} --price <fill>` with the daemon stopped")
                        else:
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
                fp = self.broker.all_future_positions(self.instrument)
                lines.append(f"{self.instrument} positions: " + (", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in fp) or "none"))
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
        if self.cfg.bond.enabled:
            lines += self._bond_status_lines(now)
        self.alert.info("Weekly status: bot running", "\n".join(lines))
        try:
            mark.write_text(key)
        except OSError:
            pass

    def _bond_status_lines(self, now) -> list:
        out = []
        try:
            from .bond import window_for
            E, X = window_for(now.date(), self.cfg.bond.hold_sessions)
            st = TradeState(self.cfg.monitoring.state_dir, str(X), prefix="bond")
            out.append(f"bond leg: next window {E} -> {X} (state {st.phase}), {self.cfg.bond.leverage:g}x IEF-equivalent")
            if self.broker.is_connected():
                zp = self.broker.all_future_positions(self.cfg.bond.instrument)
                out.append(f"{self.cfg.bond.instrument} positions: " +
                           (", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in zp) or "none"))
            bp = Path(self.cfg.monitoring.state_dir) / "bond_monitor.json"
            if bp.exists():
                import json
                out.append("bond decay monitor: " + json.loads(bp.read_text()).get("status", "?"))
        except Exception as ex:
            out.append(f"bond status unavailable ({ex.__class__.__name__})")
        return out

    def overnight_guard(self, d: dt.date):
        if d in self._overnight_checked:
            return
        pos = self.broker.all_future_positions(self.instrument)
        if not pos:
            self.broker.sleep(3)
            pos = self.broker.all_future_positions(self.instrument)
        if pos:
            self.alert.critical(f"{d}: {self.instrument} position open after the close",
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
        if not self.broker.is_connected() and self.unfinished():
            if not self._disc_alert or (now - self._disc_alert).total_seconds() > 1800:
                self._disc_alert = now
                self.alert.critical("disconnected while a trade may still be open",
                                    ", ".join(self.unfinished()))

        prev = cal.previous_session(T)
        _, prev_close = cal.session_open_close(prev)
        ann_t = prev_close + dt.timedelta(minutes=25)
        if today == prev and now >= ann_t:
            self.announce(T)
        elif today == prev:
            nxt.append(ann_t)

        if today == T:
            try:
                st = self.State(T)
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
                    else:
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
                self.resolve_entry(T, st)
                if st.phase == "ENTRY_SUBMITTED":
                    nxt.append(now + dt.timedelta(seconds=5))
            if st.phase in ("ERROR", "FLAT") and not st.get("reconciled") and self.ensure_connected():
                self.watch_fills(T, st, tm["exit"])                 # late fills of the bot's own orders
                if st.phase in ("ERROR", "FLAT"):
                    nxt.append(now + dt.timedelta(seconds=self.cfg.guardrails.disaster_poll_s))
            if st.phase == "ENTERED" and now >= tm["exit"]:
                st.update(self.ts(), "EXIT_SUBMITTED", note="scheduled exit", exit_reason=st.get("exit_reason") or "scheduled close")
            if st.phase in ("ENTERED", "EXIT_SUBMITTED"):
                if st.phase == "EXIT_SUBMITTED":
                    if self.ensure_connected():
                        self.flatten(T, st, reason=st.get("exit_reason") or "scheduled close")
                    if st.phase != "FLAT":
                        t_next = st.get("next_exit_try")
                        nxt.append(dt.datetime.fromisoformat(t_next) if t_next else now + dt.timedelta(seconds=5))
                else:
                    if self.ensure_connected():
                        self.guard_while_holding(T, st, tm["exit"])
                    nxt += [tm["exit"], now + dt.timedelta(seconds=self.cfg.guardrails.disaster_poll_s)]
            if now >= tm["reconcile"] and self.broker.is_connected():
                self.evening_watch(T, st, now)
            if now >= tm["reconcile"] and st.phase not in (None, "PLANNED", "SKIPPED", "EXIT_SUBMITTED", "ENTERED") \
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
                            f"leverage={self.cfg.strategy.leverage:g}x {self.instrument})",
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
