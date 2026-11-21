"""Leg B (research 276): the month-end Treasury bid.

Long 10-Year T-Note futures (ZN) over the last `bond.hold_sessions` NYSE sessions of each month:
  entry session E = hold_sessions sessions before the month-end session X
  E close-30m     preflight  -> PLANNED | SKIPPED       (transient blockers retried until the entry time)
  E close-2m      entry BUY  -> ENTRY_SUBMITTED -> ENTERED          (no entry after entry + grace / close-20s)
  E..X            hold over 3 nights: guards whenever CBOT trades (disaster stop, kill switch, position check)
  X close-2m      exit       -> EXIT_SUBMITTED -> FLAT               (retried until flat)
  X close+10m     reconcile; X close+2h bond decay report
State files: state/bond_<X>.json. Orders: orderRef MEB-<X>-entry / -exitN; manual closes MEB-<X>-manual.

Ownership, exits, reconcile and late-fill handling are inherited from the leg-A Executor (executions-based:
the bot owns exactly the signed sum of its own MEB-<X>- executions, persisted across days because IB only
reports executions since midnight).

Contract choice: ZN is physically delivered and IBKR liquidates positions ahead of the first notice day (the
last business day of the month BEFORE the contract month). In Feb/May/Aug/Nov the month-end exit day IS the front
contract's first notice day, so the bot always uses the first quarterly contract whose first notice day is more
than `roll_buffer_days` sessions after the exit date (e.g. the March contract for the November window).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import math
import traceback
from dataclasses import asdict, dataclass, field
from pathlib import Path

from . import calendar as cal
from .broker import Broker
from .config import BOND_SPECS, Config, HARD_MAX_BOND_LEVERAGE
from .executor import Executor, PERMANENT_PREFIXES, rates_futures_open
from .guardrails import size_contracts
from .state import StateCorrupt, kill_switch_active

log = logging.getLogger("met.bond")

BOND_REF = "MEB-"
BOND_PERMANENT = PERMANENT_PREFIXES + ("total futures exposure", "existing ZN", "existing MTN", "bond decay monitor says stop",
                                       "no ZN contract", "no MTN contract",
                                       "not the bond entry session")


# ----------------------------------------------------------------------------------------------- calendar
def entry_session(X: dt.date, hold: int) -> dt.date:
    d = X
    for _ in range(hold):
        d = cal.previous_session(d)
    return d


def window_for(today: dt.date, hold: int) -> tuple[dt.date, dt.date]:
    """(E, X) of the bond window that is current or next as of `today`."""
    X = cal.next_trade_date(today)
    return entry_session(X, hold), X


def first_notice_day(year: int, month: int) -> dt.date:
    """Treasury futures first notice day: last business day of the month before the contract month. NYSE
    sessions stand in for business days (an NYSE holiday only moves the date EARLIER: conservative)."""
    y, m = (year, month - 1) if month > 1 else (year - 1, 12)
    return cal.last_session_of_month(y, m)


def sessions_between(a: dt.date, b: dt.date) -> int:
    """Number of sessions in (a, b]."""
    if b <= a:
        return 0
    return len([d for d in cal.sessions(a + dt.timedelta(days=1), b)])


def last_safe_day(year: int, month: int, instrument: str = "ZN") -> dt.date:
    """Last day a position in the (year, month) contract may be held by this bot.
    ZN: first notice day (IBKR closes physically delivered longs before it).
    MTN (cash settled): trading terminates 2 business days before the first delivery day of the underlying Ultra
    10-Year contract (= the first business day of the contract month), i.e. inside the PREVIOUS month."""
    if instrument == "MTN":
        first = cal.next_session(dt.date(year, month, 1) - dt.timedelta(days=1))
        return cal.previous_session(cal.previous_session(first))
    return first_notice_day(year, month)


def contract_month_for(X: dt.date, buffer: int, instrument: str = "ZN") -> tuple[int, int, dt.date]:
    """First quarterly contract (y, m) whose last safe day is more than `buffer` sessions after X."""
    y, m = X.year, X.month
    for _ in range(8):
        m += 1
        if m > 12:
            y, m = y + 1, 1
        if m in (3, 6, 9, 12):
            fnd = last_safe_day(y, m, instrument)
            if sessions_between(X, fnd) > buffer:
                return y, m, fnd
    raise ValueError(f"no contract month found for {X}")


# ----------------------------------------------------------------------------------------------- preflight
@dataclass
class BondPlan:
    trade_date: str                  # month-end exit session X (state key)
    entry_date: str                  # entry session E
    direction: int
    side: str
    action: str
    qty: int
    contract_local_symbol: str
    contract_expiry: str
    contract_con_id: int
    first_notice: str
    ref_price: float
    multiplier: float
    net_liq: float
    target_leverage: float           # ZN notional / net liq target (= bond.leverage x hedge_ratio x derisk)
    effective_leverage: float        # ZN notional / net liq after rounding
    ief_equiv_leverage: float        # effective / hedge_ratio
    hedge_ratio: float
    derisk_multiplier: float
    warnings: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def read_bond_derisk(cfg: Config) -> tuple[float | None, str]:
    p = Path(cfg.monitoring.state_dir) / "bond_monitor.json"
    if not cfg.bond.auto_derisk:
        return 1.0, "auto_derisk off"
    if not p.exists():
        return 1.0, "no bond monitor data yet"
    try:
        m = json.loads(p.read_text())
        v = float(m.get("size_multiplier", 1.0))
        if v != v:
            return None, "bond_monitor.json size_multiplier is NaN"
        return min(max(v, 0.0), 1.0), m.get("status", "")
    except Exception as e:
        return None, f"bond_monitor.json unreadable: {e}"


def bond_preflight(cfg: Config, broker: Broker, X: dt.date, today: dt.date | None = None,
                   check_only: bool = False) -> tuple[BondPlan | None, list, list]:
    """(plan, blockers, warnings). check_only: run every check without requiring today to be the entry session."""
    b, g = cfg.bond, cfg.guardrails
    blockers, warnings = [], []
    E = entry_session(X, b.hold_sessions)
    if kill_switch_active(cfg.monitoring.state_dir):
        blockers.append("kill switch active")
    if not cal.is_last_session_of_month(X):
        blockers.append(f"{X} is not the last NYSE session of its month")
    if not check_only and today is not None and today != E:
        blockers.append(f"not the bond entry session ({today} vs {E})")
    if cfg.mode == "live" and not cfg.live_confirmed():
        blockers.append("live mode requires env MET_LIVE_CONFIRM=<account id>")
    if blockers:
        return None, blockers, warnings

    if not broker.is_connected():
        broker.connect()
    acct = broker.account_snapshot()
    if acct.account != cfg.ib.account:
        blockers.append(f"connected account {acct.account} != configured {cfg.ib.account}")
    if (cfg.mode == "paper") != acct.account.upper().startswith("DU"):
        blockers.append(f"account {acct.account} does not match mode={cfg.mode}")
    if acct.net_liquidation < g.min_net_liq:
        blockers.append(f"net liquidation {acct.net_liquidation:,.0f} below minimum {g.min_net_liq:,.0f}")
    open_zn = broker.all_future_positions(b.instrument)
    if open_zn:
        blockers.append(f"existing {b.instrument} positions before entry: " +
                        ", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in open_zn))

    derisk, note = read_bond_derisk(cfg)
    if derisk is None:
        blockers.append(f"bond decay monitor state unreadable ({note}); run `met bond-report`")
        derisk = 0.0
    elif derisk <= 0:
        blockers.append(f"bond decay monitor says stop trading ({note})")
    elif derisk < 1:
        warnings.append(f"bond decay monitor de-risk x{derisk:g} ({note})")

    contract = price = fnd = None
    try:
        spec = BOND_SPECS[b.instrument]
        y, m, fnd = contract_month_for(X, b.roll_buffer_days, b.instrument)
        if spec["cash_settled"]:
            # last trade date sits in the month BEFORE the contract month: look up by expiry, then verify it
            contract = broker.front_future(b.instrument, b.exchange, fnd - dt.timedelta(days=7))
            ok = sessions_between(X, contract.expiry) > b.roll_buffer_days and contract.expiry <= fnd + dt.timedelta(days=7)
        else:
            contract = broker.front_future(b.instrument, b.exchange, dt.date(y, m, 1))
            ok = (contract.expiry.year, contract.expiry.month) == (y, m)
        if not ok:
            blockers.append(f"no {b.instrument} contract for {y}-{m:02d} (broker returned {contract.local_symbol} "
                            f"{contract.expiry})")
        price = broker.price(contract)
        if abs(contract.multiplier - spec["multiplier"]) > 1e-9:
            blockers.append(f"contract multiplier {contract.multiplier} != expected {spec['multiplier']:g}")
        if not (b.price_min <= price <= b.price_max):
            blockers.append(f"ZN price {price} outside sanity band [{b.price_min}, {b.price_max}]")
    except Exception as e:
        blockers.append(f"contract/price unavailable: {e}")
    if blockers:
        return None, blockers, warnings

    target = b.leverage * b.hedge_ratio
    qty, eff = size_contracts(acct.net_liquidation, price, contract.multiplier, target, derisk,
                              b.max_leverage, b.max_contracts, b.sizing_rounding,
                              hard_max_leverage=HARD_MAX_BOND_LEVERAGE, hard_max_contracts=spec["max_contracts"])
    if qty < 1:
        blockers.append(f"position size rounds to 0 contracts (net liq {acct.net_liquidation:,.0f}, "
                        f"1 {b.instrument} = {price * contract.multiplier:,.0f}, target {target * derisk:.2f}x)")
        return None, blockers, warnings
    if target * derisk * acct.net_liquidation / (price * contract.multiplier) > b.max_contracts:
        warnings.append(f"size capped at bond.max_contracts={b.max_contracts}")
    from .guardrails import total_leverage_blocker
    tb = total_leverage_blocker(cfg, broker, b.instrument, qty * price * contract.multiplier, acct.net_liquidation)
    if tb:
        blockers.append(tb)
        return None, blockers, warnings
    try:
        mc = broker.what_if(contract, "BUY", qty)
        if not (mc.excess_after >= g.margin_buffer_pct * acct.net_liquidation):
            blockers.append(f"margin: post-trade excess {mc.excess_after:,.0f} < buffer "
                            f"{g.margin_buffer_pct:.0%} x net liq ({g.margin_buffer_pct * acct.net_liquidation:,.0f})")
    except Exception as e:
        blockers.append(f"what-if margin check failed: {e}")
    if blockers:
        return None, blockers, warnings
    plan = BondPlan(trade_date=str(X), entry_date=str(E), direction=1, side="LONG", action="BUY", qty=qty,
                    contract_local_symbol=contract.local_symbol, contract_expiry=str(contract.expiry),
                    contract_con_id=contract.con_id, first_notice=str(fnd), ref_price=price,
                    multiplier=contract.multiplier, net_liq=acct.net_liquidation, target_leverage=target * derisk,
                    effective_leverage=eff, ief_equiv_leverage=eff / b.hedge_ratio, hedge_ratio=b.hedge_ratio,
                    derisk_multiplier=derisk, warnings=warnings)
    return plan, [], warnings


# ----------------------------------------------------------------------------------------------- executor
class _Prefixed:
    """Alerter wrapper: every bond-leg alert title starts with 'BOND' so the two legs are never confused."""

    def __init__(self, inner, prefix="BOND"):
        self._inner, self._p = inner, prefix

    def info(self, t, b=""): self._inner.info(f"{self._p} {t}", b)
    def warn(self, t, b=""): self._inner.warn(f"{self._p} {t}", b)
    def critical(self, t, b=""): self._inner.critical(f"{self._p} {t}", b)

    def __getattr__(self, k):
        return getattr(self._inner, k)


class BondExecutor(Executor):
    REF = BOND_REF
    CLI_LEG = " --leg bond"
    STATE_PREFIX = "bond"
    LEG = "B"

    def __init__(self, cfg: Config, broker: Broker, alerter, fetchers: dict | None = None, reporter=None):
        super().__init__(cfg, broker, _Prefixed(alerter), fetchers or {}, reporter)
        self._bond_overnight: set = set()
        self._bond_disc_alert: dt.datetime | None = None

    @property
    def instrument(self) -> str:
        return self.cfg.bond.instrument

    @property
    def exchange(self) -> str:
        return self.cfg.bond.exchange

    @property
    def fill_timeout_s(self) -> int:
        return self.cfg.bond.fill_timeout_s

    @property
    def disaster_pct(self) -> float:
        return self.cfg.bond.disaster_stop_pct_equity

    def market_open(self, now: dt.datetime) -> bool:
        return rates_futures_open(now)

    def use_limit(self) -> bool:
        b = self.cfg.bond
        return b.order_type == "limit" or (b.order_type == "auto" and b.instrument != "ZN")

    def limit_price(self, c, action) -> float | None:
        """Marketable limit for thin contracts: last price +/- limit_ticks x 1/32 (a valid increment for ZN and MTN),
        rounded away from the market. No price -> market order (an exit must still go out)."""
        if not self.use_limit():
            return None
        px = self._try_price(c)
        if px is None:
            return None
        step = 1 / 32
        import math
        if action == "BUY":
            return math.ceil((px + self.cfg.bond.limit_ticks * step) / step - 1e-9) * step
        return math.floor((px - self.cfg.bond.limit_ticks * step) / step + 1e-9) * step

    def entry_date(self, st) -> dt.date:
        p = st.get("plan") or {}
        if p.get("entry_date"):
            return dt.date.fromisoformat(p["entry_date"])
        return entry_session(dt.date.fromisoformat(st.trade_date), self.cfg.bond.hold_sessions)

    def times(self, X: dt.date) -> dict:
        b = self.cfg.bond
        E = entry_session(X, b.hold_sessions)
        _, cE = cal.session_open_close(E)
        _, cX = cal.session_open_close(X)
        # half days: CBOT rates close at 13:00 ET with the NYSE, so leave room for exit retries before it
        lead_e = max(b.entry_lead_s, b.half_day_lead_s) if cal.is_early_close(E) else b.entry_lead_s
        lead_x = max(b.exit_lead_s, b.half_day_lead_s) if cal.is_early_close(X) else b.exit_lead_s
        entry = cE - dt.timedelta(seconds=lead_e)
        return dict(E=E, X=X, close_entry=cE, close=cX,
                    preflight=cE - dt.timedelta(minutes=30),
                    entry=entry,
                    entry_deadline=min(entry + dt.timedelta(seconds=b.entry_grace_s), cE - dt.timedelta(seconds=20)),
                    entry_reconcile=cE + dt.timedelta(minutes=10),
                    exit=cX - dt.timedelta(seconds=lead_x),
                    reconcile=cX + dt.timedelta(minutes=2 if cal.is_early_close(X) else 10),
                    report=cX + dt.timedelta(hours=2))

    def announce(self, X):
        """Evening before the entry session: proof the leg is armed (contract, size estimate, times)."""
        st = self.State(X)
        if st.get("announced") or st.get("announce_attempts", 0) >= 3:
            return
        tm = self.times(X)
        self._refresh_bond_monitor_if_stale()
        if not self.broker.is_connected():
            n = st.get("announce_attempts", 0) + 1
            st.update(self.ts(), note="announce: broker not connected", announce_attempts=n)
            if n == 1:
                self.alert.critical(f"{X}: broker not connected the evening before the bond entry",
                                    f"fix IB Gateway before {tm['preflight']:%a %H:%M} ET")
            return
        try:
            plan, blockers, warnings = bond_preflight(self.cfg, self.broker, X, check_only=True)
        except Exception as e:
            plan, blockers, warnings = None, [f"check crashed: {e!r}"], []
        if plan:
            body = (f"tomorrow {tm['entry']:%H:%M} ET: BUY ~{plan.qty} {plan.contract_local_symbol} "
                    f"(~{plan.ief_equiv_leverage:.2f}x IEF-equivalent), exit {X} {tm['exit']:%H:%M} ET")
        else:
            body = "provisional check BLOCKED (re-checked at the preflight):\n" + "\n".join(blockers)
        self.alert.info(f"Bond window {tm['E']} -> {X} starts tomorrow", body + ("\n" + "\n".join(warnings) if warnings else ""))
        st.update(self.ts(), note="announced", announced=True)

    def do_preflight(self, X: dt.date, st):
        tm = self.times(X)
        if not st.get("preflight_attempts"):
            self._refresh_bond_monitor_if_stale()
        older = [d for d in self.unfinished() if d < str(X)]
        try:
            plan, blockers, warnings = bond_preflight(self.cfg, self.broker, X, today=self.broker.now().date())
        except Exception as e:
            plan, blockers, warnings = None, [f"preflight crashed: {e!r}"], []
            log.error(traceback.format_exc())
        if older:
            plan = None
            blockers = blockers + [f"unfinished older trade state(s) {', '.join(older)}: resolve them first (met status)"]
        transient = plan is None and blockers and not any(x.startswith(BOND_PERMANENT) or "not the last NYSE session" in x
                                                          for x in blockers)
        if transient and self.broker.now() < tm["entry"] - dt.timedelta(minutes=1):
            n = st.get("preflight_attempts", 0) + 1
            st.update(self.ts(), note="preflight retry: " + "; ".join(blockers), preflight_attempts=n,
                      preflight_retry_at=(self.broker.now() + dt.timedelta(seconds=60)).isoformat())
            if n == 1:
                self.alert.warn(f"{X}: bond preflight blocked, retrying until {tm['entry'] - dt.timedelta(minutes=1):%H:%M}",
                                "\n".join(blockers))
            return
        if plan is None:
            st.update(self.ts(), "SKIPPED", note="; ".join(blockers), blockers=blockers, warnings=warnings)
            self.alert.warn(f"{X}: month-end bond trade SKIPPED", "\n".join(blockers + warnings))
            return
        st.update(self.ts(), "PLANNED", note="preflight ok", plan=plan.to_dict(), direction=1, qty_planned=plan.qty,
                  contract_local_symbol=plan.contract_local_symbol, contract_con_id=plan.contract_con_id,
                  warnings=warnings)
        self.alert.info(f"{X}: PLANNED BUY {plan.qty} {plan.contract_local_symbol} at {tm['entry']:%H:%M} ET, exit {X} "
                        f"{tm['exit']:%H:%M} ET",
                        f"ref {plan.ref_price:.4f}, net liq {plan.net_liq:,.0f}, ZN notional {plan.effective_leverage:.2f}x "
                        f"(= {plan.ief_equiv_leverage:.2f}x IEF-equivalent), first notice {plan.first_notice}"
                        + ("\nWarnings: " + "; ".join(warnings) if warnings else ""))

    # ------------------------------------------------------------------ review B1 fixes
    def _refresh_bond_monitor_if_stale(self):
        if not (self.reporter and self.cfg.bond.auto_derisk):
            return
        p = Path(self.cfg.monitoring.state_dir) / "bond_monitor.json"
        stale = True
        try:
            asof = dt.date.fromisoformat(json.loads(p.read_text())["asof"])
            stale = (self.broker.now().date() - asof).days > 40
        except Exception:
            pass
        if stale:
            try:
                self.reporter(self.broker.now().date())
            except Exception as e:
                self.alert.warn("bond decay monitor could not be refreshed", repr(e))

    def _try_adopt(self, X, st) -> bool:
        """The entry's outcome is unknowable from executions (expired). If the broker shows exactly the planned
        quantity in the planned contract, it can only be the bot's order (the preflight required 0 ZN): adopt it
        with an ESTIMATED entry price so the position is exited at month-end instead of left unmanaged."""
        p = st.get("plan") or {}
        if not (self.cfg.bond.adopt_unknown_entry and p and st.owned == 0 and st.get("entry_status") != "Rejected"
                and not st.get("adopted") and st.get("contract_con_id")):
            return False
        others = [d for d in self.unfinished() if d != st.trade_date]
        try:
            pos = self.broker_pos(self.contract_for(st))
            allz = sum(x.quantity for x in self.broker.all_future_positions(self.instrument))
        except Exception:
            return False
        if others or pos != p["qty"] or allz != pos:
            return False
        px = self._try_price(self.contract_for(st)) or p["ref_price"]
        st.merge_fills([dict(exec_id=f"adopted-{self.ts()}", rev=0, ref=f"{self.REF}{X}-entry", side="BUY",
                             qty=float(pos), price=float(p["ref_price"]), time=self.ts())])
        today = self.broker.now().date()
        late = today > dt.date.fromisoformat(st.trade_date) or self.broker.now() >= self.times(X)["exit"]
        st.update(self.ts(), "EXIT_SUBMITTED" if late else "ENTERED", note=f"adopted {pos:+g} after an unknown entry outcome",
                  adopted=True, pnl_estimated=True, reconciled=False, executions_expired=False,
                  exit_reason="adopted after deadline" if late else None)
        self.alert.critical(f"{X}: ADOPTED {pos:+g} {st.get('contract_local_symbol')} as the bot's entry",
                            f"the entry's executions expired before the bot could read them; the broker shows exactly the "
                            f"planned quantity. Entry price ESTIMATED at {p['ref_price']} (now {px}). "
                            + ("Closing it now." if late else f"Held to the scheduled exit on {X}.")
                            + " Set bond.adopt_unknown_entry: false to leave such positions to you.")
        return True

    def resolve_entry(self, trade_date, st, final_status=None, msg=""):
        super().resolve_entry(trade_date, st, final_status, msg)
        if st.phase == "ERROR" and st.get("executions_expired"):
            self._try_adopt(trade_date, st)

    def reconcile(self, trade_date, st):
        if st.phase == "ERROR" and st.owned == 0 and self.broker.now().date() > self.entry_date(st) \
                and self.sync(st) and st.owned == 0 and self._try_adopt(trade_date, st):
            return
        return super().reconcile(trade_date, st)

    def flatten(self, trade_date, st, reason="scheduled") -> bool:
        """Leg A's MES exit at close-2s has priority: no bond exit round (which can block ~45 s on a timeout) starts
        from 2 min before it until 1 min after it."""
        peer = getattr(self, "peer", None)
        now = self.broker.now()
        if peer is not None and cal.is_session(now.date()) and cal.next_trade_date(now.date()) == now.date():
            try:
                pst = peer.State(now.date())
                a_exit = peer.times(now.date())["exit"]
                if pst.phase in ("PLANNED", "ENTRY_SUBMITTED", "ENTERED", "EXIT_SUBMITTED") and \
                        a_exit - dt.timedelta(seconds=120) <= now < a_exit + dt.timedelta(seconds=60):
                    st.update(self.ts(), note="bond exit deferred: leg A exit window",
                              next_exit_try=(a_exit + dt.timedelta(seconds=60)).isoformat())
                    return False
            except StateCorrupt:
                pass
        return super().flatten(trade_date, st, reason)

    def guard_while_holding(self, trade_date, st, exit_time):
        """Overnight / weekend: when CBOT is closed, only the kill switch is checked (the exit then waits for the
        reopen). While it trades: the full leg-A guard set (disaster stop, mismatch, kill switch)."""
        if self.market_open(self.broker.now()):
            return super().guard_while_holding(trade_date, st, exit_time)
        if kill_switch_active(self.cfg.monitoring.state_dir):
            self.alert.critical(f"{trade_date}: kill switch while holding (market closed) -> flatten at the reopen")
            st.update(self.ts(), "EXIT_SUBMITTED", note="kill switch", exit_reason="kill switch")
            self.flatten(trade_date, st, reason="kill switch")

    def overnight_guard(self, d: dt.date):
        """Each session close + 15 min: outside a bond window the account should hold no ZN from the bot; inside
        one, the broker should show the bot's quantity."""
        if d in self._bond_overnight:
            return
        self._bond_overnight.add(d)
        active = [x for x in self.unfinished()]
        pos = self.broker.all_future_positions(self.instrument)
        tot = sum(p.quantity for p in pos)
        owned = 0
        for x in active:
            try:
                owned += self.State(x).owned
            except StateCorrupt:
                pass
        if tot != owned:
            self.alert.critical(f"{d}: ZN position after the close {tot:+g} differs from the bot's {owned:+d}",
                                ", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in pos) or "none at the broker")

    # ------------------------------------------------------------------ main tick
    def tick(self) -> dt.datetime:
        now = self.broker.now()
        today = now.date()
        E, X = window_for(today, self.cfg.bond.hold_sessions)
        tm = self.times(X)
        nxt = [now + dt.timedelta(seconds=60)]

        if not self.broker.is_connected() and self.unfinished():
            if not self._bond_disc_alert or (now - self._bond_disc_alert).total_seconds() > 1800:
                self._bond_disc_alert = now
                self.alert.critical("disconnected while a bond position may be open", ", ".join(self.unfinished()))

        if today < E:
            prev = cal.previous_session(E)
            ann_t = cal.session_open_close(prev)[1] + dt.timedelta(minutes=30)
            if today == prev and now >= ann_t:
                try:
                    self.announce(X)
                except StateCorrupt:
                    pass
            elif today == prev:
                nxt.append(ann_t)
            nxt.append(tm["preflight"])
        else:
            try:
                st = self.State(X)
            except StateCorrupt as e:
                if not self._today_corrupt_alert or (now - self._today_corrupt_alert).total_seconds() > 3600:
                    self._today_corrupt_alert = now
                    self.alert.critical(f"{X}: bond state file is corrupt", f"{e}. Check ZN at the broker manually.")
                return now + dt.timedelta(seconds=60)
            self._drive_window(st, X, E, tm, now, today, nxt)

        if self.broker.is_connected():
            self.drive_unfinished(today)
        if cal.is_session(today):
            _, c = cal.session_open_close(today)
            g = c + dt.timedelta(minutes=15)
            if now >= g and self.broker.is_connected():
                self.overnight_guard(today)
            elif now < g:
                nxt.append(g)
        return max(min(t for t in nxt if t > now - dt.timedelta(seconds=1)), now)

    def _drive_window(self, st, X, E, tm, now, today, nxt):
        poll = self.cfg.bond.disaster_poll_s
        if today == E:
            if st.phase is None:
                if now >= tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="started too late for the bond entry")
                    self.alert.warn(f"{X}: missed the bond entry window (bot not running in time)")
                elif now >= tm["preflight"]:
                    ra = st.get("preflight_retry_at")
                    if ra and now < dt.datetime.fromisoformat(ra):
                        nxt.append(dt.datetime.fromisoformat(ra))
                    elif self.ensure_connected():
                        self.do_preflight(X, st)
                        nxt.append(self.broker.now())
                        return
                    else:
                        self.alert.critical(f"{X}: cannot connect for the bond preflight; will retry")
                        nxt.append(now + dt.timedelta(seconds=20))
                else:
                    nxt.append(tm["preflight"])
            if st.phase == "PLANNED":
                if now > tm["entry_deadline"]:
                    st.update(self.ts(), "SKIPPED", note="bond entry window missed")
                    self.alert.warn(f"{X}: bond entry window missed; no late entry")
                elif now >= tm["entry"]:
                    if self.ensure_connected():
                        self.do_entry(X, st, tm["entry_deadline"])
                    else:
                        nxt.append(now + dt.timedelta(seconds=2))
                else:
                    nxt.append(tm["entry"])
        elif st.phase in (None, "PLANNED"):
            st.update(self.ts(), "SKIPPED", note=f"bond entry on {E} missed (daemon not running then)")
            self.alert.warn(f"{X}: bond entry on {E} was missed; no trade this month")
            return

        if st.phase == "ENTRY_SUBMITTED" and self.ensure_connected():
            self.resolve_entry(X, st)
            if st.phase == "ENTRY_SUBMITTED":
                nxt.append(now + dt.timedelta(seconds=5))
        if st.phase in ("ERROR", "FLAT") and not st.get("reconciled") and today == E and self.ensure_connected():
            self.watch_fills(X, st, tm["exit"])                      # a late entry fill becomes a normal hold
            if st.phase in ("ERROR", "FLAT"):
                nxt.append(now + dt.timedelta(seconds=poll))
        if st.phase == "ENTERED" and now >= tm["exit"]:
            st.update(self.ts(), "EXIT_SUBMITTED", note="scheduled exit", exit_reason=st.get("exit_reason") or "scheduled close")
        if st.phase == "EXIT_SUBMITTED":
            if self.ensure_connected():
                self.flatten(X, st, reason=st.get("exit_reason") or "scheduled close")
            if st.phase != "FLAT":
                t_next = st.get("next_exit_try")
                nxt.append(dt.datetime.fromisoformat(t_next) if t_next else now + dt.timedelta(seconds=5))
        elif st.phase == "ENTERED":
            if self.ensure_connected():
                self.guard_while_holding(X, st, tm["exit"])
            nxt += [tm["exit"], now + dt.timedelta(seconds=poll)]

        # entry day: a failed entry (ERROR) or a same-day kill-switch exit is reconciled after the entry close
        rec_t = tm["entry_reconcile"] if (today == E or st.phase in ("ERROR", "FLAT")) and today < X else tm["reconcile"]
        if today == X and now >= tm["reconcile"] and self.broker.is_connected():
            self.evening_watch(X, st, now)
        if now >= rec_t and st.phase in ("FLAT", "ERROR") and not st.get("reconciled") and self.ensure_connected():
            self.reconcile(X, st)
        elif now < rec_t and st.phase in ("FLAT", "ERROR", "ENTRY_SUBMITTED"):
            nxt.append(rec_t)
        if today == E and now >= tm["entry_reconcile"] and self.broker.is_connected() and st.phase in ("FLAT", "ERROR"):
            self.evening_watch(X, st, now)                           # late entry fill until midnight
        if today == X:
            if now >= tm["report"] and not st.get("reported") and self.reporter and st.phase in ("FLAT", "SKIPPED", "ERROR"):
                try:
                    res = self.reporter(X)
                    st.update(self.ts(), note="bond report done", reported=True)
                    if res:
                        self.alert.info(f"bond decay monitor after {X}: {res.get('status')}", res.get("summary", ""))
                except Exception as e:
                    self.alert.warn(f"bond decay report failed: {e!r}")
                    st.update(self.ts(), note=f"report failed {e!r}", reported=True)
            elif now < tm["report"]:
                nxt.append(tm["report"])
