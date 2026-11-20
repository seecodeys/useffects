"""Pre-trade checks and position sizing. `preflight` either returns a Plan or a list of blocking reasons."""
from __future__ import annotations

import datetime as dt
import json
import math
from dataclasses import dataclass, asdict, field
from pathlib import Path

from . import calendar as cal
from .broker import Broker, FutureContract
from .config import Config, HARD_MAX_CONTRACTS, HARD_MAX_LEVERAGE
from .signal import Signal, signals_from_sources
from .state import kill_switch_active


@dataclass
class Plan:
    trade_date: str
    direction: int
    side: str
    action: str                 # BUY / SELL
    qty: int
    contract_local_symbol: str
    contract_expiry: str
    contract_con_id: int
    ref_price: float
    multiplier: float
    net_liq: float
    target_leverage: float
    effective_leverage: float
    derisk_multiplier: float
    gap: float
    equity_mtd: float
    bond_mtd: float
    signal_sources: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


def size_contracts(net_liq: float, price: float, multiplier: float, leverage: float, derisk: float,
                   max_leverage: float, max_contracts: int) -> tuple[int, float]:
    if net_liq <= 0 or price <= 0:
        return 0, 0.0
    per = price * multiplier
    qty = math.floor(leverage * derisk * net_liq / per)
    qty = min(qty, max_contracts, HARD_MAX_CONTRACTS)
    while qty > 0 and qty * per / net_liq > min(max_leverage, HARD_MAX_LEVERAGE) + 1e-9:
        qty -= 1
    return max(qty, 0), qty * per / net_liq


def read_derisk(cfg: Config) -> tuple[float | None, str]:
    """(multiplier in [0, 1], note). None = unreadable -> preflight blocks (fail closed)."""
    p = Path(cfg.monitoring.state_dir) / "monitor.json"
    if not cfg.guardrails.auto_derisk:
        return 1.0, "auto_derisk off"
    if not p.exists():
        return 1.0, "no monitor data yet"
    try:
        m = json.loads(p.read_text())
        v = float(m.get("size_multiplier", 1.0))
        if not (v == v):
            return None, "monitor.json size_multiplier is NaN"
        return min(max(v, 0.0), 1.0), m.get("status", "")
    except Exception as e:
        return None, f"monitor.json unreadable: {e}"


def preflight(cfg: Config, broker: Broker, trade_date: dt.date, fetchers: dict) -> tuple[Plan | None, list, list]:
    """Returns (plan, blockers, warnings). plan is None iff blockers is non-empty."""
    blockers, warnings = [], []
    g, s = cfg.guardrails, cfg.strategy

    if kill_switch_active(cfg.monitoring.state_dir):
        blockers.append("kill switch active")
    if not cal.is_last_session_of_month(trade_date):
        blockers.append(f"{trade_date} is not the last NYSE session of its month")
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
    base_cur = getattr(acct, "base_currency", acct.currency)
    if base_cur != "USD":
        warnings.append(f"account base currency {base_cur}: values converted to USD at IBKR's rate")
    if acct.net_liquidation < g.min_net_liq:
        blockers.append(f"net liquidation {acct.net_liquidation:,.0f} below minimum {g.min_net_liq:,.0f}")

    open_fut = broker.all_future_positions(s.instrument)
    if open_fut:
        blockers.append("existing %s positions before entry: %s" % (
            s.instrument, ", ".join(f"{p.local_symbol} {p.quantity:+g}" for p in open_fut)))

    if g.core_policy != "ignore":
        core = sum(p.market_value for p in broker.positions() if p.sec_type == "STK" and p.symbol == g.core_symbol)
        w = core / acct.net_liquidation if acct.net_liquidation > 0 else 0
        if w < g.core_min_weight:
            msg = f"core {g.core_symbol} weight {w:.0%} below {g.core_min_weight:.0%} of net liquidation"
            (blockers if g.core_policy == "block" else warnings).append(msg)

    sig, sig_problems, all_sigs = signals_from_sources(trade_date, fetchers, cfg.signal.equity, cfg.signal.bond,
                                                       cfg.signal.require_agreement, cfg.signal.max_source_gap_diff,
                                                       len(fetchers) if cfg.signal.require_all_sources else 1)
    if sig is None:
        blockers.extend(sig_problems)
    else:
        warnings.extend(sig_problems)
        if sig.direction == 0:
            blockers.append("signal gap is exactly zero")
        if abs(sig.gap) < cfg.strategy.min_abs_gap:
            blockers.append(f"|gap| {abs(sig.gap):.3%} below min_abs_gap {cfg.strategy.min_abs_gap:.3%}")

    derisk, derisk_note = read_derisk(cfg)
    if derisk is None:
        blockers.append(f"decay monitor state unreadable ({derisk_note}); run `met report`")
        derisk = 0.0
    elif derisk < 1.0:
        warnings.append(f"decay monitor de-risk x{derisk:g} ({derisk_note})")
    if derisk <= 0 and not any("unreadable" in b for b in blockers):
        blockers.append(f"decay monitor says stop trading ({derisk_note})")

    contract = price = None
    try:
        contract = broker.front_future(s.instrument, s.exchange, trade_date + dt.timedelta(days=s.roll_min_days))
        price = broker.price(contract)
        if abs(contract.multiplier - cfg.multiplier) > 1e-9:
            blockers.append(f"contract multiplier {contract.multiplier} != expected {cfg.multiplier}")
    except Exception as e:
        blockers.append(f"contract/price unavailable: {e}")

    if blockers:
        return None, blockers, warnings

    # sanity: futures price should be ~10x SPY (S&P 500 vs SPY ratio ~ 10, plus basis)
    try:
        spy = fetchers[next(iter(fetchers))](cfg.signal.equity)
        ratio = price / float(spy.iloc[-1])
        if not (9.0 <= ratio <= 11.5):
            blockers.append(f"futures price {price} vs {cfg.signal.equity} close {float(spy.iloc[-1]):.2f}: ratio {ratio:.2f} implausible")
    except Exception as e:
        blockers.append(f"futures-vs-{cfg.signal.equity} price sanity check failed: {e}")

    qty, eff = size_contracts(acct.net_liquidation, price, contract.multiplier, s.leverage, derisk,
                              g.max_leverage, g.max_contracts)
    if qty < 1:
        blockers.append(f"position size rounds to 0 contracts (net liq {acct.net_liquidation:,.0f}, "
                        f"1 contract = {price * contract.multiplier:,.0f})")
    if s.leverage * derisk * acct.net_liquidation / (price * contract.multiplier) > g.max_contracts:
        warnings.append(f"size capped at max_contracts={g.max_contracts}")
    if blockers:
        return None, blockers, warnings

    action = "BUY" if sig.direction > 0 else "SELL"
    try:
        m = broker.what_if(contract, action, qty)
        if not (m.excess_after >= g.margin_buffer_pct * acct.net_liquidation):      # NaN-safe
            blockers.append(f"margin: post-trade excess {m.excess_after:,.0f} < buffer "
                            f"{g.margin_buffer_pct:.0%} x net liq ({g.margin_buffer_pct * acct.net_liquidation:,.0f})")
    except Exception as e:
        blockers.append(f"what-if margin check failed: {e}")
    if blockers:
        return None, blockers, warnings

    plan = Plan(trade_date=str(trade_date), direction=sig.direction, side=sig.side, action=action, qty=qty,
                contract_local_symbol=contract.local_symbol, contract_expiry=str(contract.expiry),
                contract_con_id=contract.con_id, ref_price=price,
                multiplier=contract.multiplier, net_liq=acct.net_liquidation, target_leverage=s.leverage,
                effective_leverage=eff, derisk_multiplier=derisk, gap=sig.gap, equity_mtd=sig.equity_mtd,
                bond_mtd=sig.bond_mtd, signal_sources=[x.to_dict() for x in all_sigs], warnings=warnings)
    return plan, [], warnings
