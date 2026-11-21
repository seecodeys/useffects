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
    vol_20d: float | None = None     # SPY daily-return std over vol_window sessions ending T-1 (vol tilt)
    vol_scale: float = 1.0           # 1.0 high-vol month-end, low_vol_scale otherwise (1.0 when tilt is off)

    def to_dict(self):
        return asdict(self)


def size_contracts(net_liq: float, price: float, multiplier: float, leverage: float, derisk: float,
                   max_leverage: float, max_contracts: int, rounding: str = "floor") -> tuple[int, float]:
    if net_liq <= 0 or price <= 0:
        return 0, 0.0
    per = price * multiplier
    raw = leverage * derisk * net_liq / per
    qty = math.floor(raw + 0.5) if rounding == "nearest" else math.floor(raw)
    qty = min(qty, max_contracts, HARD_MAX_CONTRACTS)
    while qty > 0 and qty * per / net_liq > min(max_leverage, HARD_MAX_LEVERAGE) + 1e-9:
        qty -= 1
    return max(qty, 0), qty * per / net_liq


def trailing_vol(closes, trade_date: dt.date, window: int) -> float:
    """Std of the `window` daily returns of adjusted closes ending at the T-1 close (research 168: rolling std
    shifted one day). Requires the exact T-1 close and window+1 consecutive closes; raises ValueError otherwise."""
    import pandas as pd
    s = pd.Series(closes).dropna()
    s.index = [pd.Timestamp(i).date() for i in s.index]
    s = s[~pd.Index(s.index).duplicated(keep="last")].sort_index()
    t1 = cal.previous_session(trade_date)
    if t1 not in s.index:
        raise ValueError(f"close for {t1} missing")
    s = s[s.index <= t1]
    want = cal.sessions(t1 - dt.timedelta(days=3 * window + 15), t1)[-(window + 1):]
    missing = [d for d in want if d not in s.index]
    if missing:
        raise ValueError(f"{len(missing)} closes missing in the {window}-day window (first {missing[0]})")
    r = s.loc[want].pct_change().dropna()
    v = float(r.std())
    if not (v == v) or v <= 0:
        raise ValueError("vol is NaN/zero")
    return v


def vol_scale(cfg: Config, trade_date: dt.date, fetchers: dict) -> tuple[float, float | None, list]:
    """(size scale, vol, warnings). Sources disagreeing on the regime, or no vol at all -> the low (smaller)
    size, never the larger one."""
    s = cfg.strategy
    if not s.vol_tilt:
        return 1.0, None, []
    vols, notes = {}, []
    for name, fetch in fetchers.items():
        try:
            vols[name] = trailing_vol(fetch(cfg.signal.equity), trade_date, s.vol_window)
        except Exception as e:
            notes.append(f"vol tilt: source {name} unavailable ({e})")
    if not vols:
        return s.low_vol_scale, None, notes + [f"vol tilt: no vol available -> low size x{s.low_vol_scale:g}"]
    high = {n: v > s.vol_threshold for n, v in vols.items()}
    v = min(vols.values())
    if len(set(high.values())) > 1:
        notes.append("vol tilt: sources disagree on the regime (" + ", ".join(f"{n} {x:.3%}" for n, x in vols.items())
                     + f" vs threshold {s.vol_threshold:.3%}) -> low size")
        return s.low_vol_scale, v, notes
    return (1.0 if all(high.values()) else s.low_vol_scale), v, notes


def other_futures_notional(broker, exclude_symbol: str) -> float:
    """|notional| of every futures position on the account except `exclude_symbol` (USD market value; IB reports
    futures marketValue as quantity x price x multiplier)."""
    return sum(abs(p.market_value) for p in broker.positions() if p.sec_type == "FUT" and p.symbol != exclude_symbol)


def total_leverage_blocker(cfg: Config, broker, exclude_symbol: str, new_notional: float, net_liq: float) -> str | None:
    try:
        other = other_futures_notional(broker, exclude_symbol)
    except Exception as e:
        return f"total exposure check failed: {e}"
    tot = (other + new_notional) / net_liq if net_liq > 0 else float("inf")
    if not (tot <= cfg.guardrails.max_total_leverage + 1e-9):
        return (f"total futures exposure {tot:.2f}x net liq (this order {new_notional / net_liq:.2f}x + open "
                f"{other / net_liq:.2f}x) above guardrails.max_total_leverage {cfg.guardrails.max_total_leverage:g}")
    return None


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


def preflight(cfg: Config, broker: Broker, trade_date: dt.date, fetchers: dict,
              provisional: bool = False) -> tuple[Plan | None, list, list]:
    """Returns (plan, blockers, warnings). plan is None iff blockers is non-empty."""
    blockers, warnings = [], []
    g, s = cfg.guardrails, cfg.strategy

    if kill_switch_active(cfg.monitoring.state_dir):
        blockers.append("kill switch active")
    if not cal.is_last_session_of_month(trade_date):
        blockers.append(f"{trade_date} is not the last NYSE session of its month")
    if provisional:
        warnings.append("PROVISIONAL check: the signal uses the latest close, not the T-1 close; nothing is traded")
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
                                                       len(fetchers) if cfg.signal.require_all_sources else 1,
                                                       provisional=provisional)
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

    vscale, vol, vnotes = vol_scale(cfg, trade_date, fetchers)
    warnings.extend(vnotes)

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

    qty, eff = size_contracts(acct.net_liquidation, price, contract.multiplier, s.leverage * vscale, derisk,
                              g.max_leverage, g.max_contracts, s.sizing_rounding)
    if qty < 1:
        blockers.append(f"position size rounds to 0 contracts (net liq {acct.net_liquidation:,.0f}, "
                        f"1 contract = {price * contract.multiplier:,.0f})")
    if s.leverage * vscale * derisk * acct.net_liquidation / (price * contract.multiplier) > g.max_contracts:
        warnings.append(f"size capped at max_contracts={g.max_contracts}")
    if blockers:
        return None, blockers, warnings

    tb = total_leverage_blocker(cfg, broker, s.instrument, qty * price * contract.multiplier, acct.net_liquidation)
    if tb:
        blockers.append(tb)
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
                bond_mtd=sig.bond_mtd, signal_sources=[x.to_dict() for x in all_sigs], warnings=warnings,
                vol_20d=vol, vol_scale=vscale)
    return plan, [], warnings
