"""Configuration loading and validation. Hard limits here cannot be overridden by the config file."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path

import yaml

# Absolute ceilings, independent of config. Changing these requires editing code on purpose.
HARD_MAX_LEVERAGE = 4.0          # overlay notional / net liquidation
HARD_MAX_CONTRACTS = 200
HARD_MIN_EXIT_LEAD_S = 1
HARD_MAX_ENTRY_GRACE_S = 600
HARD_MAX_BOND_LEVERAGE = 6.0     # ZN notional / net liquidation
HARD_MAX_BOND_CONTRACTS = 50
# hedge: beta of IEF daily returns on the futures' daily returns, 2017-2026 (roll days excluded): ZN 1.13 (corr 0.96),
# TN (MTN's underlying, Ultra 10-Year) 0.885 (corr 0.96). cost = IBKR all-in estimate per contract per side.
BOND_SPECS = {"ZN": dict(multiplier=1000.0, hedge=1.14, cost=1.70, max_contracts=50, default_contracts=20, cash_settled=False),
              "MTN": dict(multiplier=100.0, hedge=0.885, cost=0.60, max_contracts=400, default_contracts=200,
                          cash_settled=True)}
HARD_MAX_TOTAL_LEVERAGE = 8.0    # all futures notional of both legs / net liquidation


class ConfigError(ValueError):
    pass


@dataclass
class IBConfig:
    host: str = "127.0.0.1"
    port: int = 4002                 # IB Gateway: 4002 paper / 4001 live. TWS: 7497 paper / 7496 live
    client_id: int = 17
    account: str = ""                # required: DUxxxxxxx (paper) or Uxxxxxxx (live)
    market_data_type: int = 1        # 1 live, 3 delayed (sizing only; disaster stop needs live)
    connect_timeout_s: int = 15


@dataclass
class StrategyConfig:
    leverage: float = 2.0            # overlay notional as a multiple of net liquidation
    instrument: str = "MES"          # MES (micro, $5 x index) or ES ($50 x index)
    exchange: str = "CME"
    min_abs_gap: float = 0.0         # skip the month if |SPY MTD - TLT MTD| below this (fraction, e.g. 0.005)
    entry_delay_s: int = 1           # seconds after the 09:30:00 cash open
    exit_lead_s: int = 2             # seconds before the cash close
    entry_grace_s: int = 90          # never enter later than open + this
    fill_timeout_s: int = 30
    roll_min_days: int = 3           # use the first contract expiring at least this many days after the trade
    sizing_rounding: str = "floor"   # floor: never above target leverage; nearest: closest contract count (still <= max_leverage)
    # research 168: full size only when SPY's 20-day vol (std of the daily adjusted close-to-close returns ending at
    # T-1) is above its 2002-08..2014 median; otherwise low_vol_scale x size. `leverage` is the FULL-size leverage.
    vol_tilt: bool = False
    vol_threshold: float = 0.008369  # daily std (= 13.3% annualised); 2002-08..2014 median, fixed (no re-fit)
    vol_window: int = 20
    low_vol_scale: float = 0.5


@dataclass
class SignalConfig:
    equity: str = "SPY"
    bond: str = "TLT"
    sources: list = field(default_factory=lambda: ["ibkr", "yahoo"])
    require_agreement: bool = True   # if both sources available and the sign differs -> skip
    max_source_gap_diff: float = 0.0015  # sources' gaps must agree within 15bp, else treat as a data error
    require_all_sources: bool = False    # True: skip the month unless every source (IBKR and Yahoo) answers


@dataclass
class BondConfig:
    """Leg B (research 276): long 10y Treasury futures over the last `hold_sessions` sessions of each month:
    enter just before the close of the session `hold_sessions` before month-end, exit just before the month-end
    close. Sized as IEF-equivalent notional: ZN notional = leverage x hedge_ratio x net liq."""
    enabled: bool = False
    instrument: str = "ZN"           # ZN: 10-Year T-Note ($1,000 x price, ~$107k, physically delivered)
                                     # MTN: Micro Ultra 10-Year T-Note ($100 x price, ~$11k, cash settled) for small accounts
    exchange: str = "CBOT"
    leverage: float = 2.8            # IEF-equivalent notional / net liq (research: 2.81)
    hedge_ratio: float = 0.0         # IEF daily return = hedge_ratio x futures return; 0 = per instrument (BOND_SPECS)
    hold_sessions: int = 3
    entry_lead_s: int = 120          # enter 2 min before the NYSE close (15:58 ET; 12:58 on half days)
    exit_lead_s: int = 300           # exit 15:55 ET on month-end, clear of leg A's 15:59:58 MES exit
    half_day_lead_s: int = 600       # on 1 pm closes enter/exit at 12:50 ET (CBOT rates halt at 13:00 ET); also on
                                     # SIFMA early-close days (rates settle 13:00 ET): 31 Dec, before Good Friday/Memorial Day
    extra_early_days: list = field(default_factory=list)   # more YYYY-MM-DD dates to treat like that (CME notices)
    entry_grace_s: int = 90          # no entry later than entry time + this (and never in the last 20 s)
    fill_timeout_s: int = 30
    order_type: str = "auto"         # market | limit | auto (limit for thin micros like MTN, market for ZN)
    limit_ticks: int = 3             # marketable limit = last price +/- this many 1/32 steps; re-priced on every retry
    roll_buffer_days: int = 5        # contract's first notice day must be > exit date + this many business days
    sizing_rounding: str = "floor"
    max_leverage: float = 4.5        # ZN notional / net liq ceiling (2.8 x 1.14 = 3.19 at full size)
    max_contracts: int = 0           # 0 = per instrument: ZN 20, MTN 200 (MTN is 1/10 the size)
    disaster_stop_pct_equity: float = 0.04   # flatten if the bond overlay loses 4% of equity (~1.25% ZN move); 0 = off
    disaster_poll_s: int = 30
    auto_derisk: bool = True         # bond decay monitor (SPRT) can halve or stop the leg
    adopt_unknown_entry: bool = True # after a crash/timeout whose executions expired: if the broker shows EXACTLY the
                                     # planned ZN quantity in the planned contract (preflight proved 0 before), treat it
                                     # as the bot's entry (P&L ESTIMATED) instead of leaving it unmanaged
    edge_h1_bp: float = 20.0         # per-trade bp on IEF notional (2002-2026 mean 20.0, net of 2bp)
    sd_bp: float = 66.0
    price_min: float = 60.0          # ZN price sanity band
    price_max: float = 180.0


@dataclass
class GuardrailConfig:
    max_leverage: float = 3.0
    max_contracts: int = 40
    min_net_liq: float = 10_000.0
    margin_buffer_pct: float = 0.10  # post-trade excess liquidity must stay >= this x net liq
    disaster_stop_pct_equity: float = 0.06   # 0 disables. Flatten early if overlay loss >= this x equity
    disaster_poll_s: int = 15
    core_symbol: str = "SPY"
    core_min_weight: float = 0.5     # SPY value / net liq expected at least this
    core_policy: str = "warn"        # warn | block | ignore
    auto_derisk: bool = True         # size x0.5 / x0 from the SPRT decay monitor
    max_total_leverage: float = 7.0  # (all MES/ES + ZN futures notional incl. the new order) / net liq, both legs


@dataclass
class MonitoringConfig:
    start: str = "2026-11"           # live tracking (SPRT) starts with this month
    state_dir: str = "./state"
    report_dir: str = "./reports"
    log_dir: str = "./logs"
    edge_h1_bp: float = 20.0
    sd_bp: float = 78.0
    sprt_alpha: float = 0.10
    sprt_beta: float = 0.10


@dataclass
class Config:
    mode: str = "paper"
    ib: IBConfig = field(default_factory=IBConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    signal: SignalConfig = field(default_factory=SignalConfig)
    bond: BondConfig = field(default_factory=BondConfig)
    guardrails: GuardrailConfig = field(default_factory=GuardrailConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)

    @property
    def multiplier(self) -> float:
        return {"MES": 5.0, "ES": 50.0}[self.strategy.instrument]

    def validate(self, require_account: bool = True) -> "Config":
        errs = []
        if self.mode not in ("paper", "live"):
            errs.append("mode must be 'paper' or 'live'")
        if require_account and not self.ib.account:
            errs.append("ib.account is required")
        if self.ib.account and self.ib.account.upper().startswith("F"):
            errs.append("ib.account must be an individual account (U.../DU...), not an FA master or group")
        if self.ib.account:
            is_paper_acct = self.ib.account.upper().startswith("DU")
            if self.mode == "paper" and not is_paper_acct:
                errs.append(f"mode=paper but account {self.ib.account} is not a paper account (DU...)")
            if self.mode == "live" and is_paper_acct:
                errs.append(f"mode=live but account {self.ib.account} is a paper account")
        if self.mode == "live" and self.ib.port in (4002, 7497, 4004):
            errs.append("mode=live but port is a paper port (4002/7497/4004)")
        if self.mode == "paper" and self.ib.port in (4001, 7496, 4003):
            errs.append("mode=paper but port is a live port (4001/7496/4003)")
        if self.mode == "live" and self.ib.market_data_type != 1:
            errs.append("mode=live requires ib.market_data_type: 1 (live data for the disaster stop)")
        s, g = self.strategy, self.guardrails
        if s.sizing_rounding not in ("floor", "nearest"):
            errs.append("strategy.sizing_rounding must be floor or nearest")
        if s.instrument not in ("MES", "ES"):
            errs.append("strategy.instrument must be MES or ES")
        if not (0 < s.leverage <= g.max_leverage):
            errs.append(f"strategy.leverage {s.leverage} must be in (0, guardrails.max_leverage={g.max_leverage}]")
        if g.max_leverage > HARD_MAX_LEVERAGE:
            errs.append(f"guardrails.max_leverage {g.max_leverage} exceeds hard limit {HARD_MAX_LEVERAGE}")
        if not (0 < g.max_contracts <= HARD_MAX_CONTRACTS):
            errs.append(f"guardrails.max_contracts must be in (0, {HARD_MAX_CONTRACTS}]")
        if s.exit_lead_s < HARD_MIN_EXIT_LEAD_S:
            errs.append(f"strategy.exit_lead_s must be >= {HARD_MIN_EXIT_LEAD_S}")
        if not (0 <= s.entry_grace_s <= HARD_MAX_ENTRY_GRACE_S):
            errs.append(f"strategy.entry_grace_s must be in [0, {HARD_MAX_ENTRY_GRACE_S}]")
        if not (5 <= g.disaster_poll_s <= 300):
            errs.append("guardrails.disaster_poll_s must be in [5, 300]")
        if not (5 <= s.fill_timeout_s <= 120):
            errs.append("strategy.fill_timeout_s must be in [5, 120]")
        if not (0 <= s.entry_delay_s < s.entry_grace_s):
            errs.append("strategy.entry_delay_s must be >= 0 and < entry_grace_s")
        if not (1 <= s.roll_min_days <= 20):
            errs.append("strategy.roll_min_days must be in [1, 20]")
        if not (0 <= self.signal.max_source_gap_diff <= 0.01):
            errs.append("signal.max_source_gap_diff must be in [0, 0.01]")
        if not (0.02 <= g.margin_buffer_pct <= 0.9):
            errs.append("guardrails.margin_buffer_pct must be in [0.02, 0.9]")
        if g.min_net_liq < 2_000:
            errs.append("guardrails.min_net_liq must be >= 2000")
        if s.exit_lead_s > 60:
            errs.append("strategy.exit_lead_s must be <= 60")
        import re as _re
        if not _re.fullmatch(r"\d{4}-\d{2}", self.monitoring.start):
            errs.append("monitoring.start must be YYYY-MM")
        if g.core_policy not in ("warn", "block", "ignore"):
            errs.append("guardrails.core_policy must be warn|block|ignore")
        if not (0 <= g.disaster_stop_pct_equity < 1):
            errs.append("guardrails.disaster_stop_pct_equity must be in [0, 1)")
        if not set(self.signal.sources) <= {"ibkr", "yahoo"} or not self.signal.sources:
            errs.append("signal.sources must be a non-empty subset of [ibkr, yahoo]")
        if not (0 < g.max_total_leverage <= HARD_MAX_TOTAL_LEVERAGE):
            errs.append(f"guardrails.max_total_leverage must be in (0, {HARD_MAX_TOTAL_LEVERAGE}]")
        if not (0.002 <= s.vol_threshold <= 0.05):
            errs.append("strategy.vol_threshold must be a DAILY std in [0.002, 0.05]")
        if not (5 <= s.vol_window <= 60):
            errs.append("strategy.vol_window must be in [5, 60]")
        if not (0 < s.low_vol_scale <= 1):
            errs.append("strategy.low_vol_scale must be in (0, 1]")
        bd = self.bond
        if bd.instrument not in BOND_SPECS:
            errs.append(f"bond.instrument must be one of {sorted(BOND_SPECS)}")
        elif bd.hedge_ratio == 0:
            bd.hedge_ratio = BOND_SPECS[bd.instrument]["hedge"]
        if bd.instrument in BOND_SPECS and bd.max_contracts == 0:
            bd.max_contracts = BOND_SPECS[bd.instrument]["default_contracts"]
        if bd.sizing_rounding not in ("floor", "nearest"):
            errs.append("bond.sizing_rounding must be floor or nearest")
        if not (0 < bd.leverage * bd.hedge_ratio <= bd.max_leverage):
            errs.append(f"bond.leverage x hedge_ratio ({bd.leverage * bd.hedge_ratio:.2f}) must be in (0, bond.max_leverage]")
        if not (0.5 <= bd.hedge_ratio <= 2.0):
            errs.append("bond.hedge_ratio must be in [0.5, 2.0]")
        if bd.max_leverage > HARD_MAX_BOND_LEVERAGE:
            errs.append(f"bond.max_leverage exceeds hard limit {HARD_MAX_BOND_LEVERAGE}")
        cap = BOND_SPECS.get(bd.instrument, {}).get("max_contracts", HARD_MAX_BOND_CONTRACTS)
        if not (0 < bd.max_contracts <= cap):
            errs.append(f"bond.max_contracts must be in (0, {cap}] for {bd.instrument}")
        if not (1 <= bd.hold_sessions <= 5):
            errs.append("bond.hold_sessions must be in [1, 5]")
        for k in ("entry_lead_s", "exit_lead_s"):
            if not (30 <= getattr(bd, k) <= 900):
                errs.append(f"bond.{k} must be in [30, 900]")
        if not (0 <= bd.entry_grace_s <= bd.entry_lead_s - 20):
            errs.append("bond.entry_grace_s must be in [0, entry_lead_s - 20]")
        if not (5 <= bd.fill_timeout_s <= 120):
            errs.append("bond.fill_timeout_s must be in [5, 120]")
        if bd.order_type not in ("market", "limit", "auto"):
            errs.append("bond.order_type must be market, limit or auto")
        if not (1 <= bd.limit_ticks <= 20):
            errs.append("bond.limit_ticks must be in [1, 20]")
        if not (2 <= bd.roll_buffer_days <= 15):       # >15 can skip to the thin second-deferred contract
            errs.append("bond.roll_buffer_days must be in [2, 15]")
        if not (bd.exit_lead_s <= bd.half_day_lead_s <= 1800):
            errs.append("bond.half_day_lead_s must be in [exit_lead_s, 1800]")
        if not (0 <= bd.disaster_stop_pct_equity < 0.5):
            errs.append("bond.disaster_stop_pct_equity must be in [0, 0.5)")
        if not (5 <= bd.disaster_poll_s <= 300):
            errs.append("bond.disaster_poll_s must be in [5, 300]")
        if not (0 < bd.price_min < bd.price_max):
            errs.append("bond.price_min must be < price_max")
        if bd.sd_bp <= 0 or bd.edge_h1_bp <= 0:
            errs.append("bond.edge_h1_bp and sd_bp must be > 0")
        if errs:
            raise ConfigError("; ".join(errs))
        return self

    def live_confirmed(self) -> bool:
        """Live trading additionally requires MET_LIVE_CONFIRM=<account id> in the environment."""
        return self.mode == "live" and os.environ.get("MET_LIVE_CONFIRM", "") == self.ib.account


def _coerce(cur, v, name):
    if isinstance(cur, bool):
        if not isinstance(v, bool):
            raise ConfigError(f"{name} must be true/false, got {v!r}")
        return v
    if isinstance(cur, int):
        if isinstance(v, bool) or not (isinstance(v, int) or (isinstance(v, float) and v.is_integer())):
            raise ConfigError(f"{name} must be an integer, got {v!r}")
        return int(v)
    if isinstance(cur, float):
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise ConfigError(f"{name} must be a number, got {v!r}")
        return float(v)
    if isinstance(cur, list):
        if not isinstance(v, list):
            raise ConfigError(f"{name} must be a list")
        return v
    if isinstance(cur, str):
        return str(v)
    return v


def _merge(dc, data: dict, path=""):
    for k, v in (data or {}).items():
        names = {f.name: f for f in fields(dc)}
        if k not in names:
            raise ConfigError(f"unknown config key: {path}{k}")
        cur = getattr(dc, k)
        if is_dataclass(cur):
            if not isinstance(v, dict):
                raise ConfigError(f"{path}{k} must be a mapping")
            _merge(cur, v, f"{path}{k}.")
        else:
            setattr(dc, k, _coerce(cur, v, f"{path}{k}"))
    return dc


def load_config(path: str | os.PathLike | None, require_account: bool = True) -> Config:
    cfg = Config()
    base = Path(".")
    if path:
        with open(path) as f:
            _merge(cfg, yaml.safe_load(f) or {})
        base = Path(path).resolve().parent
    m = cfg.monitoring                     # relative dirs resolve against the config file's folder
    for k in ("state_dir", "report_dir", "log_dir"):
        v = Path(getattr(m, k)).expanduser()
        setattr(m, k, str(v if v.is_absolute() else (base / v).resolve()))
    if os.environ.get("MET_IB_HOST"):
        cfg.ib.host = os.environ["MET_IB_HOST"]
    if os.environ.get("MET_IB_PORT"):
        cfg.ib.port = int(os.environ["MET_IB_PORT"])
    env_acct = os.environ.get("MET_IB_ACCOUNT")
    if env_acct:
        cfg.ib.account = env_acct
    for d in (cfg.monitoring.state_dir, cfg.monitoring.report_dir, cfg.monitoring.log_dir):
        Path(d).mkdir(parents=True, exist_ok=True)
    return cfg.validate(require_account=require_account)
