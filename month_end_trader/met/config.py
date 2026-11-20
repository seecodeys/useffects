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


@dataclass
class SignalConfig:
    equity: str = "SPY"
    bond: str = "TLT"
    sources: list = field(default_factory=lambda: ["ibkr", "yahoo"])
    require_agreement: bool = True   # if both sources available and the sign differs -> skip
    max_source_gap_diff: float = 0.0015  # sources' gaps must agree within 15bp, else treat as a data error


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
    max_data_age_sessions: int = 0   # signal data must end exactly at the prior session


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
        if g.core_policy not in ("warn", "block", "ignore"):
            errs.append("guardrails.core_policy must be warn|block|ignore")
        if not (0 <= g.disaster_stop_pct_equity < 1):
            errs.append("guardrails.disaster_stop_pct_equity must be in [0, 1)")
        if not set(self.signal.sources) <= {"ibkr", "yahoo"} or not self.signal.sources:
            errs.append("signal.sources must be a non-empty subset of [ibkr, yahoo]")
        if errs:
            raise ConfigError("; ".join(errs))
        return self

    def live_confirmed(self) -> bool:
        """Live trading additionally requires MET_LIVE_CONFIRM=<account id> in the environment."""
        return self.mode == "live" and os.environ.get("MET_LIVE_CONFIRM", "") == self.ib.account


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
            setattr(dc, k, type(cur)(v) if cur is not None and not isinstance(cur, list) else v)
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
        v = Path(getattr(m, k))
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
