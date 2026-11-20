"""Broker abstraction. IBKRBroker talks to TWS / IB Gateway via ib_async; FakeBroker (fake_broker.py) is used
for tests and dry runs. The executor only uses this interface."""
from __future__ import annotations

import abc
import datetime as dt
from dataclasses import dataclass, field


@dataclass
class FutureContract:
    symbol: str
    expiry: dt.date
    multiplier: float
    local_symbol: str = ""
    con_id: int = 0
    raw: object = None          # broker-native contract


@dataclass
class AccountSnapshot:
    account: str
    net_liquidation: float
    excess_liquidity: float
    available_funds: float
    currency: str = "USD"


@dataclass
class Position:
    symbol: str
    sec_type: str               # STK / FUT
    quantity: float             # signed
    local_symbol: str = ""
    market_value: float = 0.0


@dataclass
class MarginCheck:
    init_margin_after: float
    equity_with_loan_after: float
    maint_margin_after: float

    @property
    def excess_after(self) -> float:
        return self.equity_with_loan_after - self.init_margin_after


@dataclass
class OrderResult:
    order_id: str
    status: str                 # Filled | PartiallyFilled | Cancelled | Rejected | Inactive | Timeout
    requested: int
    filled: int
    avg_price: float
    message: str = ""
    fills: list = field(default_factory=list)


class BrokerError(RuntimeError):
    pass


class Broker(abc.ABC):
    @abc.abstractmethod
    def connect(self) -> None: ...

    @abc.abstractmethod
    def is_connected(self) -> bool: ...

    @abc.abstractmethod
    def disconnect(self) -> None: ...

    @abc.abstractmethod
    def now(self) -> dt.datetime:
        """Current time, timezone-aware (America/New_York)."""

    @abc.abstractmethod
    def sleep(self, seconds: float) -> None:
        """Sleep while keeping the connection serviced."""

    @abc.abstractmethod
    def account_snapshot(self) -> AccountSnapshot: ...

    @abc.abstractmethod
    def positions(self) -> list[Position]: ...

    @abc.abstractmethod
    def front_future(self, symbol: str, exchange: str, min_expiry: dt.date) -> FutureContract: ...

    @abc.abstractmethod
    def price(self, contract: FutureContract) -> float:
        """Latest trade/mid price; raise BrokerError if unavailable."""

    @abc.abstractmethod
    def what_if(self, contract: FutureContract, action: str, qty: int) -> MarginCheck: ...

    @abc.abstractmethod
    def market_order(self, contract: FutureContract, action: str, qty: int, ref: str,
                     timeout_s: float) -> OrderResult:
        """Submit a DAY market order and wait up to timeout_s for a terminal state. Never raises for a
        broker-side rejection: returns status Rejected instead."""

    @abc.abstractmethod
    def daily_adjusted_closes(self, symbol: str, days: int = 120):
        """pd.Series of dividend-adjusted daily closes indexed by date (completed sessions only)."""

    @abc.abstractmethod
    def contract_from(self, symbol: str, exchange: str, con_id: int, local_symbol: str, expiry: dt.date,
                      multiplier: float) -> FutureContract:
        """Rebuild a tradable contract from persisted fields without a lookup (exit path must not depend on one)."""

    @abc.abstractmethod
    def working_orders(self, ref_prefix: str = "MET-") -> int:
        """Number of this bot's orders still working at the broker."""

    @abc.abstractmethod
    def executions(self, ref_prefix: str) -> list[dict]:
        """This account's fills whose orderRef starts with ref_prefix (today's session at least):
        dicts with exec_id (stable across IB corrections), ref, side ('BUY'/'SELL'), qty, price, time, local_symbol.
        Raises BrokerError if they cannot be retrieved."""

    def manual_working(self, ref: str) -> int:
        """Number of the user's working orders with exactly this orderRef (e.g. MET-<date>-manual)."""
        return 0

    def cancel_working(self, ref_prefix: str = "MET-") -> int:
        """Cancel this bot's working orders; returns how many cancels were sent."""
        return 0

    def future_position(self, contract: FutureContract) -> float:
        tot = 0.0
        for p in self.positions():
            if p.sec_type == "FUT" and (p.local_symbol == contract.local_symbol or
                                         (not p.local_symbol and p.symbol == contract.symbol)):
                tot += p.quantity
        return tot

    def all_future_positions(self, symbol: str) -> list[Position]:
        return [p for p in self.positions() if p.sec_type == "FUT" and p.symbol == symbol and p.quantity != 0]
