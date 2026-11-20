"""IBKR implementation of the Broker interface using ib_async (TWS or IB Gateway with the API enabled).

Requirements on the IBKR side:
  * API enabled (Configure > API > Settings), 'Read-Only API' OFF, trusted IP 127.0.0.1
  * Futures trading permission (US futures) on the account
  * CME real-time market data (L1) for MES/ES; the disaster stop needs live prices
"""
from __future__ import annotations

import datetime as dt
import logging
import math

import pandas as pd

from . import calendar as cal
from .broker import (AccountSnapshot, Broker, BrokerError, FutureContract, MarginCheck, OrderResult, Position)

log = logging.getLogger("met.ibkr")


def _f(x, default=float("nan")):
    try:
        v = float(x)
        return default if (math.isnan(v) or abs(v) > 1e300) else v
    except (TypeError, ValueError):
        return default


def pick_front(details, min_expiry: dt.date):
    """details: iterable of (expiry_str 'YYYYMMDD', obj). Return obj with the earliest expiry >= min_expiry."""
    cands = []
    for exp, obj in details:
        e = dt.datetime.strptime(exp[:8], "%Y%m%d").date()
        if e >= min_expiry:
            cands.append((e, obj))
    if not cands:
        raise BrokerError(f"no futures contract expiring on/after {min_expiry}")
    return min(cands, key=lambda x: x[0])


class IBKRBroker(Broker):
    def __init__(self, cfg):
        from ib_async import IB
        self.cfg = cfg
        self.ib = IB()
        self._tickers = {}

    # ---------------------------------------------------------------- connection & time
    def connect(self):
        c = self.cfg.ib
        try:
            self.ib.connect(c.host, c.port, clientId=c.client_id, timeout=c.connect_timeout_s,
                            readonly=False, account=c.account)
            self.ib.reqMarketDataType(c.market_data_type)
        except Exception as e:
            raise BrokerError(f"IB connect {c.host}:{c.port} failed: {e!r}") from e
        accts = self.ib.managedAccounts()
        if c.account not in accts:
            self.ib.disconnect()
            raise BrokerError(f"account {c.account} not in managed accounts {accts}")
        self._tickers = {}

    def is_connected(self):
        return self.ib.isConnected()

    def disconnect(self):
        self.ib.disconnect()

    def now(self):
        return dt.datetime.now(tz=cal.ET)

    def sleep(self, seconds):
        if self.ib.isConnected():
            self.ib.sleep(seconds)
        else:
            import time; time.sleep(seconds)

    def _need(self):
        if not self.ib.isConnected():
            raise BrokerError("not connected")

    # ---------------------------------------------------------------- account
    def account_snapshot(self):
        self._need()
        vals = {(v.tag, v.currency): v.value for v in self.ib.accountSummary(self.cfg.ib.account)}
        base = next((cur for (tag, cur) in vals if tag == "NetLiquidation"), "USD")
        get = lambda tag: _f(vals.get((tag, base)))
        snap = AccountSnapshot(self.cfg.ib.account, get("NetLiquidation"), get("ExcessLiquidity"),
                               get("AvailableFunds"), base)
        if math.isnan(snap.net_liquidation):
            raise BrokerError("NetLiquidation unavailable")
        return snap

    def positions(self):
        self._need()
        out = []
        port = {p.contract.conId: p for p in self.ib.portfolio(self.cfg.ib.account)}
        for p in self.ib.positions(self.cfg.ib.account):
            c = p.contract
            mv = _f(port[c.conId].marketValue, 0.0) if c.conId in port else 0.0
            out.append(Position(c.symbol, c.secType, float(p.position), c.localSymbol or "", mv))
        return out

    def front_future(self, symbol, exchange, min_expiry):
        self._need()
        from ib_async import Future
        det = self.ib.reqContractDetails(Future(symbol, exchange=exchange, currency="USD"))
        if not det:
            raise BrokerError(f"no contract details for {symbol}@{exchange}")
        _, d = pick_front([(x.contract.lastTradeDateOrContractMonth, x) for x in det], min_expiry)
        c = d.contract
        exp = dt.datetime.strptime(c.lastTradeDateOrContractMonth[:8], "%Y%m%d").date()
        return FutureContract(symbol, exp, float(c.multiplier), c.localSymbol, c.conId, raw=c)

    def price(self, contract):
        self._need()
        t = self._tickers.get(contract.con_id)
        if t is None:
            t = self.ib.reqMktData(contract.raw, "", False, False)
            self._tickers[contract.con_id] = t
        for _ in range(20):
            px = t.marketPrice()
            if px == px and px > 0:   # not NaN
                return float(px)
            mid = (_f(t.bid) + _f(t.ask)) / 2
            if mid == mid and mid > 0:
                return float(mid)
            self.ib.sleep(0.25)
        last_close = _f(t.close)
        if last_close == last_close and last_close > 0 and self.cfg.ib.market_data_type != 1:
            return last_close
        raise BrokerError(f"no market price for {contract.local_symbol} (market data subscription?)")

    def what_if(self, contract, action, qty):
        self._need()
        from ib_async import MarketOrder
        st = self.ib.whatIfOrder(contract.raw, MarketOrder(action, qty, account=self.cfg.ib.account))
        if not st:
            raise BrokerError("what-if returned nothing")
        return MarginCheck(_f(st.initMarginAfter), _f(st.equityWithLoanAfter), _f(st.maintMarginAfter))

    # ---------------------------------------------------------------- orders
    def market_order(self, contract, action, qty, ref, timeout_s):
        from ib_async import MarketOrder
        if not self.ib.isConnected():
            return OrderResult("", "Rejected", qty, 0, 0.0, "not connected")
        o = MarketOrder(action, qty, account=self.cfg.ib.account, tif="DAY", orderRef=ref, outsideRth=True)
        trade = self.ib.placeOrder(contract.raw, o)
        waited = 0.0
        while not trade.isDone() and waited < timeout_s:
            self.ib.sleep(0.25); waited += 0.25
        s = trade.orderStatus
        filled = int(round(_f(s.filled, 0.0)))
        avg = _f(s.avgFillPrice, 0.0)
        msg = "; ".join(e.message for e in trade.log if e.message)[-500:]
        status = s.status
        if not trade.isDone():
            self.ib.cancelOrder(o); self.ib.sleep(1.0)
            filled = int(round(_f(trade.orderStatus.filled, 0.0))); avg = _f(trade.orderStatus.avgFillPrice, 0.0)
            status = "Timeout"
        elif status in ("Cancelled", "ApiCancelled", "Inactive") and filled == 0:
            status = "Rejected"
        fills = [dict(time=str(f.time), shares=f.execution.shares, price=f.execution.price) for f in trade.fills]
        return OrderResult(str(o.orderId), status, qty, filled, avg, msg, fills)

    def daily_adjusted_closes(self, symbol, days=120):
        self._need()
        from ib_async import Stock
        c = Stock(symbol, "SMART", "USD")
        self.ib.qualifyContracts(c)
        bars = self.ib.reqHistoricalData(c, endDateTime="", durationStr=f"{max(days, 30)} D", barSizeSetting="1 day",
                                         whatToShow="ADJUSTED_LAST", useRTH=True, formatDate=1)
        if not bars:
            raise BrokerError(f"no historical bars for {symbol}")
        s = pd.Series({pd.Timestamp(b.date).date(): float(b.close) for b in bars}).sort_index()
        now = self.now()
        today = now.date()
        if today in s.index and cal.is_session(today):
            _, close = cal.session_open_close(today)
            if now < close + dt.timedelta(minutes=20):
                s = s.drop(index=today)
        return s
