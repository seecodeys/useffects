"""IBKR implementation of the Broker interface using ib_async 2.0.1 (TWS or IB Gateway with the API enabled).

IBKR-side requirements:
  * API enabled (Configure > API > Settings), 'Read-Only API' OFF, trusted IP 127.0.0.1
  * 'Bypass Order Precautions for API orders' ON (or raise the precautionary size/value limits), otherwise a
    multi-contract market order can be rejected (error 201/383) at 09:30:01
  * US futures trading permission; CME real-time L1 data (MES/ES); US equity data for SPY/TLT daily bars
  * Gateway auto-restart time OUTSIDE 09:00-16:30 ET (it is set in the machine's local timezone)
  * Preferably a dedicated second IBKR username for the API: logging into TWS/Mobile with the same username
    causes error 10197 (no market data) or logs the Gateway out
"""
from __future__ import annotations

import asyncio
import collections
import datetime as dt
import logging
import math
import time

import pandas as pd

from . import calendar as cal
from .broker import (AccountSnapshot, Broker, BrokerError, FutureContract, MarginCheck, OrderResult, Position)

log = logging.getLogger("met.ibkr")
PRICE_MAX_AGE_S = 30
CRITICAL_CODES = {1100, 10197, 354, 10089, 201, 326, 321}      # 10090/2110 are routine: logged only


def _f(x, default=float("nan")):
    try:
        v = float(x)
        return default if (math.isnan(v) or abs(v) > 1e300) else v
    except (TypeError, ValueError):
        return default


def pick_front(details, min_expiry: dt.date):
    """details: iterable of (expiry_str 'YYYYMMDD', obj). Return (expiry, obj) with the earliest expiry >= min_expiry."""
    cands = []
    for exp, obj in details:
        e = dt.datetime.strptime(exp[:8], "%Y%m%d").date()
        if e >= min_expiry:
            cands.append((e, obj))
    if not cands:
        raise BrokerError(f"no futures contract expiring on/after {min_expiry}")
    return min(cands, key=lambda x: x[0])


def fill_summary(trade) -> tuple[int, float]:
    """Authoritative fill quantity/price: executions first (orderStatus can lag), orderStatus as fallback."""
    fq = sum(_f(f.execution.shares, 0.0) for f in trade.fills)
    st_f = _f(trade.orderStatus.filled, 0.0)
    filled = int(round(max(st_f, fq)))
    if fq > 0:
        avg = sum(_f(f.execution.shares, 0.0) * _f(f.execution.price, 0.0) for f in trade.fills) / fq
    else:
        avg = _f(trade.orderStatus.avgFillPrice, 0.0)
    return filled, avg


class IBKRBroker(Broker):
    def __init__(self, cfg, client_id: int | None = None, on_critical=None):
        from ib_async import IB
        self.cfg = cfg
        self.client_id = client_id if client_id is not None else cfg.ib.client_id
        self.on_critical = on_critical          # callable(title, body) for IB errors that need a human
        self.ib = IB()
        self.ib.RequestTimeout = 20             # blocking requests raise instead of hanging forever
        self.ib.RaiseRequestErrors = True
        self.ib.errorEvent += self._on_error
        self.ib.disconnectedEvent += self._on_disconnect
        self._tickers = {}
        self._hist_cache = {}
        self._ib_down = False
        self._md_blocked_at = None
        self.fx_base_per_usd = 1.0
        self.last_errors = collections.deque(maxlen=100)

    # ---------------------------------------------------------------- events
    def _on_error(self, reqId, code, msg, contract=None, *args):
        self.last_errors.append((dt.datetime.now(cal.ET), reqId, code, msg))
        if code == 1100:
            self._ib_down = True
        elif code in (1101, 1102):
            self._ib_down = False
            if code == 1101:                     # data lost: resubscribe
                self._tickers = {}
        elif code == 10197:
            self._md_blocked_at = time.monotonic()
        if code in CRITICAL_CODES:
            log.error("IB error %s (req %s): %s", code, reqId, msg)
            if self.on_critical:
                try:
                    self.on_critical(f"IB error {code}", msg)
                except Exception:
                    pass

    def _on_disconnect(self):
        log.error("IB socket disconnected")
        self._tickers = {}

    def _call(self, what, fn, *a, **kw):
        try:
            return fn(*a, **kw)
        except (asyncio.TimeoutError, TimeoutError) as e:
            raise BrokerError(f"{what}: timed out") from e
        except ConnectionError as e:
            raise BrokerError(f"{what}: not connected") from e
        except Exception as e:  # ib_async RequestError and friends
            raise BrokerError(f"{what}: {e!r}") from e

    # ---------------------------------------------------------------- connection & time
    def socket_ok(self):
        return self.ib.isConnected()

    def connect(self):
        c = self.cfg.ib
        if self.ib.isConnected():
            return                       # never drop a live socket (e.g. during an IB 1100 outage)
        last = None
        for attempt in range(3):
            try:
                rt, self.ib.RequestTimeout = self.ib.RequestTimeout, 0    # connect has its own staged timeouts
                try:
                    self.ib.connect(c.host, c.port, clientId=self.client_id, timeout=c.connect_timeout_s,
                                    readonly=False, account=c.account, raiseSyncErrors=True)
                finally:
                    self.ib.RequestTimeout = rt
                break
            except Exception as e:
                last = e
                if any(code == 326 for _, _, code, _ in list(self.last_errors)[-5:]):
                    log.warning("clientId %s in use; retrying", self.client_id)
                time.sleep(3 * (attempt + 1))
        else:
            raise BrokerError(f"IB connect {c.host}:{c.port} (clientId {self.client_id}) failed: {last!r}")
        self.ib.reqMarketDataType(c.market_data_type)
        accts = self.ib.managedAccounts()
        if c.account not in accts:
            self.ib.disconnect()
            raise BrokerError(f"account {c.account} not in managed accounts {accts}")
        self._tickers, self._ib_down, self._md_blocked_at = {}, False, None
        try:
            srv = self.ib.reqCurrentTime()
            skew = abs((srv - dt.datetime.now(dt.timezone.utc)).total_seconds())
            log.info("connected to IB as %s, server time %s (skew %.1fs)", c.account, srv, skew)
            if skew > 5:
                log.error("local clock differs from IB server time by %.1fs: fix NTP", skew)
                if self.on_critical:
                    self.on_critical("clock skew", f"local clock is {skew:.1f}s off IB server time; entry/exit timing depends on it")
        except Exception:
            pass

    def is_connected(self):
        return self.ib.isConnected() and not self._ib_down

    def disconnect(self):
        self.ib.disconnect()

    def now(self):
        return dt.datetime.now(tz=cal.ET)

    def sleep(self, seconds):
        if self.ib.isConnected():
            self.ib.sleep(seconds)
        else:
            time.sleep(seconds)

    def _need(self):
        if not self.is_connected():
            raise BrokerError("not connected (or IB connectivity lost)")

    # ---------------------------------------------------------------- account (all values converted to USD)
    def account_snapshot(self):
        self._need()
        acct = self.cfg.ib.account
        rows = self._call("accountSummary", self.ib.accountSummary, acct)
        rows = [v for v in rows if v.account == acct]
        if not rows:
            raise BrokerError(f"no account summary rows for {acct}")
        vals = {(v.tag, v.currency): v.value for v in rows}
        base = next((cur for (tag, cur) in vals if tag == "NetLiquidation" and cur not in ("", "BASE")), None)
        if base is None:
            raise BrokerError("NetLiquidation unavailable")
        if base == "USD":
            fx = 1.0
        else:
            fx = _f(vals.get(("ExchangeRate", "USD")))
            if not fx > 0:
                fx = next((_f(v.value) for v in self.ib.accountValues(acct)
                           if v.tag == "ExchangeRate" and v.currency == "USD"), float("nan"))
            if not fx > 0:
                raise BrokerError(f"no USD exchange rate for base currency {base}")
        self.fx_base_per_usd = fx
        usd = lambda tag: _f(vals.get((tag, base))) / fx
        snap = AccountSnapshot(acct, usd("NetLiquidation"), usd("ExcessLiquidity"), usd("AvailableFunds"), "USD")
        if not snap.net_liquidation > 0:
            raise BrokerError("NetLiquidation unavailable")
        snap.base_currency = base
        return snap

    def positions(self):
        """Fresh snapshot from IB (not the possibly-stale cache)."""
        self._need()
        acct = self.cfg.ib.account
        pos = self._call("reqPositions", self.ib.reqPositions)
        port = {p.contract.conId: p for p in self.ib.portfolio(acct)}
        out = []
        for p in pos:
            if p.account != acct or p.position == 0:
                continue
            c = p.contract
            mv = _f(port[c.conId].marketValue, 0.0) if c.conId in port else 0.0   # contract currency (USD)
            out.append(Position(c.symbol, c.secType, float(p.position), c.localSymbol or "", mv))
        return out

    def front_future(self, symbol, exchange, min_expiry):
        self._need()
        from ib_async import Future
        det = self._call("reqContractDetails", self.ib.reqContractDetails, Future(symbol, exchange=exchange, currency="USD"))
        if not det:
            raise BrokerError(f"no contract details for {symbol}@{exchange}")
        exp, d = pick_front([(x.contract.lastTradeDateOrContractMonth, x) for x in det], min_expiry)
        c = d.contract
        return FutureContract(symbol, exp, float(c.multiplier), c.localSymbol, c.conId, raw=c)

    def contract_from(self, symbol, exchange, con_id, local_symbol, expiry, multiplier):
        from ib_async import Future
        raw = Future(conId=int(con_id), exchange=exchange)       # conId alone identifies it; extra fields risk 200/478
        return FutureContract(symbol, expiry, float(multiplier), local_symbol, int(con_id), raw=raw)

    def executions(self, ref_prefix):
        self._need()
        fills = self._call("reqExecutions", self.ib.reqExecutions)
        out = {}
        for f in fills:
            e = f.execution
            if (e.orderRef or "").startswith(ref_prefix) and e.acctNumber == self.cfg.ib.account:
                key = e.execId.rsplit(".", 1)[0]          # IB corrections replace the last segment
                out[key] = dict(exec_id=key, ref=e.orderRef, side="BUY" if e.side in ("BOT", "BUY") else "SELL",
                                qty=float(e.shares), price=float(e.price), time=str(f.time),
                                local_symbol=f.contract.localSymbol)
        return list(out.values())

    def cancel_working(self, ref_prefix="MET-"):
        n = 0
        for t in self.open_orders(ref_prefix):
            try:
                self.ib.cancelOrder(t.order); n += 1
            except Exception as e:
                log.error("cancel %s failed: %r", t.order.orderRef, e)
        if n:
            self.ib.sleep(1)
        return n

    def working_orders(self, ref_prefix="MET-"):
        if not self.ib.isConnected():
            return 0
        return len(self.open_orders(ref_prefix))

    def price(self, contract):
        self._need()
        if self._md_blocked_at is not None:
            if time.monotonic() - self._md_blocked_at < 60:
                raise BrokerError("market data blocked by a competing session (IB 10197)")
            self._md_blocked_at = None
            self._tickers.pop(contract.con_id, None)              # resubscribe after the block
        now = dt.datetime.now(dt.timezone.utc)
        t = self._tickers.get(contract.con_id)
        if t is not None and t.time is not None and (now - t.time).total_seconds() > PRICE_MAX_AGE_S:
            try:
                self.ib.cancelMktData(contract.raw)
            except Exception:
                pass
            t = None
        if t is None:
            t = self.ib.reqMktData(contract.raw, "", False, False)
            self._tickers[contract.con_id] = t
        if self.cfg.ib.market_data_type != 1 and not getattr(self, "_warned_delayed", False):
            log.warning("market_data_type=%s: prices may be delayed; disaster stop uses them", self.cfg.ib.market_data_type)
            self._warned_delayed = True
        for _ in range(40):
            fresh = t.time is not None and (dt.datetime.now(dt.timezone.utc) - t.time).total_seconds() < PRICE_MAX_AGE_S
            if fresh:
                px = t.marketPrice()
                if px == px and px > 0:
                    return float(px)
                bid, ask = _f(t.bid), _f(t.ask)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2
            self.ib.sleep(0.25)
        raise BrokerError(f"no fresh market price for {contract.local_symbol} (data subscription / connectivity?)")

    def what_if(self, contract, action, qty):
        self._need()
        from ib_async import MarketOrder, OrderState
        st = self._call("whatIfOrder", self.ib.whatIfOrder, contract.raw,
                        MarketOrder(action, qty, account=self.cfg.ib.account))
        if not isinstance(st, OrderState):
            raise BrokerError(f"what-if rejected or empty: {st!r}")
        vals = [_f(st.initMarginAfter), _f(st.equityWithLoanAfter), _f(st.maintMarginAfter)]
        if any(math.isnan(v) for v in vals):
            raise BrokerError(f"what-if margin unavailable ({st.warningText or 'no values'})")
        if st.warningText:
            log.warning("what-if warning: %s", st.warningText)
        fx = self.fx_base_per_usd or 1.0
        return MarginCheck(vals[0] / fx, vals[1] / fx, vals[2] / fx)

    # ---------------------------------------------------------------- orders
    def market_order(self, contract, action, qty, ref, timeout_s):
        """Never raises. Status: Filled | PartiallyFilled | Rejected | Timeout | Unknown.
        'Unknown' means the order may still be working at IB: the caller must reconcile with positions."""
        from ib_async import MarketOrder
        if not self.is_connected():
            return OrderResult("", "Rejected", qty, 0, 0.0, "not connected")
        o = MarketOrder(action, qty, account=self.cfg.ib.account, tif="DAY", orderRef=ref)
        try:
            trade = self.ib.placeOrder(contract.raw, o)
            end = time.monotonic() + timeout_s
            while not trade.isDone() and time.monotonic() < end:
                self.ib.sleep(0.2)
            timed_out = not trade.isDone()
            if timed_out:
                self.ib.cancelOrder(trade.order)
                end = time.monotonic() + 15
                while not trade.isDone() and time.monotonic() < end:
                    self.ib.sleep(0.2)
            elif trade.orderStatus.status == "Inactive":
                try:
                    self.ib.cancelOrder(trade.order)      # best effort; Inactive is already 'done' locally
                except Exception:
                    pass
        except Exception as e:
            log.error("order %s failed mid-flight: %r", ref, e)
            return OrderResult(str(o.orderId), "Unknown", qty, 0, 0.0, f"exception: {e!r}")
        filled, avg = fill_summary(trade)
        status = trade.orderStatus.status
        msg = "; ".join(e.message for e in trade.log if e.message)[-500:]
        if status not in ("Cancelled", "ApiCancelled", "Filled", "Inactive") and not trade.isDone():
            res_status = "Unknown"
        elif filled >= qty:
            res_status = "Filled"
        elif filled > 0:
            res_status = "PartiallyFilled" if not timed_out else "Timeout"
        else:
            res_status = "Timeout" if timed_out else "Rejected"
        fills = [dict(time=str(f.time), shares=f.execution.shares, price=f.execution.price) for f in trade.fills]
        return OrderResult(str(o.orderId), res_status, qty, filled, avg, msg, fills)

    def open_orders(self, ref_prefix="MET-"):
        """All working MET orders on the account, including those placed by another clientId (daemon vs CLI)."""
        try:
            trades = self._call("reqAllOpenOrders", self.ib.reqAllOpenOrders)
        except BrokerError:
            trades = self.ib.openTrades()
        return [t for t in trades if not t.isDone() and (t.order.orderRef or "").startswith(ref_prefix)]

    def daily_adjusted_closes(self, symbol, days=120):
        self._need()
        key = (symbol, days, self.now().strftime("%Y%m%d%H%M"))
        if key in self._hist_cache:                       # IB pacing: identical request within 15s is rejected
            return self._hist_cache[key]
        from ib_async import Stock
        c = Stock(symbol, "SMART", "USD")
        self._call("qualifyContracts", self.ib.qualifyContracts, c)
        bars = self._call("reqHistoricalData", lambda: self.ib.run(self.ib.reqHistoricalDataAsync(
            c, endDateTime="", durationStr=f"{max(days, 30)} D", barSizeSetting="1 day", whatToShow="ADJUSTED_LAST",
            useRTH=True, formatDate=1), timeout=60))
        if not bars:
            raise BrokerError(f"no historical bars for {symbol}")
        s = pd.Series({pd.Timestamp(b.date).date(): float(b.close) for b in bars}).sort_index()
        now = self.now()
        today = now.date()
        if today in s.index and cal.is_session(today):
            _, close = cal.session_open_close(today)
            if now < close + dt.timedelta(minutes=20):
                s = s.drop(index=today)
        self._hist_cache = {k: v for k, v in self._hist_cache.items() if k[2] == key[2]}
        self._hist_cache[key] = s
        return s
