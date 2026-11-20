"""IBKR adapter logic against fake ib_async objects (no network). Covers status mapping, fill accounting,
what-if NaN handling, base-currency conversion, account filtering and stale market data."""
import datetime as dt
from types import SimpleNamespace as NS

import pytest

from met.broker import BrokerError, FutureContract
from met.config import Config
from met.ibkr import IBKRBroker, fill_summary, pick_front


class Evt:
    def __iadd__(self, f): return self


class FakeTrade:
    def __init__(self, status="Filled", filled=0, avg=0.0, fills=(), done=True, cancel_to=None):
        self.order = NS(orderId=1)
        self.orderStatus = NS(status=status, filled=filled, avgFillPrice=avg)
        self.fills = [NS(execution=NS(shares=q, price=p), time="t") for q, p in fills]
        self.log = [NS(message="")]
        self._done = done
        self.cancel_to = cancel_to

    def isDone(self): return self._done


class FakeIB:
    def __init__(self):
        self.errorEvent = Evt(); self.disconnectedEvent = Evt()
        self.RequestTimeout = 0; self.RaiseRequestErrors = False
        self.connected = True
        self.summary = []; self.values = []; self.pos = []; self.port = []
        self.trade = None; self.cancelled = 0; self.whatif = None; self.ticker = None; self.execs = []

    def isConnected(self): return self.connected
    def sleep(self, s): pass
    def accountSummary(self, acct): return self.summary
    def accountValues(self, acct): return self.values
    def reqPositions(self): return self.pos
    def portfolio(self, acct): return self.port
    def placeOrder(self, c, o): return self.trade
    def cancelOrder(self, o):
        self.cancelled += 1
        t = self.trade
        if t.cancel_to:
            t.orderStatus.status, t._done = t.cancel_to, True
    def whatIfOrder(self, c, o): return self.whatif
    def reqMktData(self, *a): return self.ticker
    def cancelMktData(self, c): pass
    def openTrades(self): return []
    def reqAllOpenOrders(self): return []
    def reqExecutions(self): return self.execs


@pytest.fixture
def br(monkeypatch):
    cfg = Config(); cfg.ib.account = "DU123"
    b = IBKRBroker.__new__(IBKRBroker)
    b.cfg, b.client_id, b.on_critical = cfg, 17, None
    b.ib = FakeIB()
    b._tickers, b._hist_cache, b._ib_down, b._md_blocked_at = {}, {}, False, None
    b.fx_base_per_usd = 1.0
    import collections; b.last_errors = collections.deque(maxlen=10)
    return b


C = FutureContract("MES", dt.date(2026, 12, 18), 5.0, "MESZ6", 42, raw=object())


def row(tag, val, cur, acct="DU123"):
    return NS(tag=tag, value=str(val), currency=cur, account=acct)


def test_pick_front():
    det = [("20261218", "Z6"), ("20270319", "H7"), ("20260918", "U6")]
    assert pick_front(det, dt.date(2026, 11, 3))[1] == "Z6"
    assert pick_front(det, dt.date(2026, 12, 19))[1] == "H7"
    with pytest.raises(BrokerError):
        pick_front(det, dt.date(2028, 1, 1))


def test_fill_summary_prefers_executions():
    t = FakeTrade(status="Filled", filled=3, avg=0.0, fills=[(3, 7000.0), (4, 7001.0)])
    q, px = fill_summary(t)
    assert q == 7 and px == pytest.approx((3 * 7000 + 4 * 7001) / 7)


@pytest.mark.parametrize("trade,expect_status,expect_filled,cancels", [
    (FakeTrade("Filled", 7, 7000.0, [(7, 7000.0)]), "Filled", 7, 0),
    (FakeTrade("Cancelled", 0, 0.0, [], done=True), "Rejected", 0, 0),
    (FakeTrade("Inactive", 0, 0.0, [], done=True), "Rejected", 0, 1),
    (FakeTrade("Submitted", 0, 0.0, [], done=False, cancel_to="Cancelled"), "Timeout", 0, 1),
    (FakeTrade("Submitted", 3, 7000.0, [(3, 7000.0)], done=False, cancel_to="Cancelled"), "Timeout", 3, 1),
    (FakeTrade("Submitted", 0, 0.0, [], done=False, cancel_to=None), "Unknown", 0, 1),   # cancel never confirmed
])
def test_market_order_status_mapping(br, trade, expect_status, expect_filled, cancels, monkeypatch):
    import met.ibkr as m
    t0 = [0.0]
    monkeypatch.setattr(m.time, "monotonic", lambda: (t0.__setitem__(0, t0[0] + 1.0) or t0[0]))
    br.ib.trade = trade
    r = br.market_order(C, "BUY", 7, "MET-x", timeout_s=3)
    assert r.status == expect_status and r.filled == expect_filled and br.ib.cancelled == cancels


def test_market_order_never_raises(br):
    def boom(*a): raise ConnectionError("Not connected")
    br.ib.placeOrder = boom
    r = br.market_order(C, "BUY", 1, "MET-x", 3)
    assert r.status == "Unknown"


def test_what_if_nan_raises(br):
    from ib_async import OrderState
    st = OrderState(); st.initMarginAfter = "1.7976931348623157E308"; st.equityWithLoanAfter = ""; st.maintMarginAfter = ""
    br.ib.whatif = st
    with pytest.raises(BrokerError):
        br.what_if(C, "BUY", 1)
    br.ib.whatif = []
    with pytest.raises(BrokerError):
        br.what_if(C, "BUY", 1)


def test_what_if_converted_to_usd(br):
    from ib_async import OrderState
    st = OrderState(); st.initMarginAfter = "13500"; st.equityWithLoanAfter = "135000"; st.maintMarginAfter = "12000"
    br.ib.whatif = st; br.fx_base_per_usd = 1.35
    m = br.what_if(C, "BUY", 1)
    assert m.init_margin_after == pytest.approx(10_000) and m.equity_with_loan_after == pytest.approx(100_000)


def test_account_snapshot_sgd_base(br):
    br.ib.summary = [row("NetLiquidation", 135000, "SGD"), row("ExcessLiquidity", 81000, "SGD"),
                     row("AvailableFunds", 81000, "SGD"), row("ExchangeRate", 1.35, "USD"),
                     row("NetLiquidation", 999999, "SGD", acct="U_OTHER")]
    s = br.account_snapshot()
    assert s.currency == "USD" and s.net_liquidation == pytest.approx(100_000) and s.excess_liquidity == pytest.approx(60_000)
    assert s.account == "DU123"


def test_account_snapshot_missing_fx_raises(br):
    br.ib.summary = [row("NetLiquidation", 135000, "SGD")]
    with pytest.raises(BrokerError):
        br.account_snapshot()


def test_positions_filter_account_and_zero(br):
    mk = lambda acct, sym, q, local, con: NS(account=acct, position=q,
                                            contract=NS(symbol=sym, secType="FUT", localSymbol=local, conId=con))
    br.ib.pos = [mk("DU123", "MES", 3, "MESZ6", 42), mk("DU999", "MES", 5, "MESZ6", 42), mk("DU123", "MES", 0, "MESH7", 43)]
    ps = br.positions()
    assert [(p.local_symbol, p.quantity) for p in ps] == [("MESZ6", 3.0)]


def test_stale_ticker_rejected(br):
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)
    br.ib.ticker = NS(time=old, bid=7000.0, ask=7000.5, close=6990.0, marketPrice=lambda: 7000.25)
    with pytest.raises(BrokerError):
        br.price(C)
    br._tickers = {}
    br.ib.ticker = NS(time=dt.datetime.now(dt.timezone.utc), bid=7000.0, ask=7000.5, close=6990.0, marketPrice=lambda: 7000.25)
    assert br.price(C) == 7000.25


def test_competing_session_blocks_price(br):
    br._on_error(-1, 10197, "No market data during competing live session")
    with pytest.raises(BrokerError):
        br.price(C)


def test_connectivity_lost_means_not_connected(br):
    br._on_error(-1, 1100, "Connectivity between IB and TWS has been lost")
    assert not br.is_connected()
    br._on_error(-1, 1102, "restored, data maintained")
    assert br.is_connected()


def test_competing_session_block_expires(br, monkeypatch):
    import met.ibkr as m
    t = [1000.0]
    monkeypatch.setattr(m.time, "monotonic", lambda: t[0])
    br._on_error(-1, 10197, "competing session")
    with pytest.raises(BrokerError):
        br.price(C)
    t[0] += 61
    br.ib.ticker = NS(time=dt.datetime.now(dt.timezone.utc), bid=7000.0, ask=7000.5, close=6990.0, marketPrice=lambda: 7000.25)
    assert br.price(C) == 7000.25


def test_connect_keeps_live_socket(br):
    br.ib.connect = lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not reconnect"))
    br.connect()     # socket is up -> no-op


def test_executions_filter_and_corrections(br):
    mk = lambda eid, ref, side, q, px, acct="DU123": NS(execution=NS(execId=eid, orderRef=ref, side=side, shares=q, price=px,
                                                                    acctNumber=acct), time="t", contract=NS(localSymbol="MESZ6"))
    br.ib.execs = [mk("0001.aa.01.01", "MET-2026-11-30-entry", "BOT", 4, 7000.0),
                   mk("0001.aa.01.02", "MET-2026-11-30-entry", "BOT", 4, 7000.25),    # IB correction of the same fill
                   mk("0002.aa.01.01", "MET-2026-11-30-entry", "BOT", 3, 7001.0),
                   mk("0003.aa.01.01", "user", "BOT", 2, 7002.0),
                   mk("0004.aa.01.01", "MET-2026-11-30-entry", "BOT", 9, 7000.0, acct="DU999")]
    ex = br.executions("MET-2026-11-30-")
    assert sorted((e["qty"], e["side"]) for e in ex) == [(3.0, "BUY"), (4.0, "BUY")]
    assert next(e for e in ex if e["qty"] == 4.0)["price"] == 7000.25
