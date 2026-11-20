"""In-memory broker + clock for tests and dry runs. Prices follow a path from open to close for each day
(configurable), fills happen at the current path price plus slippage, and failure modes can be injected."""
from __future__ import annotations

import datetime as dt
import itertools

import pandas as pd

from . import calendar as cal
from .broker import (AccountSnapshot, Broker, BrokerError, FutureContract, MarginCheck, OrderResult, Position)

ET = cal.ET


def _third_friday(y, m):
    d = dt.date(y, m, 15)
    return d + dt.timedelta(days=(4 - d.weekday()) % 7)


class FakeClock:
    def __init__(self, start: dt.datetime):
        self.t = start if start.tzinfo else start.replace(tzinfo=ET)

    def now(self):
        return self.t

    def advance(self, seconds: float):
        self.t += dt.timedelta(seconds=seconds)


class FakeBroker(Broker):
    def __init__(self, clock: FakeClock, account="DU1234567", cash=20_000.0, core_shares=110, spy_price=733.0,
                 fut_price=7340.0, multiplier=5.0, closes: dict | None = None, raw_closes: dict | None = None):
        self.clock = clock
        self.account = account
        self.cash = cash
        self.core_shares = core_shares
        self.spy_price = spy_price
        self.multiplier = multiplier
        self.base_fut = fut_price
        self.fut_pos = 0
        self.fut_cost = 0.0               # signed cost basis (qty * price)
        self.realised = 0.0
        self.connected = False
        self.orders = []
        self.closes = closes or {}        # symbol -> pd.Series adjusted closes (adjusted as of the data end)
        self.raw_closes = raw_closes or {}  # symbol -> pd.Series unadjusted closes, to re-base like live data
        # behaviour knobs
        self.day_path = {}                # date -> (open_px, close_px, shock: list[(seconds_after_open, px)])
        self.slippage_ticks = 1
        self.reject_next = 0
        self.partial_next = None          # fill only this many on the next order
        self.connect_fail = 0             # fail the next N connects
        self.disconnect_at = None         # datetime at which connection drops
        self.price_unavailable = False
        self.whatif_excess = None         # override post-trade excess liquidity
        self.late_fill_next = 0           # next N orders report Timeout/0 filled but DO fill (IB cancel race)
        self.pos_glitch_reads = 0         # next N positions() calls omit the futures position (stale cache)
        self.account_reported = None      # account id the broker reports (default: self.account)
        self.stuck_working = 0            # working_orders() result (orders stuck at the broker)
        self.cancel_clears_stuck = True
        self.stale_after_fill = 0         # after the next fill, N positions() reads still show the pre-fill qty
        self._stale_left, self._stale_pos = 0, 0
        self.expiries = [_third_friday(y, m) for y in range(1993, 2041) for m in (3, 6, 9, 12)]
        self.commission = 0.62
        self._ids = itertools.count(1)

    # ---------------------------------------------------------------- connection & time
    def connect(self):
        if self.connect_fail > 0:
            self.connect_fail -= 1
            raise BrokerError("connect refused (simulated)")
        self.connected = True

    def is_connected(self):
        if self.disconnect_at and self.clock.now() >= self.disconnect_at:
            self.connected = False; self.disconnect_at = None
        return self.connected

    def disconnect(self):
        self.connected = False

    def now(self):
        return self.clock.now()

    def sleep(self, seconds):
        self.clock.advance(seconds)

    def _need(self):
        if not self.is_connected():
            raise BrokerError("not connected")

    # ---------------------------------------------------------------- prices
    def fut_price_now(self) -> float:
        now = self.clock.now()
        d = now.date()
        if d not in self.day_path:
            return self.base_fut
        o_px, c_px, shocks = self.day_path[d]
        o, c = cal.session_open_close(d)
        if now <= o:
            return o_px
        if now >= c:
            return c_px
        frac = (now - o).total_seconds() / (c - o).total_seconds()
        px = o_px + frac * (c_px - o_px)
        for secs, spx in sorted(shocks or []):
            if (now - o).total_seconds() >= secs:
                px = spx
        return px

    def price(self, contract):
        self._need()
        if self.price_unavailable:
            raise BrokerError("no market data (simulated)")
        return self.fut_price_now()

    # ---------------------------------------------------------------- account
    def unrealised(self):
        return self.fut_pos * self.fut_price_now() * self.multiplier - self.fut_cost * self.multiplier

    def account_snapshot(self):
        self._need()
        nl = self.cash + self.core_shares * self.spy_price + self.unrealised()
        return AccountSnapshot(self.account_reported or self.account, nl, nl * 0.6, nl * 0.6)

    def positions(self):
        self._need()
        out = []
        glitch = self.pos_glitch_reads > 0
        if glitch:
            self.pos_glitch_reads -= 1
        if self.core_shares:
            out.append(Position("SPY", "STK", self.core_shares, "SPY", self.core_shares * self.spy_price))
        shown = self.fut_pos
        if self._stale_left > 0:
            self._stale_left -= 1; shown = self._stale_pos
        if shown and not glitch:
            out.append(Position("MES", "FUT", shown, self._local(self._front(self.clock.now().date())),
                                shown * self.fut_price_now() * self.multiplier))
        return out

    def _front(self, min_expiry):
        return next(e for e in self.expiries if e >= min_expiry)

    @staticmethod
    def _local(expiry):
        return "MES" + "FGHJKMNQUVXZ"[expiry.month - 1] + str(expiry.year)[-1]

    def front_future(self, symbol, exchange, min_expiry):
        self._need()
        e = self._front(min_expiry)
        return FutureContract(symbol, e, self.multiplier, self._local(e), con_id=1000 + e.month)

    def contract_from(self, symbol, exchange, con_id, local_symbol, expiry, multiplier):
        return FutureContract(symbol, expiry, multiplier, local_symbol, con_id)

    def working_orders(self, ref_prefix="MET-"):
        return self.stuck_working

    def cancel_working(self, ref_prefix="MET-"):
        n = self.stuck_working
        if self.cancel_clears_stuck:
            self.stuck_working = 0
        return n

    def what_if(self, contract, action, qty):
        self._need()
        nl = self.account_snapshot().net_liquidation
        init = qty * self.fut_price_now() * self.multiplier * 0.079   # ~$2,900 per MES at 7,340
        if self.whatif_excess is not None:
            return MarginCheck(nl - self.whatif_excess, nl, init * 0.9)
        return MarginCheck(init, nl, init * 0.9)

    # ---------------------------------------------------------------- orders
    def market_order(self, contract, action, qty, ref, timeout_s):
        oid = str(next(self._ids))
        if not self.is_connected():
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="NotConnected"))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "not connected")
        from .executor import futures_open
        if not futures_open(self.clock.now()):
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="Rejected", t=self.clock.now()))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "market closed (simulated)")
        if self.reject_next > 0:
            self.reject_next -= 1
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="Rejected", t=self.clock.now()))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "simulated rejection")
        fill = qty if self.partial_next is None else min(self.partial_next, qty)
        self.partial_next = None
        sgn = 1 if action == "BUY" else -1
        if self.stale_after_fill:
            self._stale_pos, self._stale_left, self.stale_after_fill = self.fut_pos, self.stale_after_fill, 0
        px = self.fut_price_now() + sgn * 0.25 * self.slippage_ticks
        self.fut_pos += sgn * fill
        self.fut_cost += sgn * fill * px
        self.cash -= self.commission * fill
        if self.fut_pos == 0:
            self.cash -= self.fut_cost * self.multiplier   # realise
            self.fut_cost = 0.0
        self.clock.advance(0.3)
        status = "Filled" if fill == qty else "PartiallyFilled"
        if self.late_fill_next > 0:
            self.late_fill_next -= 1
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, filled=fill, price=px, status="LateFill",
                                    t=self.clock.now()))
            return OrderResult(oid, "Timeout", qty, 0, 0.0, "simulated: cancel raced a fill")
        self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, filled=fill, price=px, status=status,
                                t=self.clock.now()))
        return OrderResult(oid, status, qty, fill, px)

    def daily_adjusted_closes(self, symbol, days=120):
        self._need()
        s = self.closes[symbol]
        today = self.clock.now().date()
        _, c = cal.session_open_close(today) if cal.is_session(today) else (None, None)
        if c is not None and self.clock.now() < c + dt.timedelta(minutes=20):
            s = s[[pd.Timestamp(i).date() < today for i in s.index]]
        else:
            s = s[[pd.Timestamp(i).date() <= today for i in s.index]]
        raw = self.raw_closes.get(symbol)
        if raw is not None and len(s) and s.index[-1] in raw.index:
            s = s * (float(raw.loc[s.index[-1]]) / float(s.iloc[-1]))   # live adjusted series end at the raw price
        return s
