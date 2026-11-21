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


def _zn_last_trade(y, m):
    """ZN last trading day: 7th business day before the last business day of the contract month (approx.)."""
    d = (pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0)).date()
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    n = 0
    while n < 7:
        d -= dt.timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return d


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
        self.manual_open = 0              # user's working MET-<date>-manual orders
        self.cancel_clears_stuck = True
        self.stale_after_fill = 0         # after the next fill, N positions() reads still show the pre-fill qty
        self._stale_left, self._stale_pos = 0, 0
        self.execs = []                   # list of execution dicts
        self.exec_hide_reads = 0          # next N executions() calls miss the newest fill
        self.fill_delay_next = None       # (seconds, visible_working): next order reports Timeout/0 and fills later
        self.executions_fail = 0          # next N executions() calls raise
        self._pending = []                # (fill_time, action, qty, ref, visible)
        self.expiries = [_third_friday(y, m) for y in range(1993, 2041) for m in (3, 6, 9, 12)]
        self.commission = 0.62
        self._ids = itertools.count(1)
        # second instrument: 10y T-note futures (bond leg). Price path: zn_price_fn(now) -> price
        self.zn_pos = 0
        self.zn_cost = 0.0
        self.zn_multiplier = 1000.0
        self.zn_price_fn = lambda now: 110.0
        self.zn_tick = 1 / 64
        self.zn_commission = 1.70
        self.zn_local = ""
        self.zn_expiries = [_zn_last_trade(y, m) for y in range(1993, 2041) for m in (3, 6, 9, 12)]

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

    def zn_price_now(self) -> float:
        return float(self.zn_price_fn(self.clock.now()))

    def price(self, contract):
        self._need()
        if self.price_unavailable:
            raise BrokerError("no market data (simulated)")
        return self.zn_price_now() if contract.symbol == "ZN" else self.fut_price_now()

    # ---------------------------------------------------------------- account
    def unrealised(self):
        return (self.fut_pos * self.fut_price_now() * self.multiplier - self.fut_cost * self.multiplier +
                self.zn_pos * self.zn_price_now() * self.zn_multiplier - self.zn_cost * self.zn_multiplier)

    def account_snapshot(self):
        self._need()
        nl = self.cash + self.core_shares * self.spy_price + self.unrealised()
        return AccountSnapshot(self.account_reported or self.account, nl, nl * 0.6, nl * 0.6)

    def positions(self):
        self._need()
        self._process_pending()
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
        if self.zn_pos and not glitch:
            out.append(Position("ZN", "FUT", self.zn_pos, self.zn_local, self.zn_pos * self.zn_price_now() * self.zn_multiplier))
        return out

    def _front(self, min_expiry):
        return next(e for e in self.expiries if e >= min_expiry)

    @staticmethod
    def _local(expiry):
        return "MES" + "FGHJKMNQUVXZ"[expiry.month - 1] + str(expiry.year)[-1]

    def front_future(self, symbol, exchange, min_expiry):
        self._need()
        if symbol == "ZN":
            e = next(x for x in self.zn_expiries if x >= min_expiry)
            return FutureContract("ZN", e, self.zn_multiplier, "ZN" + "FGHJKMNQUVXZ"[e.month - 1] + str(e.year)[-1],
                                  con_id=2000 + e.year % 100 * 100 + e.month)
        e = self._front(min_expiry)
        return FutureContract(symbol, e, self.multiplier, self._local(e), con_id=1000 + e.month)

    def contract_from(self, symbol, exchange, con_id, local_symbol, expiry, multiplier):
        return FutureContract(symbol, expiry, multiplier, local_symbol, con_id)

    def _process_pending(self):
        now = self.clock.t
        due = [p for p in self._pending if p[0] <= now]
        self._pending = [p for p in self._pending if p[0] > now]
        for _, action, qty, ref, _vis, *sym in due:
            self._apply_fill(action, qty, ref, sym[0] if sym else "MES")

    def _apply_fill(self, action, qty, ref, symbol="MES"):
        sgn = 1 if action == "BUY" else -1
        if symbol == "ZN":
            return self._apply_zn_fill(sgn, action, qty, ref)
        px = self.fut_price_now() + sgn * 0.25 * self.slippage_ticks
        if self.stale_after_fill:
            self._stale_pos, self._stale_left, self.stale_after_fill = self.fut_pos, self.stale_after_fill, 0
        self.fut_pos += sgn * qty
        self.fut_cost += sgn * qty * px
        self.cash -= self.commission * qty
        if self.fut_pos == 0:
            self.cash -= self.fut_cost * self.multiplier
            self.fut_cost = 0.0
        self.execs.append(dict(exec_id=f"E{len(self.execs) + 1}", rev=0, ref=ref, side=action, qty=float(qty), price=px,
                               time=str(self.clock.t), local_symbol=self._local(self._front(self.clock.t.date()))))
        return px

    def _apply_zn_fill(self, sgn, action, qty, ref):
        px = self.zn_price_now() + sgn * self.zn_tick * self.slippage_ticks / 2
        if not self.zn_local:
            self.zn_local = self._last_zn_local or "ZN?"
        self.zn_pos += sgn * qty
        self.zn_cost += sgn * qty * px
        self.cash -= self.zn_commission * qty
        if self.zn_pos == 0:
            self.cash -= self.zn_cost * self.zn_multiplier
            self.zn_cost = 0.0
        self.execs.append(dict(exec_id=f"E{len(self.execs) + 1}", rev=0, ref=ref, side=action, qty=float(qty), price=px,
                               time=str(self.clock.t), local_symbol=self.zn_local))
        return px

    _last_zn_local = ""

    def executions(self, ref_prefix):
        self._need()
        self._process_pending()
        if self.executions_fail > 0:
            self.executions_fail -= 1
            raise BrokerError("executions unavailable (simulated)")
        today = self.clock.t.date()
        ex = [e for e in self.execs if e["ref"].startswith(ref_prefix)
              and dt.datetime.fromisoformat(e["time"]).date() == today]          # IB Gateway: since midnight only
        if self.exec_hide_reads > 0 and ex:
            self.exec_hide_reads -= 1
            ex = ex[:-1]
        return [dict(e) for e in ex]

    def working_orders(self, ref_prefix="MET-"):
        self._process_pending()
        return self.stuck_working + sum(1 for p in self._pending if p[4] and p[3].startswith(ref_prefix))

    def manual_working(self, ref):
        return self.manual_open

    def cancel_working(self, ref_prefix="MET-"):
        n = self.stuck_working
        if self.cancel_clears_stuck:
            self.stuck_working = 0
        return n

    def what_if(self, contract, action, qty):
        self._need()
        nl = self.account_snapshot().net_liquidation
        init = qty * self.fut_price_now() * self.multiplier * 0.079   # ~$2,900 per MES at 7,340
        if contract.symbol == "ZN":
            init = qty * self.zn_price_now() * self.zn_multiplier * 0.02          # ~$2,200 per ZN
        init += abs(self.zn_pos) * self.zn_price_now() * self.zn_multiplier * 0.02 if contract.symbol != "ZN" else 0
        if self.whatif_excess is not None:
            return MarginCheck(nl - self.whatif_excess, nl, init * 0.9)
        return MarginCheck(init, nl, init * 0.9)

    # ---------------------------------------------------------------- orders
    def market_order(self, contract, action, qty, ref, timeout_s):
        oid = str(next(self._ids))
        if contract.symbol == "ZN":
            self._last_zn_local = contract.local_symbol
            if self.zn_pos == 0:
                self.zn_local = contract.local_symbol
        if not self.is_connected():
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="NotConnected"))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "not connected")
        from .executor import futures_open, rates_futures_open
        if not (rates_futures_open if contract.symbol == "ZN" else futures_open)(self.clock.now()):
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="Rejected", t=self.clock.now()))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "market closed (simulated)")
        if self.reject_next > 0:
            self.reject_next -= 1
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, status="Rejected", t=self.clock.now()))
            return OrderResult(oid, "Rejected", qty, 0, 0.0, "simulated rejection")
        if self.fill_delay_next is not None:
            secs, visible = self.fill_delay_next
            self.fill_delay_next = None
            self._pending.append((self.clock.t + dt.timedelta(seconds=secs), action, qty, ref, visible, contract.symbol))
            self.clock.advance(0.3)
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, filled=0, status="Pending", t=self.clock.now()))
            return OrderResult(oid, "Unknown" if visible else "Timeout", qty, 0, 0.0, "simulated: fills later")
        fill = qty if self.partial_next is None else min(self.partial_next, qty)
        self.partial_next = None
        px = self._apply_fill(action, fill, ref, contract.symbol)
        self.clock.advance(0.3)
        status = "Filled" if fill == qty else "PartiallyFilled"
        if self.late_fill_next > 0:
            self.late_fill_next -= 1
            self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, filled=fill, price=px, status="LateFill",
                                    t=self.clock.now()))
            return OrderResult(oid, "Timeout", qty, 0, 0.0, "simulated: cancel raced a fill")
        self.orders.append(dict(id=oid, ref=ref, action=action, qty=qty, filled=fill, price=px, status=status,
                                t=self.clock.now()))
        return OrderResult(oid, status, qty, fill, px, fills=[dict(self.execs[-1])] if fill else [])

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
