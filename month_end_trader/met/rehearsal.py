"""Rehearse the next real trade on the simulator: extends the price history past its end with a synthetic path
(so the month-to-date signal matches what is known so far, then drifts), runs the real daemon from two
sessions before the trade date through the evening after, and prints the alert timeline with ET times.
Use it to know exactly what messages to expect on the first paper run."""
from __future__ import annotations

import datetime as dt
import logging
import tempfile

import numpy as np
import pandas as pd

from . import calendar as cal
from .dryrun import load_fixture, make_env


def _extend(df: pd.DataFrame, until: dt.date, seed: int, drift_bp=0.0, vol_bp=90.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    last = df.index[-1]
    rows = {}
    px = float(df["close"].iloc[-1]); adj = float(df["adjclose"].iloc[-1])
    for d in cal.sessions(last + dt.timedelta(days=1), until):
        r_on = rng.normal(0, vol_bp / 3) / 1e4
        r_id = rng.normal(drift_bp, vol_bp) / 1e4
        o = px * (1 + r_on); c = o * (1 + r_id)
        adj *= c / px; px = c
        rows[d] = dict(open=round(o, 2), close=round(c, 2), adjclose=adj)
    return pd.concat([df, pd.DataFrame.from_dict(rows, orient="index")])


def rehearse(trade_date: dt.date | None = None, seed: int = 7, verbose=True):
    logging.basicConfig(level=logging.ERROR)
    spy, tlt = load_fixture()
    T = trade_date or cal.next_trade_date(dt.date.today())
    end = cal.next_session(T)
    spy = _extend(spy, end, seed); tlt = _extend(tlt, end, seed + 1, vol_bp=60)
    start = dt.datetime.combine(cal.previous_session(cal.previous_session(T)), dt.time(8, 0), tzinfo=cal.ET)
    with tempfile.TemporaryDirectory() as tmp:
        cfg, clock, b, ex, sink = make_env(start, tmp, spy, tlt)
        stamped = []
        orig = ex.alert.send

        def send(level, title, body=""):
            stamped.append((clock.now(), level, title, body))
            orig(level, title, body)
        ex.alert.send = send
        stop = dt.datetime.combine(T, dt.time(20, 30), tzinfo=cal.ET)
        ex.run_forever(stop=lambda: clock.now() >= stop)
        from .state import TradeState
        s = TradeState(tmp, str(T))
        if verbose:
            o, c = cal.session_open_close(T)
            print(f"Rehearsal of {T} (synthetic prices after the data end; open {o:%H:%M}, close {c:%H:%M} ET"
                  f"{', HALF DAY' if cal.is_early_close(T) else ''})")
            for t, lvl, title, body in stamped:
                print(f"  {t:%a %d %b %H:%M:%S} ET  [{lvl}] {title}")
                for line in (body or "").splitlines()[:3]:
                    print(f"  {'':>24}      {line}")
            print(f"Final state: {s.phase}; orders: " + ", ".join(f"{o['action']} {o['qty']} @ {o['t']:%H:%M:%S}" for o in b.orders))
        return s, b, stamped
