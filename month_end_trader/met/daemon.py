"""One daemon, one IB connection, both legs: leg A (Executor, month-end MES/ES day trade) and, when
bond.enabled, leg B (BondExecutor, month-end ZN hold). Each tick runs every leg; one leg's exception never stops
the other. Leg A's tick owns the connection checks and the heartbeat."""
from __future__ import annotations

import datetime as dt
import logging
import traceback
from pathlib import Path

from . import calendar as cal
from .state import DaemonLock

log = logging.getLogger("met.daemon")


class Daemon:
    def __init__(self, legs: list):
        self.legs = legs
        self.cfg = legs[0].cfg
        self.broker = legs[0].broker
        self.alert = legs[0].alert
        for leg in legs[1:]:
            leg.peer = legs[0]          # the bond leg yields to leg A's exit window on month-end day

    def tick(self) -> dt.datetime:
        wakes = []
        for leg in self.legs:
            try:
                wakes.append(leg.tick())
            except Exception as e:
                log.error(traceback.format_exc())
                self.alert.critical(f"daemon tick error (leg {getattr(leg, 'LEG', '?')}): {e!r}")
                wakes.append(self.broker.now() + dt.timedelta(seconds=15))
        return min(wakes)

    def run_forever(self, stop=lambda: False, lock=True):
        lk = DaemonLock(self.cfg.monitoring.state_dir).acquire() if lock else None
        try:
            c, b = self.cfg, self.cfg.bond
            today = self.broker.now().date()
            legs = [f"A: {c.strategy.leverage:g}x {c.strategy.instrument}" +
                    (f" (x{c.strategy.low_vol_scale:g} in calm months)" if c.strategy.vol_tilt else "")]
            if len(self.legs) > 1:
                from .bond import window_for
                E, X = window_for(today, b.hold_sessions)
                legs.append(f"B: {b.leverage:g}x IEF-equivalent in {b.instrument} (next window {E} -> {X})")
            self.alert.info(f"met daemon started (mode={c.mode}, account={c.ib.account})",
                            "; ".join(legs) + f"\nnext leg-A trade date: {cal.next_trade_date(today)}; "
                            f"state dir {Path(c.monitoring.state_dir).resolve()}")
            while not stop():
                wake = self.tick()
                wait = (wake - self.broker.now()).total_seconds()
                self.broker.sleep(max(0.2, min(wait, 60)))
        finally:
            if lk:
                lk.release()


def build_daemon(cfg, broker, alerter, fetchers, reporter=None, bond_reporter=None):
    from .executor import Executor
    legs = [Executor(cfg, broker, alerter, fetchers, reporter=reporter)]
    if cfg.bond.enabled:
        from .bond import BondExecutor
        legs.append(BondExecutor(cfg, broker, alerter, fetchers, reporter=bond_reporter))
    return Daemon(legs)
