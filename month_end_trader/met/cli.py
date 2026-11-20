"""Command line.

  python -m met.cli run        --config config.yaml      run the daemon (forever)
  python -m met.cli status     --config config.yaml      next trade date, current signal, state, monitor status
  python -m met.cli signal     --config config.yaml      compute the signal for the next trade date (Yahoo + IBKR)
  python -m met.cli check      --config config.yaml      connect to IBKR and run every preflight check, no orders
  python -m met.cli test-order --config config.yaml      PAPER ONLY: buy 1 contract and sell it immediately
  python -m met.cli flatten    --config config.yaml      close any open position the bot owns (asks first)
  python -m met.cli kill | unkill                         create / remove the kill switch file
  python -m met.cli report     --config config.yaml      run the decay monitor now
  python -m met.cli dry-run    [--months N]              full daemon run against the simulated broker
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import logging.handlers
import sys
from pathlib import Path

from . import calendar as cal
from .alerts import Alerter
from .config import load_config
from .signal import yahoo_daily, drop_partial_today, signals_from_sources
from .state import TradeState


def setup_logging(log_dir: str, verbose=False):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    fh = logging.handlers.TimedRotatingFileHandler(Path(log_dir) / "met.log", when="midnight", backupCount=90)
    fh.setFormatter(fmt); root.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout); sh.setFormatter(fmt); root.addHandler(sh)
    logging.getLogger("ib_async").setLevel(logging.WARNING)


def make_fetchers(cfg, broker, now_fn):
    f = {}
    for src in cfg.signal.sources:
        if src == "yahoo":
            f["yahoo"] = lambda sym: drop_partial_today(yahoo_daily(sym, days=120), now_fn())["adjclose"]
        elif src == "ibkr" and broker is not None:
            f["ibkr"] = lambda sym: broker.daily_adjusted_closes(sym, days=120)
    return f


def main(argv=None):
    ap = argparse.ArgumentParser(prog="met")
    ap.add_argument("cmd")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--months", type=int, default=3)
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("-v", action="store_true")
    a = ap.parse_args(argv)

    if a.cmd == "dry-run":
        from .dryrun import dry_run
        dry_run(months=a.months)
        return 0
    if a.cmd in ("kill", "unkill"):
        cfg = load_config(a.config, require_account=False)
        p = Path(cfg.monitoring.state_dir) / "KILL"
        if a.cmd == "kill":
            p.write_text(dt.datetime.now().isoformat()); print(f"kill switch ON ({p})")
        else:
            p.unlink(missing_ok=True); print("kill switch OFF")
        return 0

    cfg = load_config(a.config)
    setup_logging(cfg.monitoring.log_dir, a.v)
    alerter = Alerter(cfg.mode)
    now = lambda: dt.datetime.now(tz=cal.ET)

    if a.cmd == "report":
        from .monitor import run_report
        res = run_report(cfg)
        print(json.dumps(res, indent=2, default=str)); return 0

    if a.cmd == "signal":
        T = cal.next_trade_date(now().date())
        broker = _broker(cfg, connect=False)
        try:
            broker.connect()
        except Exception as e:
            print(f"(IBKR unavailable: {e}; Yahoo only)"); broker = None
        sig, problems, sigs = signals_from_sources(T, make_fetchers(cfg, broker, now), cfg.signal.equity, cfg.signal.bond,
                                                   cfg.signal.require_agreement)
        print(f"Next trade date: {T} (early close: {cal.is_early_close(T)})")
        for s in sigs:
            print(f"  {s.source:<6} through {s.signal_through}: {cfg.signal.equity} {s.equity_mtd:+.2%}  "
                  f"{cfg.signal.bond} {s.bond_mtd:+.2%}  gap {s.gap:+.2%} -> {s.side}")
        for p in problems:
            print("  !", p)
        if T != cal.next_session(now().date()) and now().date() != T:
            print("  (signal is provisional: it is fixed by the close of", cal.previous_session(T), ")")
        return 0

    if a.cmd == "status":
        T = cal.next_trade_date(now().date())
        st = TradeState(cfg.monitoring.state_dir, str(T))
        o, c = cal.session_open_close(T)
        print(f"mode={cfg.mode} account={cfg.ib.account} leverage={cfg.strategy.leverage}x {cfg.strategy.instrument}")
        print(f"next trade date {T}: open {o:%H:%M} ET, exit {c:%H:%M} ET minus {cfg.strategy.exit_lead_s}s; state={st.phase}")
        mp = Path(cfg.monitoring.state_dir) / "monitor.json"
        if mp.exists():
            m = json.loads(mp.read_text()); print("monitor:", m.get("summary"))
        print("kill switch:", (Path(cfg.monitoring.state_dir) / "KILL").exists())
        return 0

    broker = _broker(cfg)
    from .executor import Executor
    from .monitor import run_report
    ex = Executor(cfg, broker, alerter, make_fetchers(cfg, broker, broker.now), reporter=lambda d: run_report(cfg))

    if a.cmd == "run":
        ex.run_forever(); return 0

    if a.cmd == "check":
        from .guardrails import preflight
        T = cal.next_trade_date(now().date())
        acct = broker.account_snapshot()
        print(f"connected: {acct.account}  net liq {acct.net_liquidation:,.0f} {acct.currency}  "
              f"excess liq {acct.excess_liquidity:,.0f}")
        for p in broker.positions():
            print(f"  position {p.sec_type} {p.local_symbol or p.symbol} {p.quantity:+g} value {p.market_value:,.0f}")
        # the check runs as if today were the trade date's preflight
        plan, blockers, warnings = preflight(cfg, broker, T, ex.fetchers)
        print(f"preflight for {T}:", "OK" if plan else "BLOCKED")
        for b in blockers: print("  BLOCK:", b)
        for w in warnings: print("  warn:", w)
        if plan:
            print(json.dumps(plan.to_dict(), indent=2, default=str))
        return 0 if plan else 2

    if a.cmd == "test-order":
        if cfg.mode != "paper":
            print("test-order is paper-only"); return 2
        c = broker.front_future(cfg.strategy.instrument, cfg.strategy.exchange, now().date() + dt.timedelta(days=3))
        print("contract", c.local_symbol, c.expiry, "price", broker.price(c))
        r1 = broker.market_order(c, "BUY", 1, "MET-test-buy", 30); print("buy:", r1)
        broker.sleep(2)
        r2 = broker.market_order(c, "SELL", r1.filled or 1, "MET-test-sell", 30); print("sell:", r2)
        print("position now:", broker.future_position(c)); return 0

    if a.cmd == "flatten":
        T = cal.next_trade_date(now().date())
        for d in {T, cal.previous_session(T), now().date()}:
            st = TradeState(cfg.monitoring.state_dir, str(d))
            if st.open_quantity:
                if not a.yes and input(f"flatten {st.open_quantity:+d} {st.get('contract_local_symbol')}? [y/N] ") != "y":
                    return 1
                ex.do_exit(d, st, reason="manual flatten")
        print("bot-owned positions flat. Broker futures positions:",
              broker.all_future_positions(cfg.strategy.instrument)); return 0

    ap.error(f"unknown command {a.cmd}")


def _broker(cfg, connect=True):
    from .ibkr import IBKRBroker
    b = IBKRBroker(cfg)
    if connect:
        b.connect()
    return b


if __name__ == "__main__":
    sys.exit(main())
