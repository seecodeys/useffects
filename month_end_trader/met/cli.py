"""Command line.

  python -m met.cli run        --config config.yaml      run the daemon (forever)
  python -m met.cli status     --config config.yaml      next trade date, current signal, state, monitor status
  python -m met.cli signal     --config config.yaml      compute the signal for the next trade date (Yahoo + IBKR)
  python -m met.cli check      --config config.yaml      connect to IBKR and run every preflight check, no orders
  python -m met.cli test-order --config config.yaml      PAPER ONLY: buy 1 contract and sell it immediately
  python -m met.cli flatten    --config config.yaml      close any open position the bot owns (asks first)
  python -m met.cli resolve DATE --config ...            record a manual close the bot cannot see (daemon stopped)
  python -m met.cli alert-test --config config.yaml      send a test alert on every configured channel
  python -m met.cli kill | unkill                         create / remove the kill switch file
  python -m met.cli report     --config config.yaml      run the decay monitor now
  python -m met.cli dry-run    [--months N]              full daemon run against the simulated broker
  python -m met.cli rehearse                              alert timeline of the next trade on the simulator
  python -m met.cli validate-data --config config.yaml   compare IBKR ADJUSTED_LAST with Yahoo adjclose (run once)
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
    ap.add_argument("date", nargs="?", help="trade date for `resolve`")
    ap.add_argument("--price", type=float, default=None, help="close price for `resolve` (default: current price)")
    ap.add_argument("-v", action="store_true")
    a = ap.parse_args(argv)

    if a.cmd == "rehearse":
        from .rehearsal import rehearse
        rehearse()
        return 0
    if a.cmd == "dry-run":
        from .dryrun import dry_run
        dry_run(months=a.months)
        return 0
    if a.cmd in ("kill", "unkill"):
        cfg = load_config(a.config, require_account=False)
        p = (Path(cfg.monitoring.state_dir) / "KILL").resolve()
        try:
            return _kill(a.cmd, p)
        except OSError as e:
            print(f"cannot write {p}: {e}\nRun this as the daemon's user, or inside the container:\n"
                  f"  docker compose -f deploy/docker-compose.yml exec trader python -m met.cli {a.cmd} --config /app/config.yaml")
            return 2
        if a.cmd == "kill":
            p.write_text(dt.datetime.now().isoformat())
            print(f"kill switch ON: {p}\n(the daemon must use the same state_dir; it alerts when it sees the switch)")
        else:
            p.unlink(missing_ok=True); print(f"kill switch OFF: {p}")
        return 0

    cfg = load_config(a.config)
    setup_logging(cfg.monitoring.log_dir, a.v)
    alerter = Alerter(cfg.mode)
    now = lambda: dt.datetime.now(tz=cal.ET)

    if a.cmd == "alert-test":
        ok = alerter.self_test()
        print("alert delivered" if ok else "NO alert channel delivered (check MET_TELEGRAM_* / MET_SMTP_* and logs)")
        return 0 if ok else 2

    if a.cmd == "report":
        from .monitor import run_report
        res = run_report(cfg)
        print(json.dumps(res, indent=2, default=str)); return 0

    if a.cmd == "signal":
        T = cal.next_trade_date_after(now())
        broker = _broker(cfg, connect=False, cli=True)
        try:
            broker.connect()
        except Exception as e:
            print(f"(IBKR unavailable: {e}; Yahoo only)"); broker = None
        fx = make_fetchers(cfg, broker, now)
        prov = now() < cal.session_open_close(cal.previous_session(T))[1] + dt.timedelta(minutes=20)
        sig, problems, sigs = signals_from_sources(T, fx, cfg.signal.equity, cfg.signal.bond,
                                                   cfg.signal.require_agreement, cfg.signal.max_source_gap_diff,
                                                   len(fx) if cfg.signal.require_all_sources else 1, provisional=prov)
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
        from .state import unfinished
        T = cal.next_trade_date_after(now())
        for d in unfinished(cfg.monitoring.state_dir):
            print(f"!! unfinished trade state {d}: check the broker / run `met flatten`")
        try:
            st = TradeState(cfg.monitoring.state_dir, str(T))
        except Exception as e:
            print(f"!! trade state for {T} is unreadable: {e}"); return 2
        o, c = cal.session_open_close(T)
        print(f"mode={cfg.mode} account={cfg.ib.account} leverage={cfg.strategy.leverage}x {cfg.strategy.instrument}")
        print(f"next trade date {T}: open {o:%H:%M} ET, exit {c:%H:%M} ET minus {cfg.strategy.exit_lead_s}s; state={st.phase}")
        mp = Path(cfg.monitoring.state_dir) / "monitor.json"
        if mp.exists():
            try:
                m = json.loads(mp.read_text())
                print(f"monitor (as of {m.get('asof')}): {m.get('summary')}")
            except Exception as e:
                print(f"monitor.json UNREADABLE ({e}): run `met report`")
        print("kill switch:", (Path(cfg.monitoring.state_dir) / "KILL").exists())
        return 0

    if a.cmd in ("test-order", "flatten", "resolve"):
        from .state import DaemonLock
        try:
            _lk = DaemonLock(cfg.monitoring.state_dir).acquire()
        except (RuntimeError, PermissionError):
            print("the met daemon is running (or its state dir belongs to another user): stop it first (or use `met kill`, which makes the daemon flatten)")
            return 2

    # flatten/test-order/resolve hold the daemon lock, so they can use the daemon's clientId and see its orders
    broker = _broker(cfg, cli=(a.cmd not in ("run", "flatten", "test-order", "resolve")), alerter=alerter,
                     connect=(a.cmd != "run"))
    from .executor import Executor
    from .monitor import run_report
    ex = Executor(cfg, broker, alerter, make_fetchers(cfg, broker, broker.now), reporter=lambda d: run_report(cfg))

    if a.cmd == "run":
        ex.run_forever(); return 0

    if a.cmd == "validate-data":
        from .signal import yahoo_daily
        bad = 0
        for sym in (cfg.signal.equity, cfg.signal.bond):
            ib_s = broker.daily_adjusted_closes(sym, days=150)
            yh = yahoo_daily(sym, days=220)["adjclose"]
            j = ib_s.to_frame("ib").join(yh.rename("yahoo"), how="inner").dropna()
            r_ib, r_yh = j["ib"].pct_change().dropna(), j["yahoo"].pct_change().dropna()
            diff = (r_ib - r_yh).abs() * 1e4
            print(f"{sym}: {len(j)} common days, daily return diff median {diff.median():.2f}bp, max {diff.max():.2f}bp "
                  f"on {diff.idxmax()}")
            big = diff[diff > 5]
            for d, v in big.items():
                print(f"   {d}: {v:.1f}bp (ex-dividend handled differently?)")
            bad += len(big)
        T = cal.next_trade_date(now().date())
        print("OK: sources consistent" if bad == 0 else f"WARNING: {bad} days differ by >5bp; signals near zero may disagree")
        return 0 if bad == 0 else 3

    if a.cmd == "check":
        from .guardrails import preflight
        T = cal.next_trade_date(now().date())
        acct = broker.account_snapshot()
        print(f"connected: {acct.account}  net liq {acct.net_liquidation:,.0f} {acct.currency}  "
              f"excess liq {acct.excess_liquidity:,.0f}")
        for p in broker.positions():
            print(f"  position {p.sec_type} {p.local_symbol or p.symbol} {p.quantity:+g} value {p.market_value:,.0f}")
        # the check runs as if today were the trade date's preflight
        prov = now() < cal.session_open_close(cal.previous_session(T))[1] + dt.timedelta(minutes=20)
        plan, blockers, warnings = preflight(cfg, broker, T, ex.fetchers, provisional=prov)
        print(f"preflight for {T}:", "OK" if plan else "BLOCKED")
        for b in blockers: print("  BLOCK:", b)
        for w in warnings: print("  warn:", w)
        if plan:
            print(json.dumps(plan.to_dict(), indent=2, default=str))
        return 0 if plan else 2

    if a.cmd == "test-order":
        from .state import unfinished
        if cfg.mode != "paper":
            print("test-order is paper-only"); return 2
        if now().date() == cal.next_trade_date(now().date()) or unfinished(cfg.monitoring.state_dir):
            print("refusing on a trade day or while a trade is unfinished"); return 2
        c = broker.front_future(cfg.strategy.instrument, cfg.strategy.exchange, now().date() + dt.timedelta(days=3))
        print("contract", c.local_symbol, c.expiry, "price", broker.price(c))
        r1 = broker.market_order(c, "BUY", 1, "TEST-buy", 30); print("buy:", r1)
        broker.sleep(2)
        r2 = broker.market_order(c, "SELL", r1.filled or 1, "TEST-sell", 30); print("sell:", r2)
        print("position now:", broker.future_position(c)); return 0

    if a.cmd == "resolve":
        st = TradeState(cfg.monitoring.state_dir, a.date)
        if st.get("fills") is None:
            print("no fills recorded for", a.date); return 2
        c = ex.contract_for(st)
        if not ex.sync(st):
            print("cannot read executions from IB; refusing to record anything"); return 2
        pos = broker.future_position(c)
        print(f"{a.date}: state {st.phase}, bot's fills say {st.owned:+d}, broker shows {pos:+g} {c.local_symbol}")
        if st.owned == 0:
            print("nothing owned according to the bot's fills; marking reconciled")
            st.update(ex.ts(), note="manual resolve: nothing owned", reconciled=True); return 0
        if pos != 0 and (pos > 0) == (st.owned > 0):
            print("the broker still shows a position in the bot's direction: use `met flatten` instead"); return 2
        px = a.price or (ex._try_price(c) or 0.0)
        if not px or px <= 0:
            print("no price available: pass --price <your actual close price>"); return 2
        if not a.yes and input(f"record a manual close of {-st.owned:+d} @ {px:.2f} (P&L ESTIMATED)? [y/N] ") != "y":
            return 1
        side = "SELL" if st.owned > 0 else "BUY"
        st.merge_fills([dict(exec_id=f"manual-{ex.ts()}", rev=0, ref=f"MET-{a.date}-manual", side=side,
                             qty=float(abs(st.owned)), price=float(px), time=ex.ts())])
        st.update(ex.ts(), "EXIT_SUBMITTED", note="manual resolve", exit_reason="manual", exit_price_estimated=True)
        ex._finalise(dt.date.fromisoformat(a.date), st)
        st.update(ex.ts(), note="reconciled after manual resolve", reconciled=True, pnl_estimated=True)
        print("resolved:", st.phase, "net P&L (estimated)", st.get("net_pnl")); return 0

    if a.cmd == "flatten":
        from .state import unfinished
        T = cal.next_trade_date(now().date())
        dates = set(unfinished(cfg.monitoring.state_dir))
        st_t = TradeState(cfg.monitoring.state_dir, str(T))
        if st_t.phase in ("ENTRY_SUBMITTED", "ENTERED", "EXIT_SUBMITTED", "ERROR", "FLAT"):
            dates.add(str(T))
        for d in sorted(dates):
            st = TradeState(cfg.monitoring.state_dir, d)
            c = ex.contract_for(st)
            if not ex.sync(st):
                print(f"{d}: cannot read executions from IB; not acting"); return 2
            pos = broker.future_position(c)
            print(f"{d}: state {st.phase}, broker {pos:+g} {c.local_symbol}")
            if pos == 0:
                continue
            if not a.yes and input(f"flatten {pos:+g} {c.local_symbol}? [y/N] ") != "y":
                return 1
            if st.owned == 0:
                print("  the bot does not own this position; close it yourself in TWS if intended"); continue
            if st.phase != "EXIT_SUBMITTED":
                st.update(ex.ts(), "EXIT_SUBMITTED", note="manual flatten", exit_reason="manual flatten")
            for _ in range(6):
                st.data.pop("next_exit_try", None)
                if ex.flatten(dt.date.fromisoformat(d), st, reason="manual flatten"):
                    break
                broker.sleep(5)
        print("done. Broker futures positions:", broker.all_future_positions(cfg.strategy.instrument)); return 0

    ap.error(f"unknown command {a.cmd}")


def _kill(cmd, p):
    if cmd == "kill":
        p.write_text(dt.datetime.now().isoformat())
        print(f"kill switch ON: {p}\n(the daemon must use the same state_dir; it alerts when it sees the switch)")
    else:
        p.unlink(missing_ok=True); print(f"kill switch OFF: {p}")
    return 0


def _broker(cfg, connect=True, cli=False, alerter=None):
    """CLI commands use clientId+1 so they never collide with the running daemon (IB error 326)."""
    from .ibkr import IBKRBroker
    b = IBKRBroker(cfg, client_id=cfg.ib.client_id + (1 if cli else 0),
                   on_critical=(alerter.critical if alerter and not cli else None))
    if connect:
        b.connect()
    return b


if __name__ == "__main__":
    sys.exit(main())
