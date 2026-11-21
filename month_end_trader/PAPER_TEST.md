# Paper-test checklist: first window 24 → 30 Nov 2026 (both legs)

Run everything on the always-on machine against your **paper** account (DU…). Tick each box. Commands assume you are in `month_end_trader/` with the venv active, and `config.yaml` has `mode: paper`.

**This month's schedule (ET)**

| When | Leg | What |
|---|---|---|
| Mon 23 Nov 16:30 | B | Alert `BOND Bond window 2026-11-24 -> 2026-11-30 starts tomorrow` |
| Tue 24 Nov 15:30 | B | Preflight, then `BOND … PLANNED BUY n <March 2027 contract>` (IB may show `ZNH7` or `ZN   MAR 27`; MTN likewise) |
| Tue 24 Nov 15:58 | B | Entry, long ZN/MTN, held over Wed, Thanksgiving (CBOT open until 13:00 and from 18:00 ET, thin) and Fri (CBOT closes 13:00 ET) |
| Fri 27 Nov 13:25 | A | `Tomorrow (2026-11-30) month-end trade: LONG/SHORT` |
| Mon 30 Nov 09:10 / 09:30:01 | A | MES preflight, then entry |
| Mon 30 Nov 15:55 | B | Bond exit (clear of leg A's exit) |
| Mon 30 Nov 15:59:58 | A | MES exit |

The bond contract is **March 2027** (ZNH7 / MTNH7). December's last safe day (first notice for ZN, last trade for MTN) falls inside the window, so the bot rolls to March.

## A. Once, before Mon 23 Nov (about 45 min)

**IB Gateway / TWS settings**
- [ ] Paper login works, under a **dedicated API username** if you have one.
- [ ] API enabled and Read-Only **off** (Configure → API → Settings).
- [ ] **Bypass Order Precautions for API orders** on (Configure → API → Precautions).
- [ ] Timezone **America/New_York**.
- [ ] Daily auto-restart at **17:15 ET** (inside the CME/CBOT 17:00–18:00 halt; Docker: `AUTO_RESTART_TIME=05:15 PM`).
- [ ] You can confirm the **Sunday 29 Nov** weekly login/2FA before **18:00 ET** (ZN is held over that weekend).
- [ ] Market data on the API username:
  - CME real-time L1 (MES) **and CBOT real-time L1 (ZN/MTN)**, e.g. the non-pro "US Securities Snapshot and Futures Value Bundle".
  - US equity data for the SPY/TLT daily bars.
- [ ] Trading permission: futures, United States.

**Paper account**
- [ ] Reset the paper account's balance to roughly your live equity (Client Portal → Settings → Paper Trading Account). A default $1M paper account hits the contract caps (40 MES / 20 ZN) and makes the ZN/MTN choice meaningless.
- [ ] Share your real-time market data subscriptions with the paper account (Client Portal → Settings → Paper Trading Account). Confirm this works for the dedicated API username too.
- [ ] `ib.account` is your paper id (DU…) and `ib.port` is 4002 (Gateway) or 7497 (TWS). The placeholder DU0000000 passes validation, so `status` alone won't catch it. **Docker:** set `MET_IB_ACCOUNT` in `deploy/trader.env`; it overrides config.yaml.

**Config:** leg B is in `bond:`.
- [ ] `enabled: true`.
- [ ] `instrument: ZN` above about $150k, or `MTN` below that (micro, cash settled; the bot uses limit orders for it).
- [ ] `leverage: 2.8` (IEF-equivalent) and `strategy.leverage: 2.8` with `vol_tilt: true`. Lower both for the first paper run if you prefer.
- [ ] `strategy.sizing_rounding`: `floor` (never above target; small accounts skip calm months) or `nearest`.

**Bot checks.** Each should succeed before you go on:

```bash
python -m met.cli status --config config.yaml
```
- [ ] Shows the leg-A trade date 2026-11-30 and `bond leg: … next window entry 2026-11-24 … -> exit 2026-11-30`.

```bash
python -m met.cli check --config config.yaml
python -m met.cli bond-check --config config.yaml
```
- [ ] `check` prints `preflight for 2026-11-30: OK` (PROVISIONAL before Fri 27 Nov's close).
- [ ] `bond-check` prints `bond preflight for window 2026-11-24 -> 2026-11-30: OK` with a plan for the **H7** contract.
- [ ] Read and resolve any BLOCK line.

```bash
python -m met.cli signal --config config.yaml
python -m met.cli validate-data --config config.yaml
python -m met.cli bond-report --config config.yaml
```
- [ ] Both `ibkr` and `yahoo` signal lines agree.
- [ ] `OK: sources consistent`.
- [ ] The bond report prints a status (it writes `state/bond_monitor.json`).

**Order and execution tests.** Run these on **Mon 23 Nov between 09:30 and 14:30 ET** (CME/CBOT are closed on Saturday), with the daemon stopped, before you start it in B:

```bash
python -m met.cli test-order --config config.yaml
python -m met.cli test-order --leg bond --config config.yaml
```
- [ ] Leg A buys and sells 1 MES; the position returns to 0.
- [ ] The bond test prints the contract IB resolved. Check:
  - its **localSymbol** and **IB last trade** date;
  - `multiplier 1000` (ZN) or `100` (MTN);
  - whether MTN's limit orders filled. If they don't fill, raise `bond.limit_ticks`.
- [ ] All four fills appear in TWS → Trades with Order Ref `TEST-buy` / `TEST-sell`.
- [ ] In TWS **Contract Info** for the H7 contract, note IBKR's close-out date (ZN). It must be well after 30 Nov.

**Execution reports survive a Gateway restart.** This is what the bot's ownership depends on:
- [ ] After the test orders, restart IB Gateway.
- [ ] This lists today's TEST fills:
  ```bash
  python -c "from met.config import load_config; from met.ibkr import IBKRBroker; c=load_config('config.yaml'); b=IBKRBroker(c); b.connect(); print(b.executions('TEST-'))"
  ```

**Alerts**
- [ ] `python -m met.cli alert-test --config config.yaml` prints `alert delivered`, and the message arrives. Run it with the daemon's environment (MET_TELEGRAM_* / MET_SMTP_*), otherwise it prints "NO alert channel delivered".
- [ ] If you use a heartbeat monitor: `MET_HEARTBEAT_URL` is set and the check is green.

## B. Start the daemon by Mon 23 Nov 15:00 ET

```bash
sudo cp deploy/met.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now met
# or Docker:
docker compose -f deploy/docker-compose.yml up -d
python -m met.cli status --config config.yaml
```
- **Docker users:** run read-only CLI commands inside the container (`docker compose -f deploy/docker-compose.yml exec trader python -m met.cli status …`). `test-order`, `flatten` and `resolve` need the daemon stopped: `docker compose … stop trader`, then `docker compose … run --rm trader python -m met.cli <cmd> … --config /app/config.yaml`, then `start trader`.
- **systemd users:** `deploy/met.service` assumes user `met` and `/opt/month_end_trader`: install there or edit both. To run a CLI command with the daemon's environment: `sudo systemd-run --uid=met -p EnvironmentFile=/etc/met.env -p WorkingDirectory=/opt/month_end_trader --pty --wait .venv/bin/python -m met.cli status --config config.yaml`.
- **Run every `met` command as the daemon's user and from its folder**, so it sees the same `state/` (especially `kill`).
- [ ] The daemon is running, and its start alert lists both legs.
- [ ] `status` shows no unfinished trades.

## C. What you should see

| Time (ET) | Alert | If not |
|---|---|---|
| Mon 23 (at daemon start) | `Weekly status: bot running` (bond leg: next window 2026-11-24 -> 2026-11-30, ZN positions: none) | — |
| Mon 23 16:30 | `BOND Bond window … starts tomorrow` with a size estimate | Check the log; run `met bond-check` |
| Tue 24 15:30 | `BOND 2026-11-30: PLANNED BUY n ZNH7 at 15:58 ET` | A `BOND … SKIPPED` alert lists the reasons |
| Tue 24 15:58 | `BOND 2026-11-30: ENTERED BUY n/n …` | `BOND … ENTRY FAILED`: the bot watches its executions until midnight |
| Wed–Fri | nothing (guards run whenever CBOT trades: Thanksgiving until 13:00 and from 18:00, Fri until 13:00) | `BOND … DISASTER STOP` means the bond overlay lost 4% of equity |
| Fri 27 13:25 | `Tomorrow (2026-11-30) month-end trade: LONG/SHORT` | Check the log; `met signal` |
| Mon 30 08:00 | `Weekly status` (includes the bond leg and ZN positions) | The daemon is down: start it |
| Mon 30 09:10 / 09:30 | `PLANNED …` / `ENTERED … MESZ6` | SKIPPED lists the reasons |
| Mon 30 15:55 | `BOND 2026-11-30: FLAT. Net P&L …` | Exit alerts: the bot retries |
| Mon 30 ~16:00 | `2026-11-30: FLAT. Net P&L …` | Same |
| Mon 30 18:00 | decay monitor reports (both legs) | Harmless if missing; run `met report` / `met bond-report` |

After 18:00 ET on 30 Nov, check Trade History / the activity statement in Client Portal (logging into TWS with the API username can log the Gateway out; re-login it afterwards, and note the TWS Trades window only shows today):
- [ ] Orders `MEB-2026-11-30-entry` (24 Nov), `MEB-2026-11-30-exit1`, `MET-2026-11-30-entry` and `MET-2026-11-30-exit1`.
- [ ] MES and ZN/MTN positions are 0.
- [ ] `met status` shows no unfinished trades.

## If something goes wrong

- **Stop everything now:** `python -m met.cli kill`. Both legs flatten what they own (the bond leg at the next CBOT reopen if it is closed) and make no new trades. Undo with `unkill`.
- **Close a bot position yourself in TWS:** use Order Ref **`MET-2026-11-30-manual`** (MES) or **`MEB-2026-11-30-manual`** (ZN/MTN). The key is the trade's month-end date, even for the bond leg.
- **Closed it without the ref, or IB liquidated it:** stop the daemon, then run `met resolve 2026-11-30 --price <fill> --config config.yaml`. Add **`--leg bond`** for ZN/MTN.
- **`BOND … ADOPTED`:** after a crash or late fill the bot proved the broker holds exactly its planned bond quantity and took it over (P&L estimated). It exits on schedule. Check TWS.
- **"Outcome UNKNOWN" / "cannot tell whether it is its own":** check TWS. If it is the bot's, close it with the `-manual` ref.
- **`BOND … broker position differs from the bot's fills`** (repeats every 4 h while CBOT trades): if you closed it yourself without the ref, stop the daemon and run `met resolve 2026-11-30 --leg bond --price <fill>`; otherwise check TWS.
- **`met flatten` while CBOT is closed** (Thanksgiving afternoon, Fri 27 after 13:00 until Sun 18:00) only queues the exit: it prints that, and the daemon sends it at the reopen.
- **"RESOLVE NEEDED":** place no new orders. Stop the daemon and run `met resolve <date> [--leg bond] --price <fill>`.
- Logs are in `logs/met.log`. State files are `state/trade_2026-11-30.json` (leg A) and `state/bond_2026-11-30.json` (leg B).

## Then

- [ ] Paper-trade December too:
  - Bond window 28 → 31 Dec, still in H7. **The 31 Dec bond exit is at 12:50 ET** (SIFMA early close; CME settles rates at 13:00). Check CME's 31 Dec 2026 holiday notice.
  - Leg A on 31 Dec, rolling to MESH7.
- [ ] Only then consider live:
  - `mode: live`, the live account `U…` and port 4001.
  - `MET_LIVE_CONFIRM=U…`.
  - Restart the daemon.
