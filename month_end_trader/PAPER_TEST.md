# Paper-test checklist (before the first trade, 30 Nov 2026)

Run everything on the always-on machine against your **paper** account (DU…). Tick each box. Commands assume you are in `month_end_trader/` with the venv active, and `config.yaml` has `mode: paper`.

## A. Once, any day before the trade (about 30 min)

**IB Gateway / TWS settings**
- [ ] Paper login works, under a **dedicated API username** if you have one.
- [ ] API enabled and Read-Only **off** (Configure → API → Settings).
- [ ] **Bypass Order Precautions for API orders** on (Configure → API → Precautions).
- [ ] Timezone **America/New_York**.
- [ ] Auto-restart time outside 09:00–16:30 ET.

**Bot checks.** Each should succeed before you go on:

```bash
python -m met.cli signal --config config.yaml
```
- [ ] Both `ibkr` and `yahoo` lines are printed and agree (it says LONG for 30 Nov if the gap stays negative).

```bash
python -m met.cli check --config config.yaml
```
- [ ] Prints `preflight for 2026-11-30: OK` and a plan. "Not the last session" is expected if you run it earlier.
- [ ] Any other BLOCK line has been read and resolved.

```bash
python -m met.cli validate-data --config config.yaml
```
- [ ] Prints `OK: sources consistent`.

```bash
python -m met.cli rehearse
```
- [ ] Read the alert timeline. These are the messages you will get on 27 and 30 Nov.

**Order and execution test.** Not on a trade day, and with the daemon stopped:

```bash
python -m met.cli test-order --config config.yaml
```
- [ ] It buys 1 MES and sells it.
- [ ] The position returns to 0.
- [ ] Both fills appear in TWS → Trades with Order Ref `TEST-buy` / `TEST-sell`.

**Execution reports survive a Gateway restart.** This is what the bot's ownership depends on:
- [ ] After the test order, restart IB Gateway (File → Restart, or `docker compose restart ib-gateway`).
- [ ] Run this; it should list the two TEST fills from today:
  ```bash
  python -c "from met.config import load_config; from met.ibkr import IBKRBroker; c=load_config('config.yaml'); b=IBKRBroker(c); b.connect(); print(b.executions('TEST-'))"
  ```

**Alerts**
- [ ] Telegram/email alerts arrive. Start the daemon briefly and check you receive "met daemon started", then stop it with Ctrl-C.

## B. Start the daemon (by Friday 27 Nov, 13:00 ET)

```bash
sudo systemctl enable --now met   # or: docker compose -f deploy/docker-compose.yml up -d
python -m met.cli status --config config.yaml
```
- [ ] The daemon is running.
- [ ] `status` shows `next trade date 2026-11-30` and no unfinished trades.
- [ ] The heartbeat ping (if configured) is green.

## C. Trade day, Monday 30 Nov. What you should see (all ET)

| Time | Alert | If not |
|---|---|---|
| Fri 27 Nov 13:25 | `Tomorrow (2026-11-30) month-end trade: LONG/SHORT` | Check the daemon log, then run `met signal` |
| Mon 08:00 | `Weekly status: bot running` | The daemon is down: start it |
| 09:10 | `PLANNED BUY/SELL N MESZ6` | A SKIPPED alert lists the reasons |
| 09:30:01 | `ENTERED …` | ENTRY FAILED: read the alert; the bot keeps watching its executions |
| 15:59:58 | `FLAT. Net P&L …` | Exit alerts: the bot retries; see "if something goes wrong" |
| 18:00 | `Decay monitor after 2026-11-30` | Harmless if missing (Yahoo rate limits); run `met report` later |

Verify in TWS after the close:
- [ ] Two MES trades with Order Refs `MET-2026-11-30-entry` and `MET-2026-11-30-exit1`.
- [ ] The MES position is 0.
- [ ] `met status` shows no unfinished trades.

## If something goes wrong

- **Stop everything now:** `python -m met.cli kill`. The bot flattens what it owns and makes no new trades. Undo it with `unkill`.
- **Close the bot's position yourself in TWS:** use Order Ref **`MET-2026-11-30-manual`**, so the bot sees the close.
- **Closed it without the ref, or IB liquidated it:** stop the daemon, then run `python -m met.cli resolve 2026-11-30 --config config.yaml`.
- **"Outcome UNKNOWN" / "cannot tell whether it is its own":** check TWS. If the bot's order filled, close it with the MET ref, or run `resolve`.
- Logs are in `logs/met.log`, and the state is in `state/trade_2026-11-30.json`.

## Then

- [ ] Paper-trade 31 Dec 2026 as well. 31 Dec is a full session, and the contract rolls to MESH7.
- [ ] Only then consider live:
  - `mode: live`, the live account `U…` and port 4001.
  - `MET_LIVE_CONFIRM=U…` in the environment.
  - Restart the daemon.
