# Month-end trader (IBKR)

Automates research strategy 013: hold SPY as the core, and on the **last NYSE trading day of each month** add
an **MES futures overlay of 2x net liquidation** from the open to the close.

- **Direction:** −sign(SPY MTD − TLT MTD), measured through the previous close. If stocks lagged bonds this month the bot goes LONG; if they beat bonds it goes SHORT.
- **Entry:** 09:30:01 ET.
- **Exit:** 15:59:58 ET, or 12:59:58 on 1 pm half days.
- **Overnight:** never holds anything overnight. Your SPY core is never touched.

> Research code, not investment advice. Run it in **paper mode** first, for at least one or two month-ends.

## How a trade day runs (all times ET)

| When | What | State |
|---|---|---|
| Previous session close + 25 min | Compute the signal and send a "Tomorrow: LONG/SHORT" alert. CRITICAL if IB is not connected. Refresh the decay monitor if it is missing or stale | announced |
| Open − 20 min | **Preflight**: all guardrails, sizing, what-if margin. Transient blockers (data, price, margin query) are retried every minute until 09:27 | `PLANNED` or `SKIPPED` |
| 09:30:01 | Market order for N MES; no late entries after open + 90 s | `ENTRY_SUBMITTED` → `ENTERED` |
| While holding | Every 15 s, until 45 s before the exit: disaster stop, kill switch, broker-vs-state position check (alert only) | |
| Close − 2 s | Opposite market order for the quantity the bot owns, retried with backoff until flat | `EXIT_SUBMITTED` → `FLAT` |
| Close + 10 min | Reconcile. The bot's executions must net to zero. Any other position in the contract is CRITICAL-alerted and left alone | reconciled |
| Close + 2 h | Decay monitor report (SPRT and the other indicators) | reported |
| Every tick | Earlier-dated unfinished trades are resolved and exited, including after midnight or a weekend | |
| Every session close + 15 min | Overnight guard: alert if any MES position is open | |

The daemon is a single loop of idempotent `tick()`s over the state saved in `state/trade_<date>.json`. You can kill and restart it at any moment, including mid-trade, and it starts even while IB is unreachable; it alerts and keeps reconnecting.
- It never enters twice in one month.
- After a crash between sending and confirming an order, it resolves the outcome from its own executions, on any later day too.
- **Ownership comes from IBKR execution reports.** The bot owns exactly the signed sum of the fills of its own orders (orderRef `MET-<date>-…`), persisted in the trade state. Late fills, timeouts, cancel races, crashes and reversals are all visible there. Fill prices, and so the P&L, are exact. Positions you open yourself never appear in the bot's executions and are never touched.
- Position snapshots are only a cross-check. An exit order is sent only when the bot's executions and the broker's position agree there is something to close, sized to the smaller of the two. If they disagree, no order is sent and a CRITICAL alert fires.
- The bot keeps reading its own executions until midnight after the close. An order IB reported as dead that fills later is caught and closed.
- **IB Gateway only reports executions since midnight.** Set the Gateway/TWS timezone to **America/New_York**, so that "midnight" is not inside the US session (e.g. Singapore midnight is 11:00 ET). Fills the bot has already saved stay valid across days. If the bot was down and a day's executions aged out, it says so ("outcome UNKNOWN") instead of guessing.
- **Closing a bot position yourself:** in TWS, put **Order Ref `MET-<trade date>-manual`** on the order (e.g. `MET-2026-11-30-manual`). The bot then sees the close. If you closed it without that ref, or IB liquidated it, stop the daemon and run `met resolve <trade date>`. It records the close (P&L marked ESTIMATED) and unblocks future trades.
- No new trade starts while an older trade state is unfinished (`met status` lists them).

## Guardrails

**Before the trade.** These block the trade:
- Kill switch (`state/KILL` file or `MET_KILL=1`).
- Not the last session of the month.
- An account that doesn't match the mode. Paper must be `DU…`; live must not be. The account must also be one of the connected session's managed accounts (checked at connect).
- A port that doesn't match the mode, for the standard ports 4001–4004 and 7496/7497.
- **Live mode without `MET_LIVE_CONFIRM=<account id>` in the environment**, or with delayed market data.
- Net liquidation below the minimum.
- Any existing MES position.
- No signal source available. Set `signal.require_all_sources: true` to require both IBKR and Yahoo.
- Sources disagreeing on direction, or on the gap by more than 15bp.
- Data missing for the base date or the T−1 date.
- Contract multiplier mismatch, or a futures price that is implausible compared with SPY (also blocks if the check can't run).
- Size rounding to 0 contracts.
- IBKR what-if margin missing, or leaving less than `margin_buffer_pct` (default 10%) of net liquidation.
- The decay monitor says stop (size ×0), or its state is unreadable.
- Optional: SPY core below a minimum weight.

**Size** is capped, with a warning, at `max_leverage` (hard cap 4x) and `max_contracts` (hard cap 200). A non-USD base currency (e.g. SGD) is converted to USD at IBKR's exchange rate.

**During and after the trade:**
- No entry later than open + 90 s.
- Partial fills: only the filled quantity is exited.
- Exits close only the quantity the bot owns (its entry fills minus its exit fills), and never more than the broker shows in that direction, so a stale position read cannot reverse the position.
- Exit retries:
  - With backoff from 5 s up to 5 min.
  - No orders while CME equity futures are closed: weekends, the daily 17:00–18:00 halt, after the early halt on NYSE half days and holidays, and the full closures on 25 Dec, 1 Jan and Good Friday.
  - One MANUAL INTERVENTION alert after 10 failed rounds, then one round per hour. Identical alerts are sent at most every 30 minutes.
- Disaster stop: flatten early if the overlay loses 6% of equity, about a 3% SPY move at 2x. Set it to 0 to trade exactly like the backtest.
- Kill switch during the trade flattens immediately.
- Position mismatch alerts while holding. The bot never concludes "closed outside the bot" from a position read.
- Reconcile after the close, and an overnight guard every day.
- `met flatten` and `met test-order` refuse to run while the daemon holds its lock.
- **Alerts:**
  - CRITICAL whenever the broker is unreachable for more than 10 minutes, at most every 3 hours.
  - On a trade day, CRITICAL immediately.
  - CRITICAL the evening before if not connected.

## Monitoring and decay

`met report` (run automatically after each trade) writes `reports/decay_<date>.md/.png` and `state/monitor.json`. It covers:
- **Paper ledger** from official SPY open and close prices, for every month since 2002.
- **SPRT** from `monitoring.start`: H1 = 20bp per trade, H0 = 0, sd = 78bp, bounds ±2.2. Below −1.1 the size is halved; below −2.2 the bot stops (`auto_derisk`).
- **Response slope:** last-day SPY move per 1% of gap, over all trades and the last 36.
- **Leading indicators** over the last 24 trades: signed overnight gap into the last day, T−1 open→close, TLT on the last day. Warnings fire when the move appears to be shifting earlier.
- **Execution gap:** live fills compared with the paper result.

## Alerts (environment variables only)

| What | Variables |
|---|---|
| Telegram | `MET_TELEGRAM_TOKEN`, `MET_TELEGRAM_CHAT_ID` |
| Email, for WARN and CRITICAL | `MET_SMTP_HOST`, `MET_SMTP_PORT`, `MET_SMTP_USER`, `MET_SMTP_PASSWORD`, `MET_ALERT_EMAIL_TO` |
| Dead-man switch (pinged every 5 min while the daemon runs) | `MET_HEARTBEAT_URL`, e.g. a healthchecks.io check |

## Setup

1. **IBKR:**
   - Enable US futures trading permission.
   - Subscribe to CME real-time L1 data (needed for MES prices and the disaster stop).
   - Open a paper account (DU…).
   - In TWS/Gateway: Configure → API → enable ActiveX/Socket clients, untick Read-Only API, trusted IP 127.0.0.1.
   - **Operational settings that matter:**
     - Turn on "Bypass Order Precautions for API orders" under Global Configuration → API → Precautions. Otherwise a multi-contract market order can be rejected at 09:30:01 (error 201/383).
     - Set the IB Gateway/TWS auto-restart **outside 09:00–16:30 ET**. It is set in the machine's local time, and US daylight saving moves it relative to Asia. In Singapore, pick a time between **06:00 and 20:30 SGT**, which is safe all year. The Docker setup instead sets the Gateway's timezone to ET (`AUTO_RESTART_TIME=11:45 PM`).
     - Use a **dedicated second IBKR username** for the API (free in Client Portal). Logging into TWS or the Mobile app with the same username either blocks market data (error 10197) or logs the Gateway out.
     - US equity market data, so SPY/TLT `ADJUSTED_LAST` daily bars are available for the signal.
     - The machine's clock must be NTP-synced. The bot alerts if it is more than 5 s off IB server time.
     - Set the Gateway/TWS **timezone to America/New_York** (login dialog / IBC `TIME_ZONE`). IB reports executions only since that timezone's midnight.
2. **Install the bot:**
   ```bash
   python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
   cp config.example.yaml config.yaml      # set ib.account to your DU... paper id
   ```
3. **Check everything without trading:**
   ```bash
   python -m met.cli signal  --config config.yaml   # current signal from IBKR + Yahoo
   python -m met.cli check   --config config.yaml   # connect + full preflight, no orders
   python -m met.cli validate-data --config config.yaml  # IBKR vs Yahoo adjusted prices agree?
   python -m met.cli test-order --config config.yaml # PAPER ONLY: buy 1 MES and sell it
   python -m met.cli dry-run --months 4             # whole daemon against the simulator
   ```
4. **Run it continuously** on a machine that is always on (a VPS or home server):
   - systemd: `deploy/met.service`
   - Docker: `deploy/docker-compose.yml`, which uses IB Gateway with IBC auto-login. Copy `deploy/gateway.env.example` and `deploy/trader.env.example` to `.env` files (chmod 600); the Gateway settings persist in `ibgw-settings/`. Set "Bypass Order Precautions" once over VNC, then run `met check` and `met test-order` inside the container before the first trade day.

   IBKR forces a daily Gateway restart and periodic re-authentication. IBC handles most of it, but the bot alerts when it can't connect on a trade day, so keep alerts on.
5. **Paper-trade at least one month-end**, following `PAPER_TEST.md`. The next trade dates are 30 Nov 2026 and 31 Dec 2026.
6. **Going live** (deliberately):
   - Set `mode: live`, the live account `U…` and port 4001 (Gateway) or 7496 (TWS).
   - Export `MET_LIVE_CONFIRM=U…` (your account id).
   - Restart the bot.

## Commands

| Command | What it does |
|---|---|
| `run` | Start the daemon |
| `status` | Show the next trade date, the current state and the monitor result |
| `signal` | Compute the signal for the next trade date |
| `check` | Connect and run the full preflight, with no orders |
| `test-order` | Paper only: buy 1 contract and sell it immediately |
| `flatten` | Close any position the bot owns (asks first; daemon must be stopped) |
| `resolve DATE [--price P]` | Record a manual close the bot cannot see, e.g. one done in TWS without the MET ref (daemon stopped) |
| `kill` / `unkill` | Create or remove the kill switch file |
| `report` | Run the decay monitor now |
| `dry-run --months N` | Run the whole daemon against the simulated broker |

## Tests

```bash
python -m pytest -q
```

The tests cover:
- **Calendar:** holidays, half days, Hurricane Sandy, Memorial Day.
- **Signal:** reproduces the research backtest on all 290+ TLT-era months.
- **Sizing and config validation.**
- **Executor and daemon on simulated clocks:**
  - The happy path (long and short), half-day exit, December contract roll.
  - Kill switch before entry and while holding.
  - Entry rejection, partial fills, exit rejections and endless retries.
  - Connection failure (no late entry), late start, restart mid-trade, disconnect while holding, crash after entry submit.
  - Disaster stop, foreign positions, margin buffer, core policy, source disagreement, missing price.
  - Auto de-risking, live-mode confirmation, wrong account, overnight guard.
- **Decay monitor:** SPRT and the monitor report.
- **Historical replay:** every month-end since 2002 through the simulated broker, checked against the paper ledger.

## Unscheduled exchange closures

The NYSE calendar comes from `exchange_calendars` and is refreshed daily, 5 years ahead. A closure announced at short notice (such as a national day of mourning) is only included after a library upgrade. Until then, set `MET_EXTRA_CLOSURES=2027-01-15` (comma-separated dates) in the bot's environment and restart.

## Known limitations

- **Instrument size:** MES only, capped at `max_contracts` (default 40, about $1.5M notional). Accounts large enough to need ES are not supported yet.

- **Entry and exit prices.** The overlay uses MES at 09:30:01 and 15:59:58. The backtest used SPY's official open and close prints, so expect a small tracking difference; the monitor measures it.
- **IB Gateway login and 2FA** happen outside the bot. Use IBC, and watch for alerts.
