# Alpha research: US open→close signals

An autonomous research loop that searched for signals to **buy or short US-listed ETFs/stocks at the open and exit at
the close**. It started from one question: do the rest of the world's markets since the last US close predict the US
trading day? Every idea was tested under fixed, pre-registered rules (`PROTOCOL.md`), and every variant was logged
(`results/registry.csv`, ~420 tests so far). Later rounds widened the scope to overnight/multi-day holds, behavioral
signals, participant flows and options.

## Bottom line

| | Result |
|---|---|
| **Original thesis** (Asia closes, Europe at 9:30 ET, overnight FX/gold → US open→close) | **Dead.** With correct timing, all of it is already in the opening gap (000, 003, 005). |
| **Main finding: month-end stock-bond rebalancing (013)** | **Survived the untouched 2023-2026 holdout:** t=3.3, Sharpe 1.7, +37bp/trade, 67% hit. Confirmed on IWM/QQQ/DIA. |
| **Second finding: month-end bond bid (275-284)** | Long Treasuries over the last 3 sessions of each month: positive in 21/21 years 2002-22 (TLT), t>3 on IEF/SHY/TIP/MBB in both periods, also IG credit, Canada; weak in munis. Holdout: IEF Sharpe 0.83 (t=1.6), TLT 0.31 — weak-but-alive. Mechanical (index extension), independent of the stock-bond gap. Back-cast on FRED yields (315): absent 1962-82, present in every decade since 1983 — matches the rise of bond indexing. |
| OPEX-Friday short (001) | Small persistent tilt; about half as strong in holdout (t=0.8). |
| ADR catch-up (004), single-stock gap reversal (006) | Looked strong, but vanish if you enter 5 minutes after the open → likely opening-print artifacts (015). |
| **Month-end signal via options (083)** | Selling a same-day ATM option against the signal survived holdout (model-priced): t=3.5, Sharpe 1.8, 80% hit. Model overstates seller edge ~35% (087) → realistic ~+10bp/trade. |
| Weak but consistent (never passed the gate) | Monthly country reversal (062b, ~1-1.5bp/day gross since 2010); leveraged-ETF overnight reversal after >2% days (112, growing since 2009); Treasury auction cycle on TLT (133, passes gate but concentrated in 2009-10/2020). |
| Behavioral / psychology round (050-095) | 30+ variants (over-reaction, attention, streaks, fear, anchoring, lottery, mood, weather, DST, lunar…): none survive in liquid US assets after ~2010. |
| Flow round (110-134) and paper-sourced ideas (120-123) | Turn-of-month, buyback blackout, futures roll, Ponzi-fund flows, LETF pre-positioning, vol-control, LETF trend: dead or risk overlays only. |
| Everything else (calendar, VIX regime, credit, relative gaps, vol-target, Russell, FOMC, jobs days…) | Dead or too weak. |
| New-data round (298-312): CFTC futures positioning (equity, Treasury, VIX, commodities), Fed net liquidity, tax-date reserve drains, FINRA short volume (DIX-style), weekend crypto, weekly-expiry Fridays | All dead or weak (best: net-liquidity long/flat overlay, timing t=1.2). Mechanism checks on the two findings: the bond bid doesn't scale with issuance or futures positioning, is uniform across month types and absent in bond-proxy equities; the equity month-end trade survives delayed entry (309). |

## The month-end rebalancing signal

On the **last trading day of each month**, using month-to-date returns through the **previous close**:

```
gap = SPY month-to-date return − TLT month-to-date return
position in SPY from open to close = −sign(gap)      # stocks beat bonds → short; stocks lagged → long
```

Why it could work: balanced and pension funds rebalance back to target stock/bond weights at month-end. When stocks
outperformed, they sell stocks into the close, and the reverse when stocks lagged. The evidence fits that story:

- The bigger the imbalance, the bigger the move: mean edge of +5 / +18 / +39bp across |gap| terciles.
- The last hour (15:30→close) has the right sign 81% of the time, consistent with orders placed for the closing auction.
- It works with long-duration Treasuries (TLT) and fades with intermediate ones (IEF). It is weaker for international
  equities and absent for sector/size rotation.

| Period | t | Sharpe | bp/trade |
|---|---|---|---|
| Train 2002-2014 | +3.34 | 0.95 | — |
| Valid 2015-2022 | +2.01 | 0.71 | +17 |
| **Holdout 2023-Nov 2026** | **+3.30** | **1.68** | **+37.5** (46 trades, 67% hit) |
| Holdout at 2bp/side costs | +3.09 | 1.57 | +34.5 |

Refinements that also survived holdout: the SPY−TLT pair version (013g: +39bp/trade, 76% hit) and a volatility filter
(168: trade only when SPY's 20-day vol is above its 2002-14 median → +61.5bp/trade, 78% hit, 18 trades). Caveat: the
month-end family has now had ~8 holdout looks (013, 013c×3, 013d, 013g, 083, 168), so treat these as one finding
with variants, not independent confirmations.

International replication (257): the same rule on local equity/bond ETFs is positive in Canada, the UK, Australia
and the euro area in 2015-22 (pooled t=3.35), but Canada — the only one passing the gate — failed its 2023+ holdout.

Non-parametric check (013h, 2002-22): shuffling the trade signs never matches the real +21bp/trade (p<0.0001), and
the same rule on random non-month-end days earns +0.8bp (p≈0.0005) — the effect is tied to the last trading day.

Extra detail: the edge sits in ORDINARY month-ends (+27bp/trade, t=4.1 in 2002-22) more than quarter-ends (+5.6bp)
or year-ends (+18.6bp) (116); it is a last-day effect (no mid-month, day −2/−3 or first-day-unwind effect).

Limits: back-cast to 1993-2002 (never-seen data, bond leg proxied by the 30y yield) it is only weakly positive
(t=0.6, +6.6bp/trade), and a 1962-92 back-cast (316) shows nothing at all, so the effect looks **era-dependent** (likely tied to the growth of rebalancing-driven assets
after ~2006) rather than permanent. Also: 12 trades a year, so about +2.6%/yr at 1× notional (capital is idle the other days). Even good results
from 46 holdout trades leave a wide uncertainty band. It's also a published, known effect, so it could be traded away.

Check the current state with `python alpha_research/signals.py`. `--backtest` plots it combined with the OPEX tilt.


## Month-end flow book (279, construction on 2002-22)
Combining the SPY month-end trade (vol-tilted), the IEF last-3-sessions month-end bond bid (276) and monthly country
reversal (062b) — uncorrelated legs — gives Sharpe 1.14 (2003-14) and 1.38 (2015-22) with ~2% max drawdown. Scaled to 10% volatility (≈4.6× notional on active days, e.g. via futures) the combo
would have returned ≈+11-14%/yr before financing and slippage (279b). The
SPY leg is holdout-confirmed; the IEF leg is weak-but-alive in 2023+ (Sharpe 0.83, t=1.6, 276b).

## Month-end playbook (how the surviving signal would actually be traded)

1. **Day before the last trading day, after the close:** run `python alpha_research/signals.py`. It reports SPY and TLT
   month-to-date returns, the stock-bond gap, the date of the next month-end, and whether SPY's 20-day volatility is above
   its 2002-14 median.
2. **Direction:** stocks beat bonds month-to-date → short SPY for the day; stocks lagged → buy SPY. The sign is
   computed from the prior close, so it's known before the open.
3. **Execution:** enter with a market-on-open order (or at the 9:30 auction), exit with market-on-close. In 2010-22 1-min
   data (287, clock-corrected) the move is spread across the session, so enter at the open and hold to the close. Entering up to 30 minutes late keeps essentially all of the edge on the clock-corrected 1-min data (309), so it is not an opening-print artifact.
4. **Sizing:** risk per trade is big compared with the edge. 2002-22: worst trade −176bp, 5th percentile −108bp, up to
   7 losses in a row, average |move| 58bp vs a mean edge of ~17-37bp. Size so that a −2% day is tolerable. Size up
   on high-volatility month-ends (168: ~+44-62bp/trade historically) and down on calm ones.
5. **Variants:** the SPY−TLT pair (013g) gives a higher hit rate with the same edge; selling a same-day option against the
   signal (083) is smoother but model-priced, so treat it as an upper bound.
6. **Caveats:** it's one finding with ~8 holdout looks across variants; it was weak before 2002 and in 2010-14; it's
   published, and crowding could erode it. Paper-trade it for a few months first.

## What didn't work, and why it matters

- **The open already prices in the world.** Overseas equity moves correlate 0.4-0.9 with SPY's opening gap and about 0
  with what happens after the open. That holds for Asian closes, European levels at exactly 9:30 ET (HistData 1-min),
  and overnight USDJPY/AUDUSD/gold.
- **Pre-2010 mirages.** Several open-based relative-value signals (country ETFs vs home markets, sector gap reversal,
  country-ETF opens predicting SPY) earned 20-45bp/day in the early 2000s and decayed to below costs by about 2015.
  Huge train-period t-stats from opening prices are a warning sign, not a discovery.
- **Opening-print artifacts.** The "edges" that survived into recent data (ADRs, single-stock gap reversal) exist
  only at the printed open. Entering at 9:35 erases them, which is the signature of bid/ask bounce in the open price.

## Method

- Periods are fixed: TRAIN 2000-14 (explore), VALID 2015-22 (confirm), HOLDOUT 2023+ (one look per idea that passed
  the gate). Gate: train t≥2, valid t≥2, valid Sharpe≥0.5, ≥1bp/trade after costs.
- Only information available before 9:30 ET is used. Exchange timezones, holidays and dividends are handled
  (`lib.py`, `global_open_close/features.py`).
- Costs per side: 0.5bp for liquid index ETFs, 1-3bp for other ETFs, ADRs and stocks.
- **Caveats:**
  - Yahoo data; the hand-picked stock universe in 006 is survivorship-biased.
  - The SPY+overseas holdout was partly seen by the pre-protocol study.
  - With 157 tests, luck alone produces a best t of about 3.2. Only 013 has an out-of-sample result at that level.

## Files

| Path | What |
|---|---|
| `PROTOCOL.md`, `BACKLOG.md`, `LOG.md`, `FINDINGS.md` | Rules, idea queue, full lab notebook, survivors |
| `lib.py` | Data download/cache, period splits, scoring, registry, holdout logging |
| `experiments/NNN_*.py` | One script per experiment, re-runnable (`data/` is a cache and isn't committed) |
| `tools/histdata.py` | Free 1-minute European index and FX bars (HistData.com) |
| `signals.py` | Live month-end/OPEX signal status + combined backtest |
| `results/` | Registry, holdout log, per-experiment outputs, charts |
