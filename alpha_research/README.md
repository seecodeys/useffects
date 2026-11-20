# Alpha research: US open→close signals

An autonomous research loop that searched for signals to **buy or short US-listed ETFs/stocks at the open and exit at
the close**. It started from one question: do the rest of the world's markets since the last US close predict the US
trading day? Every idea was tested under fixed, pre-registered rules (`PROTOCOL.md`), and every variant was logged
(`results/registry.csv`, 157 tests so far).

## Bottom line

| | Result |
|---|---|
| **Original thesis** (Asia closes, Europe at 9:30 ET, overnight FX/gold → US open→close) | **Dead.** With correct timing, all of it is already in the opening gap (000, 003, 005). |
| **Main finding: month-end stock-bond rebalancing (013)** | **Survived the untouched 2023-2026 holdout:** t=3.3, Sharpe 1.7, +37bp/trade, 67% hit. Confirmed on IWM/QQQ/DIA. |
| OPEX-Friday short (001) | Small persistent tilt; about half as strong in holdout (t=0.8). |
| ADR catch-up (004), single-stock gap reversal (006) | Looked strong, but vanish if you enter 5 minutes after the open → likely opening-print artifacts (015). |
| Everything else (calendar, VIX regime, credit, relative gaps, vol-target, Russell…) | Dead or too weak. |

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

Limits: back-cast to 1993-2002 (never-seen data, bond leg proxied by the 30y yield) it is only weakly positive
(t=0.6, +6.6bp/trade), so the effect looks **era-dependent** (likely tied to the growth of rebalancing-driven assets
after ~2006) rather than permanent. Also: 12 trades a year, so about +2.6%/yr at 1× notional (capital is idle the other days). Even good results
from 46 holdout trades leave a wide uncertainty band. It's also a published, known effect, so it could be traded away.

Check the current state with `python alpha_research/signals.py`. `--backtest` plots it combined with the OPEX tilt.

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
