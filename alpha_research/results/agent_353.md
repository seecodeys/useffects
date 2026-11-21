# 353 — SPY ex-dividend day vs S&P 500 twins (IVV, VOO) — card 6, ideas_options_etf.md

Script: `experiments/353_spy_exdiv_twins.py`; registry `results/registry_agent_353.csv` (4 variants); events
`results/353_events.csv` (≤2022 only). Raw Yahoo closes/opens plus the Yahoo `events=div` dividend amount on each fund's
own ex-date; spread = ex-fund total return minus twin total return. Cost 0.5bp/side per leg (1.0bp/side on the spread).
Ex-dates are announced in advance, so the position is known at the prior close. No twin ever went ex on the same day.

| Variant | Events tr/va | Train t | Valid t | Valid Sharpe | Valid net bp/trade | Gate |
|---|---|---|---|---|---|---|
| A long SPY/short IVV, SPY ex-date, close→close | 60/32 | -1.28 | +0.78 | +0.28 | +0.78 | fail |
| B long IVV/short SPY, IVV ex-date, close→close | 60/33 | +0.38 | -2.26 | -0.80 | -2.24 | fail |
| C long VOO/short SPY, VOO ex-date, close→close (2010+) | 17/32 | -1.79 | -1.44 | -0.51 | -1.24 | fail |
| D pooled A–C, prev close→ex open | 134/93 | -0.02 | -2.79 | -0.99 | -1.55 | fail |

Gross mean spreads are ±1–4bp against a ~45bp dividend. The median drop ratio ΔP/D sits at 0.96–1.02 in every
variant and period, so the dividend is fully priced on the ex-day, both overnight and close to close. The signs flip
between train and valid. Train noise is large (A's SD is 20bp, driven by 2000 and 2008). With 4 tests the luck
threshold is about 1.67. The only |t|>2 results (B and D in valid) have the wrong sign, and they are cost drag on a
gross value near zero.

**Verdict: dead.** I found no ex-day mispricing between twins. Any early-exercise or dividend-capture flow is fully
arbitraged at the 1bp level.
