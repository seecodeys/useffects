# 355 — Scheduled-event volatility premium (FOMC / CPI): short VIXY over the event day, WPUT in event weeks

Idea card 7, results/ideas_options_etf.md (Londono-Samadi IFDP 1376; Wright NBER 28306).
Script: experiments/355_event_vol_premium.py. Registry: results/registry_agent_355.csv (8 rows). Output: results/355_event_vol_premium.csv.
This does not duplicate earlier work. 024 traded SPY direction on FOMC days, 291 faded the 14:00 FOMC reaction, and 220 split WPUT by weekday. None of them priced the vol premium itself.

## Data and timing
- **FOMC:** `tools/fomc_dates.csv`, scheduled meetings only.
- **CPI release dates:** taken from the ALFRED vintage dates of CPIAUCSL, using the keyless `alfredgraph.csv?id=…&vintage_date=…`. A vintage counts as a CPI release only when it adds a new latest month. This drops the February seasonal-factor revisions. The result is exactly 12 releases per year for 2000-22. Cached in `data/cpi_release_dates.csv` and `data/cpi_vintages.csv`.
- **BLS:** the BLS schedule page returns 403. It was not needed and not bypassed.
- **Instruments:** VIXY (Yahoo, 2011+, adjusted cc) and CBOE WPUT (2006+).
- **Timing:** event dates are published in advance. Weights are decided at close t-2 and run through `lib.backtest_weights(..., lag=1)`, so each position is held from close(t-1) to close(t). CPI prints at 08:30 and FOMC at 14:00, both inside that window.
- **Costs:** 2bp/side.
- **Periods:** train is 2011-14 for VIXY and 2006-14 for WPUT. Valid is 2015-22. Nothing from 2023 onward was loaded or printed.
- **Timing value (TV):** net P&L minus an exposure-matched always-on position (always-short VIXY or always-long WPUT; benchmark not charged costs). The verdict uses the TV rows.

## Pre-specified variants (4; each registered as raw and TV)
| Variant | Events tr/va | raw t tr / va | **TV t tr / va** | net bp/event va | TV bp/event tr / va | gate |
|---|---|---|---|---|---|---|
| V1 short VIXY on FOMC day | 32 / 63 | 1.62 / 1.94 | **1.38 / 1.71** | +110 | +109 / +96 | no |
| V2 short VIXY on CPI day | 48 / 94 | −0.17 / 0.89 | **−0.50 / 0.58** | +41 | −34 / +26 | no |
| V3 long WPUT in FOMC weeks | 72 / 63 | 1.16 / −1.42 | **0.63 / −1.67** | −29 | +13 / −32 | no |
| V4 long WPUT in CPI weeks | 107 / 96 | 0.56 / −0.05 | **−0.23 / −0.24** | −1 | −4 / −4 | no |

The always-on comparison, over the same number of days, earns +22 / +15 bp per event for short VIXY and +13 / +3 bp per week for WPUT.

## Reading the results
- **V1 (VIX crush on FOMC day)** is the only variant with a consistent sign. Short VIXY earns about 1% per FOMC day net of costs, and about 95-110bp per event above the carry baseline. Train TV t is 1.38 and valid TV t is 1.71. Yearly TV is positive in 10 of 12 years, with the losing years 2017 and 2021 both small. The largest contributions are 2012, 2020 and 2022. It fails the gate because train t is below 2. The VIXY train sample is only 4 years and 32 events, so the test has little power. There were 8 tests in this run (luck t ≈ 2.0), and the project has roughly 500 overall (luck t ≈ 3.5).
- **V2 (CPI day)** reverses sign between train and valid: −34bp/event in train, +26bp in valid. The valid gain is mostly 2014-19. Dead.
- **V3 and V4 (WPUT in event weeks)** show no extra put-write premium in weeks that contain FOMC or CPI. V3 is negative in valid and V4 is about zero. With real weekly option prices, any event premium is either not there or is cancelled by the realized moves on event days. Dead.

## Verdict
No variant passes `lib.passes_gate`, and there was no holdout look. The FOMC-day VIX-ETP crush (V1) is **weak / promising**: right sign in both periods, about +100bp/event TV and stable across years, but underpowered (t 1.4 / 1.7). Possible follow-up: a longer history for the same mechanism, for example VIX-futures or VXX-replica returns over FOMC days 2006-10 as a fresh pre-sample, tested with the exact V1 rule. The CPI-day and WPUT event-week versions are dead.
