# Agent 375 - card E8: month-end Treasury bid implemented with futures (ZN/ZB/ZT) + cash-vs-futures basis

Script: experiments/375_bond_bid_futures.py. Registry: results/registry_agent_375.csv (7 rows: 4 pre-specified
variants + 3 labelled ETF benchmarks). Implementation/robustness study of finding 2 (LOG 275-319): **no holdout look**,
nothing dated 2023+ computed into any printed table.

## Setup (pre-specified in the docstring)
- Window W3 = last 3 NYSE sessions (close of 4th-last -> last close), calendar signal so lag=0 as in 275.
- Costs: futures 0.25bp/side, ETFs 1bp/side. Futures returns are excess returns; ETF "excess of bills" rows subtract ^IRX.
- Roll handling: Feb/May/Aug/Nov excluded (card), plus any month whose W3 contains a detected Yahoo contract switch
  (volume >5x prior day and above 20d median). Volume inspection showed Yahoo switches ZN/ZB ~19th-22nd of
  Mar/Jun/Sep/Dec (outside W3, so those months stay) and ZT at the last business day of Mar/Jun/Sep/Dec (inside W3, so
  they drop: ZT keeps only Jan/Apr/Jul/Oct + one Dec). Same months used for each future and its ETF.
- V1 ZN, V2 ZB, V3 ZT, V4 basis IEF - h*ZN - bills (h=0.98, train 5-day OLS). Card's entry-timing variant dropped (<=4).
- Rate-risk unit: TRAIN beta to IEF on non-overlapping 5-day returns (ZN 0.88, ZB 1.40, ZT 0.15, TLT 1.84, SHY 0.15).

## Results (events = months; train <=2014, valid 2015-22; net bp per event incl. entry+exit cost)
| variant | n tr/va | train bp/ev (t) | valid bp/ev (t) | beta to IEF | valid bp per IEF-equiv | gate |
|---|---|---|---|---|---|---|
| V1 ZN=F | 97/64 | +11.9 (1.8) | +19.3 (3.0) | 0.88 | +21.9 | False |
| bench IEF | 97/64 | +17.4 (2.5) | +25.7 (3.2) | 1.00 | +25.7 | True |
| bench IEF excess of bills | 97/64 | +15.8 (2.3) | +24.5 (3.1) | 1.00 | +24.5 |  |
| V2 ZB=F | 99/64 | +20.5 (1.9) | +28.9 (2.4) | 1.40 | +20.6 | False |
| bench TLT | 99/64 | +35.4 (2.6) | +36.1 (2.3) | 1.84 | +19.7 | True |
| bench TLT excess of bills | 99/64 | +33.8 (2.5) | +34.9 (2.2) | 1.84 | +19.0 |  |
| V3 ZT=F | 50/32 | +1.8 (0.8) | +2.3 (1.0) | 0.15 | +15.4 | False |
| bench SHY | 50/32 | +3.9 (2.3) | +4.1 (1.7) | 0.15 | +26.5 | False |
| bench SHY excess of bills | 50/32 | +2.3 (1.3) | +3.0 (1.3) | 0.15 | +19.4 |  |
| V4 basis IEF-0.98ZN-bills | 97/64 | +3.2 (1.8) | +4.6 (2.0) | - | - | False |

Basis control, non-window sessions, gross bp/day x3: train +3.68, valid +0.46

Registry rows:
- V1 long ZN=F last-3 sessions, non-roll months: train_sharpe=+0.45  train_t=+1.72  valid_sharpe=+1.08  valid_t=+3.05  valid_bp_per_trade=+4.84  valid_traded_%=+12.71
- benchmark long IEF last-3, same months as V1: train_sharpe=+0.60  train_t=+2.34  valid_sharpe=+1.14  valid_t=+3.21  valid_bp_per_trade=+6.42  valid_traded_%=+12.71
- V2 long ZB=F last-3 sessions, non-roll months: train_sharpe=+0.46  train_t=+1.79  valid_sharpe=+0.82  valid_t=+2.32  valid_bp_per_trade=+7.22  valid_traded_%=+12.71
- benchmark long TLT last-3, same months as V2: train_sharpe=+0.63  train_t=+2.43  valid_sharpe=+0.72  valid_t=+2.04  valid_bp_per_trade=+9.02  valid_traded_%=+12.71
- V3 long ZT=F last-3 sessions, non-roll months: train_sharpe=+0.21  train_t=+0.80  valid_sharpe=+0.40  valid_t=+1.13  valid_bp_per_trade=+0.56  valid_traded_%=+6.36
- benchmark long SHY last-3, same months as V3: train_sharpe=+0.49  train_t=+1.91  valid_sharpe=+0.66  valid_t=+1.88  valid_bp_per_trade=+1.03  valid_traded_%=+6.36
- V4 basis IEF - 0.98*ZN last-3, minus bills: train_sharpe=+0.34  train_t=+1.33  valid_sharpe=+0.53  valid_t=+1.50  valid_bp_per_trade=+1.15  valid_traded_%=+12.71

V1 ZN=F yearly net bp: {2002: 295, 2003: -133, 2004: -14, 2005: 47, 2006: 27, 2007: 172, 2008: -97, 2009: -55, 2010: 450, 2011: 171, 2012: 136, 2013: 100, 2014: 50, 2015: 97, 2016: 280, 2017: 33, 2018: 139, 2019: 207, 2020: 179, 2021: -62, 2022: 366}  positive 16/21
V2 ZB=F yearly net bp: {2002: 345, 2003: -193, 2004: -24, 2005: 200, 2006: 29, 2007: 350, 2008: -226, 2009: -10, 2010: 790, 2011: 347, 2012: 240, 2013: 96, 2014: 88, 2015: 32, 2016: 328, 2017: 46, 2018: 326, 2019: 450, 2020: 168, 2021: -50, 2022: 548}  positive 16/21
V3 ZT=F yearly net bp: {2002: 14, 2003: -50, 2004: -16, 2005: -11, 2006: 39, 2007: -27, 2008: 46, 2009: 0, 2010: 54, 2011: 33, 2012: 0, 2013: 11, 2014: -5, 2015: -16, 2016: 51, 2017: 9, 2018: -15, 2019: 46, 2020: 31, 2021: -1, 2022: -32}  positive 10/21
V1 months-of-year used: [1, 3, 4, 6, 7, 9, 10, 12]
V2 months-of-year used: [1, 3, 4, 6, 7, 9, 10, 12]
V3 months-of-year used: [1, 4, 7, 10, 12]
ZT Dec months kept (switch not detected inside W3): ['2002-12']; their net bp: [-23.7]

## Reading
- **Futures carry the bid, at roughly the same size per unit of rate risk as ETFs for the long end and somewhat less
  for the belly:** valid bp per IEF-equivalent ZN +21.9 vs IEF +25.7 (+24.5 excess of bills); ZB +20.6 vs TLT +19.7.
  Train t's for futures (1.8-1.9) are below the ETFs' (2.5-2.6), partly because futures sample starts 2002 and the
  futures close is ~15:00 ET vs 16:00 for ETFs (the last hour of month-end, where the closing bid concentrates, is
  missed - consistent with LOG 287/310 timing work; not separately testable on daily data).
- V1 valid t 3.05, Sharpe 1.08, 4.8bp per traded day; fails gate only on train t (1.72). V2 similar (train 1.79).
- ZT/SHY: tiny, insignificant in futures (t ~1) - expected, little duration and only 4-5 months/yr usable.
- **V4 cash-vs-futures basis:** positive (+3.2 / +4.6bp per event, event t 1.8/2.0; daily registry t 1.33/1.50), in the
  predicted direction (cash richens), but the non-window control in train is just as large (+3.7bp per 3 days),
  so in train the month-end basis is not distinct from its drift; only valid shows a gap (+4.6 vs +0.5). Weak.
- Benchmarks IEF/TLT pass lib.passes_gate on this month subset, i.e. finding 2 survives dropping roll months.

## Verdict
Gate (lib.passes_gate): V1 FAIL (train t 1.72), V2 FAIL (1.79), V3 FAIL, V4 FAIL. No holdout look (none allowed here).
Implementation conclusion: ZN/ZB futures are a viable, cheaper (0.25bp) and leverageable vehicle for finding 2, capturing
~85% (ZN) to ~100% (ZB) of the ETF bid per unit of duration in valid; the cash-futures basis tilt is in the predicted
direction but not convincingly separated from its unconditional drift. Multiple testing: with N~685 registered tests,
luck alone gives a best t of ~3.6; these are implementation checks of an already-holdout-tested finding, not new signals.
Caveats: Yahoo continuous futures (unadjusted, switch dates inferred from volume), futures close ~15:00 ET vs ETF 16:00.
