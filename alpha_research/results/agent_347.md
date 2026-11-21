# 347: Norges Bank's pre-announced daily NOK transactions (card X3)

**Data:** Norges Bank table "Foreign exchange transactions on behalf of the government" (NOK m per business day,
monthly, Jan 2000 to Nov 2026), scraped from the public statistics page and cached as `data/norgesbank_fx_gov.html`.
Sign: negative means NOK purchases. Each month's amount is announced on the last business day of the prior month.
Five cells were changed within the month (May 2003, Oct 2008, Mar 2020, Nov 2020, Nov 2021). For these the table shows
the adjusted amount, not the original pre-announced one, so those months are not traded. December is also excluded.
**Prices:** Yahoo EURNOK=X (2000+), NOK=X (2001+), NORW-EFA (2009+). Costs 2bp/side.
**Timing:** close-to-close returns using `backtest_weights(lag=1)`. The signal is fixed at the close of the
announcement day, and the position runs from the next close to one day after the month-end close.

## Results (stats stop at 2022; registry `results/registry_agent_347.csv`, 4 tests)
| Variant | Train trades / net bp per trade / t (monthly) | Valid trades / net bp per trade / t (monthly) | Registry train t / valid t / valid Sharpe | Gate |
|---|---|---|---|---|
| a EURNOK, change of 200m/day or more (long EURNOK when NOK selling steps up) | 29 / +22.8 / +0.59 | 20 / +89.2 / +1.38 | +0.57 / +1.27 / +0.44 | fail |
| b EURNOK, sign of the level | 116 / +0.5 / +0.03 | 83 / -3.7 / -0.14 | +0.02 / -0.13 / -0.05 | fail |
| c USDNOK, same as a | 29 / +13.6 / +0.19 | 20 / +121.2 / +1.26 | +0.19 / +1.30 / +0.45 | fail |
| d NORW-EFA, same as a | 9 / -104.8 / -1.15 | 20 / +5.4 / +0.07 | -0.95 / +0.06 / +0.02 | fail |

The valid results for a and c come mostly from 2020: +1032bp of about +1760bp for a, and +1673bp for c. In April 2020
NOK buying stepped up and NOK rallied. Without 2020 the change signal is close to zero. The level (b) has no information.
The tradable ETF version (d) has the wrong sign in train. With N=4, luck alone gives a t-stat of about 1.7.

**Verdict: dead.** The flow is fully pre-announced and therefore priced. All four variants fail `passes_gate`, so there
was no holdout look.
