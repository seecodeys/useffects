# 333 - Buyback desks support dips only in the open window (card B10)

**Verdict: dead.** No variant passes `lib.passes_gate`; the open-minus-blackout interaction flips sign between train and valid.
Registry: `results/registry_agent_333.csv` (4 tests; luck t for best of 4 ~1.67). Script: `experiments/333_buyback_dip_support.py`.

## Setup
- Blackout calendar from EDGAR 8-K Item 2.02 dates (15 hindsight mega-caps, `data/edgar_202_*.csv`, same report-date
  rule as 328). Company in blackout from quarter-end -14d through its report day. Session = BLACKOUT if >= 8/15 in
  blackout (43% of sessions 2005-22), else OPEN. Status taken on the trigger day.
- Trigger: SPY close-to-close <= threshold at close t. Long SPY close t+1 -> close t+4 (`backtest_weights`, lag=1,
  overlaps capped at 1), 0.5bp/side. Events 2005-2022 only, nothing 2023+ read.
- Timing value = net minus exposure-matched always-long (held weight x period-mean SPY daily return).

## Events (trigger days)
| Threshold | Train open / blackout | Valid open / blackout |
|---|---|---|
| -1.5% | 107 / 71 | 74 / 62 |
| -1.0% | 201 / 134 | 119 / 112 |

## Results (pre-specified variants)
| Variant | train Sh / t | valid Sh / t | timing t (train / valid) | timing bp/held day | gate |
|---|---|---|---|---|---|
| A open-window dips <=-1.5% | +0.23 / +0.73 | -0.18 / -0.52 | +0.44 / -0.80 | +5.8 / -12.7 | no |
| B blackout dips <=-1.5% | -0.17 / -0.52 | +0.54 / +1.52 | -0.72 / +1.21 | -13.4 / +17.7 | no |
| C open minus blackout (-1.5%) | +0.30 / +0.94 | -0.49 / -1.39 | +0.85 / -1.42 | +8.7 / -15.6 | no |
| D open minus blackout (-1.0%) | +0.23 / +0.72 | -0.31 / -0.86 | +0.59 / -0.88 | +3.9 / -6.7 | no |

The interaction (C, D) is the test: right sign in train but small (t < 1) and driven by 2008 (+5249bp in C), then the
wrong sign in valid (2020 -3251bp: the March-2020 crash days fell in the open window). In valid, dips during the
blackout rebounded MORE than open-window dips, the opposite of the hypothesis. Consistent with 113 and 328: no
index-level buyback-blackout timing, now including the dip interaction.
