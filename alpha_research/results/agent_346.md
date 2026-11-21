# 346 — London gold PM fix (card X2)

Script: `experiments/346_gold_pm_fix.py`; table: `results/346_gold_pm_fix.csv`; registry: `results/registry_agent_346.csv` (4 rows).
Data: HistData XAUUSD 1-min 2009-2022 (no 2023+ loaded). Train 2009-2014, valid 2015-2022. Cost 1bp/side.

Clock: checked on US/UK DST-mismatch days with the 08:30 NY data-release |1m return| spike. 2009-2018 spike at file
08:30, so the file is NY local. 2019+ spike at file 07:30, so the file is London-5h (2020-21 are noisier). This matches 330.
Converted to Europe/London.

| variant | ev train/valid | train t | valid t | valid Sharpe | gross bp tr/va | net bp tr/va | yrs+ | gate |
|---|---|---|---|---|---|---|---|---|
| a short 14:30→15:00 London | 1495/2063 | +1.48 | -1.91 | -0.67 | +3.00/+1.00 | +1.00/-1.00 | 5/14 | no |
| b long 15:05→16:00 London | 1496/2061 | +0.39 | -2.02 | -0.71 | +2.34/+0.84 | +0.34/-1.16 | 5/14 | no |
| c a+b same day | 1495/2061 | +1.23 | -2.78 | -0.97 | +3.34/-0.15 | +1.34/-2.15 | 4/14 | no |
| d placebo: short 09:30→10:00 NY on mismatch days | 99/134 | -2.62 | +0.04 | +0.01 | -5.04/+2.09 | -7.04/+0.09 | 4/14 | no |

Descriptive (gross, signed, through 2022):
- (a) Panel era (2009 to 2015-03-19): +3.0bp, t=4.6. IBA era: +0.9bp, t=1.7.
- (b) Panel era: +2.3bp, t=2.7. IBA era: +0.8bp, t=1.4.
- On mismatch days, the real London window gave +2.0bp for (a) (t=1.3) and +0.9bp for (b) (t=0.5). The NY-clock placebo gave about 0.

Verdict: **dead as a trade.** The pre-fix dip and post-fix recovery were real during the bank-panel era, mostly in 2009-11.
The effect decayed to about 1bp gross after the 2015 IBA auction, which is the card's prior. That is below the 2bp
round-trip cost, so valid net is negative for every variant. On mismatch days the placebo is flat while the London
window is positive but weak. That direction fits a London-clock effect, but there is too little data to call it.
Registry here has 4 tests, but across the project the luck threshold sqrt(2 ln N) is about 3.4+.
