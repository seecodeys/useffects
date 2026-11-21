# 329 — Gotobi-day Tokyo-fix dollar buying (card B1), USDJPY

Script: `experiments/329_gotobi_fix.py`. Registry: `results/registry_agent_329.csv` (4 rows).
Data: HistData USDJPY 1-min 2001-2022 (fixed EST → JST = EST+14h; 09:55 JST = 19:55 EST previous day). Tokyo business
days from a rule-based JP bank-holiday calendar (matches 1306.T trading days 2009-22 except 4 data gaps / the 2020-10-01
TSE outage). Gotobi = 5/10/15/20/25/month-end, rolled to the preceding business day. Cost 0.5bp/side. 2023+ not loaded.

| Variant (pre-specified) | events train/valid | train t | valid t | valid Sharpe | train bp/trade | valid bp/trade | gate |
|---|---|---|---|---|---|---|---|
| V1 long 08:00→09:55 JST, gotobi | 911 / 569 | +3.68 | +0.62 | +0.22 | +1.86 | +0.37 | no |
| V2 short 09:55→11:00 JST, gotobi (fade) | 967 / 571 | +0.43 | −1.69 | −0.61 | +0.23 | −0.89 | no |
| V3 as V1, 25th + month-end only | 300 / 188 | +1.55 | −0.88 | −0.32 | +1.45 | −1.06 | no |
| V4 control: long 08:00→09:55, non-gotobi | 2190 / 1373 | −1.63 | −2.19 | −0.79 | −0.56 | −0.70 | (control) |

Timing value (gross, gotobi minus non-gotobi, 08:00→09:55): train +2.41bp (t=3.98), valid +1.08bp (t=1.57).
Card (d) "2015+ only" = V1 valid column: +1.37bp gross / +0.37bp net. V1 gross by year is positive in 19/22 years
(2016 −2.2bp), so the effect exists and is not one-year driven, but it roughly halved after 2014 and is ~1 cost round trip.
Post-fix fade (V2) gross was +1.2bp in train and ~0 in valid. V3 (big settlement days) is no stronger than all gotobi days.

Multiple testing: with N≈474 registered tests across registries, sqrt(2 ln N) ≈ 3.5, so the train t of 3.7 is only just above the luck level.

**Verdict: weak.** A real but small pre-fix drift on gotobi days (train t 3.7; valid gross +1.4bp, t 1.6 vs control),
too small after 0.5bp/side costs. Nothing passes the gate (no holdout look). At the card's 1 pip/side cost it is dead.
