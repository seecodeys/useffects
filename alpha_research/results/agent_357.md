# 357 — Dealer gamma regime from the monthly OPEX roll (card P5)

Script: `experiments/357_gamma_regime_opex.py`. Registry: `results/registry_agent_357.csv` (4 rows).
Data: clock-corrected SPX CFD 1-min (HistData SPXUSD, NY local time). The CFD series effectively starts in Nov 2010,
so TRAIN is 2010-11 to 2014 and VALID is 2015-22. No 2023+ data was loaded.
Regime R(t) = CFD 16:00 close(t−1) / CFD 16:00 close on the last monthly OPEX on or before t−1, minus 1. OPEX is the
last trading day on or before the calendar third Friday. Every trade is held 15:30→16:00 at 0.5bp/side.

| Variant (pre-specified) | Events tr/va | Gross train t / valid t | Net train t / valid t | Net bp/trade tr / va | Valid Sharpe (net) | Gate |
|---|---|---|---|---|---|---|
| A R<−2%: sign(prev close→15:30) momentum | 116/227 | +1.91 / −0.72 | +1.72 / −1.06 | +8.6 / −3.1 | −0.37 | no |
| B R>+2%: −sign(09:30→15:30) reversal | 288/420 | −0.15 / −0.87 | −0.99 / −1.73 | −1.2 / −2.0 | −0.61 | no |
| C A+B | 404/647 | +1.58 / −1.09 | +0.98 / −1.87 | +1.6 / −2.4 | −0.66 | no |
| D R<−4% momentum | 47/99 | +0.78 / −0.85 | +0.69 / −1.02 | +7.8 / −5.8 | −0.36 | no |

Yearly gross for A: positive in 2011-16 (511, 173, 85, 344, 188, 111 bp summed), then flat or negative in 2017-22
(2018 −326, 2020 −128, 2021 −171, 2022 −191). The train edge was concentrated in 2011 and 2014 and reversed in VALID.
B (reversal when SPX is above the OPEX level) is negative in both periods. The positive-gamma side of the hypothesis
gets no support.

**Verdict: dead.** No variant passes `lib.passes_gate`, and the valid sign is the opposite of the train sign for A, C and D.
Conditioning on the OPEX-roll gamma proxy does not rescue the intraday momentum/reversal that 288-290/334 found dead
unconditionally. Possible reasons: the proxy is weak (it ignores open interest, 0DTE growth after 2020 and weekly
expiries), or the last-half-hour effect is arbitraged away. A real GEX series is not freely available.
