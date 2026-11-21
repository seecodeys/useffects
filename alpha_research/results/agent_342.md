# 342 — Mutual-fund Oct-31 fiscal year-end: YTD losers sold into Oct 31, rebound in November (card B9)

Script: `experiments/342_oct31_fund_taxloss.py` · registry: `results/registry_agent_342.csv` (4 rows) · events: `results/342_events.csv`

**Setup.** Universe: 9 sector SPDRs + IWM + 20 iShares country ETFs (062 list), survivorship-free. YTD rank =
adj. close on last day ≤ Oct 20 (Jun 20 for placebo) vs prior year-end; bottom/top floor(N/5) names, equal-weight,
dollar-neutral. Close→close, `lib.backtest_weights(lag=1)`, 2bp/side. 23 events (2000-2022), one per year.
Not a duplicate of 046 (Dec/Jan IWM−SPY).

| Variant | Train t | Valid t | Valid Sharpe | Valid net bp/day held | Train / valid bp per event (event t) | Gate |
|---|---|---|---|---|---|---|
| A long losers−winners, Oct-end→Nov-end | −1.03 | +1.75 | 0.62 | +14.9 | −136 (−1.6) / +308 (+1.7) | no |
| B pressure leg (short losers) Oct 21→Oct-end | −0.78 | +0.60 | 0.21 | +5.5 | −78 (−1.0) / +37 (+1.6) | no |
| C as A, first 10 Nov sessions | −1.08 | +2.23 | 0.79 | +33.2 | −112 (−2.5) / +332 (+2.1) | no |
| D placebo Jun-end→Jul-end | +0.17 | −0.28 | −0.10 | −1.7 | +20 (+0.2) / −35 (−0.2) | no |

Timing benchmark (same Oct-20 loser−winner basket held all year, gross): train −0.6 bp/day, valid +1.7 bp/day.

**Findings.** The November rebound has the WRONG sign in train (2000-14: losers lagged winners in 11 of 15
Novembers for A) and the right sign in valid only because of 2020 (+1007bp, vaccine rotation into energy/financials)
and 2022 (+1262bp). Excluding those two, valid A averages ≈ +32bp/event. The pressure leg (B) is also negative in
train. The placebo behaves as a placebo. The sign flip between periods and two-year concentration show no stable
October-deadline effect in sector/country ETFs. One caveat: the fund-level effect may exist only in single stocks,
which ETFs dilute. With N≈4 here (and hundreds in the program), sqrt(2 ln N) is about 1.7 locally and much larger globally.

**Verdict: dead.** No variant passes `lib.passes_gate` (train t negative for every rebound variant). No holdout look.
Nothing from 2023 onward was read.
