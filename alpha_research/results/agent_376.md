# 376: month-end bid in convertibles (CWB) and preferreds (PFF) beyond SPY/IEF exposure (idea card E3)

Script: `experiments/376_hybrid_monthend_resid.py`. Output: `results/376_output.txt`. Registry:
`results/registry_agent_376.csv` (4 rows). No holdout look, and nothing after 2022 was computed.

**Index schedule.** The SSGA CWB page says the Bloomberg US Convertible Liquid Bond Index is "rebalanced on the
last business day of the month", so W3 ends on the index rebalance date. PFF tracks the ICE Exchange-Listed
Preferred & Hybrid Securities Index. The iShares page does not state a rebalance frequency, so it is unconfirmed.

**Design (fixed before any results).** W3 = enter at the close of the 4th-to-last session and exit at the month-end
close. The residual is ETF − bs·SPY − bb·IEF. The betas are Dimson betas (contemporaneous plus one lag, to handle
stale closes), estimated by OLS on the trailing 252 sessions. They use data up to the close before the entry decision
and stay fixed for the window. Costs are 2bp/side on every leg, charged on turnover. Mean entry betas: CWB bs 0.72 /
bb 0.17; PFF bs 0.48 / bb 0.31.

| Variant | Events tr/va | Train t | Valid t | Valid Sh | Gross / net bp per event (valid) | Timing value vs always-on, t tr/va | Gate |
|---|---|---|---|---|---|---|---|
| V1 CWB residual (2010-04+) | 57/95 | +0.87 | −0.56 | −0.20 | +3.7 / −4.5 | +1.06 / −0.43 | no |
| V2 PFF residual (2008-04+) | 81/95 | +1.60 | +0.56 | +0.20 | +11.6 / +4.4 | +1.53 / +1.26 | no |
| V3 pooled residual | 81/95 | +1.73 | −0.01 | −0.00 | +7.6 / −0.1 | +1.79 / +0.51 | no |
| V4 raw 0.5 CWB + 0.5 PFF | 69/95 | +2.31 | +1.12 | +0.40 | +16.1 / +12.1 | +0.81 / +0.46 | no |

- **Permutation control.** This compares the gross residual in W3 with a random 3-session window from the same
  month, 2000 draws. CWB: one-sided p = 0.004 in train and 0.26 in valid. PFF: p = 0.06 in train and 0.011 in valid
  (+11.7bp versus a random-window mean of −8.3bp). Part of PFF's gap comes from a negative drift in the residual
  outside W3, not from a large bid inside it.
- **Costs.** The two hedge legs cost about 7-8bp per round trip, which absorbs most of the gross residual.
- **Yearly valid P&L.** It is unstable. PFF depends on 2021 (+676bp) and loses 430bp in 2020. CWB is negative in 5
  of 8 years.
- **Raw basket (V4).** It makes money, but the timing value versus always-long is insignificant. Its month-end gain is
  mostly SPY/IEF beta plus the general Treasury-curve bid (LOG 371).
- **Multiple testing.** With about 690 registered tests, luck alone produces a best t of about 3.6. Nothing here
  comes close.

**Verdict: dead or weak.** After removing SPY and IEF there is no reliable extra month-end bid in convertibles, even
though CWB's index rebalances on the last business day. Preferreds show a small extra bid in gross terms (perm p 0.01
in valid), but it is too small to trade after hedge costs. This is consistent with the reading from 371: the
month-end bid is a move in the Treasury curve that passes to rate-linked assets. It is not a broad pattern of
benchmarked cash being deployed at month-end. No variant passes `lib.passes_gate`.
