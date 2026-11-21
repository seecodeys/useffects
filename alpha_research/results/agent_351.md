# 351 — Nasdaq-100 covered-call (QYLD) roll: dealer net-delta flow on the session before monthly OPEX

Card 2 of results/ideas_options_etf.md. Script: experiments/351_qyld_roll.py. Registry: results/registry_agent_351.csv (4 rows).
No 2023+ data used (everything sliced to 2022-12-31); no holdout call.

**Signal** (known at the prior close): m = NDX close the session before the roll / NDX close on the previous roll day − 1
(old ATM strike proxy); S = sign(m). Roll day = session before monthly OPEX (3rd Friday or prior session if holiday).
Story: dealer net hedge trade on the roll ≈ (Δ_old − 0.5) × QYLD notional → buy NDX if above old strike.

Not a duplicate of 001/021/297 (unconditional OPEX Friday/week) or 245 (Thu close → Fri open): this is the conditional,
strike-signed Thursday.

## Variants (pre-specified, 4 registered)
| | Variant | Train t | Valid t | Valid Sharpe | Valid net bp/trade | Gate |
|---|---|---|---|---|---|---|
| A | S × (QQQ − β·SPY) open→close, 0.5bp/side/leg | −0.49 | +0.75 | 0.27 | +3.0 | no |
| B | S × NSX CFD 12:00→15:59 (2010+), 0.5bp/side | +1.31 | +2.05 | 0.73 | +14.0 | no (train t) |
| C | A only if \|m\| > 2% | −0.23 | +1.31 | 0.46 | +6.4 | no |
| P | Placebo: A's rule one week earlier | +0.32 | −0.34 | −0.12 | −1.6 | (placebo ≈ 0, as expected) |

Events: 12/yr; train 179 (A), valid 96. β (QQQ on SPY, rolling 250d) averages ~1.1.

## By period (net bp/trade, t) — AUM dose-response (QYLD < $1bn before ~2018, $1–8bn 2019-22)
| | 2000-13 (no fund) | 2014-18 | 2019-22 |
|---|---|---|---|
| A spread | −3.3 (−0.49) | +1.9 (+0.49) | +3.6 (+0.56) |
| A at 1bp/side/leg | −5.5 | −0.3 | +1.4 |
| C | −2.1 | +5.4 (+1.10) | +6.7 (+0.90) |
| B NSX afternoon | +13.8 (+1.59, 2010-13) | −5.9 (−0.92) | +36.0 (+3.48) |
| P placebo | +3.3 | −6.4 | +1.4 |

By year (bp sum, ~12 trades/yr), 2014→2022: A 1, 28, −57, 96, 45, −29, −86, 339, −49; B −52, −48, −3, 150, −400, 304, 373, 184, 723.

## Reading
- The market-neutral test of the mechanism (A, QQQ − β·SPY) is flat: a few bp/trade, t < 1 in every sub-period;
  corr(m, spread) on roll days 2014-22 = 0.10. Rising monotonically with AUM (−3 → +2 → +4bp), but tiny and
  insignificant; at 1bp/side/leg it is ≈ 0. Scale check: even at $8bn AUM the roll hedge is at most ~±$4bn, small vs
  NDX futures + QQQ daily volume, so a weak price impact is what you would expect.
- B (outright NDX afternoon) looks better (valid t 2.05, 2019-22 t 3.5), but it fails train t, it was negative in 2014-18,
  2022 alone is ~45% of the 2019-22 sum, and as an outright directional bet on sign(month-to-date NDX move) it is
  confounded by trend/afternoon momentum in a trending 2020-22 market. The spread version that strips out the market
  shows almost nothing, so B's edge is unlikely to be the QYLD flow. Luck benchmark: with the project's ~400+ tests,
  best-of-noise t ≈ 3.5.
- Caveat: the Thursday VWAP roll is the BXNT (V2) rule; older BXN rolled on the Friday (11:30-13:30 VWAP), and the date
  QYLD switched indices was not verified — pre-switch the flow would sit on OPEX Friday, where 001/297 already live.
- Deviation note: the card suggested 2014-18 as "train"; registry uses the standard periods (train is mostly a no-fund
  control), with the 2014-18 / 2019-22 split reported above.

**Verdict: dead (A/C), B weak and confounded — no gate passer.** Possible follow-up (new test, counts against N): B's
rule on placebo Thursdays / non-roll afternoons to check whether it is just afternoon trend-following.
