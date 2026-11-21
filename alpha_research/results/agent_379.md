# 379 — Target-date-fund glide-path resets in early January

**Idea.** Many TDF series step their equity/bond mix down once a year (often effective January; some quarterly): an
unconditional, calendar-known sale of equities / purchase of bonds. Distinct from 046 (IWM−SPY January effect), 110
(TOM long SPY), 213 (fade prior-month SPY−AGG over first 3 sessions) and 250 (YTD-gap fade at year-end): here the
prediction is direction-fixed — bonds beat equities (vol-matched) in early January, more so post-2010.

**Setup.** Spread = −1 SPY + h·bond, h = trailing-252d vol(SPY)/vol(bond) (cap 5), set at the close before session 1,
held close→close (calendar-known → lag 0). Costs 0.5bp/side SPY, 1bp/side TLT/AGG per leg turnover. TLT from 2003, AGG
from 2004. Script: `experiments/379_tdf_glide_january.py`; registry `results/registry_agent_379.csv` (4 rows).

## Pre-specified variants (train t / valid t, net)
| Variant | train t | valid t | valid bp/unit-day | events train/valid | net bp/event train / valid | gate |
|---|---|---|---|---|---|---|
| 379a −SPY+h·TLT, Jan sessions 1-5 | −1.49 | −0.33 | −5.0 | 12 / 8 | −190 / −53 | no |
| 379b −SPY+h·TLT, Jan sessions 1-10 | +0.08 | +0.40 | +3.7 | 12 / 8 | +14 / +78 | no |
| 379c −SPY+h·AGG, Jan sessions 1-5 | −0.27 | −0.41 | −2.3 | 11 / 8 | −30 / −57 | no |
| 379d −SPY+h·TLT, sessions 1-5 of Jan/Apr/Jul/Oct | −1.65 | −1.49 | −9.7 | 48 / 32 | −106 / −106 | no |

## Descriptive (gross, first-5-session sums, ≤2022)
- January vs other month-starts (TLT spread): 2003-09 Jan −124bp vs others −49 (diff t −0.27); 2010-22 Jan −139 vs
  −84 (diff t −0.46). AGG: 2004-09 +97 vs −31; 2010-22 −99 vs −67. No January-specific bond-over-equity premium, and
  the post-2010 (high TDF AUM) period is if anything *more* negative — opposite to the mechanism's prediction.
- Timing value vs exposure-matched same-month average: TLT 2010-22 −159bp t −1.9; AGG −138bp t −1.9 (wrong sign).
- Legs, first 5 sessions: Jan SPY +59bp / TLT −57bp vs other months SPY +37 / TLT −30 — equities are, if anything,
  stronger in early January (new-year inflows dominate any glide-path selling).
- Jan by year (TLT spread, bp): 03:+0 04:−184 05:+201 06:−353 07:+184 08:+774 09:−1491 10:−375 11:−319 12:−457 13:−366
  14:+122 15:+291 16:+826 17:+96 18:−329 19:−292 20:+26 21:−838 22:−190. Noisy, one event per year, no trend up.
- Note: a generic early-month weakness in the vol-matched bond-minus-SPY spread (TLT −30bp over sessions 1-5 on
  average) is visible in all months — consistent with the month-end bond bid (013 mirror) unwinding; not TDF-specific.

## Verdict
**Dead.** No variant passes `lib.passes_gate`; the January sign is opposite to the hypothesis in 3 of 4 variants and
in the post-2010 subsample. Glide-path steps are likely too small (~1-2% of TDF equity once a year), often implemented
gradually/monthly, and swamped by January equity inflows. Power is low (one event/yr; 20 Jan events). With 4 tests in
this registry, luck t ≈ 1.67; nothing reaches it in the hypothesised direction. No holdout look taken.
