# 364 — Treasury Quarterly Refunding Announcement (QRA) window in TLT/IEF (idea card S6, results/ideas_settlement.md)

Script: `experiments/364_qra_window.py`. Events: `results/364_qra_events.csv` (QRAs through 2022-11 only).
Registry: `results/registry_agent_364.csv` (8 rows = 4 pre-specified variants × {raw, timing value}). No holdout look.

**QRA dates come from Treasury's archive and are not reconstructed.** The public page "Official Remarks on Quarterly
Refunding by Calendar Year" (home.treasury.gov quarterly-refunding archives) links every quarterly refunding statement.
For 2002-2022 the script fetches each press release (84 statements, cached in `data/qra/`) and reads the statement date
from the page. None of the pages were blocked. The dates match the usual schedule, a Wednesday in the last week of
Jan/Apr/Jul/Oct or the first week of Feb/May/Aug/Nov. One window is short: 2012-10-31 has a single session, because
markets were closed Mon-Tue for Hurricane Sandy. TLT and IEF start on 2002-07-30, which leaves 81 events from Oct 2002
to Nov 2022: 49 in train and 32 in valid.

**Coupon-size change.** This was set as an objective rule before testing. It is the sign of the change in the new-issue
10-year note size announced in the refunding statement versus the previous statement. In TLT-era events that gives 14
increases (2008-09, 2018, 2020) and 11 decreases (2003-05, 2010-05, 2016-02, 2021-11 to 2022-08). All other
statements count as unchanged.

**Setup.** All variants use `lib.backtest_weights(lag=1)` with 1bp/side. Calendar windows put on the weight 2 closes
early, which is legitimate because the dates are known in advance. In (C) the weight is set at the QRA close, so entry
happens at the QRA+1 close. Timing value = net minus mean signed exposure × the always-long ETF.

| Variant (pre-specified) | train t | valid t | valid Sharpe | valid net bp/held day | TV train t / valid t | gate |
|---|---|---|---|---|---|---|
| A long TLT, close of Fri before borrowing-estimate Mon → QRA Wed close | +0.40 | −1.07 | −0.38 | −7.0 | −0.06 / −1.11 | no |
| B long TLT on QRA Wednesday only | −1.24 | −0.37 | −0.13 | −2.9 | −1.56 / −0.39 | no |
| C −sign(Δ10y size) TLT for 5 sessions (QRA+1 → +6 close) | −0.12 | +0.70 | +0.25 | +7.8 | −0.10 / +0.70 | no |
| D A on IEF | +0.51 | −1.17 | −0.41 | −3.0 | −0.20 / −1.26 | no |

**Per-event means (descriptive, not registered, ≤2022).**
- **Window A, TLT.** Mean +10.9bp per event in train (t=0.47) and −26.1bp in valid (t=−1.11). Net of the 2bp round
  trip, that is about +9 and −28bp per trade.
- **FOMC.** 31/81 windows contain a scheduled FOMC decision. Excluding them: train +19.1bp (t=0.71), valid −50.0
  (t=−1.31). FOMC-window events only: −4.4 / +4.7. No FOMC-free premium appears.
- **Overlap with 281 (turn of month).** The same window shifted 4 weeks earlier or later is the control:
  - Train controls: +2.7bp and −11.8bp.
  - Valid controls: −28.3bp and −37.5bp.
  - A minus control: +14.7bp (t=0.55) in train and +8.0bp (t=0.27) in valid.
  - The negative valid mean of A comes from the early-month TLT weakness that 281 already found, not from the QRA. The
    QRA-specific part is small and insignificant.
- **Post-QRA 5-session TLT return by size change.** Train: up −4bp (n=7), down −14 (n=6), unchanged −20 (n=36). Valid:
  up −98 (n=7), down −19 (n=5), unchanged −59 (n=20). The sign agrees with supply repricing only in valid, and only
  because of 2020 (+542bp). The 2022 rate shock cuts the other way (−562bp). With n≈12 per period there is no power.
- **IEF window (D).** +7.0bp in train (t=0.68) and −9.9bp in valid (t=−1.19).

**Overlap checks.**
- LOG 133 (auction cycle): no long-duration auction falls inside any QRA window. The refunding 10y/30y auctions are the
  following week. Only 1/81 QRAs has a long auction 1-3 sessions later, so this result is separate from 133.
- LOG 276d (refunding months): A enters at the Friday close and holds Mon-Wed. 39/81 windows include one of the last 3
  sessions of the month, when the QRA is the last Wednesday of Jan/Apr/Jul/Oct. A therefore partly picks up the
  month-end bid from 276, which pushes A upward. A still fails. 276d found no refunding-month difference, which is
  consistent with this.

**Verdict: dead.** No variant passes `lib.passes_gate`. The best |t| is 1.56 (B's timing value, wrong sign), far below
the luck level of ≈3.5 for N≈450. The window's sign flips between train and valid, and the controls explain the valid
loss. The yearly P&L is dominated by 2009 (−920bp), 2011 (+1013bp) and 2015 (−685bp). There is no pre-QRA
duration-premium drift like the pre-FOMC drift in TLT or IEF. No holdout look was taken.
