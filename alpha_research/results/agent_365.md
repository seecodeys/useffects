# 365 - Corporate-issuance windows / issuer rate-lock hedging (card S8)

Script: `experiments/365_corp_issuance_windows.py`. Registry: `results/registry_agent_365.csv` (4 rows).
Windows come from the calendar alone and are known years ahead, so weights use **lag 0** (stated; no price input).
Costs 1bp/side on turnover. TLT/IEF/LQD Yahoo adjusted, 2002-2022 only (nothing from 2023+ looked at).

| Variant | Events tr/va | train t | valid t | valid Sharpe | net bp/event tr / va | timing t tr / va | gate |
|---|---|---|---|---|---|---|---|
| A short TLT, Fri before Labor Day -> LD+7 | 13 / 8 | +0.42 | +1.79 | +0.63 | +25.6 / +154.2 | +0.66 / +2.00 | no |
| B short TLT, Jan session 1 -> 8 | 12 / 8 | -0.11 | -0.52 | -0.18 | -7.2 / -41.2 | +0.15 / -0.32 | no |
| C short LQD - beta*IEF over A+B windows | 23 / 16 | -0.91 | -0.62 | -0.22 | -10.4 / -5.7 | -0.52 / -0.28 | no |
| D CONTROL long TLT sessions 8-12 after windows | 25 / 16 | +1.83 | -0.02 | -0.01 | +73.2 / -2.1 | +1.58 / -0.25 | no |

Per-year net bp, A (Sept window): 2002 -70, 2003 -168, 2004 -83, 2005 +176, 2006 +2, 2007 -234, 2008 -290, 2009 -26,
2010 +120, 2011 -63, 2012 +432, 2013 +228, 2014 +323, 2015 +317, 2016 +317, 2017 +32, 2018 +147, 2019 +452, 2020 -53,
2021 -164, 2022 +193. Positive in 6/13 train years, 6/8 valid years. Only 1 scheduled FOMC day (2020-09-16) fell
inside an A window, so A is not FOMC timing.
Per-year B: 2003 +67, 2004 -356, 2005 -67, 2006 +29, 2007 +114, 2008 +17, 2009 +197, 2010 +56, 2011 +190, 2012 -25,
2013 -27, 2014 -269, 2015 -353, 2016 -292, 2017 -189, 2018 +92, 2019 +98, 2020 -77, 2021 +311, 2022 +88.

## Verdict: dead (A weak/regime-dependent)
- No variant passes `lib.passes_gate`. The January window (B) and the credit-concession leg (C) show nothing in
  either period, though the story predicts both.
- A (post-Labor Day short TLT) is positive in validation (t 1.79, timing t 2.0), but train is flat (t 0.42). Its
  sign flips around 2010; it did well after 2012, when IG issuance and rate-lock hedging grew. With about 21 events
  and many tests in total (luck bound sqrt(2 ln N) is about 3.5+), this is not enough. The control leg (D) does not
  show the predicted recovery in validation either.
- Possible follow-up, not run: a dose test of A against SIFMA September IG issuance volume. Do not re-tune the window.
