# 374 — Equal-weight quarterly reset (idea card E6)

**Rebalance day, confirmed from public documents:**
- S&P 500 EW family: an Invesco SEC filing for an S&P 500 EW sector ETF (485APOS 2018, sec.gov/Archives/edgar/data/1209466/000119312518245722)
  says: "The Index Provider rebalances the Underlying Index on a quarterly basis after the close of business on the third Friday
  in March, June, September and December." RSP's 2025 summary prospectus (497K) confirms quarterly rebalancing that follows the index schedule.
  The S&P DJI pages returned 403 through the proxy and were not bypassed. A search snippet of S&P's Dec-2024 notice says the *weighting reference* date moved earlier (to around the 2nd Friday). The trade date is still the 3rd Friday close.
- NDXE (QQQE): indexes.nasdaq.com/docs/methodology_NDXE.pdf change log. Before 5/1/2026 it used "Last Sale Prices as of the close of
  trading on the third Friday in March, June, September and December". It now uses the last trading day of Feb/May/Aug/Nov, which is outside the test window.
- The assumption holds: D = 3rd Friday (or the previous trading day if a holiday), with trading at the close of D.

**Duplicates check:** 019 = Russell recon IWM−SPY. 245 = overnight into SPX AM settlement. 180 = fades the SPY quad-witching move.
None of them conditions on the EW vs cap-weight drift. This test is new.

**Setup:** g = QTD(EW) − QTD(cap) through close D−1. Costs are 1bp/side for RSP/QQQE plus 0.5bp/side for SPY/QQQ. Data runs 2003–2022 (QQQE from 2012).

| Variant | Train t | Train bp/trade | Valid t | Valid Sharpe | Valid bp/trade | Gate |
|---|---|---|---|---|---|---|
| V1 RSP−SPY −sign(g), D open→close | −1.28 | −7.9 (n=46) | +0.18 | +0.06 | +0.8 (n=32) | no |
| V2 same, close D−1→D (lag-1 signal through D−2) | −1.63 | −6.9 | −0.29 | −0.10 | −1.2 | no |
| V3 QQQE−QQQ −sign(g), D open→close | −0.22 | −2.9 (n=11) | +0.79 | +0.28 | +4.0 (n=32) | no |
| V4 RSP−SPY +sign(g), close D→D+1 (lag-1) | −1.50 | −10.4 | −1.52 | −0.54 | −11.2 | no |
| Benchmark: unconditional long RSP−SPY on D (o→c) | +0.48 | +2.9 | −0.92 | −0.33 | −4.5 | — |

- Pooled V1+V3: train −6.9bp/event (t −1.25), valid +2.4bp/event (t +0.70).
- Timing value for V1 compared with the unconditional long spread: train −7.9 vs +2.9bp, valid +0.8 vs −4.5bp. The signal shows no consistent timing value.
- corr(g, D spread) is +0.12 in train and −0.18 in valid. The sign flips between periods, so there is no stable price pressure.
- No variant passes the gate. The train sign is wrong for every fade variant. The luck threshold at the project's registry size is about 3.4.

**Verdict: dead.** The RSP reset (about $70bn, trading at the 3rd-Friday close, which is also quad-witching) leaves no tradable footprint in the RSP−SPY or QQQE−QQQ spread.
Most likely the creation/redemption and MOC liquidity at quad witching absorb it.
Files: experiments/374_equal_weight_reset.py, results/374_equal_weight_reset.txt, results/registry_agent_374.csv (5 rows).
