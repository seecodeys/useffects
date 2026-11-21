# New free flow/positioning data sources (scout, 2026-11-21)

Scope: free, scriptable, no login, ≥10 years, capturing flows or positioning, and NOT already used
(Yahoo, FRED, CBOE index CSV / P/C JSON, CFTC, fiscaldata, NY Fed, HistData, FINRA short volume,
EDGAR 8-K, SNB, Norges, Japan MOF, SPDR GLD). Every URL below was hit from this container on
2026-11-21 with curl / python (status code + size + first rows checked). Downloaded samples were
kept only in the scratchpad, not in the repo. Hypotheses were checked against `grep '^## ' LOG.md`
(001-360); none of them is a re-run of a logged test (overlaps noted where close).

| # | Dataset | Start | Freq | Lag | Verified |
|---|---|---|---|---|---|
| 1 | SPDR ETF NAV history (shares outstanding) | May 2006 | daily | T+1 | yes |
| 2 | FINRA margin statistics | Jan 1997 | monthly | ~3 weeks | yes |
| 3 | Treasury TIC S-form + SLT | Jan 1977 (S1 to Jan 2023; SLT 2020+) | monthly | ~6-7 weeks | yes |
| 4 | Treasury auction allotments by investor class | Jan 2000 (coupons), Aug 2001 (bills) | per auction | ~1 month | yes |
| 5 | OFR Money-Market-Fund / Hedge-Fund Monitor API | Nov 2010 (MMF); Q1 2013 (Form PF) | monthly / quarterly | ~1 mo / ~9-12 mo | yes |
| 6 | Cboe CFE VX futures per-contract daily OI & volume | Nov 2005 (first file; VX listed 2004) | daily | next morning | yes |
| 7 | EIA weekly petroleum status (XLS) | Aug 1982 | weekly | Wed for prior Fri | yes |
| 8 | ECB Eurosystem weekly financial statement (ILM, data API) | 1999 (APP line from mid-2009) | weekly | Tue for prior Fri | yes |
| 9 | JPX trading by investor type (weekly) | Jan 2016 in English archive | weekly | ~4 business days | yes (≈10.9 y, borderline) |
| 10 | Fed Z.1 Financial Accounts (CSV zip) | 1945 | quarterly | ~10 weeks | yes |

Checked and rejected: FINRA consolidated short interest API (works, no key, but only from 2017-12-29 → <10 y);
OCC `marketdata.theocc.com/volume-query` (refuses report dates older than 2 years); ICI flow pages (HTTP 403 to
scripted client — not bypassed); Invesco QQQ product page (HTTP 406); iShares `.ajax?fileType=xls&dataType=fund`
(returns the HTML product page, not the file — no shares-outstanding history obtained); OFR `/v1/series/search`
(403; the `metadata/mnemonics` + `series/timeseries` endpoints work).

---

## 1. SPDR ETF NAV / shares-outstanding history (State Street)
- URL: `https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/navhist-us-en-{ticker}.xlsx`
  (tested `spy` → 279 KB, `xlk` → 270 KB; columns Date, NAV, Shares Outstanding, Total Net Assets).
- History: 31-May-2006 to the previous trading day (5,155 rows for SPY and XLK). Same pattern should work for the
  other SPDR tickers (XLE, XLF, …, MDY, DIA, SPDR bond ETFs) — only spy/xlk tested.
- Frequency/lag: daily; file shows T-1 (fetched 21 Nov, last row 19 Nov). Note shares outstanding usually reflect
  creations/redemptions with a 1-day booking lag — treat the change on date d as the flow of d-1 and lag the signal.
- Terms: State Street website content; personal/internal research use, do not redistribute raw files (keep in the
  uncommitted data/ cache like the GLD file in 359).
- Hypotheses:
  - H1a: Sector-ETF flow-pressure reversal. Rank the 11 Select Sector SPDRs by 5-day % change in shares outstanding;
    long the heaviest-redemption sector, short the heaviest-creation sector for the next 5-20 sessions
    (ETF-flow price-pressure reversal, Ben-David/Franzoni/Moussawi). Not in LOG (056 sector herding used prices only).
  - H1b: SPY large redemption days (Δshares < −1.5% in a day, i.e. AP/short-creation unwinds or institutional
    exits) → SPY vs IVV/VOO and SPY outright next 1-5 sessions; tests whether ETF primary-market outflows are
    information or liquidity.

## 2. FINRA margin statistics
- URL: `https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx` (200, 21 KB; the path name is
  stale but the file is current: rows 1997-01 … 2026-10). Columns: debit balances in customers' margin accounts,
  free credit balances in cash and margin accounts.
- Frequency/lag: monthly, published around the 3rd week of the following month (use release date + 1 day).
- Terms: public FINRA statistics; cite FINRA. (Pre-2010 numbers are the NYSE series carried over — check break.)
- Hypotheses:
  - H2a: Leverage-unwind regime: 12-month % change in margin debt crossing from >+20% to negative (deleveraging) →
    underweight SPY/QQQ next 1-3 months; margin calls as a mechanical seller.
  - H2b: "Dry powder": free-credit balances / margin debt ratio in top decile → forward 1-3 month SPY/IWM excess
    return (sideline cash redeployed). Neither margin series appears in LOG.

## 3. Treasury International Capital (TIC)
- URLs: `https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/s1_globl.txt` (3.2 MB,
  tab-separated, gross purchases/sales by foreigners by country and instrument, 1977-01 → 2023-01 when the S form
  ended); `.../slt_table1.txt` (984 KB, holdings / net U.S. sales / valuation change by country, 2020-01 onward);
  `.../mfhhis01.txt` (major foreign holders of Treasuries, 2000+).
- Frequency/lag: monthly, released ~18th of month t+2 (≈6-7 weeks).
- Terms: U.S. government, public domain.
- Splice caveat: S1 (transactions) and SLT (holdings-based net sales) are different concepts; use S1 for 1977-2022
  testing and SLT for live use, after checking the 2020-22 overlap.
- Hypotheses:
  - H3a: Foreign private net purchases of U.S. equities (3-month sum, scaled by market cap) → next-month SPY vs
    EFA. Foreign investors as trend-chasing marginal buyers; reversal when flow is extreme.
  - H3b: Caribbean-centre (Cayman) net Treasury purchases as a hedge-fund basis-trade proxy: large 3-month build-ups
    → TLT/IEF drawdown risk in the following quarter (unwind). Differs from 341 (H.4.1 official custody, weekly).

## 4. Treasury auction allotments by investor class
- URLs (from `https://home.treasury.gov/data/investor-class-auction-allotments`):
  `https://home.treasury.gov/system//files/276/November-9_2026-IC-Coupons.xls` (468 KB, 2009-10 → 2026-11),
  `.../IC-Bills.xls`, plus history files `Website-PDO-4-A-Coupons-Jan-2000-Sep%202009.xls` (2000-02 → 2009-09) and
  `Website-IC-allotments-Bills-Aug-2001-Sep-2009.xls`. The current file name changes monthly — scrape the page for it.
- Fields per auction: SOMA, depository institutions, individuals, dealers & brokers, pension & retirement,
  investment funds, foreign & international, other (billions; millions in the old file).
- Frequency/lag: per auction; file updated roughly monthly (use the file date, not the auction date, as
  availability — so this is a slow-moving demand regime, not an auction-day signal).
- Terms: public domain.
- Hypotheses:
  - H4a: Real-money demand regime: trailing-3-month investment-funds + pension share of 10y/30y allotments vs its
    2-year average; low share → TLT underperforms IEF next month (duration supply must clear through dealers).
    Distinct from 331 (dealer takedown at the auction itself, from fiscaldata bidder stats).
  - H4b: Foreign share of coupon allotments falling 3 months in a row → USD/Treasury term-premium widening:
    short TLT vs SHY next month.

## 5. OFR Financial Data API (Money Market Fund Monitor, Hedge Fund Monitor)
- Endpoints (no key, JSON): `https://data.financialresearch.gov/v1/series/timeseries?mnemonic=MMF-MMF_RP_TOT-M`
  (MMF repo holdings, 2010-11 → monthly); mnemonic list `.../v1/metadata/mnemonics?dataset=mmf`;
  hedge-fund monitor `https://data.financialresearch.gov/hf/v1/series/timeseries?mnemonic=FPF-ASSETCLASS_STREASURY_SUM`
  (Form PF qualifying hedge funds' short Treasury exposure, quarterly 2013-Q1+), `FICC-SPONSORED_REPO_VOL` (2020+,
  <10 y), plus `TFF-*` DV01 series (CFTC-derived — already covered).
- Frequency/lag: MMF monthly (SEC N-MFP, ~1 month); Form PF quarterly with ~9-12 month lag (descriptive only).
- Terms: U.S. government (OFR) open data; cite OFR.
- Hypotheses:
  - H5a: MMF T-bill vs Fed-RRP allocation: when MMFs shift from RRP into bills (bill share rising), reserves are
    being drained less → risk-asset support; test next-month SPY / HYG–IEF. Complements 142/304 (net liquidity)
    with the composition of MMF assets rather than the Fed balance sheet.
  - H5b (descriptive due to lag): hedge-fund gross short-Treasury exposure (basis-trade size) vs subsequent
    Treasury-futures/cash dislocations (e.g. March 2020) — risk overlay only.

## 6. Cboe CFE VX futures — per-contract daily volume and open interest
- URLs: `https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{M}{YY}_VX.csv` (e.g. `CFE_F06_VX.csv`,
  first rows 11/18/2005) for 2004-2012 expiries; `https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{YYYY-MM-DD}.csv`
  for 2013+ expiries (2013-01-16 → 200; 2012 path → 403, so use the archive path for older contracts).
  Columns: Trade Date, Futures, OHLC, Settle, Change, Total Volume, EFP, Open Interest.
- Frequency/lag: daily, available next morning.
- Terms: Cboe market data, internal/personal research; no redistribution.
- Hypotheses:
  - H6a: Daily total VX open interest 20-day change z-score > +2 while the curve is in contango (new short-vol
    supply, mostly ETP/dealer hedging) → long VIXY / short SPY next 5-10 sessions. Daily analogue of 301
    (weekly CFTC), with term detail.
  - H6b: Front-vs-second-month OI migration speed during the roll window as a measure of ETP roll flow; slow roll
    (OI stuck in front) → front-month settle pressure on the final days before VIX expiry (032 tested only the
    Wednesday itself, without OI).

## 7. EIA Weekly Petroleum Status Report (history XLS)
- URL: `https://www.eia.gov/dnav/pet/hist_xls/WCESTUS1w.xls` (US crude stocks ex-SPR; 1982-08-20 → 2026-11-13).
  Same pattern for `WCSSTUS1w` (Cushing), `WGTSTUS1w` (gasoline), `WCRFPUS2w` (production), `WCREXUS2w` (exports).
  Free EIA API v2 also exists (needs free self-serve key); the XLS route needs none. Requires `xlrd` to parse.
- Frequency/lag: weekly, Wednesday 10:30 ET for the week ending prior Friday (holiday weeks shift).
- Terms: U.S. government, public domain.
- Hypotheses:
  - H7a: Inventory surprise vs 5-year seasonal average change → USO open→close on release day (Wed 10:30 sits
    inside the session; with daily data use close(Tue)→close(Wed) and a lag-free entry check).
  - H7b: Cushing stocks above the 80th percentile of their 5-year range (storage stress, contango) → USO
    underperforms the deferred-roll USL over the next month (roll-yield mechanics of front-month ETP).

## 8. ECB Eurosystem weekly financial statement (ECB Data Portal API)
- URL: `https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A070100.U2.EUR?format=csvdata`
  (securities held for monetary-policy purposes, 2009-W27 → 2026-W46, 907 rows; tested also
  `ILM/W.U2.C.L020200.U2.EUR`, current-account/deposit liabilities). No key.
- Frequency/lag: weekly (Friday positions), published Tuesday 15:00 CET.
- Terms: ECB statistics may be reused with source attribution.
- Hypotheses:
  - H8a: Euro-area "net liquidity": 4-week change in Eurosystem excess liquidity (deposit facility + current
    accounts) → next-week EFA/FEZ and SPY. Euro analogue of 304 (US only).
  - H8b: ECB QE/QT pace shocks: weekly Δ securities held above/below its 13-week average → next-week Bund-sensitive
    US duration (IEF/TLT) via portfolio-balance spillover; test 2015-2026 APP/PEPP/QT era.

## 9. JPX trading by type of investors (Tokyo/Nagoya)
- URLs: index `https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html` (weekly
  `stock_1_w_YYYYMMDD_YYYYMMDD.xlsx`); archive pages `.../investor-type/00-00-archives-00.html` … `-10.html`
  (archive-10 holds the January 2016 files `stock_val_1_160101.xls`). Weekly English archive verified back to
  Jan 2016 only (≈10.9 y); older data may exist on the Japanese site — not checked.
- Frequency/lag: weekly; published 4th business day after the week (Thursday).
- Terms: JPX copyright; internal use only, no redistribution.
- Hypotheses:
  - H9a: Trust-bank (pension/GPIF rebalancing) net buying after a >5% TOPIX monthly drop → next-week EWJ return;
    a mechanical rebalancing buyer. Distinct from 348 (MOF cross-border totals; no domestic investor split).
  - H9b: Foreigners' net cash-equity selling combined with futures buying (hedged exposure) → USDJPY next week
    (unhedged vs hedged flow tells whether yen is bought). Retail (individuals) as contrarian check.

## 10. Federal Reserve Z.1 Financial Accounts (CSV bundle)
- URL: `https://www.federalreserve.gov/releases/z1/current/z1_csv_files.zip` (8.3 MB, 616 CSVs; flows `F*`, levels
  `L*`, sector `S*`). The Data Download Program (`datadownload/Output.aspx`) timed out from here; use the zip.
- Frequency/lag: quarterly, ~10 weeks after quarter end; history to 1945 for most series.
- Terms: public domain.
- Hypotheses:
  - H10a: Nonfinancial-corporate net equity issuance (buybacks minus issuance, % of market cap, trailing 4q) as a
    flow regime → next-quarter SPY vs EFA (buyback bid). 328/333 used blackout calendars, not buyback size.
  - H10b: Household + mutual-fund equity share of financial assets (allocation) in its top quintile → weaker
    next-4-quarter SPY vs IEF (aggregate allocation as positioning). Long-horizon; low trade count.
