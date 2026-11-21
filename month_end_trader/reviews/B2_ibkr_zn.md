# Review B2: IBKR adapter for ZN (bond leg), 2026-11-21

Agent review of met/ibkr.py and met/bond.py against IBKR / ib_async 2.0.1 behaviour.
Tags: [sure] verified in code / ib_async source; [likely] IBKR behaviour from knowledge; [verify] check on paper.

## Findings and status
1. **Medium: half days.** CBOT rate futures halt at 13:00 ET; the bot used the CME equity half-day halt (13:15)
   and entered/exited at 12:58. A failed exit round could slip past 13:00 and carry the position over the weekend.
   **Fixed:** `rates_futures_open()` (13:00 ET halt on half days and NYSE weekday holidays) is used by the bond leg
   and by the FakeBroker for ZN; `bond.half_day_lead_s` (default 600 s, so 12:50 ET) on half days.
2. **Medium: roll_buffer_days range.** Values of 19-20 could select the thin second-deferred contract.
   **Fixed:** the range is now [2, 15].
3. **Low-medium: CBOT data missing from the requirements.** **Fixed** in the ibkr.py docstring.
4. **Low-medium: outsideRth not set.** IB treats Globex futures market orders as working the whole session [likely].
   Paper-test exits at the Sunday 18:00 ET reopen and after 16:00 ET [verify].
5. **Low: price freshness.** The 30 s tick check plus resubscribe is fine for ZN, whose book updates constantly.
6. **Low: what-if margin.** Conservative (EWL minus initial margin); intraday vs overnight margin at the 15:28
   preflight is unclear [verify]; the 10% buffer covers it.
7. **Correct as written:**
   - `CBOT` exchange code, and `lastTradeDateOrContractMonth` = last trading day inside the contract month, so
     `pick_front(first of month)` returns exactly the contract month [sure].
   - Multiplier `"1000"`, execId corrections, account filter, persisted fills.
   - localSymbol format ("ZNH7" vs "ZN   MAR 27") does not matter because the bot compares IB's own string [verify].

## Answers
- **DAY time in force:** lasts to the 17:00 ET Globex close [sure].
- **Market orders:** run as CME "market with protection"; the timeout and retry logic covers partial fills.
- **Order precautions:** keep "Bypass Order Precautions for API orders" ON; 3-4 ZN is about $330-450k notional per order.
- **Liquidity at 15:58 ET:** ZN settles at 15:00 ET; the book thins after that but is deep for 1-20 lots.
- **Delivery:** IBKR closes out and blocks longs a few business days before first notice [verify the date in TWS
  Contract Info]. The bot skips the front contract in Feb/May/Aug/Nov.
- **Funding:** futures margin and variation are swept from the securities segment. With SPY 100% invested this
  creates a small debit at about benchmark + 1.5%; a $10k debit for 4 days costs about $6.

## Feasibility, $100-150k account (approximate)
- **Margin:** maintenance on month-end day is about 50% of net liq, initial about 75%. Feasible with an ~50% cushion.
- **Auto-liquidation:** if excess liquidity goes below 0. With MES long that takes about a 12-13% one-day S&P fall;
  the disaster stops (6% / 4% of equity) fire well before that.
- **Portfolio margin** is not needed. Plan on Reg T.
- **Shorting through MES:** no borrow or locate.
- **PDT rule:** does not apply to futures.
- **Permissions:** margin account plus "Futures – United States".
- **Costs:** commissions about $20-30/month; market data about $10/month; SPY bars may need the US equity add-on (~$4.50/month).
