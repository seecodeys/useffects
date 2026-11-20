"""Live signal helper for the two signals that survived testing.

  013  Month-end rebalancing (main finding, survived holdout t=3.3):
       On the LAST TRADING DAY of each month, buy/short SPY at the open and exit at the close:
         position = -sign( SPY month-to-date return - TLT month-to-date return ), MTD through the PRIOR close.
       (Stocks beat bonds this month -> short SPY that day; stocks lagged -> long.)
  001  OPEX tilt (weak, holdout t=0.8): short QQQ open->close on the monthly options-expiry Friday (3rd Friday).

    python signals.py            # status + upcoming dates
    python signals.py --backtest # combined daily backtest 2002-now, writes results/combined_*.{csv,png}

Not investment advice: small edges, ~12 trades/yr each, in-sample periods included in the backtest.
"""
import os, sys, datetime as dt
import numpy as np, pandas as pd
from dateutil.easter import easter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lib


def nyse_closed(d):
    d = pd.Timestamp(d).normalize()
    if d.dayofweek >= 5:
        return True
    gf = pd.Timestamp(easter(d.year)) - pd.Timedelta(days=2)
    memorial = max(x for x in pd.date_range(f"{d.year}-05-01", f"{d.year}-05-31") if x.dayofweek == 0)
    fixed = {(1, 1), (6, 19), (7, 4), (12, 25)}
    return d == gf or d == memorial or (d.month, d.day) in fixed    # month-end-relevant holidays only


def last_trading_day(year, month):
    d = pd.Timestamp(year, month, 1) + pd.offsets.MonthEnd(0)
    while nyse_closed(d):
        d -= pd.Timedelta(days=1)
    return d


def third_friday(year, month):
    d = pd.Timestamp(year, month, 15)
    return d + pd.Timedelta(days=(4 - d.dayofweek) % 7)


def status():
    spy, tlt = lib.get("SPY", refresh=True), lib.get("TLT", refresh=True)
    asof = min(spy.index[-1], tlt.index[-1])
    month_start = asof.to_period("M").start_time
    base = lambda x: x["aC"][x.index < month_start].iloc[-1]
    m_spy = spy["aC"].loc[asof] / base(spy) - 1
    m_tlt = tlt["aC"].loc[asof] / base(tlt) - 1
    gap = m_spy - m_tlt
    ltd = last_trading_day(asof.year, asof.month)
    if ltd <= asof:
        nxt = asof + pd.offsets.MonthBegin(1)
        ltd = last_trading_day(nxt.year, nxt.month)
    print(f"Data through close of {asof.date()}")
    print(f"  SPY MTD {m_spy:+.2%}   TLT MTD {m_tlt:+.2%}   stock-bond gap {gap:+.2%}")
    print(f"  Next month-end (last trading day): {ltd.date()}  -> if the gap keeps this sign at the prior close: "
          f"{'SHORT' if gap > 0 else 'LONG'} SPY open->close")
    print(f"  (historical edge grows with |gap|: ~+5bp small, +18bp mid, +39bp large tercile; |gap| median ~3.6%)")
    t = third_friday(asof.year, asof.month)
    if t <= asof:
        n = asof + pd.offsets.MonthBegin(1); t = third_friday(n.year, n.month)
    while nyse_closed(t):
        t -= pd.Timedelta(days=1)
    print(f"  Next monthly OPEX: {t.date()}  -> weak tilt: short QQQ open->close")


def backtest(cost_bp=1.0):
    spy, tlt, qqq = lib.get("SPY"), lib.get("TLT"), lib.get("QQQ")
    idx = spy.index.intersection(tlt.index)
    idx = idx[idx >= "2002-08-01"]
    ym = idx.to_period("M")
    rank_bwd = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    last = (rank_bwd == 1) & pd.Series(ym != ym[-1], index=idx)
    def mtd(x):
        x = x.reindex(idx)
        me = x["aC"].where(rank_bwd == 1).ffill().shift(1)
        first = pd.Series(ym != pd.Series(ym, index=idx).shift(1).values, index=idx)
        b = me.where(~first, x["aC"].shift(1)).where(first).ffill()
        return x["aC"].shift(1) / b - 1
    p013 = (-np.sign(mtd(spy) - mtd(tlt))).where(last, 0)
    opex = pd.Series((idx.dayofweek == 4) & (idx.day >= 15) & (idx.day <= 21), index=idx)
    p001 = -opex.astype(float)
    r013 = lib.daily_pnl(p013, spy.oc.reindex(idx), cost_bp)
    r001 = lib.daily_pnl(p001, qqq.oc.reindex(idx), cost_bp)
    df = pd.DataFrame({"013_month_end_SPY": r013, "001_opex_QQQ": r001})
    df["combined"] = df.sum(axis=1)
    out = os.path.join(lib.HERE, "results")
    yearly = df.groupby(df.index.year).sum() * 100
    yearly.round(2).to_csv(os.path.join(out, "combined_yearly_pct.csv"))
    print(f"Combined backtest {idx[0].date()}..{idx[-1].date()} at {cost_bp}bp/side (1x notional on trade days)")
    for c in df:
        m = lib.evaluate(pd.Series(1.0, index=idx), df[c], 0)
        print(f"  {c:<20} ann {m['ann_ret_%']:+.2f}%  Sharpe {m['sharpe']:.2f}  maxDD {m['max_dd_%']:.1f}%  t {m['t']:+.2f}")
    print(yearly.round(1).to_string())
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(10, 5))
    (1 + df).cumprod().plot(ax=ax, lw=1.3)
    for x, lab in [("2015-01-01", "valid"), ("2023-01-01", "holdout")]:
        ax.axvline(pd.Timestamp(x), color="grey", ls="--", lw=0.8); ax.text(pd.Timestamp(x), ax.get_ylim()[1] * 0.98, lab, fontsize=8)
    ax.set_title(f"Month-end rebalancing (SPY) + OPEX tilt (QQQ), open->close, {cost_bp}bp/side")
    ax.grid(alpha=0.3); fig.tight_layout(); fig.savefig(os.path.join(out, "combined_equity.png"), dpi=130)


if __name__ == "__main__":
    backtest() if "--backtest" in sys.argv else status()
