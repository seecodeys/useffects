"""347 - Norges Bank's pre-announced daily FX transactions on behalf of the government -> NOK returns (card X3).

Data: Norges Bank table "Foreign exchange transactions on behalf of the government" (NOK m per business day, monthly,
2000-2026; scraped from https://www.norges-bank.no/en/topics/statistics/foreign-exchange-transactions-daily/).
Sign convention from the page: NEGATIVE = Norges Bank buys NOK, POSITIVE = Norges Bank sells NOK.
The amount for month m is announced on the last business day of month m-1, so it is known at that day's close.
Five cells carry an intramonth-adjustment annotation (May 2003, Oct 2008, Mar 2020, Nov 2020, Nov 2021): the table shows
the ADJUSTED amount, the original pre-announced one is not in the table -> those months are not traded (signal 0).
December is excluded (bank winds down late December). January's change is measured vs December's amount.

Timing: signal for month m switches on at the close of the last business day of m-1 (FX calendar from Yahoo) and is
held to the close of m's last business day; lib.backtest_weights(lag=1) shifts implementation one more day
(conservative: day 1 of the month is missed, first day of next month is kept). Costs 2bp/side (FX and ETFs).
Yahoo FX daily returns with |r| > 10% are treated as bad ticks (set to 0) - pre-specified.

Pre-specified variants (all registered, no others):
  a) EURNOK: chg = amt_m - amt_{m-1}. chg <= -200 (NOK buying steps up >=200m/day) -> short EURNOK (long NOK);
     chg >= +200 -> long EURNOK. Else flat.
  b) EURNOK level: amt < 0 (net NOK buyer) -> short EURNOK; amt > 0 -> long EURNOK; 0 -> flat.
  c) as (a) on USDNOK (NOK=X).
  d) as (a) on NORW minus EFA (long NORW / short EFA when NOK buying steps up), 2010+.
"""
import os
import re
import html
import sys

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import lib  # noqa: E402

EXP = "347_norges_bank_nok_flows"
END = "2022-12-31"
URL = "https://www.norges-bank.no/en/topics/statistics/foreign-exchange-transactions-daily/"
COST = 2.0
THR = 200


def load_table():
    path = os.path.join(lib.DATA, "norgesbank_fx_gov.html")
    if not os.path.exists(path):
        r = requests.get(URL, headers=lib._HDR, timeout=30)
        r.raise_for_status()
        open(path, "w").write(r.text)
    t = pd.read_html(path)[0]
    t = t.rename(columns={t.columns[0]: "year"}).set_index("year")
    rows = []
    for y, row in t.iterrows():
        for i, m in enumerate(t.columns, 1):
            v = row[m]
            if pd.isna(v):
                continue
            s = str(v)
            ann = "*" in s
            num = float(re.match(r"\s*(-?\d+(?:\.\d+)?)", s).group(1))
            rows.append({"month": pd.Period(f"{int(y)}-{i:02d}", "M"), "amt": num, "annot": ann})
    df = pd.DataFrame(rows).set_index("month").sort_index()
    return df


def monthly_signals(nb):
    nb = nb.copy()
    nb["chg"] = nb["amt"] - nb["amt"].shift(1)
    nb["sig_a"] = np.where(nb["chg"] <= -THR, -1.0, np.where(nb["chg"] >= THR, 1.0, 0.0))  # + = long EURNOK
    nb["sig_b"] = np.sign(nb["amt"])
    bad = nb["annot"] | (nb.index.month == 12)
    nb.loc[bad, ["sig_a", "sig_b"]] = 0.0
    return nb


def daily_signal(sig_m, idx):
    """Map monthly signal to trading days: month m's signal is active from the close of the last trading day of m-1
    up to (excluding) the close of the last trading day of m."""
    s = pd.Series(0.0, index=idx)
    per = idx.to_period("M")
    last_day = pd.Series(idx, index=idx).groupby(per).max()
    for m, v in sig_m.items():
        if (m - 1) not in last_day.index or m not in last_day.index:
            continue
        a, b = last_day[m - 1], last_day[m]
        s[(idx >= a) & (idx < b)] = v
    return s


def fx_ret(sym):
    r = lib.get(sym)["Close"].pct_change()
    return r.where(r.abs() <= 0.10, 0.0).fillna(0.0)


def run():
    nb = monthly_signals(load_table())
    tr = nb.loc[:"2022-12"]
    print("NB table months:", len(nb), "| annotated:", list(nb.index[nb.annot].astype(str)))
    print("months <=2022 with |chg|>=200 (ex Dec/annot):", int((tr.sig_a != 0).sum()),
          " long-EURNOK:", int((tr.sig_a > 0).sum()), " short-EURNOK:", int((tr.sig_a < 0).sum()))

    eur = fx_ret("EURNOK=X")
    usd = fx_ret("NOK=X")
    norw = lib.get("NORW")["cc"]
    efa = lib.get("EFA")["cc"]
    rel = (norw - efa).dropna()

    specs = [("a", "EURNOK chg>=200 lag1", eur, nb.sig_a, +1),
             ("b", "EURNOK level sign lag1", eur, nb.sig_b, +1),
             ("c", "USDNOK chg>=200 lag1", usd, nb.sig_a, +1),
             ("d", "NORW-EFA chg>=200 lag1", rel, nb.sig_a, -1)]   # long NORW when NOK buying steps up (sig_a<0)
    out = []
    for tag, desc, R, sig, mult in specs:
        R = R.loc[:END]
        start = R.index[0]
        w = mult * daily_signal(sig, R.index)
        W = w.to_frame("x")
        net = lib.backtest_weights(W, R.to_frame("x"), COST, lag=1).loc[start:END]
        held = W["x"].shift(2).reindex(net.index).fillna(0).abs()
        r = lib.register(EXP, f"347{tag} {desc}", held, net, 0.0, notes=f"cost {COST}bp/side in net; Dec+annotated months excluded")
        # per-month trade stats
        key = pd.Series(net.index, index=net.index).shift(1).bfill().dt.to_period("M")  # lag-1: day 1 of m+1 belongs to trade m
        mon = net.groupby(key.values).sum()
        hm = held.groupby(key.values).max()
        tm = mon[hm > 0]
        for per, (a, b) in lib.PERIODS.items():
            x = tm.loc[a[:7]:b[:7]]
            tt = x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 else np.nan
            print(f"  {tag} {per}: trades={len(x)} net bp/trade={1e4 * x.mean():+.1f} t(monthly)={tt:+.2f}")
        yr = net.groupby(net.index.year).sum() * 1e4
        print(f"{tag} {desc}: {lib.fmt(r)}  gate={lib.passes_gate(r)}")
        print("   yearly bp:", " ".join(f"{y % 100:02d}:{v:+.0f}" for y, v in yr.items()))
        out.append((tag, desc, r, tm))
    print("N tests in agent registry:", lib.n_tests())
    return out


if __name__ == "__main__":
    run()
