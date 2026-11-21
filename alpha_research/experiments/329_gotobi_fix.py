"""329 - Gotobi-day Tokyo-fix dollar buying (idea card B1), USDJPY HistData 1-min 2001-2022.

Mechanism: Japanese importers settle USD invoices on 'gotobi' days (5th/10th/15th/20th/25th/month-end; if that is a
weekend or Japanese bank holiday, the preceding Tokyo business day). They buy USD at the 09:55 JST bank fixing; banks
pre-hedge, so USDJPY should rise into 09:55 JST and fade afterwards. Calendar is known in advance.

Clock: HistData bars are fixed EST (UTC-5, no DST) and timestamped at bar open. JST = UTC+9 = EST + 14h, so 09:55 JST
on Tokyo day D = 19:55 EST on calendar day D-1. "Price at hh:mm JST" = close of the bar that opens at hh:mm-1
(last close at or before that minute, tolerance 5 min).

Japanese bank holidays: rule-based national holidays (fixed dates, Happy-Monday rules, equinox formula, substitute
and sandwiched days, 2019/2020/2021 one-offs) + Dec 31 / Jan 2 / Jan 3. Cross-checked against 1306.T trading days
(2009-2022) - mismatches printed.

Universe of days: Tokyo business days 2001-2022 with valid prices. pos = +/-1 on event days, 0 otherwise.
Costs 0.5bp/side (task spec; card says 1 pip ~ 1bp, reported as cost sensitivity in the md only for gate passers).

PRE-SPECIFIED VARIANTS (written before looking at any result; no tuning afterwards):
  V1 gotobi_pre_fix_long : long 08:00->09:55 JST on gotobi days.                       (card a)
  V2 gotobi_post_fix_fade: short 09:55->11:00 JST on gotobi days.                      (card b)
  V3 gotobi25_eom_long   : as V1, only 25th-gotobi and month-end-gotobi days.          (card c)
  V4 nongotobi_control   : long 08:00->09:55 JST on non-gotobi business days (control;
                           timing value of V1 = V1 mean minus this mean).
  Card (d) "2015+ only" is the VALID column of V1 (not a separate registration).
"""
import os, sys
import datetime as dt
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "tools"))
import histdata

EXP = "329_gotobi_fix"
YEARS = range(2001, 2023)          # never load 2023+


# ------------------------------------------------------------------ Japanese holidays
def _nth_monday(y, m, n):
    d = dt.date(y, m, 1)
    d += dt.timedelta(days=(0 - d.weekday()) % 7)
    return d + dt.timedelta(weeks=n - 1)


def _equinox(y, spring):
    if spring:
        return dt.date(y, 3, int(20.8431 + 0.242194 * (y - 1980) - (y - 1980) // 4))
    return dt.date(y, 9, int(23.2488 + 0.242194 * (y - 1980) - (y - 1980) // 4))


def jp_national_holidays(y):
    h = {dt.date(y, 1, 1), dt.date(y, 2, 11), dt.date(y, 4, 29), dt.date(y, 5, 3), dt.date(y, 5, 5),
         dt.date(y, 11, 3), dt.date(y, 11, 23), _equinox(y, True), _equinox(y, False)}
    if y <= 2018:
        h.add(dt.date(y, 12, 23))                       # Emperor Akihito birthday
    if y >= 2020:
        h.add(dt.date(y, 2, 23))                        # Emperor Naruhito birthday
    if y >= 2007:
        h.add(dt.date(y, 5, 4))                         # Greenery day
    h.add(_nth_monday(y, 1, 2))                         # Coming of age
    # Marine day
    if y == 2020:
        h.add(dt.date(2020, 7, 23))
    elif y == 2021:
        h.add(dt.date(2021, 7, 22))
    elif y >= 2003:
        h.add(_nth_monday(y, 7, 3))
    else:
        h.add(dt.date(y, 7, 20))
    # Mountain day (2016+)
    if y == 2020:
        h.add(dt.date(2020, 8, 10))
    elif y == 2021:
        h.add(dt.date(2021, 8, 8))
    elif y >= 2016:
        h.add(dt.date(y, 8, 11))
    # Respect for aged
    h.add(_nth_monday(y, 9, 3) if y >= 2003 else dt.date(y, 9, 15))
    # Sports day
    if y == 2020:
        h.add(dt.date(2020, 7, 24))
    elif y == 2021:
        h.add(dt.date(2021, 7, 23))
    else:
        h.add(_nth_monday(y, 10, 2))
    if y == 2019:
        h |= {dt.date(2019, 4, 30), dt.date(2019, 5, 1), dt.date(2019, 5, 2), dt.date(2019, 10, 22)}
    # sandwiched day (citizens' holiday): weekday between two holidays (e.g. Sept 2009/2015)
    for d in sorted(h):
        mid = d + dt.timedelta(days=1)
        if mid not in h and (d + dt.timedelta(days=2)) in h and mid.weekday() != 6:
            h.add(mid)
    if y <= 2006:
        if dt.date(y, 5, 4).weekday() not in (0, 6):
            h.add(dt.date(y, 5, 4))
    # substitute holidays: holiday on Sunday -> next non-holiday day
    for d in sorted(h):
        if d.weekday() == 6:
            s = d + dt.timedelta(days=1)
            while s in h:
                if y < 2007:
                    break
                s += dt.timedelta(days=1)
            h.add(s)
    return h


def jp_bank_holidays(years):
    h = set()
    for y in years:
        h |= jp_national_holidays(y)
        h |= {dt.date(y, 1, 2), dt.date(y, 1, 3), dt.date(y, 12, 31)}
    return h


def tokyo_business_days(a, b):
    hol = jp_bank_holidays(range(a.year - 1, b.year + 2))
    days = pd.bdate_range(a, b)
    return pd.DatetimeIndex([d for d in days if d.date() not in hol])


def gotobi_flags(bdays):
    """Return DataFrame indexed by business day: gotobi (bool), big (25th or month-end)."""
    bset = bdays
    first, last = bdays[0], bdays[-1]
    out = pd.DataFrame(False, index=bdays, columns=["gotobi", "big"])
    for per in pd.period_range(first, last, freq="M"):
        y, m = per.year, per.month
        eom = per.days_in_month
        targets = [(5, False), (10, False), (15, False), (20, False), (25, True), (eom, True)]
        for day, big in targets:
            t = pd.Timestamp(y, m, day)
            i = bset.searchsorted(t, side="right") - 1          # preceding (or same) business day
            if i < 0:
                continue
            d = bset[i]
            if d in out.index:
                out.loc[d, "gotobi"] = True
                out.loc[d, "big"] = out.loc[d, "big"] or big
    return out


# ------------------------------------------------------------------ prices
def jst_minute_close(sym, years):
    df = pd.concat([histdata.load(sym, y) for y in years])
    df.index = df.index + pd.Timedelta(hours=14)        # fixed EST -> JST (no DST in either)
    return df["Close"].sort_index()


def level_at(px, dates, hh, mm, tol_min=5):
    targets = pd.DatetimeIndex(dates) + pd.Timedelta(hours=hh, minutes=mm)
    idx = px.index.searchsorted(targets, side="right") - 1
    vals = np.full(len(targets), np.nan)
    t_found = px.index[np.clip(idx, 0, None)]
    good = (idx >= 0) & ((targets - t_found) <= pd.Timedelta(minutes=tol_min))
    vals[good] = px.values[idx[good]]
    return pd.Series(vals, index=dates)


if __name__ == "__main__":
    bdays = tokyo_business_days(pd.Timestamp("2001-01-01"), pd.Timestamp("2022-12-31"))
    # holiday-calendar check against TOPIX ETF trading days
    t = pd.read_csv(os.path.join(lib.DATA, "1306.T.csv"), parse_dates=["Date"]).set_index("Date")
    t = t.loc["2009-02-01":"2022-12-31"]
    t = t[t["Volume"] > 0].index
    mine = bdays[(bdays >= "2009-02-01")]
    print("calendar check 2009-22: in mine not TSE:", len(mine.difference(t)), list(mine.difference(t).date)[:10])
    print("                        in TSE not mine:", len(t.difference(mine)), list(t.difference(mine).date)[:10])

    flags = gotobi_flags(bdays)
    px = jst_minute_close("USDJPY", YEARS)
    p0800 = level_at(px, bdays, 7, 59)     # close of 07:59 bar = price at 08:00
    p0955 = level_at(px, bdays, 9, 54)     # price at 09:55 (fix)
    p1100 = level_at(px, bdays, 10, 59)    # price at 11:00
    pre = (p0955 / p0800 - 1).dropna()
    post = (p1100 / p0955 - 1).dropna()
    flags = flags.reindex(pre.index.union(post.index)).fillna(False)
    print("days pre:", len(pre), "gotobi:", int(flags["gotobi"].reindex(pre.index).sum()),
          "big:", int(flags["big"].reindex(pre.index).sum()))

    g = flags["gotobi"].astype(bool)
    big = flags["big"].astype(bool)
    variants = {
        "V1_gotobi_pre_fix_long": (g.reindex(pre.index).astype(float), pre),
        "V2_gotobi_post_fix_fade": (-g.reindex(post.index).astype(float), post),
        "V3_gotobi25_eom_long": (big.reindex(pre.index).astype(float), pre),
        "V4_nongotobi_control": ((~g.reindex(pre.index)).astype(float), pre),
    }
    rows = {}
    for name, (pos, ret) in variants.items():
        row = lib.register(EXP, name, pos, ret, cost_bp=0.5, notes="USDJPY HistData JST windows; 2001-2022")
        rows[name] = row
        print(f"{name:26s} {lib.fmt(row)}  train_bp={row['train_bp_per_trade']:+.2f} "
              f"n_tr={row['train_traded_%']*row['train_n_days']/100:.0f} "
              f"n_va={row['valid_traded_%']*row['valid_n_days']/100:.0f} gate={lib.passes_gate(row)}")

    # descriptive (gross, train/valid only): gotobi vs non-gotobi window means, yearly V1 gross
    for lab, (a, b) in lib.PERIODS.items():
        r = pre.loc[a:b]; gg = g.reindex(r.index)
        d = 1e4 * (r[gg].mean() - r[~gg].mean())
        se = 1e4 * np.sqrt(r[gg].var() / gg.sum() + r[~gg].var() / (~gg).sum())
        rp = post.loc[a:b]; gp = g.reindex(rp.index)
        print(f"{lab}: pre gross bp gotobi={1e4*r[gg].mean():+.2f} non={1e4*r[~gg].mean():+.2f} "
              f"diff={d:+.2f} (t={d/se:+.2f}) | post gross gotobi={1e4*rp[gp].mean():+.2f} non={1e4*rp[~gp].mean():+.2f}")
    yr = (1e4 * pre[g.reindex(pre.index)]).groupby(pre[g.reindex(pre.index)].index.year).mean()
    print("V1 gross bp/event by year:", yr.round(1).to_dict())
    yr2 = (-1e4 * post[g.reindex(post.index)]).groupby(post[g.reindex(post.index)].index.year).mean()
    print("V2 gross bp/event by year:", yr2.round(1).to_dict())
