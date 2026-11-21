"""364 - Idea card S6 (results/ideas_settlement.md): Treasury Quarterly Refunding Announcement (QRA) window in TLT.
Duration holders demand a premium for carrying coupon-supply uncertainty into the QRA (Monday borrowing estimate ->
Wednesday policy statement, first week of Feb/May/Aug/Nov); an announced coupon-size change then reprices supply.

QRA DATES (documented, not reconstructed): Treasury's public archive page "Official Remarks on Quarterly Refunding by
Calendar Year" (home.treasury.gov ... /quarterly-refunding-archives/official-remarks-on-quarterly-refunding-by-calendar-
year) links each quarter's refunding statement press release; the statement date is read from the press-release page.
Only 2002-2022 statements are fetched (cached in data/qra/). The window rule (Friday before the borrowing-estimate
Monday = QRA Wednesday - 2 days) is mechanical; the dates are fixed well in advance, so a calendar position can be
decided at any earlier close.
COUPON-SIZE CHANGE (pre-specified, objective): sign of the change in the refunding NEW-ISSUE 10-year note size announced
in this statement vs the previous statement (read from "10-year note in the amount of $X billion"). Up = more
long-duration supply. If either statement has no new 10-year (e.g. May 2002 reopening) -> 0 (flat).

Instrument returns: Yahoo adjusted closes (TLT/IEF start 2002-07-30, so events start Oct-2002). Costs 1bp/side.
All positions via lib.backtest_weights(..., lag=1): for calendar windows the weight is set two closes before each held
session (dates known in advance; no price information used). For (c) the size change is public at ~8:30-9:00 ET on QRA
day, the weight is set at the QRA close and lag=1 delays entry to the QRA+1 close (realistic, conservative).

PRE-SPECIFIED VARIANTS (fixed before any results):
  A  long TLT from the close of the Friday before the borrowing-estimate Monday to the close of QRA Wednesday.
  B  long TLT on QRA Wednesday only (Tue close -> Wed close).
  C  directional: after a 10y size increase short TLT, after a decrease long TLT, 5 sessions (entry QRA+1 close via
     lag=1); unchanged QRAs flat.
  D  A on IEF.
Each registered twice: raw net (pos = held-day indicator), and timing value = net - mean signed exposure(period) x TLT/IEF
daily return (exposure-matched holding, uncosted).
Descriptive (not registered, <=2022 only): with/without QRAs whose window contains a scheduled FOMC decision; a control
for the turn-of-month overlap with 281 (same window shifted -4 and +4 weeks, i.e. into Jan/Apr/Jul/Oct and
Mar/Jun/Sep/Dec at a similar day-of-month); overlap with 133 long-duration auction days and 276 month-end window.
"""
import os, sys, re, json, time, html
import numpy as np, pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP, COST, END = "364_qra_window", 1.0, "2022-12-31"
BASE = "https://home.treasury.gov"
ARCH = BASE + ("/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/"
               "official-remarks-on-quarterly-refunding-by-calendar-year")
QDIR = os.path.join(lib.DATA, "qra")


def fetch(url, path):
    if not os.path.exists(path) or os.path.getsize(path) < 1000:
        r = requests.get(url, headers=lib._HDR, timeout=60)
        r.raise_for_status()
        open(path, "w").write(r.text)
        time.sleep(0.5)
    return open(path, errors="ignore").read()


def qra_table():
    os.makedirs(QDIR, exist_ok=True)
    t = fetch(ARCH, os.path.join(QDIR, "archive.html"))
    i = t.find("<table class=table>")
    seg = t[i:t.find("publicdebt.treas.gov", i)]
    yr, links = None, []
    for m in re.finditer(r'<p>(\d{4})</p>|<a href="?([^" >]+)"?[^>]*>(.*?)</a>', seg, re.S):
        if m.group(1):
            yr = int(m.group(1))
        elif yr and 2002 <= yr <= 2022:
            links.append(m.group(2))
    rows = []
    for u in links:
        p = fetch(BASE + u, os.path.join(QDIR, os.path.basename(u) + ".html"))
        d = re.search(r'datetime="?(\d{4}-\d{2}-\d{2})', p).group(1)
        s = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>", "", p, flags=re.S))))
        m = re.search(r"10-year note in the amount of \$([0-9.]+)", s)
        rows.append({"qra": pd.Timestamp(d), "release": os.path.basename(u), "size10": float(m.group(1)) if m else np.nan})
    q = pd.DataFrame(rows).sort_values("qra").reset_index(drop=True)
    q["d10"] = q["size10"].diff()
    q["chg"] = np.sign(q["d10"]).fillna(0.0)
    return q


def held_from_sessions(idx, sessions):
    """daily held exposure (earning return on that session) -> weights for backtest_weights(lag=1): W_t = held_{t+2}."""
    h = pd.Series(0.0, index=idx)
    for s, v in sessions:
        h.loc[s] += v
    return h, h.shift(-2).fillna(0.0)


def reg(name, held, W, R, notes):
    net = lib.backtest_weights(W.to_frame("X"), R.to_frame("X"), COST, lag=1).loc[:END]
    held = held.loc[:END]
    act = ((held != 0) | (net.abs() > 1e-12)).astype(float)
    raw = lib.register(EXP, name + " (raw, 1bp/side)", act, net, 0.0, notes=notes)
    tv = pd.Series(0.0, index=net.index)
    for a, b in [lib.TRAIN, lib.VALID]:
        tv.loc[a:b] = net.loc[a:b] - held.loc[a:b].mean() * R.loc[a:b].reindex(net.loc[a:b].index).fillna(0.0)
    tvr = lib.register(EXP, name + " (timing value vs exposure-matched holding)", pd.Series(1.0, index=net.index), tv,
                       0.0, notes="net minus mean signed exposure x always-long (uncosted)")
    return raw, tvr, net


def tstat(x):
    x = pd.Series(x).dropna()
    return x.mean(), (x.mean() / x.std() * np.sqrt(len(x)) if len(x) > 2 and x.std() > 0 else np.nan), len(x)


def main():
    q = qra_table()
    q = q[q.qra <= END].copy()
    px = pd.concat({s: lib.get(s)["aC"] for s in ["TLT", "IEF"]}, axis=1).dropna().loc[:END]
    R = px.pct_change()
    idx = px.index
    fomc = pd.read_csv(os.path.join(lib.HERE, "tools", "fomc_dates.csv"), parse_dates=["date"])
    fomc = set(fomc[fomc.type == "scheduled"].date)
    a = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "auctions.json")))["data"])
    lt = a.security_term.str.contains(r"^(10-Year|9-Year|20-Year|19-Year|30-Year|29-Year)")
    auc = set(pd.to_datetime(a[lt].auction_date))
    me = pd.Series(idx, index=idx).groupby(idx.to_period("M")).max()
    last3 = set()
    for d in me.values:
        k = idx.get_loc(pd.Timestamp(d)); last3.update(idx[k - 2:k + 1])

    def window(qd):
        """sessions earned in window A for QRA date qd (Fri close before Monday qd-2 -> close of qd)."""
        if qd not in idx:
            return None
        mon = qd - pd.Timedelta(days=2)
        kf = idx.searchsorted(mon) - 1            # last session strictly before Monday
        kq = idx.get_loc(qd)
        return idx[kf + 1:kq + 1] if kf >= 0 else None

    ev, sa, sb, sc, sd = [], [], [], [], []
    for _, r in q.iterrows():
        w = window(r.qra)
        if w is None or len(w) == 0 or w[0] < idx[2]:
            continue
        kq = idx.get_loc(r.qra)
        sa += [(s, 1.0) for s in w]; sd += [(s, 1.0) for s in w]
        sb += [(r.qra, 1.0)]
        post = idx[kq + 2:kq + 7]                 # lag=1: weight at QRA close -> earns QRA+2..QRA+6
        if r.chg != 0:
            sc += [(s, -r.chg) for s in post]
        ctl = {}
        for lab, off in [("m4w", -28), ("p4w", 28)]:
            cw = window(r.qra + pd.Timedelta(days=off)) if (r.qra + pd.Timedelta(days=off)) in idx else None
            ctl[lab] = 1e4 * ((1 + R.TLT.loc[cw]).prod() - 1) if cw is not None and len(cw) else np.nan
        ev.append({"qra": r.qra.date(), "release": r.release, "size10": r.size10, "d10": r.d10, "chg": r.chg,
                   "n_sess": len(w), "A_TLT_bp": 1e4 * ((1 + R.TLT.loc[w]).prod() - 1),
                   "D_IEF_bp": 1e4 * ((1 + R.IEF.loc[w]).prod() - 1), "B_TLT_bp": 1e4 * R.TLT.loc[r.qra],
                   "post5_TLT_bp": 1e4 * ((1 + R.TLT.loc[post]).prod() - 1) if len(post) == 5 else np.nan,
                   "fomc_in_window": any(s in fomc for s in w), "auction_in_window": any(s in auc for s in w),
                   "pre_auction_overlap": any(s in auc for s in idx[kq + 1:kq + 4]),
                   "month_end_overlap": any(s in last3 for s in w),
                   "ctl_m4w_TLT_bp": ctl["m4w"], "ctl_p4w_TLT_bp": ctl["p4w"]})
    ev = pd.DataFrame(ev)
    ev.to_csv(os.path.join(lib.HERE, "results", "364_qra_events.csv"), index=False)

    res = {}
    for name, ses, col in [("A long TLT Fri-before-Monday -> QRA Wed close", sa, "TLT"),
                           ("B long TLT on QRA Wednesday", sb, "TLT"),
                           ("C -sign(d 10y size) TLT, 5 sessions from QRA+1 close", sc, "TLT"),
                           ("D A on IEF", sd, "IEF")]:
        h, W = held_from_sessions(idx, ses)
        res[name] = reg(name, h, W, R[col], f"QRA dates from Treasury archive; n_sessions={int((h != 0).sum())}")

    ev["per"] = np.where(pd.to_datetime(ev.qra) <= lib.TRAIN[1], "train", "valid")
    print(f"QRA events (TLT era, <=2022): {len(ev)}; size changes up={int((ev.chg > 0).sum())} "
          f"down={int((ev.chg < 0).sum())}; FOMC-in-window={int(ev.fomc_in_window.sum())}; "
          f"long-auction-in-window={int(ev.auction_in_window.sum())}; month-end-overlap={int(ev.month_end_overlap.sum())}")
    print("\nDescriptive event means (bp, t, n) - <=2022 only")
    for per in ["train", "valid"]:
        e = ev[ev.per == per]
        out = [("A TLT all", e.A_TLT_bp), ("A TLT ex-FOMC", e.A_TLT_bp[~e.fomc_in_window]),
               ("A TLT FOMC only", e.A_TLT_bp[e.fomc_in_window]),
               ("ctl -4w TLT", e.ctl_m4w_TLT_bp), ("ctl +4w TLT", e.ctl_p4w_TLT_bp),
               ("A - mean(ctl)", e.A_TLT_bp - e[["ctl_m4w_TLT_bp", "ctl_p4w_TLT_bp"]].mean(axis=1)),
               ("D IEF all", e.D_IEF_bp), ("B TLT Wed", e.B_TLT_bp),
               ("post5 TLT | up", e.post5_TLT_bp[e.chg > 0]), ("post5 TLT | down", e.post5_TLT_bp[e.chg < 0]),
               ("post5 TLT | unch", e.post5_TLT_bp[e.chg == 0])]
        for lab, x in out:
            m, t, n = tstat(x)
            print(f"  {per:5s} {lab:18s} {m:+7.1f}  t={t:+5.2f}  n={n}")
    print()
    for name, (raw, tvr, net) in res.items():
        yr = net.groupby(net.index.year).sum() * 1e4
        print(name)
        print("  raw:", lib.fmt(raw), "gate", lib.passes_gate(raw))
        print("  TV :", lib.fmt(tvr), "gate", lib.passes_gate(tvr))
        print("  yearly bp:", " ".join(f"{k}:{v:+.0f}" for k, v in yr.loc[2002:2022].items()))
    print("registry rows:", lib.n_tests())


if __name__ == "__main__":
    main()
