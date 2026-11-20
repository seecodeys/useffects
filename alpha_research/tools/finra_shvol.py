"""Download FINRA consolidated daily short-volume files (cdn.finra.org/equity/regsho/daily/CNMSshvolYYYYMMDD.txt,
available 2019+) and keep only per-day aggregates: SPY row, QQQ row, and share-weighted totals over the current
S&P 500 constituents. Output: data/finra_shvol.csv (resumable)."""
import os, sys, io, time
import pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
out = os.path.join(lib.DATA, "finra_shvol.csv")
spx = set(pd.read_html(io.StringIO(requests.get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
          headers={"User-Agent": "research-script/1.0"}, timeout=30).text))[0].Symbol.str.replace(".", "", regex=False))
done = set(pd.read_csv(out).date.astype(str)) if os.path.exists(out) else set()
days = lib.get("SPY").loc[sys.argv[1]:sys.argv[2]].index
rows = []
for d in days:
    k = d.strftime("%Y%m%d")
    if k in done: continue
    for a in range(4):
        try:
            r = requests.get(f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{k}.txt", timeout=30); break
        except requests.RequestException: time.sleep(2 ** a)
    if r.status_code != 200: print("miss", k, r.status_code, flush=True); continue
    x = pd.read_csv(io.StringIO(r.text), sep="|").dropna(subset=["Symbol"])
    s = x[x.Symbol.isin(spx)]
    g = lambda sym, c: x.loc[x.Symbol == sym, c].sum()
    rows.append({"date": k, "spx_short": s.ShortVolume.sum(), "spx_total": s.TotalVolume.sum(),
                 "spy_short": g("SPY", "ShortVolume"), "spy_total": g("SPY", "TotalVolume"),
                 "qqq_short": g("QQQ", "ShortVolume"), "qqq_total": g("QQQ", "TotalVolume")})
    if len(rows) % 50 == 0:
        pd.DataFrame(rows).to_csv(out, mode="a", header=not os.path.exists(out), index=False); rows = []; print(k, flush=True)
    time.sleep(0.2)
if rows: pd.DataFrame(rows).to_csv(out, mode="a", header=not os.path.exists(out), index=False)
print("done")
