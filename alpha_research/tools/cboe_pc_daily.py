"""CBOE daily put/call ratios from the market-statistics JSON (cdn.cboe.com/data/us/options/market_statistics/daily/
YYYY-MM-DD_daily_options), Oct 2019 onward -> data/cboe_pc_daily.csv (resumable). Extends the CSV history (ends 2019-10-04)."""
import os, sys, time, json
import pandas as pd, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
out = os.path.join(lib.DATA, "cboe_pc_daily.csv")
done = set(pd.read_csv(out).date) if os.path.exists(out) else set()
rows = []
for d in lib.get("SPY").loc[sys.argv[1]:sys.argv[2]].index.strftime("%Y-%m-%d"):
    if d in done: continue
    try:
        r = requests.get(f"https://cdn.cboe.com/data/us/options/market_statistics/daily/{d}_daily_options", timeout=30)
        rat = {x["name"]: float(x["value"]) for x in r.json()["ratios"]}
    except Exception as e:
        print("miss", d, e, flush=True); continue
    rows.append({"date": d, "equity": rat.get("EQUITY PUT/CALL RATIO"), "index": rat.get("INDEX PUT/CALL RATIO"),
                 "total": rat.get("TOTAL PUT/CALL RATIO")})
    if len(rows) == 100:
        pd.DataFrame(rows).to_csv(out, mode="a", header=not os.path.exists(out), index=False); rows = []; print(d, flush=True)
    time.sleep(0.15)
if rows: pd.DataFrame(rows).to_csv(out, mode="a", header=not os.path.exists(out), index=False)
print("done")
