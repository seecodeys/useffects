"""Slowly download many HistData years in the background, tolerating rate limits."""
import sys, time, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import histdata

jobs = [(s, y) for s in sys.argv[1].split(",") for y in range(int(sys.argv[2]), int(sys.argv[3]) + 1)]
for sym, y in jobs:
    for attempt in range(6):
        try:
            print(sym, y, histdata.download(sym, y, pause=25), flush=True)
            break
        except Exception as e:
            print(sym, y, "retry", attempt, e, flush=True)
            time.sleep(120 * (attempt + 1))
