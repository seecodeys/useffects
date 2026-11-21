"""Download 8-K Item 2.02 (Results of Operations) filing dates from SEC EDGAR submissions JSON for a fixed set of
long-lived mega-caps. Cached to data/edgar_202_<TICKER>.csv. Generic descriptive User-Agent as EDGAR requires."""
import os, sys, time, requests, pandas as pd
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(HERE, "data")
UA = {"User-Agent": "research-script admin@example.com"}
CIKS = {"AAPL": [320193], "MSFT": [789019], "AMZN": [1018724], "GOOGL": [1288776, 1652044], "XOM": [34088],
        "GE": [40545], "JNJ": [200406], "JPM": [19617], "WMT": [104169], "PG": [80424], "CVX": [93410],
        "IBM": [51143], "PFE": [78003], "INTC": [50863], "CSCO": [858877]}

def _rows(j):
    return pd.DataFrame({k: j[k] for k in ("form", "filingDate", "acceptanceDateTime", "items")})

def fetch(tic):
    path = os.path.join(DATA, f"edgar_202_{tic}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    out = []
    for cik in CIKS[tic]:
        j = requests.get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json", headers=UA, timeout=30).json()
        out.append(_rows(j["filings"]["recent"]))
        for f in j["filings"].get("files", []):
            time.sleep(0.2)
            out.append(_rows(requests.get(f"https://data.sec.gov/submissions/{f['name']}", headers=UA, timeout=30).json()))
        time.sleep(0.2)
    df = pd.concat(out)
    df = df[df.form.isin(["8-K", "8-K/A"]) & (df.form == "8-K") & df["items"].fillna("").str.contains("2.02")]
    df = df.drop_duplicates("filingDate").sort_values("filingDate")
    df.insert(0, "ticker", tic)
    df.to_csv(path, index=False)
    return df

if __name__ == "__main__":
    for t in CIKS:
        d = fetch(t); print(t, len(d), d.filingDate.min(), d.filingDate.max())
