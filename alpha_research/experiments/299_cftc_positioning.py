"""299 - CFTC Traders-in-Financial-Futures positioning (new data: publicreporting.cftc.gov, dataset gpe5-46if).
Mechanism (flows/participants): leveraged funds (CTAs, hedge funds) crowd into trends and are forced to unwind;
asset managers' position changes proxy real-money allocation flows that persist; dealers are the liquidity
providers on the other side. Positions are as of Tuesday, published Friday 15:30 ET -> to stay safe the signal is
applied from the close of the first trading day AFTER the Friday release (lag=1), held for one week.
Pre-specified variants (all registered; z = net/OI vs trailing 156 weeks):
 A SPY  -sign(z_lev ES)   when |z|>1     (crowded leveraged funds unwind)
 B SPY  sign(4w change in asset-mgr net/OI, ES)   (real-money flow persistence)
 C TLT  -sign(z_lev TY)   when |z|>1
 D TLT  sign(4w change in asset-mgr net/OI, TY)
 E TLT  -sign(z_lev US bond) when |z|>1
 F SPY  sign(z_dealer ES) when |z|>1     (follow the liquidity providers)
Reported with the exposure-matched always-long baseline (mean position x buy-and-hold)."""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

def load(code):
    d = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "cftc", f"tff_{code}.json"))))
    d["date"] = pd.to_datetime(d.report_date_as_yyyy_mm_dd.str[:10])
    d = d.set_index("date").drop(columns="report_date_as_yyyy_mm_dd").astype(float).sort_index()
    oi = d.open_interest_all
    out = pd.DataFrame({"lev": (d.lev_money_positions_long - d.lev_money_positions_short) / oi,
                        "am": (d.asset_mgr_positions_long - d.asset_mgr_positions_short) / oi,
                        "dl": (d.dealer_positions_long_all - d.dealer_positions_short_all) / oi})
    out.index = out.index + pd.Timedelta(days=3)           # Friday release date
    return out

def z(s, n=156):
    return (s - s.rolling(n, min_periods=104).mean()) / s.rolling(n, min_periods=104).std()

def to_daily(sig, idx):
    # value known at Friday release -> becomes the decision at the close of the first trading day after it (lag handled below)
    return lib.asof(sig, idx, strict=True)       # strictly before the day: Friday's value first used on Monday's close

ES, TY, US = load("13874A"), load("043602"), load("020601")
def thr(zz): return (-np.sign(zz)).where(zz.abs() > 1, 0.0)
variants = {
    "A SPY -sign(z lev ES) |z|>1": ("SPY", thr(z(ES.lev)), 0.5),
    "B SPY sign(4w chg asset-mgr ES)": ("SPY", np.sign(ES.am.diff(4)), 0.5),
    "C TLT -sign(z lev TY) |z|>1": ("TLT", thr(z(TY.lev)), 1.0),
    "D TLT sign(4w chg asset-mgr TY)": ("TLT", np.sign(TY.am.diff(4)), 1.0),
    "E TLT -sign(z lev US) |z|>1": ("TLT", thr(z(US.lev)), 1.0),
    "F SPY sign(z dealer ES) |z|>1": ("SPY", -thr(z(ES.dl)), 0.5),
}
for name, (sym, sig, cost) in variants.items():
    d = lib.get(sym)
    w = to_daily(sig.dropna(), d.index).fillna(0.0)
    net = lib.backtest_weights(w.to_frame("X"), d.cc.to_frame("X"), cost, lag=0).loc["2009":]
    r = lib.register("299_cftc", f"299{name}", pd.Series(1.0, index=net.index), net, 0.0)
    held = w.loc["2009":"2022"].shift(1)
    base = held.mean() * d.cc.loc["2009":"2022"]
    excess = net.loc[:"2022"] - base.reindex(net.loc[:"2022"].index).fillna(0)
    t_ex = excess.mean() / excess.std() * np.sqrt(len(excess))
    print(f"299{name}: {lib.fmt(r)}  mean pos {held.mean():+.2f}  timing-value t (2009-22) {t_ex:+.2f}")
print("registry size:", lib.n_tests())
