"""308 - Commodity hedging pressure and speculator crowding (de Roon, Nijman & Veld 2000; Bessembinder 1992), CFTC
disaggregated COT (publicreporting.cftc.gov/resource/72hh-3qpy). Commodities: WTI crude (USO), gold (GLD),
natural gas (UNG), silver (SLV). Same timing as 299 (Tuesday positions, Friday release, first used at the next close).
Pre-specified, equal-weight across the 4 ETFs, weekly-updated positions, 2bp/side:
 A crowding: -sign(z managed-money net/OI) when |z|>1   (speculators crowded -> unwind)
 B hedging pressure: +sign(z producer net SHORT share) when |z|>1 (producers hedging heavily -> pay a premium to longs)
Reported as timing value over an exposure-matched long (USO/UNG carry big roll costs)."""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
def z(s, n=156): return (s - s.rolling(n, min_periods=104).mean()) / s.rolling(n, min_periods=104).std()
legs = {"USO": "067651", "GLD": "088691", "UNG": "023651", "SLV": "084691"}
R, WA, WB = {}, {}, {}
for etf, code in legs.items():
    d = pd.DataFrame(json.load(open(os.path.join(lib.DATA, "cftc", f"dis_{code}.json"))))
    d.index = pd.to_datetime(d.pop("report_date_as_yyyy_mm_dd").str[:10]) + pd.Timedelta(days=3)
    d = d.astype(float)
    mm = (d.m_money_positions_long_all - d.m_money_positions_short_all) / d.open_interest_all
    pr = (d.prod_merc_positions_short - d.prod_merc_positions_long) / d.open_interest_all
    px = lib.get(etf); R[etf] = px.cc
    zm, zp = z(mm), z(pr)
    WA[etf] = lib.asof((-np.sign(zm)).where(zm.abs() > 1, 0.0).dropna(), px.index, strict=True)
    WB[etf] = lib.asof(np.sign(zp).where(zp.abs() > 1, 0.0).dropna(), px.index, strict=True)
R = pd.DataFrame(R).loc["2006":]
for name, W in [("A crowding fade (managed money)", WA), ("B hedging pressure (producers)", WB)]:
    W = pd.DataFrame(W).reindex(R.index).fillna(0.0) / len(legs)
    net = lib.backtest_weights(W, R.fillna(0.0), 2.0).loc["2009":]
    held = W.shift(1).loc["2009":"2022"]
    base = (held.mean() * R.loc["2009":"2022"].fillna(0)).sum(axis=1)
    tv = net.loc[:"2022"] - base
    r = lib.register("308_hedging_pressure", f"308{name} (4 commodity ETFs, EW)", pd.Series(1.0, index=net.index), net, 0.0)
    print(f"308{name}: {lib.fmt(r)}  timing-value t (2009-22) {tv.mean()/tv.std()*np.sqrt(len(tv)):+.2f}")
    for etf in legs:
        w1 = W[[etf]]; n1 = lib.backtest_weights(w1, R[[etf]].fillna(0), 2.0)
        print(f"    {etf}: 2009-14 t {n1.loc['2009':'2014'].mean()/n1.loc['2009':'2014'].std()*np.sqrt(len(n1.loc['2009':'2014'])):+.2f}, "
              f"2015-22 t {n1.loc['2015':'2022'].mean()/n1.loc['2015':'2022'].std()*np.sqrt(len(n1.loc['2015':'2022'])):+.2f}")
print("registry size:", lib.n_tests())
