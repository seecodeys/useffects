"""050/051 - Behavioral: over-reaction vs news, and attention-driven buying. Large-cap panel (survivorship-biased,
hand-picked; flagged). ALL signals use information known at the prior close; trade next day open->close.

resid  = stock close-to-close return - SPY close-to-close return (day t-1)
vratio = volume(t-1) / mean volume(t-21..t-2)          (abnormal volume = attention / news proxy)
Eligible: prior close >= $20.

050a  reversal, LOW-volume moves  (vratio < 1): long bottom-decile resid, short top decile   [over-reaction, no news]
050b  continuation, HIGH-volume moves (vratio > 2): long top-decile resid, short bottom      [news, under-reaction]
050c  plain reversal, all stocks (baseline)
051   attention: short stocks with vratio > 2 AND resid in the top decile, hedged with SPY (long SPY same $)
Costs 2bp/side per leg.
"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib
sys.path.insert(0, os.path.join(lib.HERE, "experiments"))
import importlib
U = importlib.import_module("006_stock_gap_reversal").UNIVERSE
EXP = "050_overreaction"
COST = 2.0

spy = lib.get("SPY")
OC, CC, V, PX = {}, {}, {}, {}
for s in U:
    try:
        d = lib.get(s).loc["1999-10":]
    except Exception:
        continue
    OC[s], CC[s], V[s], PX[s] = d.oc, d.cc, d.Volume.astype(float), d.Close
OC, CC, V, PX = (pd.DataFrame(x).loc["2000":] for x in (OC, CC, V, PX))
resid = CC.sub(spy.cc.reindex(CC.index), axis=0)
vratio = V / V.shift(1).rolling(20).mean()
elig = PX.shift(1) >= 20
sig = resid.shift(1).where(elig & resid.shift(1).abs().lt(0.5))
vr = vratio.shift(1)


def decile_ls(signal, long_low):
    rk = signal.rank(axis=1, pct=True)
    n = signal.notna().sum(axis=1)
    lo, hi = rk.le(0.1), rk.gt(0.9)
    w = lo.astype(float).div(lo.sum(axis=1), axis=0) * 0.5 - hi.astype(float).div(hi.sum(axis=1), axis=0) * 0.5
    w = (w if long_low else -w).where(n >= 20, 0).fillna(0)
    gross = (w * OC.fillna(0)).sum(axis=1)
    return gross, gross - w.abs().sum(axis=1) * 2 * COST / 1e4


def reg(name, net, gross):
    r = lib.register(EXP, name, pd.Series(1.0, index=net.index), net, 0.0, notes="2bp/side/leg in series; survivorship-biased universe")
    g = gross.loc[:"2022"]
    print(f"{name:<52} {lib.fmt(r)}  gate={lib.passes_gate(r)}")
    print("     gross bp/day by 5y:", (g.groupby((g.index.year // 5) * 5).mean() * 1e4).round(1).to_dict())
    return r


if "--followup" not in sys.argv:
  reg("050a reversal | low-volume moves (vratio<1)", *decile_ls(sig.where(vr < 1), True)[::-1])
  reg("050b continuation | high-volume moves (vratio>2)", *decile_ls(sig.where(vr > 2), False)[::-1])
  reg("050c plain reversal (all)", *decile_ls(sig, True)[::-1])

if "--followup" not in sys.argv:
  # 051 attention: short top-decile resid among vratio>2, hedge with SPY
  rk = sig.rank(axis=1, pct=True)
  att = (vr > 2) & (rk > 0.9)
  n_att = att.sum(axis=1)
  w = -att.astype(float).div(n_att.replace(0, np.nan), axis=0).fillna(0)
  stock_leg = (w * OC.fillna(0)).sum(axis=1)
  hedge = (n_att > 0).astype(float) * spy.oc.reindex(OC.index).fillna(0)
  gross = stock_leg + hedge
  net = gross - (n_att > 0).astype(float) * 2 * 2 * COST / 1e4
  reg("051 attention: short high-vol top movers, SPY-hedged", net, gross)
  print("   051 avg names/day when active:", round(n_att[n_att > 0].mean(), 1), " active days %:", round(100 * (n_att > 0).mean(), 1))
  print("registry size:", lib.n_tests())


# ---- follow-up (run with --followup): threshold designs that actually trade on high-volume days ----
if "--followup" in sys.argv:
    hv = vr > 2
    up, dn = hv & (sig > 0.02), hv & (sig < -0.02)
    def hedged(longs, shorts, name):
        nl, ns_ = longs.sum(axis=1), shorts.sum(axis=1)
        wl = longs.astype(float).div(nl.replace(0, np.nan), axis=0).fillna(0) * 0.5
        ws = -shorts.astype(float).div(ns_.replace(0, np.nan), axis=0).fillna(0) * 0.5
        w = wl + ws
        net_beta = w.sum(axis=1)                      # residual market exposure hedged with SPY
        gross = (w * OC.fillna(0)).sum(axis=1) - net_beta * spy.oc.reindex(OC.index).fillna(0)
        cost = (w.abs().sum(axis=1) + net_beta.abs()) * 2 * COST / 1e4
        reg(name, gross - cost, gross)
        print("     active days %:", round(100 * (w.abs().sum(axis=1) > 0).mean(), 1),
              " avg names:", round((nl + ns_)[(nl + ns_) > 0].mean(), 1))
    hedged(up, dn, "050b' continuation | high-vol: long up>2%, short down<-2%, SPY-hedged")
    hedged(up, up & False, "051r long attention winners (high-vol, up>2%), SPY-hedged")
    print("registry size:", lib.n_tests())
