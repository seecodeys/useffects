"""004 - Asian ADRs vs their home-listed shares.

Home shares close before 9:30 ET, so home_ret (all home sessions since the last
US close, local currency) is known. implied ADR gap = a + b*home_ret + c*spy_prev,
fitted on TRAIN (or first 5y of data if the ADR starts later).
surprise = implied - actual ADR gap; catch-up = long when surprise > 0.
Also: idiosyncratic version using home_ret minus home-index return.
FX omitted (noise). Costs 3bp/side (single-stock ADRs).
"""
import os, sys
import numpy as np
import pandas as pd
import statsmodels.api as sm
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lib

EXP = "004_adr_catchup"
COST = 3.0
ADRS = {  # ADR: (home share, home index)
    "TSM": ("2330.TW", "^TWII"), "UMC": ("2303.TW", "^TWII"), "CHT": ("2412.TW", "^TWII"),
    "SONY": ("6758.T", "^N225"), "TM": ("7203.T", "^N225"), "HMC": ("7267.T", "^N225"),
    "MUFG": ("8306.T", "^N225"), "SMFG": ("8316.T", "^N225"), "MFG": ("8411.T", "^N225"),
    "NMR": ("8604.T", "^N225"),
    "INFY": ("INFY.NS", "^BSESN"), "WIT": ("WIPRO.NS", "^BSESN"), "HDB": ("HDFCBANK.NS", "^BSESN"),
    "IBN": ("ICICIBANK.NS", "^BSESN"),
    "KB": ("105560.KS", "^KS11"), "PKX": ("005490.KS", "^KS11"), "SHG": ("055550.KS", "^KS11"),
}


def since_last_us_close(series, D):
    P = pd.DatetimeIndex(pd.Series(D, index=D).shift(1))
    return pd.Series(lib.asof(series, D).values / lib.asof(series, P).values - 1, index=D)


def build():
    spy = lib.get("SPY")
    S, Si, Y = {}, {}, {}
    for adr, (home, idx) in ADRS.items():
        try:
            a, h, ix = lib.get(adr).loc["2000":], lib.get(home)["aC"], lib.get(idx)["Close"]
        except Exception as e:
            print("skip", adr, e); continue
        D = a.index
        df = pd.DataFrame({"y": a.oc, "gap": a.gap, "home": since_last_us_close(h, D),
                           "hidx": since_last_us_close(ix, D),
                           "spy_prev": spy.cc.shift(1).reindex(D)}).dropna()
        df = df[(df.gap.abs() < 0.25) & (df.home.abs() < 0.25)]
        if len(df) < 1500:
            print("skip", adr, "short history"); continue
        tr = df.loc[lib.TRAIN[0]:lib.TRAIN[1]]
        if len(tr) < 750:
            tr = df.iloc[:1250]
        m = sm.OLS(tr.gap, sm.add_constant(tr[["home", "spy_prev"]])).fit()
        df["surprise"] = m.predict(sm.add_constant(df[["home", "spy_prev"]])) - df.gap
        # idiosyncratic: residual of home stock vs its index, compared with ADR gap residual vs index
        mi = sm.OLS(tr.gap, sm.add_constant(tr[["home", "hidx", "spy_prev"]])).fit()
        df["surprise_full"] = mi.predict(sm.add_constant(df[["home", "hidx", "spy_prev"]])) - df.gap
        S[adr], Si[adr], Y[adr] = df.surprise, df.surprise_full, df.y
        print(f"{adr:<5} from {df.index[0].date()} beta={m.params['home']:.2f} R2={m.rsquared:.2f} train_n={len(tr)}")
    return pd.DataFrame(S), pd.DataFrame(Si), pd.DataFrame(Y)


def xs(Sx, Y, top=3, min_n=8):
    rk, k = Sx.rank(axis=1), Sx.notna().sum(axis=1)
    w = pd.DataFrame(0.0, index=Sx.index, columns=Sx.columns)
    w[rk.ge(k.values[:, None] - top + 1)] = 0.5 / top
    w[rk.le(top)] = -0.5 / top
    w = w[k >= min_n].reindex(Sx.index).fillna(0.0)
    gross = (w * Y.fillna(0)).sum(axis=1)
    return gross, gross - w.abs().sum(axis=1) * 2 * COST / 1e4


if __name__ == "__main__":
    S, Si, Y = build()
    Y = Y.reindex(S.index)
    out = []
    for name, Sx in [("simple", S), ("with home-index", Si)]:
        pos = np.sign(Sx); n = pos.notna().sum(axis=1); pw = pos.div(n, axis=0)
        net = (pw * Y).sum(axis=1) - pw.abs().sum(axis=1) * 2 * COST / 1e4
        r = lib.register(EXP, f"pooled catch-up ({name})", pd.Series(1.0, index=net.index), net, 0.0,
                         notes="3bp/side in series")
        out.append(r); print(f"pooled catch-up {name:<16}", lib.fmt(r))
        gross, net = xs(Sx, Y)
        r = lib.register(EXP, f"XS catch-up L/S top3-bottom3 ({name})", pd.Series(1.0, index=net.index), net, 0.0,
                         notes="3bp/side in series")
        out.append(r); print(f"XS catch-up L/S {name:<16}", lib.fmt(r))
        g = gross.loc[:"2022"]
        print("   gross bp/day by year:", (g.groupby(g.index.year).mean() * 1e4).round(1).to_dict())
    for adr in S:
        s_ = S[adr].dropna()
        r = lib.register(EXP, f"{adr} catch-up sign(surprise)", np.sign(s_), Y[adr].reindex(s_.index), COST)
        out.append(r); print(f"{adr:<5}", lib.fmt(r))
    pd.DataFrame(out).to_csv(os.path.join(lib.HERE, "results", "004_adr_catchup.csv"), index=False)
    print("registry size:", lib.n_tests())
