"""Feasibility study: does overseas market performance since the last US close
predict SPY's open -> close return?

Design
  * Train (in-sample)  : 2000-01-01 .. 2014-12-31  -> choose directions/params
  * Test (out-of-sample): 2015-01-01 .. latest     -> evaluate untouched
  * Positions in {-1, 0, +1}: buy/short SPY at the open, exit at the close.
  * Costs charged per side, in basis points of notional (both entry and exit).

Outputs go to results/.
"""
import os

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from features import build

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "results")
TRAIN_END = "2014-12-31"
TEST_START = "2015-01-01"
COSTS_BP = [0.0, 0.5, 1.0]   # per side


# ----------------------------------------------------------------- helpers
def split(df):
    return df.loc[:TRAIN_END], df.loc[TEST_START:]


def hac_ols(y, X, lags=5):
    X = sm.add_constant(X)
    return sm.OLS(y, X, missing="drop").fit(cov_type="HAC", cov_kwds={"maxlags": lags})


def metrics(pos, y, cost_bp):
    pos = pos.fillna(0)
    r = pos * y - np.abs(pos) * 2 * cost_bp / 1e4
    traded = pos != 0
    n = len(r)
    ann_ret = r.mean() * 252
    ann_vol = r.std() * np.sqrt(252)
    eq = (1 + r).cumprod()
    return {
        "ann_ret_%": 100 * ann_ret,
        "ann_vol_%": 100 * ann_vol,
        "sharpe": ann_ret / ann_vol if ann_vol > 0 else np.nan,
        "t_stat": r.mean() / (r.std() / np.sqrt(n)) if r.std() > 0 else np.nan,
        "hit_%": 100 * (np.sign(pos[traded]) == np.sign(y[traded])).mean() if traded.any() else np.nan,
        "bp_per_trade": 1e4 * r[traded].mean() if traded.any() else np.nan,
        "traded_%": 100 * traded.mean(),
        "max_dd_%": 100 * (eq / eq.cummax() - 1).min(),
    }


# ------------------------------------------------------------- strategies
def strategies(f):
    """Each returns a position Series over the full sample. Anything fitted is
    fitted on the training window only (or walk-forward, expanding)."""
    tr, _ = split(f)
    S = {}
    sgn = np.sign

    S["Always long"] = pd.Series(1.0, index=f.index)
    S["Gap follow"] = sgn(f.spy_gap)
    S["Gap fade"] = -sgn(f.spy_gap)

    # Raw overseas direction, both ways; we'll pick the train-better one
    for name, col in [("Asia", "asia_avg"), ("Europe-open", "eu_avg"), ("Global", "global_avg")]:
        S[f"{name} follow"] = sgn(f[col])
        S[f"{name} fade"] = -sgn(f[col])

    # Catch-up / over-reaction: how far the US gap deviates from what overseas
    # moves imply. gap_hat fitted on train only.
    Xc = ["asia_avg", "eu_avg", "spy_prev_ret"]
    m = hac_ols(tr.spy_gap, tr[Xc])
    gap_hat = m.predict(sm.add_constant(f[Xc]))
    f["surprise"] = gap_hat - f.spy_gap          # >0: US gapped less than overseas implies
    S["Catch-up (follow surprise)"] = sgn(f.surprise)
    S["Over-reaction (fade surprise)"] = -sgn(f.surprise)

    # Agreement: only trade when Asia, Europe open and the US gap all agree
    agree = (sgn(f.asia_avg) == sgn(f.eu_avg)) & (sgn(f.eu_avg) == sgn(f.spy_gap))
    S["Agreement follow"] = sgn(f.spy_gap).where(agree, 0)
    S["Agreement fade"] = -sgn(f.spy_gap).where(agree, 0)

    # Big-move filter: |global| in top 20% of train distribution
    thr = tr.global_avg.abs().quantile(0.8)
    big = f.global_avg.abs() > thr
    S["Big global move follow"] = sgn(f.global_avg).where(big, 0)
    S["Big global move fade"] = -sgn(f.global_avg).where(big, 0)

    # Linear models, walk-forward with annual expanding refit (first fit on
    # data up to 2004, so predictions start 2005)
    base = ["spy_gap", "spy_prev_ret", "spy_prev_intraday", "asia_avg", "eu_avg"]
    S["Linear model (walk-fwd)"] = walk_forward(f, base)
    S["Linear model, top-50% conviction"] = walk_forward(f, base, conviction=0.5)
    etf = base + ["etf_eu_avg", "etf_asia_avg"]
    S["[best-case] + country ETF opens"] = walk_forward(f, etf)
    return S


def walk_forward(f, cols, conviction=None, first_year=2005):
    pos = pd.Series(0.0, index=f.index)
    for yr in range(first_year, f.index.year.max() + 1):
        fit = f[f.index.year < yr].dropna(subset=cols)
        pred_rows = f[f.index.year == yr]
        if pred_rows.empty:
            continue
        m = sm.OLS(fit.y, sm.add_constant(fit[cols])).fit()
        pred = m.predict(sm.add_constant(pred_rows[cols].fillna(0), has_constant="add"))
        p = np.sign(pred)
        if conviction is not None:
            in_pred = m.predict(sm.add_constant(fit[cols]))
            thr = in_pred.abs().quantile(1 - conviction)
            p = p.where(pred.abs() > thr, 0)
        pos.loc[pred_rows.index] = p
    return pos


# ------------------------------------------------------------------- main
def main():
    os.makedirs(OUT, exist_ok=True)
    f = build()
    tr, te = split(f)
    lines = []
    P = lambda *a: (print(*a), lines.append(" ".join(str(x) for x in a)))

    P(f"Sample {f.index.min().date()} -> {f.index.max().date()}  "
      f"(train {len(tr)} days to {TRAIN_END}, test {len(te)} days from {TEST_START})\n")

    # 1. Correlations with the target, and with the US gap (is it priced in?)
    cols = ["spy_gap", "spy_prev_ret", "spy_prev_intraday", "asia_avg", "eu_avg", "global_avg",
            "etf_eu_avg", "etf_asia_avg"] + [c for c in f if c.startswith(("asia_", "eu_")) and "avg" not in c]
    corr = pd.DataFrame({
        "corr_y_train": tr[cols].corrwith(tr.y),
        "corr_y_test": te[cols].corrwith(te.y),
        "corr_with_spy_gap": f[cols].corrwith(f.spy_gap),
    })
    corr.to_csv(os.path.join(OUT, "correlations.csv"))
    P("== 1. Correlation with SPY open->close return ==")
    P(corr.round(3).to_string(), "\n")

    # 2. Regressions (HAC t-stats) - does overseas info add anything beyond the gap?
    P("== 2. OLS of open->close return, Newey-West t-stats ==")
    specs = {
        "gap only": ["spy_gap"],
        "overseas only": ["asia_avg", "eu_avg"],
        "full": ["spy_gap", "spy_prev_ret", "spy_prev_intraday", "asia_avg", "eu_avg"],
        "full + ETF (best-case)": ["spy_gap", "spy_prev_ret", "spy_prev_intraday", "asia_avg", "eu_avg",
                                   "etf_eu_avg", "etf_asia_avg"],
    }
    reg_rows = []
    for name, X in specs.items():
        for per, d in [("train", tr), ("test", te)]:
            m = hac_ols(d.y, d[X])
            row = {"spec": name, "period": per, "R2_%": 100 * m.rsquared, "n": int(m.nobs)}
            for x in X:
                row[f"b_{x}"] = m.params[x]
                row[f"t_{x}"] = m.tvalues[x]
            reg_rows.append(row)
    reg = pd.DataFrame(reg_rows)
    reg.to_csv(os.path.join(OUT, "regressions.csv"), index=False)
    for name in specs:
        P(f"-- {name}")
        sub = reg[reg.spec == name].set_index("period").drop(columns="spec").dropna(axis=1)
        P(sub.round(3).T.to_string())
    P("")

    # 3. Buckets
    S = strategies(f)  # also adds f['surprise']
    tr, te = split(f)
    P("== 3. Mean open->close return (bp) by quintile of signal ==")
    bucket_rows = []
    for col in ["asia_avg", "eu_avg", "global_avg", "spy_gap", "surprise"]:
        edges = tr[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy().copy()
        edges[0], edges[-1] = -np.inf, np.inf
        for per, d in [("train", tr), ("test", te)]:
            q = pd.cut(d[col], edges, labels=["Q1 (most neg)", "Q2", "Q3", "Q4", "Q5 (most pos)"])
            g = d.groupby(q, observed=True).y
            for lab, mean in g.mean().items():
                bucket_rows.append({"signal": col, "period": per, "quintile": lab,
                                    "mean_bp": 1e4 * mean, "up_%": 100 * (d.y[q == lab] > 0).mean(),
                                    "n": int((q == lab).sum())})
    buckets = pd.DataFrame(bucket_rows)
    buckets.to_csv(os.path.join(OUT, "buckets.csv"), index=False)
    P(buckets.pivot_table(index=["signal", "quintile"], columns="period", values="mean_bp").round(1).to_string(), "\n")

    # 4. Strategy backtests
    P("== 4. Strategy backtests ==")
    rows = []
    for name, pos in S.items():
        for per, idx in [("train", tr.index), ("test", te.index)]:
            for c in COSTS_BP:
                m = metrics(pos.loc[idx], f.y.loc[idx], c)
                rows.append({"strategy": name, "period": per, "cost_bp_side": c, **m})
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(OUT, "backtests.csv"), index=False)
    view = res[res.cost_bp_side == 0.5].pivot_table(
        index="strategy", columns="period", values=["sharpe", "ann_ret_%", "hit_%", "bp_per_trade", "traded_%", "t_stat"])
    view = view.reindex(list(S.keys()))
    P("(cost 0.5bp per side; linear models trade from 2005, so their 'train' = 2005-2014)")
    P(view.round(2).to_string(), "\n")

    # pick follow vs fade on TRAIN, report TEST
    P("== 5. Direction chosen in-sample, evaluated out-of-sample (0.5bp/side) ==")
    pairs = [("Gap follow", "Gap fade"), ("Asia follow", "Asia fade"), ("Europe-open follow", "Europe-open fade"),
             ("Global follow", "Global fade"), ("Catch-up (follow surprise)", "Over-reaction (fade surprise)"),
             ("Agreement follow", "Agreement fade"), ("Big global move follow", "Big global move fade")]
    r05 = res[res.cost_bp_side == 0.5].set_index(["strategy", "period"])
    chosen = []
    for a, b in pairs:
        pick = a if r05.loc[(a, "train"), "sharpe"] >= r05.loc[(b, "train"), "sharpe"] else b
        chosen.append(pick)
        P(f"{pick:<34} train Sharpe {r05.loc[(pick, 'train'), 'sharpe']:+.2f}  "
          f"-> test Sharpe {r05.loc[(pick, 'test'), 'sharpe']:+.2f}  "
          f"(test t={r05.loc[(pick, 'test'), 't_stat']:+.2f}, {r05.loc[(pick, 'test'), 'bp_per_trade']:+.2f} bp/trade)")
    for extra in ["Always long", "Linear model (walk-fwd)", "Linear model, top-50% conviction",
                  "[best-case] + country ETF opens"]:
        P(f"{extra:<34} train Sharpe {r05.loc[(extra, 'train'), 'sharpe']:+.2f}  "
          f"-> test Sharpe {r05.loc[(extra, 'test'), 'sharpe']:+.2f}  "
          f"(test t={r05.loc[(extra, 'test'), 't_stat']:+.2f}, {r05.loc[(extra, 'test'), 'bp_per_trade']:+.2f} bp/trade)")
    P("")

    # 6. Year-by-year for chosen strategies
    yearly = {}
    for name in chosen + ["Always long", "Linear model (walk-fwd)"]:
        r = S[name].fillna(0) * f.y - np.abs(S[name].fillna(0)) * 2 * 0.5 / 1e4
        yearly[name] = r.groupby(f.index.year).sum() * 100
    yearly = pd.DataFrame(yearly)
    yearly.to_csv(os.path.join(OUT, "yearly_returns_pct.csv"))
    P("== 6. Calendar-year return % (0.5bp/side) ==")
    P(yearly.round(1).to_string(), "\n")

    # 7. Regime check: does anything work when VIX is high?
    P("== 7. Test-period Sharpe by prior-day VIX regime (0.5bp/side) ==")
    reg_rows = []
    hi = te.vix_prev > te.vix_prev.median()
    for name in chosen + ["Linear model (walk-fwd)"]:
        pos = S[name].loc[te.index]
        reg_rows.append({"strategy": name,
                         "low VIX": metrics(pos[~hi], te.y[~hi], 0.5)["sharpe"],
                         "high VIX": metrics(pos[hi], te.y[hi], 0.5)["sharpe"]})
    P(pd.DataFrame(reg_rows).set_index("strategy").round(2).to_string(), "\n")

    # charts
    fig, ax = plt.subplots(figsize=(11, 6))
    for name in chosen + ["Always long", "Linear model (walk-fwd)", "[best-case] + country ETF opens"]:
        pos = S[name].loc[te.index].fillna(0)
        r = pos * te.y - np.abs(pos) * 2 * 0.5 / 1e4
        ax.plot((1 + r).cumprod(), label=name, lw=1.4 if "best-case" not in name else 1, ls="--" if "best-case" in name else "-")
    ax.axhline(1, color="grey", lw=0.8)
    ax.set_title("Out-of-sample (2015+) growth of $1, SPY open->close, 0.5bp/side costs")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "test_equity_curves.png"), dpi=130)

    fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharey=True)
    for ax, col in zip(axes, ["asia_avg", "eu_avg", "surprise"]):
        b = buckets[buckets.signal == col].pivot(index="quintile", columns="period", values="mean_bp")
        b[["train", "test"]].plot.bar(ax=ax, rot=30)
        ax.set_title(col)
        ax.axhline(0, color="k", lw=0.8)
        ax.set_xlabel("")
    axes[0].set_ylabel("mean SPY open->close (bp)")
    fig.suptitle("SPY intraday return by quintile of overseas signal")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "quintiles.png"), dpi=130)

    with open(os.path.join(OUT, "summary.txt"), "w") as fh:
        fh.write("\n".join(lines))


if __name__ == "__main__":
    main()
