"""Full-timeline, trade-by-trade backtest of the month_end_trader account as it would run at IBKR.

Account = SPY core (100% of net liq, dividends reinvested) + two futures overlays:
  Leg A (research 013 + 168 vol tilt): last NYSE session of each month, MES open->close,
        direction -sign(SPY MTD - TLT MTD) through T-1 (dividend-adjusted closes),
        notional LEV_A x net liq at full size if SPY 20d vol (T-1) > 2002-08..2014 median, else half.
  Leg B (research 276): long ZN from the close of the 4th-last session to the close of the last session
        of each month. Notional LEV_B x net liq x hedge ratio (beta of IEF on ZN). Return modelled as
        (IEF total return - T-bill) / hedge ratio per contract (futures earn excess return).

Run:  python3 simulate.py          (writes trades.csv, yearly.csv, scenarios.csv, *.png, summary.md here)
"""
import os, sys, math, warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

# =============================================================== PARAMETERS
LIB_DIR = "/tmp/claude-0/-home-user-useffects/bdec50ca-f87a-5f19-aaff-f84bd7c2facb/scratchpad/ar/alpha_research"
OUT = os.path.dirname(os.path.abspath(__file__))

START = "2003-01-02"            # first simulated session (TLT starts 2002-07-30; vol window needs ~1 month)
END = None                      # None = last session of the latest COMPLETE month in the data
START_EQUITY = 100_000.0
ACCOUNT_SIZES = [25_000, 100_000, 250_000, 1_000_000]

LEV_A = 2.85                    # MES notional / net liq at full size (half size = LEV_A / 2)
LEV_B = 2.81                    # IEF-equivalent notional / net liq
VOL_WIN = 20
VOL_MEDIAN_WINDOW = ("2002-08-01", "2014-12-31")
B_HOLD_RANK = 4                 # enter at close of the 4th-last session, exit at close of the last one
BETA_WIN, BETA_MIN = 252, 60    # trailing window (sessions) for the IEF-on-ZN hedge ratio, roll days excluded

MES_MULT, MES_TICK = 5.0, 0.25          # $5 x index, tick 0.25 pt = $1.25
ZN_MULT, ZN_TICK = 1000.0, 1 / 64       # $1000 x price, tick 1/64 pt = $15.625
COSTS = {   # per contract per side: all-in fees $, slippage in ticks
    "base":   {"mes_fee": 0.62, "mes_slip_ticks": 1.0, "zn_fee": 1.70, "zn_slip_ticks": 0.5},
    "stress": {"mes_fee": 0.62, "mes_slip_ticks": 2.0, "zn_fee": 1.70, "zn_slip_ticks": 1.0},
    "zero":   {"mes_fee": 0.0,  "mes_slip_ticks": 0.0, "zn_fee": 0.0,  "zn_slip_ticks": 0.0},
}
# SPY core
CORE_BAND = 0.10                # rebalance core back to 100% of net liq when |core/netliq - 1| > band (None = never)
SPY_COMM_PER_SH, SPY_COMM_MIN, SPY_COMM_MAX_PCT = 0.0035, 0.35, 0.01   # IBKR Pro tiered
SPY_FEES_PER_SH = 0.0005        # exchange/clearing/FINRA TAF, rough
SPY_SLIP_PER_SH = 0.005         # half a penny spread at the close
# cash & margin
CASH_INTEREST = True            # cash balance earns/pays IBKR-like rates (benchmark ~ ^IRX)
CREDIT_SPREAD, DEBIT_SPREAD = -0.005, 0.015   # credit BM-0.5% (floored at 0), debit BM+1.5%
ZN_OVERNIGHT_MARGIN = 0.02      # ZN maintenance margin ~2% of notional; funded from cash (borrowed if cash < margin)
# =============================================================================

sys.path.insert(0, LIB_DIR)
import lib  # noqa: E402
import exchange_calendars as xcals  # noqa: E402


def load():
    spy, tlt, ief = lib.get("SPY"), lib.get("TLT"), lib.get("IEF")
    gspc, zn, irx = lib.get("^GSPC"), lib.get("ZN=F"), lib.get("^IRX")
    idx = spy.index.intersection(tlt.index).intersection(ief.index)
    idx = idx[idx >= "2002-08-01"]
    d = pd.DataFrame(index=idx)
    d["spy_open"], d["spy_close"], d["spy_aC"], d["spy_cc"], d["spy_oc"] = (spy.loc[idx, c] for c in
                                                                            ["Open", "Close", "aC", "cc", "oc"])
    d["tlt_aC"] = tlt.loc[idx, "aC"]
    d["ief_aC"], d["ief_cc"] = ief.loc[idx, "aC"], ief.loc[idx, "cc"]
    d["gspc_close"] = gspc.Close.reindex(idx).ffill()
    d["gspc_open_raw"] = gspc.Open.reindex(idx)
    d["zn_close"] = zn.Close.reindex(idx).ffill()
    d["zn_cc"] = zn.Close.reindex(idx).pct_change()          # NaN around missing days (used for the beta)
    d["zn_cc_f"] = d.zn_close.pct_change().fillna(0)          # forward-filled, for marking a ZN=F-priced position
    d["irx"] = irx.Close.reindex(idx).ffill() / 100
    # calendar
    cal = xcals.get_calendar("XNYS")
    closes = cal.schedule.loc[idx[0]:idx[-1], "close"].dt.tz_convert("America/New_York")
    d["close_time"] = closes.reindex(idx).dt.strftime("%H:%M").fillna("16:00").values
    d["days"] = pd.Series(idx, index=idx).diff().dt.days.fillna(1).values   # calendar days since prior session
    ym = pd.Series(idx.to_period("M"), index=idx)
    d["ym"] = ym
    d["rank_bwd"] = pd.Series(1, index=idx)[::-1].groupby(ym[::-1]).cumsum()[::-1]
    # ---- leg A signal (same construction as research 013 / 349)
    first_cc = lambda col: d[col].groupby(ym).transform("first")
    s_prev = d.spy_aC.groupby(ym).transform("first") / (1 + d.spy_cc.groupby(ym).transform("first"))
    tlt_cc = d.tlt_aC.pct_change()
    t_prev = d.tlt_aC.groupby(ym).transform("first") / (1 + tlt_cc.groupby(ym).transform("first"))
    d["spy_mtd"] = d.spy_aC.shift(1) / s_prev - 1
    d["tlt_mtd"] = d.tlt_aC.shift(1) / t_prev - 1
    d["gap"] = d.spy_mtd - d.tlt_mtd
    d["vol20"] = d.spy_cc.rolling(VOL_WIN).std().shift(1)
    med = d.vol20.loc[VOL_MEDIAN_WINDOW[0]:VOL_MEDIAN_WINDOW[1]].median()
    d["sizeA"] = np.where(d.vol20 > med, 1.0, 0.5)
    # ---- hedge ratio: trailing beta of IEF daily returns on ZN=F daily returns, excluding roll windows
    # (Yahoo's continuous ZN=F rolls ~19th-23rd of Mar/Jun/Sep/Dec; exclude day 15-25 of those months)
    roll = d.index.month.isin([3, 6, 9, 12]) & (d.index.day >= 15) & (d.index.day <= 25)
    x = d.zn_cc.where(~roll); y = d.ief_cc.where(~roll)
    ok = x.notna() & y.notna()
    x, y = x.where(ok), y.where(ok)
    cov = (x * y).rolling(BETA_WIN, min_periods=BETA_MIN).mean() - x.rolling(BETA_WIN, min_periods=BETA_MIN).mean() * \
        y.rolling(BETA_WIN, min_periods=BETA_MIN).mean()
    var = (x * x).rolling(BETA_WIN, min_periods=BETA_MIN).mean() - x.rolling(BETA_WIN, min_periods=BETA_MIN).mean() ** 2
    d["beta"] = (cov / var).ffill()
    xs, ys = x.loc[START:].dropna(), y.loc[START:].dropna()
    full_beta = np.polyfit(xs, ys, 1)[0]
    wk = d[["ief_aC"]].assign(zn=d.zn_close)[~roll].resample("W-FRI").last().pct_change().dropna()  # rough weekly check
    info = {"vol_median": med, "beta_full": full_beta, "beta_corr": np.corrcoef(xs, ys)[0, 1],
            "beta_weekly": np.polyfit(wk.zn, wk.ief_aC, 1)[0], "n_roll_excluded": int(roll.sum())}
    end = END
    if end is None:
        complete = d.ym != d.ym.iloc[-1]
        end = d.index[complete][-1]
    d = d.loc[:end]
    return d, info


def spy_trade_cost(shares, price):
    sh = abs(shares)
    if sh < 1e-9:
        return 0.0
    comm = min(max(SPY_COMM_MIN, SPY_COMM_PER_SH * sh), SPY_COMM_MAX_PCT * sh * price)
    return comm + (SPY_FEES_PER_SH + SPY_SLIP_PER_SH) * sh


def simulate(d, equity0=START_EQUITY, cost="base", fractional=False, research=False, core_band="param", b_src="ief"):
    """research=True: mimic the research P&L (fractional contracts, no costs, core rebalanced daily,
    B earns IEF total return rather than excess, no cash interest)."""
    c = COSTS["zero" if research else cost]
    band = CORE_BAND if core_band == "param" else core_band
    rnd = (lambda v: v) if (fractional or research) else (lambda v: math.floor(max(v, 0.0) + 1e-9))
    dd = d.loc[START:]
    idx = dd.index
    core, cash = equity0, 0.0
    nl_prev = equity0
    B = None                      # open leg-B position
    rows, trades = [], []
    rf_prev = dd.irx.iloc[0]
    for i, t in enumerate(idx):
        r = dd.loc[t]
        # --- overnight: core return, cash interest (on cash net of ZN margin funding)
        core_ret = core * r.spy_cc if i > 0 else 0.0
        core += core_ret
        interest = 0.0
        if CASH_INTEREST and not research and i > 0:
            margin = B["n"] * ZN_MULT * B["P"] * ZN_OVERNIGHT_MARGIN if B else 0.0
            bal = cash - margin
            rate = max(rf_prev + CREDIT_SPREAD, 0.0) if bal > 0 else rf_prev + DEBIT_SPREAD
            interest = bal * rate * r.days / 360
            cash += interest
        rf_d = rf_prev * r.days / 360           # T-bill accrual since prior close
        a_pnl = b_pnl = cost_day = 0.0
        # --- leg B mark-to-market / exit
        if B is not None:
            ief_ret = r.ief_cc if research else r.ief_cc - rf_d
            P_new = B["P"] * (1 + (r.zn_cc_f if b_src == "znf" else ief_ret / B["beta"]))
            pnl = B["n"] * ZN_MULT * (P_new - B["P"])
            B["P"] = P_new; B["gross"] += pnl; b_pnl += pnl; cash += pnl
            if r.rank_bwd == 1:
                fee = B["n"] * c["zn_fee"]; slip = B["n"] * c["zn_slip_ticks"] * ZN_TICK * ZN_MULT
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B["fee"] += fee; B["slip"] += slip
                B.update(exit_date=t, exit_time=r.close_time, exit_price=P_new)
                B["closed"] = True
        # --- leg A (intraday, last session of the month)
        if r.rank_bwd == 1 and np.isfinite(r.gap) and r.gap != 0 and np.isfinite(r.vol20):
            dirn = -np.sign(r.gap)
            px_prev = dd.gspc_close.iloc[i - 1] if i > 0 else r.gspc_close
            n = rnd(LEV_A * r.sizeA * nl_prev / (MES_MULT * px_prev))
            # entry = S&P level implied by SPY's 09:30 open (Yahoo's ^GSPC opens are stale pre-2014)
            p_in = r.gspc_close / (1 + r.spy_oc); p_out = r.gspc_close
            gross = dirn * n * MES_MULT * (p_out - p_in)
            fee = 2 * n * c["mes_fee"]; slip = 2 * n * c["mes_slip_ticks"] * MES_TICK * MES_MULT
            net = gross - fee - slip
            cash += net; a_pnl += net; cost_day += fee + slip
            trades.append(dict(leg="A", entry_date=t.date(), entry_time="09:30", exit_date=t.date(),
                               exit_time=r.close_time, direction="long" if dirn > 0 else "short", contracts=n,
                               contract_notional=MES_MULT * p_in, position_notional=n * MES_MULT * p_in,
                               lev_x_netliq=n * MES_MULT * p_in / nl_prev,
                               entry_price=p_in, exit_price=p_out, gross_pnl=gross, commissions=fee,
                               slippage=slip, net_pnl=net, net_pct_account=100 * net / nl_prev,
                               netliq_at_entry=nl_prev, vol_tilt_size=r.sizeA, signal_gap_pct=100 * r.gap,
                               hedge_ratio=np.nan))
        nl = core + cash
        # --- leg B record exit / entry at the close
        if B is not None and B.get("closed"):
            net = B["gross"] - B["fee"] - B["slip"]
            trades.append(dict(leg="B", entry_date=B["entry_date"].date(), entry_time=B["entry_time"],
                               exit_date=t.date(), exit_time=B["exit_time"], direction="long", contracts=B["n"],
                               contract_notional=ZN_MULT * B["P0"], position_notional=B["n"] * ZN_MULT * B["P0"],
                               lev_x_netliq=B["n"] * ZN_MULT * B["P0"] / B["nl0"],
                               entry_price=B["P0"], exit_price=B["exit_price"], gross_pnl=B["gross"],
                               commissions=B["fee"], slippage=B["slip"], net_pnl=net,
                               net_pct_account=100 * net / B["nl0"], netliq_at_entry=B["nl0"],
                               vol_tilt_size=np.nan, signal_gap_pct=np.nan, hedge_ratio=B["beta"]))
            B = None
        if r.rank_bwd == B_HOLD_RANK and t < idx[-1]:
            beta = r.beta
            n = rnd(LEV_B * nl * beta / (ZN_MULT * r.zn_close))
            if n > 0:
                fee = n * c["zn_fee"]; slip = n * c["zn_slip_ticks"] * ZN_TICK * ZN_MULT
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B = dict(n=n, P=r.zn_close, P0=r.zn_close, beta=beta, gross=0.0, fee=fee, slip=slip,
                         entry_date=t, entry_time=r.close_time, nl0=nl)
            else:
                trades.append(dict(leg="B", entry_date=t.date(), entry_time=r.close_time, exit_date=None,
                                   exit_time=None, direction="skipped (0 contracts)", contracts=0,
                                   contract_notional=ZN_MULT * r.zn_close, position_notional=0, lev_x_netliq=0,
                                   entry_price=r.zn_close, exit_price=np.nan, gross_pnl=0, commissions=0,
                                   slippage=0, net_pnl=0, net_pct_account=0, netliq_at_entry=nl,
                                   vol_tilt_size=np.nan, signal_gap_pct=np.nan, hedge_ratio=beta,
                                   _need=LEV_B * nl * beta / (ZN_MULT * r.zn_close)))
        # --- core rebalance at the close
        nl = core + cash
        reb = 0.0
        if research:
            core, cash = nl, 0.0
        elif band is not None and abs(core / nl - 1) > band:
            sh = (nl - core) / r.spy_close
            reb = spy_trade_cost(sh, r.spy_close)
            core, cash = nl - reb, 0.0
            cost_day += reb
        nl = core + cash
        if trades and trades[-1]["exit_date"] == t.date():
            for tr in trades[::-1]:
                if tr["exit_date"] != t.date():
                    break
                tr["equity_after"] = nl
        rows.append(dict(date=t, netliq=nl, core=core, cash=cash, core_pnl=core_ret, a_pnl=a_pnl, b_pnl=b_pnl,
                         interest=interest, costs=cost_day, rebalance_cost=reb, rf_d=rf_d))
        nl_prev = nl
        rf_prev = r.irx
    eq = pd.DataFrame(rows).set_index("date")
    tr = pd.DataFrame(trades)
    return eq, tr


# =============================================================== stats
def perf(eq, tr, bench_ret):
    r = eq.netliq.pct_change().fillna(eq.netliq.iloc[0] / START_EQUITY_OF(eq) - 1)
    rf = eq.rf_d
    yrs = len(r) / 252
    n_years = (eq.index[-1] - eq.index[0]).days / 365.25
    e0 = START_EQUITY_OF(eq)
    cagr = (eq.netliq.iloc[-1] / e0) ** (1 / n_years) - 1
    x = r - rf; bx = bench_ret.loc[r.index] - rf
    beta = np.cov(x, bx)[0, 1] / bx.var(); res = x - beta * bx
    dn = x[x < 0]
    dd = eq.netliq / eq.netliq.cummax() - 1
    # longest drawdown (peak -> recovery, calendar days)
    peak = eq.netliq.cummax(); under = eq.netliq < peak
    longest, start = 0, None
    for t, u in under.items():
        if u and start is None:
            start = t
        if not u and start is not None:
            longest = max(longest, (t - start).days); start = None
    if start is not None:
        longest = max(longest, (eq.index[-1] - start).days)
    out = {"final equity $": eq.netliq.iloc[-1], "CAGR %": 100 * cagr, "vol %": 100 * r.std() * np.sqrt(252),
           "Sharpe": x.mean() / x.std() * np.sqrt(252), "Sortino": x.mean() / np.sqrt((dn ** 2).sum() / len(x)) * np.sqrt(252),
           "max DD %": 100 * dd.min(), "longest DD days": longest, "beta": beta,
           "alpha %/yr": 100 * res.mean() * 252, "alpha t": res.mean() / res.std() * np.sqrt(len(res))}
    for leg in ["A", "B"]:
        t = tr[(tr.leg == leg) & (tr.contracts > 0)]
        p = t.net_pnl
        out[f"{leg} trades"] = len(t)
        out[f"{leg} skipped"] = int(((tr.leg == leg) & (tr.contracts == 0)).sum())
        if len(t):
            out[f"{leg} win %"] = 100 * (p > 0).mean()
            out[f"{leg} avg % acct"] = t.net_pct_account.mean()
            out[f"{leg} median % acct"] = t.net_pct_account.median()
            out[f"{leg} profit factor"] = p[p > 0].sum() / -p[p < 0].sum() if (p < 0).any() else np.inf
            out[f"{leg} best % acct"] = t.net_pct_account.max()
            out[f"{leg} worst % acct"] = t.net_pct_account.min()
            out[f"{leg} avg lev x"] = t.lev_x_netliq.mean()
            out[f"{leg} net P&L $"] = p.sum()
    out["costs $"] = eq.costs.sum(); out["interest $"] = eq.interest.sum()
    return out


def START_EQUITY_OF(eq):
    return eq.attrs.get("e0", START_EQUITY)


def bench_stats(eq, bench_ret):
    r = bench_ret.loc[eq.index].copy(); r.iloc[0] = 0.0
    e = (1 + r).cumprod()
    fake = pd.DataFrame({"netliq": e * START_EQUITY, "rf_d": eq.rf_d, "costs": 0.0, "interest": 0.0})
    fake.attrs["e0"] = START_EQUITY
    s = perf(fake, pd.DataFrame(columns=["leg", "contracts", "net_pnl", "net_pct_account", "lev_x_netliq"]), bench_ret)
    return {k: v for k, v in s.items() if not k.startswith(("A ", "B ", "costs", "interest"))}


def yearly(eq, tr, bench_ret):
    y = eq.index.year
    r = eq.netliq.pct_change(); r.iloc[0] = eq.netliq.iloc[0] / START_EQUITY_OF(eq) - 1
    b = bench_ret.loc[eq.index].copy(); b.iloc[0] = 0.0
    Y = pd.DataFrame({"strategy %": 100 * ((1 + r).groupby(y).prod() - 1),
                      "S&P 500 TR %": 100 * ((1 + b).groupby(y).prod() - 1)})
    Y["excess pp"] = Y["strategy %"] - Y["S&P 500 TR %"]
    Y["max DD %"] = 100 * eq.netliq.groupby(y).apply(lambda s: (s / np.maximum.accumulate(
        np.r_[eq.netliq.shift(1).loc[s.index[0]] if s.index[0] != eq.index[0] else START_EQUITY_OF(eq), s.values])[1:] - 1).min())
    Y["S&P max DD %"] = 100 * (1 + b).groupby(y).apply(lambda s: (s.cumprod() / s.cumprod().cummax() - 1).min().clip(max=0))
    Y["A net P&L $"] = eq.a_pnl.groupby(y).sum()
    Y["B net P&L $"] = eq.b_pnl.groupby(y).sum()
    Y["costs $"] = eq.costs.groupby(y).sum()
    Y["interest $"] = eq.interest.groupby(y).sum()
    t = tr[tr.contracts > 0].copy(); t["year"] = pd.to_datetime(t.exit_date).dt.year
    for leg in ["A", "B"]:
        g = t[t.leg == leg].groupby("year").net_pnl
        Y[f"{leg} trades"] = g.count().reindex(Y.index).fillna(0).astype(int)
        Y[f"{leg} win %"] = (100 * g.apply(lambda p: (p > 0).mean())).reindex(Y.index)
    Y["year-end equity $"] = eq.netliq.groupby(y).last()
    Y.index.name = "year"
    return Y


# =============================================================== charts
C1, C2, C3, C4, INK, INK2, GRID, SURF = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def charts(eq, eq_stress, bench_ret):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mt
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2,
                         "ytick.color": INK2, "axes.spines.top": False, "axes.spines.right": False,
                         "figure.facecolor": SURF, "axes.facecolor": SURF})
    b = bench_ret.loc[eq.index].copy(); b.iloc[0] = 0
    beq = START_EQUITY * (1 + b).cumprod()
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(11, 7.5), sharex=True, gridspec_kw={"height_ratios": [3, 1.2]})
    ax.plot(eq.index, eq.netliq, color=C1, lw=2, label="Strategy: SPY core + A + B overlay (base costs)")
    ax.plot(eq_stress.index, eq_stress.netliq, color=C1, lw=1, ls="--", alpha=0.8, label="Strategy, stress costs")
    ax.plot(beq.index, beq, color=C2, lw=2, label="S&P 500 total return (SPY, dividends reinvested)")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}k"))
    ax.yaxis.set_minor_formatter(mt.NullFormatter())
    ax.set_yticks([1e5, 2e5, 5e5, 1e6, 2e6, 5e6, 1e7, 2e7, 5e7])
    ax.set_ylim(eq.netliq.min() * 0.85, max(eq.netliq.max(), beq.max()) * 1.2)
    ax.set_ylabel("Account equity (log scale)")
    ax.grid(True, color=GRID, lw=0.8); ax.set_axisbelow(True)
    for s, lbl, col in [(eq.netliq, f"${eq.netliq.iloc[-1]/1e6:.1f}M", C1), (beq, f"${beq.iloc[-1]/1e6:.2f}M", C2)]:
        ax.annotate(lbl, (s.index[-1], s.iloc[-1]), xytext=(4, 0), textcoords="offset points", va="center",
                    color=INK, fontsize=9)
    ax.axvline(pd.Timestamp("2023-01-01"), color=INK2, lw=0.8, ls=":")
    ax.text(pd.Timestamp("2023-02-01"), ax.get_ylim()[0] * 1.15, "holdout 2023+", color=INK2, fontsize=8)
    ax.legend(loc="upper left", frameon=False)
    ax.set_title(f"Month-end trader account from ${START_EQUITY/1e3:.0f}k, {eq.index[0]:%b %Y} - {eq.index[-1]:%b %Y} "
                 "(whole contracts, IBKR costs)", loc="left", color=INK, fontsize=12)
    ddS = 100 * (eq.netliq / eq.netliq.cummax() - 1); ddB = 100 * (beq / beq.cummax() - 1)
    ax2.fill_between(ddB.index, ddB, 0, color=C2, alpha=0.25, lw=0, label="S&P 500 TR")
    ax2.plot(ddS.index, ddS, color=C1, lw=1.2, label="Strategy")
    ax2.set_ylabel("Drawdown from peak (%)"); ax2.set_xlabel("Date")
    ax2.grid(True, color=GRID, lw=0.8); ax2.set_axisbelow(True)
    ax2.legend(loc="lower left", frameon=False, ncol=2)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "equity.png"), dpi=130); plt.close(fig)

    fig, (ax, axd) = plt.subplots(2, 1, figsize=(11, 8.5), sharex=True, gridspec_kw={"height_ratios": [1.3, 1]})
    prev = eq.netliq.shift(1).fillna(START_EQUITY_OF(eq))
    parts = {"SPY core (price + dividends)": eq.core_pnl, "Leg A: MES month-end (net)": eq.a_pnl,
             "Leg B: ZN last 3 sessions (net)": eq.b_pnl,
             "Cash interest & core rebalancing (net)": eq.interest - eq.rebalance_cost}
    cols = [C2, C1, C3, C4]
    for (k, s), col in zip(parts.items(), cols):
        pc = 100 * (s / prev).cumsum()
        ax.plot(pc.index, pc, color=col, lw=2, label=k)
        dy = {"Leg A: MES month-end (net)": 6, "Leg B: ZN last 3 sessions (net)": -6}.get(k, 0)
        ax.annotate(f"{pc.iloc[-1]:+.0f} pts", (pc.index[-1], pc.iloc[-1]), xytext=(4, dy),
                    textcoords="offset points", va="center", fontsize=9, color=INK)
        cd = s.cumsum() / 1e6
        axd.plot(cd.index, cd, color=col, lw=2, label=k)
        axd.annotate(f"${cd.iloc[-1]:.2f}M", (cd.index[-1], cd.iloc[-1]), xytext=(4, 0),
                     textcoords="offset points", va="center", fontsize=9, color=INK)
    for a_ in (ax, axd):
        a_.axhline(0, color=INK2, lw=0.8); a_.grid(True, color=GRID, lw=0.8); a_.set_axisbelow(True)
    ax.set_ylabel("Cumulative contribution\n(sum of daily P&L / prior net liq, % pts)")
    axd.set_ylabel("Cumulative P&L ($ millions)"); axd.set_xlabel("Date")
    ax.legend(loc="upper left", frameon=False)
    ax.set_title(f"Where the return came from (${START_EQUITY/1e3:.0f}k start, base costs). Top: return-weighted, "
                 "bottom: dollars (late years dominate)", loc="left", color=INK, fontsize=11)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "legs_contribution.png"), dpi=130); plt.close(fig)


# =============================================================== main
def run(d, e0, **kw):
    eq, tr = simulate(d, e0, **kw)
    eq.attrs["e0"] = e0
    return eq, tr


def main():
    d, info = load()
    bench = d.spy_cc
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 50)
    scen, store = [], {}
    for e0 in ACCOUNT_SIZES:
        for cost in ["base", "stress"]:
            eq, tr = run(d, e0, cost=cost)
            store[(e0, cost)] = (eq, tr)
            scen.append({"account": e0, "case": cost, **perf(eq, tr, bench)})
    for name, kw in [("research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", dict(research=True)),
                     ("fractional contracts, base costs", dict(fractional=True, cost="base")),
                     ("fractional contracts, zero costs", dict(fractional=True, cost="zero")),
                     ("leg B priced on Yahoo ZN=F settlements instead of IEF-minus-bills, base costs",
                      dict(b_src="znf")),
                     ("never rebalance core (overlay P&L stays in cash), base costs", dict(core_band=None))]:
        eq, tr = run(d, START_EQUITY, **kw)
        store[name] = (eq, tr)
        scen.append({"account": START_EQUITY, "case": name, **perf(eq, tr, bench)})
    bs = bench_stats(store[(START_EQUITY, "base")][0], bench)
    scen.append({"account": START_EQUITY, "case": "S&P 500 TR (SPY) benchmark", **bs})
    S = pd.DataFrame(scen)
    S.round(3).to_csv(os.path.join(OUT, "scenarios.csv"), index=False)

    eq, tr = store[(START_EQUITY, "base")]
    cols = ["leg", "entry_date", "entry_time", "exit_date", "exit_time", "direction", "contracts", "contract_notional",
            "position_notional", "lev_x_netliq", "entry_price", "exit_price", "gross_pnl", "commissions", "slippage",
            "net_pnl", "net_pct_account", "netliq_at_entry", "equity_after", "vol_tilt_size", "signal_gap_pct",
            "hedge_ratio"]
    T = tr[cols].sort_values(["exit_date", "leg"], na_position="last")
    T.round(4).to_csv(os.path.join(OUT, "trades.csv"), index=False)
    Y = yearly(eq, tr, bench)
    Y.round(2).to_csv(os.path.join(OUT, "yearly.csv"))
    eq.round(2).to_csv(os.path.join(OUT, "equity_daily.csv"))
    charts(eq, store[(START_EQUITY, "stress")][0], bench)

    # holdout split stats for the $100k base case
    split = []
    for lbl, (a, b) in {"2003-2014": ("2003", "2014"), "2015-2022": ("2015", "2022"), "2023+ holdout": ("2023", None),
                        "2003-2022 in-sample": ("2003", "2022")}.items():
        e = eq.loc[a:b].copy(); e.attrs["e0"] = eq.netliq.shift(1).loc[a:b].iloc[0] if a != "2003" else START_EQUITY
        t = tr[(pd.to_datetime(tr.exit_date.astype(str), errors="coerce") >= pd.Timestamp(a)) &
               (pd.to_datetime(tr.exit_date.astype(str), errors="coerce") <= pd.Timestamp(f"{b or 2100}-12-31"))]
        p = perf(e, t, bench); be = e.copy(); be.attrs["e0"] = START_EQUITY
        bb = bench.loc[e.index]; bcagr = 100 * ((1 + bb).prod() ** (365.25 / (e.index[-1] - e.index[0]).days) - 1)
        split.append({"period": lbl, "CAGR %": p["CAGR %"], "S&P CAGR %": bcagr, "Sharpe": p["Sharpe"],
                      "max DD %": p["max DD %"], "alpha %/yr": p["alpha %/yr"], "beta": p["beta"],
                      "A win %": p.get("A win %"), "B win %": p.get("B win %"),
                      "A avg % acct": p.get("A avg % acct"), "B avg % acct": p.get("B avg % acct")})
    SP = pd.DataFrame(split)
    SP.round(3).to_csv(os.path.join(OUT, "periods.csv"), index=False)

    print("data info:", {k: round(v, 5) if isinstance(v, float) else v for k, v in info.items()})
    print("period:", eq.index[0].date(), "->", eq.index[-1].date())
    print(S.set_index(["account", "case"]).round(2).T.to_string())
    print(Y.round(1).to_string())
    print(SP.round(2).to_string())
    beta = tr[tr.leg == "B"].hedge_ratio
    info["beta_trades"] = beta.describe().to_dict()
    info["n_rebalances"] = int((eq.rebalance_cost > 0).sum())
    info["cash_wt"] = (eq.cash / eq.netliq).describe().to_dict()
    # leg-B model vs Yahoo ZN=F over the same holds (ZN=F settles ~15:00 ET; IEF closes 16:00 ET)
    b = tr[(tr.leg == "B") & (tr.contracts > 0)].copy()
    zc = d.zn_close
    b["zn_f_ret"] = [zc.loc[str(x)] / zc.loc[str(y)] - 1 for x, y in zip(b.exit_date, b.entry_date)]
    b["model_ret"] = b.exit_price / b.entry_price - 1
    b["zn_f_pnl"] = b.contracts * ZN_MULT * b.entry_price * b.zn_f_ret - b.commissions - b.slippage
    info["b_check"] = {"corr": np.corrcoef(b.zn_f_ret, b.model_ret)[0, 1],
                       "model_mean_bp": 1e4 * b.model_ret.mean(), "znf_mean_bp": 1e4 * b.zn_f_ret.mean(),
                       "model_win": 100 * (b.net_pnl > 0).mean(), "znf_win": 100 * (b.zn_f_pnl > 0).mean(),
                       "model_pnl": b.net_pnl.sum(), "znf_pnl": b.zn_f_pnl.sum(),
                       "te_bp": 1e4 * (b.zn_f_ret - b.model_ret).std()}
    # contract counts at the latest prices for each account size
    last = d.iloc[-1]
    info["counts_now"] = pd.DataFrame([{"account $": e0,
        "SPX": last.gspc_close, "ZN": last.zn_close, "hedge ratio": last.beta,
        "A full (2.85x)": math.floor(LEV_A * e0 / (MES_MULT * last.gspc_close)),
        "A full lev x": math.floor(LEV_A * e0 / (MES_MULT * last.gspc_close)) * MES_MULT * last.gspc_close / e0,
        "A half (1.425x)": math.floor(LEV_A * 0.5 * e0 / (MES_MULT * last.gspc_close)),
        "A half lev x": math.floor(LEV_A * 0.5 * e0 / (MES_MULT * last.gspc_close)) * MES_MULT * last.gspc_close / e0,
        "B ZN contracts": math.floor(LEV_B * e0 * last.beta / (ZN_MULT * last.zn_close)),
        "B IEF-equiv lev x": math.floor(LEV_B * e0 * last.beta / (ZN_MULT * last.zn_close)) * ZN_MULT * last.zn_close / last.beta / e0,
        "min acct for 1 ZN $": ZN_MULT * last.zn_close / (LEV_B * last.beta)} for e0 in ACCOUNT_SIZES])
    print(info["b_check"]); print(info["counts_now"].round(2).to_string()); print(info["n_rebalances"], info["cash_wt"])
    s25 = store[(25_000, "base")][1]
    sk = s25[(s25.leg == "B") & (s25.contracts == 0)]
    info["skip25"] = (str(sk.entry_date.min()), str(sk.entry_date.max()), len(sk))
    print(info["skip25"])
    write_summary(d, info, S, Y, SP, eq, tr)


def md_table(df, floatfmt=2):
    df = df.copy()
    head = "| " + " | ".join(str(c) for c in df.columns) + " |"
    sep = "|" + "|".join("---" for _ in df.columns) + "|"
    lines = [head, sep]
    for _, r in df.iterrows():
        cells = []
        for v in r.values:
            if isinstance(v, (float, np.floating)):
                cells.append("" if not np.isfinite(v) else (f"{v:,.0f}" if abs(v) >= 1000 else f"{v:.{floatfmt}f}"))
            else:
                cells.append(str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_summary(d, info, S, Y, SP, eq, tr):
    f = lambda v, n=1: f"{v:,.{n}f}"
    get = lambda acct, case, k: S[(S.account == acct) & (S.case == case)][k].iloc[0]
    base = lambda k: get(START_EQUITY, "base", k)
    # headline table
    keys = ["CAGR %", "vol %", "Sharpe", "Sortino", "max DD %", "longest DD days", "beta", "alpha %/yr", "alpha t",
            "A win %", "B win %", "A avg % acct", "A median % acct", "B avg % acct", "B median % acct",
            "A profit factor", "B profit factor", "A best % acct", "A worst % acct", "B best % acct", "B worst % acct",
            "A avg lev x", "B avg lev x", "B skipped", "costs $", "final equity $"]
    H = S.copy()
    H["scenario"] = [f"${a/1e3:,.0f}k {c}" if c in ("base", "stress") else f"${a/1e3:,.0f}k {c}" for a, c in zip(H.account, H.case)]
    H = H.set_index("scenario")[keys].T
    H.insert(0, "metric", H.index)
    main_cols = ["metric"] + [c for c in H.columns if c.endswith(("base", "stress"))] + \
                [c for c in H.columns if "benchmark" in c]
    other_cols = ["metric"] + [c for c in H.columns if c not in main_cols]
    bc = info["b_check"]; bt = info["beta_trades"]; cw = info["cash_wt"]
    y_tab = Y.reset_index().rename(columns={"year": "year"})
    y_tab["year"] = y_tab["year"].astype(str)
    split = SP.copy()
    counts = info["counts_now"].copy()
    for c_ in ["A full (2.85x)", "A half (1.425x)", "B ZN contracts"]:
        counts[c_] = counts[c_].astype(int).astype(str)
    research = get(START_EQUITY, "research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "CAGR %")
    frac0 = get(START_EQUITY, "fractional contracts, zero costs", "CAGR %")
    fracb = get(START_EQUITY, "fractional contracts, base costs", "CAGR %")
    znf = get(START_EQUITY, "leg B priced on Yahoo ZN=F settlements instead of IEF-minus-bills, base costs", "CAGR %")
    never = S[S.case.str.startswith("never")].iloc[0]
    bench = S[S.case.str.contains("benchmark")].iloc[0]
    sk = info["skip25"]
    yrs_cost = 100 * (eq.costs / eq.netliq.shift(1).fillna(START_EQUITY)).groupby(eq.index.year).sum()
    md = f"""# Month-end trader: full-timeline IBKR backtest (SPY core + MES leg A + ZN leg B)

Generated by `simulate.py` (re-run it to regenerate every file here). Period **{eq.index[0].date()} to {eq.index[-1].date()}**
(the last complete month; November 2026 is excluded because its month-end has not happened). Start equity
**${START_EQUITY:,.0f}** unless stated. All numbers are a simulation on daily Yahoo data, not live fills.

Files: `trades.csv` (one row per trade, $100k base case), `yearly.csv`, `periods.csv`, `scenarios.csv` (all account sizes and
cost cases), `equity_daily.csv`, `equity.png`, `legs_contribution.png`.

## Headline ($100k, base costs)

| | Strategy | S&P 500 TR (SPY) |
|---|---|---|
| CAGR | **{f(base("CAGR %"))}%** | {f(bench["CAGR %"])}% |
| Final equity | ${base("final equity $"):,.0f} | ${bench["final equity $"]:,.0f} |
| Volatility | {f(base("vol %"))}% | {f(bench["vol %"])}% |
| Sharpe (excess of T-bills) | {f(base("Sharpe"), 2)} | {f(bench["Sharpe"], 2)} |
| Sortino | {f(base("Sortino"), 2)} | {f(bench["Sortino"], 2)} |
| Max drawdown | {f(base("max DD %"))}% | {f(bench["max DD %"])}% |
| Longest drawdown (peak to recovery, calendar days) | {base("longest DD days"):.0f} | {bench["longest DD days"]:.0f} |
| Beta / alpha vs S&P (daily OLS, excess returns) | {f(base("beta"), 2)} / {f(base("alpha %/yr"))}%/yr (t={f(base("alpha t"))}) | 1.00 / 0 |
| Leg A: trades, win rate, avg / median net per trade (% of account) | {base("A trades"):.0f}, {f(base("A win %"))}%, {f(base("A avg % acct"), 2)}% / {f(base("A median % acct"), 2)}% | |
| Leg B: trades, win rate, avg / median net per trade | {base("B trades"):.0f}, {f(base("B win %"))}%, {f(base("B avg % acct"), 2)}% / {f(base("B median % acct"), 2)}% | |
| Profit factor A / B | {f(base("A profit factor"), 2)} / {f(base("B profit factor"), 2)} | |
| Best / worst trade A | {f(base("A best % acct"), 2)}% / {f(base("A worst % acct"), 2)}% | |
| Best / worst trade B | {f(base("B best % acct"), 2)}% / {f(base("B worst % acct"), 2)}% | |
| Total trading costs (commissions + slippage + SPY rebalancing) | ${base("costs $"):,.0f} | 0 |

Stress costs (2 MES ticks, 1 ZN tick per side) at $100k: CAGR **{f(get(START_EQUITY, "stress", "CAGR %"))}%**, Sharpe
{f(get(START_EQUITY, "stress", "Sharpe"), 2)}, max DD {f(get(START_EQUITY, "stress", "max DD %"))}%.

![equity](equity.png)

![legs](legs_contribution.png)

## Strategy as simulated

- **Core:** 100% of net liquidation in SPY, dividends reinvested on the ex-date (DRIP, modelled with Yahoo adjusted closes).
  Futures P&L settles to cash. **Rebalancing choice: drift band.** When the SPY core drifts more than
  {CORE_BAND:.0%} from 100% of net liq (i.e. accumulated overlay cash > +/-{CORE_BAND:.0%}), SPY is bought/sold at the close
  back to 100%. This happened **{info["n_rebalances"]} times** in {len(Y)} years. Cash weight averaged {100*cw["mean"]:.1f}%
  (range {100*cw["min"]:.1f}% to {100*cw["max"]:.1f}%). "Never rebalance" is shown as a sensitivity below: it lets overlay
  profits pile up in cash and turns the account into a low-beta overlay fund (beta {never["beta"]:.2f}, CAGR {f(never["CAGR %"])}%).
- **Leg A (013 + 168 tilt):** on the last NYSE session of each month, direction = -sign(SPY MTD - TLT MTD), MTD from the prior
  month's last adjusted close to the T-1 adjusted close. Size = floor({LEV_A} x net liq(T-1) x tilt / (5 x S&P 500 close(T-1)))
  MES, tilt = 1 if SPY's 20-day std of daily adjusted returns (ending T-1) > {info["vol_median"]:.5f} (median over
  2002-08..2014), else 0.5. Entry 09:30, exit at the close (16:00, or 13:00 on half days per the XNYS calendar).
  **Prices:** exit = ^GSPC close; entry = ^GSPC close / (1 + SPY open-to-close return). Yahoo's ^GSPC *opens* are stale (equal
  to the prior close) on most days in 2002-2007 and many in 2011-2013, and the index's own opening print is computed from
  stale component prices anyway, so SPY's 09:30 print is the better proxy for where MES trades at the open. Futures basis
  is assumed constant intraday. MES only exists since May 2019; earlier trades are hypothetical MES at $5 x index (an ES
  trader then would have had 10x coarser rounding).
- **Leg B (276):** long ZN from the close of the 4th-last NYSE session to the close of the last session (3 overnight holds).
  Contracts = floor({LEV_B} x net liq x hedge ratio / (1000 x ZN=F close)). Per contract the price path is modelled as
  ZN_t = ZN_(t-1) x (1 + (IEF total return_t - T-bill accrual_t) / hedge ratio): futures earn the excess return. T-bill
  accrual = ^IRX x calendar days / 360. P&L is marked daily (variation margin to cash).
- **Hedge ratio** = beta of IEF daily returns on ZN=F daily returns, trailing {BETA_WIN} sessions (min {BETA_MIN}), excluding
  Yahoo's continuous-contract roll windows (day 15-25 of Mar/Jun/Sep/Dec, {info["n_roll_excluded"]} days). Full-sample
  2003-2026 estimate: **{info["beta_full"]:.3f}** (corr {info["beta_corr"]:.3f}; a weekly-return check gives
  {info["beta_weekly"]:.3f}, so the daily number is only slightly attenuated by ZN settling at ~15:00 ET vs IEF at 16:00).
  The trailing value used at the trades ranged {bt["min"]:.2f} to {bt["max"]:.2f} (mean {bt["mean"]:.2f}). The ZN notional is
  therefore ~{bt["mean"]*LEV_B:.1f}x net liq for {LEV_B}x of IEF-equivalent exposure.
- Both legs overlap on the last session of the month, as intended.

## Costs (IBKR Pro tiered, US)

| Item | Base | Stress |
|---|---|---|
| MES all-in fees (IBKR commission ~$0.25 + CME exchange ~$0.35 + NFA/regulatory ~$0.02) | $0.62 / contract / side | same |
| MES slippage (market orders at open and close) | 1 tick = $1.25 / contract / side | 2 ticks = $2.50 |
| ZN all-in fees (IBKR ~$0.85 + CBOT exchange ~$0.80 + NFA/clearing ~$0.05; my estimate) | $1.70 / contract / side | same |
| ZN slippage | half a tick (tick = 1/64 pt = $15.625) = $7.81 / contract / side | 1 tick = $15.63 |
| SPY rebalancing | $0.0035/share (min $0.35, max 1% of value) + $0.0005/share fees + $0.005/share spread | same |
| Cash | positive cash earns max(^IRX - 0.5%, 0); negative cash (after funding ZN overnight margin of {ZN_OVERNIGHT_MARGIN:.0%} of notional) pays ^IRX + 1.5% | same |

No financing on futures notional (futures P&L is an excess return). MES is never held overnight, so its margin costs no
interest (IBKR charges interest on end-of-day balances). Round trip at $100k is ~${2*(0.62+1.25)*7:.0f} for A (7 MES) and
~${2*(1.70+7.8125)*2:.0f} for B (2 ZN) at today's prices; in 2003, with the S&P near 900, one MES tick was ~2.8bp per side, so
costs were ~2-3% of equity per year early on, falling to ~{yrs_cost.loc[2024:].mean():.1f}% recently (per-year costs as a % of
equity: max {yrs_cost.max():.1f}% in {yrs_cost.idxmax()}).

## All account sizes and cost cases

{md_table(H[main_cols].reset_index(drop=True))}

Sensitivities ($100k):

{md_table(H[other_cols].reset_index(drop=True))}

**Rounding effect.** Accounts compound, so whole-contract rounding only bites while the account is small. The $25k account
could not trade leg B (needs {LEV_B} x net liq x hedge ratio >= one ZN, ~$110-140k notional) in **{sk[2]} months**,
on and off between {sk[0]} and {sk[1]}, until it had grown enough; those B trades are logged as "skipped" (0 contracts). Leg A
always traded (MES is small). Contract counts a *fresh* account would get **today** (S&P {counts["SPX"].iloc[0]:,.0f},
ZN {counts["ZN"].iloc[0]:.2f}, hedge ratio {counts["hedge ratio"].iloc[0]:.2f}):

{md_table(counts.drop(columns=["SPX", "ZN", "hedge ratio"]))}

So a fresh $25k account today gets **1 MES at full size, 0 at half size, and no ZN at all** (minimum ~${counts["min acct for 1 ZN $"].iloc[0]:,.0f}
for one ZN at {LEV_B}x); $100k gets 7 / 3 MES and 2 ZN (2.05x instead of 2.81x for B). Rounding costs only about
{f(fracb - base("CAGR %"), 2)} pp/yr of CAGR at $100k over the full history because the account grew quickly.

## Reconciliation with research 350

350 reports CAGR 26.2% (SPY + A+B, 2003-01 to 2026-11-20, 0.5bp/1bp per side costs, A 62.2% / B 62.2% win).

| Step ($100k) | CAGR % |
|---|---|
| Research-equivalent here (fractional, zero costs, B = IEF *total* return, core rebalanced daily) | {f(research, 2)} |
| ... minus 350's 0.5bp (A) / 1bp (B) per-side costs (~0.9 pp/yr) | ~{f(research - 0.9)} (350: 26.2; the rest is 350's window running to 2026-11-20, when the S&P was ~9% below its October close) |
| B earns IEF *minus T-bills* (correct for futures), core on a 10% band, cash interest, fractional, no costs | {f(frac0, 2)} |
| + IBKR fees and slippage (base) | {f(fracb, 2)} |
| + whole contracts (the base case) | **{f(base("CAGR %"), 2)}** |
| + stress slippage | {f(get(START_EQUITY, "stress", "CAGR %"), 2)} |

The two legs reproduce 350 (A win {f(get(START_EQUITY, "research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "A win %"))}%,
B win {f(get(START_EQUITY, "research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "B win %"))}% before costs).
The one modelling difference that matters: **350 credits leg B with IEF's total return**, but with SPY already 100% invested
the bond exposure has to come from futures (or borrowed money), which earn only the excess over bills. That (together with the band
rebalancing and cash interest, which are small) is worth ~{f(research - frac0, 1)} pp/yr in this sample and much more when bills yield 4-5% (2023+: B averages
{f(split.set_index("period").loc["2023+ holdout", "B avg % acct"], 2)}% of account per trade here vs ~0.47% in 350).

## Leg B model check against actual ZN=F prices

Over the {base("B trades"):.0f} B holds, the IEF-minus-bills model and Yahoo's ZN=F settlement prices agree in sign on most
trades (return correlation {bc["corr"]:.2f}, tracking error {bc["te_bp"]:.0f}bp per trade) but **ZN=F made less**: mean
{bc["znf_mean_bp"]:.1f}bp per trade vs {bc["model_mean_bp"]:.1f}bp modelled. Re-running the whole account with B priced on ZN=F
settlements gives CAGR **{f(znf, 2)}%** instead of {f(base("CAGR %"), 2)}%. Causes: ZN=F settles at ~15:00 ET, an hour before
the IEF close (part of the month-end bid shows up in that last hour, which the bot's 16:00 trade would capture), IEF's
cash-bond basket differs from the futures CTD, and Yahoo's continuous ZN=F holds the expiring contract through first-notice
day in Feb/May/Aug/Nov. **Treat leg B's true P&L as somewhere between the two.** In live trading, hold the next quarterly
contract in Feb/May/Aug/Nov (the front month's first notice day is the last business day of those months).

## Yearly ($100k, base costs)

Costs = futures commissions + slippage + SPY rebalancing. Max DD is within the calendar year.

{md_table(y_tab)}

## In-sample vs holdout

{md_table(split)}

The sizing rule, leverages ({LEV_A}x / {LEV_B}x) and the vol median come from 2003-2014 data and the legs were selected on
2003-2022, so **only 2023+ is out of sample** (about four years, 46 trades per leg).

## Caveats

- **In-sample.** 2003-2022 was used to discover and select both signals, the vol tilt and the leverage. The holdout
  (2023-2026) looks strong for A and weaker for B; four years is short. Expect live results below the backtest.
- **Leverage and gap risk.** Leg A puts up to {LEV_A}x net liq on top of a 100% SPY core for one session, leg B ~3x in ZN
  notional for three nights: total gross exposure is ~7x on the last day of the month. The worst single A day here cost
  {f(-base("A worst % acct"), 1)}% of the account and the worst B hold {f(-base("B worst % acct"), 1)}%; a crash-day (Oct 1987-style -20%)
  on a month-end with A long at full size would cost ~57% of the account *on top of* the core's loss. **No stops are
  modelled** (the bot has a disaster stop; its effect is not in these numbers).
- **Fills.** Open/close fills are modelled from daily bars plus fixed tick slippage. Real 09:30 MES fills can be wider in fast
  markets (the stress case doubles slippage), and month-end closes are crowded. ZN liquidity at 16:00 ET (after the 15:00 settlement) is
  thinner than during the day session.
- **Data limits.** Yahoo daily data (adjusted closes for dividends; ^GSPC opens unreliable before ~2014, so SPY opens are used);
  ^IRX as the bills/benchmark rate; ZN=F is a non-back-adjusted continuous contract with roll jumps (excluded only from the
  hedge-ratio estimate). MES did not exist before 2019. No survivorship issue (index products).
- **Margin.** MES intraday margin and ZN overnight margin (~2% of notional) are assumed always available against a 100% SPY
  core (IBKR lends against SPY). A margin call or IBKR raising margins in a crisis could force a smaller size.
- **Taxes ignored.** Futures are 60/40 Section 1256 in the US; SPY rebalancing realises gains.
- **Rebalancing choice matters.** With no core rebalancing, overlay profits accumulate as cash and the account de-risks
  (see "never rebalance" above).
"""
    open(os.path.join(OUT, "summary.md"), "w").write(md)


if __name__ == "__main__":
    main()
