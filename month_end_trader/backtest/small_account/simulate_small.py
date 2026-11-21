"""Small-account (~$18k) variant of ../simulate.py: leg B in CME Micro 10-Year Yield futures (10Y) instead of ZN.

Reuses ../simulate.py (imported, not edited) for data loading, the leg-A signal, the SPY core, cash interest,
the stats and the yearly table. What is new here:
  * Leg B = SHORT the Micro 10Y Yield future (price = 10y yield in index points, 0.001 = $1, DV01 $10/contract),
    contracts = floor(LEV_B x net liq x IEF duration x 1e-4 / $10). IEF duration = trailing regression of IEF daily
    returns on -(change in the 10y yield), so one contract is ~ $100,000 / duration of IEF-equivalent notional.
    P&L model "tnx" (headline): -contracts x change in ^TNX yield (pts) x 1000 $.
    P&L model "ief" (check):    contracts x $100,000/duration x (IEF total return - T-bills), marked daily.
  * Leg A rounding variants: "floor" (skip when 0 MES) or "nearest" (round half up, capped at MAX_LEV_A).
  * Margin per trade (MES % of notional, 10Y $ per contract) vs net liq, flagged above MARGIN_FLAG.
Run:  python3 simulate_small.py      (writes trades_18k*.csv, yearly_18k.csv, scenarios_small.csv, equity_18k.png,
                                      summary.md in this directory)
"""
import os, sys, math, warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

sys.dont_write_bytecode = True          # do not leave __pycache__ in ../ (imported module lives there)
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import simulate as S          # noqa: E402  (the full-size simulation; imported only)
import lib                    # noqa: E402  (on sys.path via simulate.LIB_DIR)

# =============================================================== PARAMETERS
OUT = HERE
HEADLINE_EQUITY = 18_000.0
ACCOUNT_SIZES = [10_000, 18_000, 25_000, 50_000, 100_000]
LEV_A, LEV_B = S.LEV_A, S.LEV_B          # 2.85x MES at full size (half = 1.425x); 2.81x IEF-equivalent for B
MAX_LEV_A = 3.0                          # "nearest" rounding may not exceed this MES notional / net liq
START = S.START

# Micro 10-Year Yield futures (CME symbol 10Y; launched 2021-08-16)
Y10_DV01 = 10.0                          # $ per bp per contract (0.001 index pt = $1, 1bp = 0.010 pt)
Y10_PT_VALUE = 1000.0                    # $ per 1.000 index point (= 1 percentage point of yield)
Y10_TICK = 0.001                         # index points; = $1
Y10_LAUNCH = "2021-08-16"
MES_LAUNCH = "2019-05-06"
DUR_WIN, DUR_MIN = 252, 60               # trailing sessions for the IEF duration regression
DUR_FIXED = None                         # set e.g. 7.2 to size with a fixed duration instead of the trailing estimate
B_MODEL = "tnx"                          # headline leg-B P&L model: "tnx" (yield changes x DV01) or "ief"

COSTS = {   # per contract per side
    #           MES all-in fee, MES slippage ticks ($1.25), 10Y all-in fee, 10Y slippage ticks ($1)
    "base":   {"mes_fee": 0.62, "mes_slip_ticks": 1.0, "y10_fee": 0.85, "y10_slip_ticks": 1.0},
    "stress": {"mes_fee": 0.62, "mes_slip_ticks": 2.0, "y10_fee": 0.85, "y10_slip_ticks": 2.0},
    "zero":   {"mes_fee": 0.0,  "mes_slip_ticks": 0.0, "y10_fee": 0.0,  "y10_slip_ticks": 0.0},
}
# margin assumptions (initial, per contract). MES as a % of notional (7.5% ~ $2,840 at S&P 7,580); 10Y fixed $.
MES_MARGIN_PCT = 0.075
Y10_MARGIN = 400.0
MARGIN_STRESS_MULT = 2.0                 # crisis check: exchanges/IBKR double margins
MARGIN_FLAG = 0.60                       # flag trades where futures margin > 60% of net liq
SPY_MAINT = 0.25                         # Reg T maintenance on the SPY core (for the total-requirement check)
IBKR_MIN_EQUITY = 2_000.0
# =============================================================================


def load():
    d, info = S.load()
    tnx = lib.get("^TNX").Close
    scale = 10.0 if tnx.loc["2003":].median() > 20 else 1.0      # Yahoo ^TNX was historically 10x the yield
    info["tnx_scale"] = scale
    full = (tnx / scale).reindex(d.index)
    info["tnx_missing"] = int(full.loc[START:].isna().sum())
    d["y10"] = full.ffill()                                      # 10y yield in % = Micro 10Y index points
    dy = d.y10.diff()
    x = -dy / 100.0                                              # yield change in decimal, sign flipped
    y = d.ief_cc
    ok = x.notna() & y.notna()
    x, y = x.where(ok), y.where(ok)
    R = lambda s: s.rolling(DUR_WIN, min_periods=DUR_MIN).mean()
    d["dur"] = ((R(x * y) - R(x) * R(y)) / (R(x * x) - R(x) ** 2)).ffill()
    if DUR_FIXED:
        d["dur"] = DUR_FIXED
    xs, ys = x.loc[START:].dropna(), y.loc[START:].dropna()
    info["dur_full"] = np.polyfit(xs, ys, 1)[0]
    info["dur_corr"] = np.corrcoef(xs, ys)[0, 1]
    w = d[["ief_aC", "y10"]].loc[START:].resample("W-FRI").last()
    info["dur_weekly"] = np.polyfit(-w.y10.diff().dropna() / 100, w.ief_aC.pct_change().dropna(), 1)[0]
    info["dur_by_period"] = {f"{a}-{b}": np.polyfit(xs.loc[a:b], ys.loc[a:b], 1)[0]
                             for a, b in [("2003", "2008"), ("2009", "2014"), ("2015", "2020"), ("2021", "2026")]}
    return d, info


def simulate(d, equity0=HEADLINE_EQUITY, cost="base", a_round="floor", b_model=B_MODEL, mes_margin_pct=MES_MARGIN_PCT,
             y10_margin=Y10_MARGIN, today=None):
    """today: None, or dict(levA={1.0: x, 0.5: y}, b_per_dollar=z, spx_now=...) = "today's rounding held fixed":
    MES leverage and 10Y contracts per $ of equity frozen at what a fresh account of this size gets at today's
    prices (fractional contracts scale with equity, MES costs scaled to today's cost in bp of notional)."""
    c = COSTS[cost]
    dd = d.loc[START:]
    idx = dd.index
    core, cash, nl_prev = equity0, 0.0, equity0
    B = None
    rows, trades = [], []
    rf_prev = dd.irx.iloc[0]
    for i, t in enumerate(idx):
        r = dd.loc[t]
        core_ret = core * r.spy_cc if i > 0 else 0.0
        core += core_ret
        interest = 0.0
        if i > 0:
            margin = B["n"] * y10_margin if B else 0.0        # 10Y overnight margin funded from cash
            bal = cash - margin
            rate = max(rf_prev + S.CREDIT_SPREAD, 0.0) if bal > 0 else rf_prev + S.DEBIT_SPREAD
            interest = bal * rate * r.days / 360
            cash += interest
        rf_d = rf_prev * r.days / 360
        a_pnl = b_pnl = cost_day = 0.0
        # --- leg B mark / exit at the close
        if B is not None:
            p_tnx = -B["n"] * Y10_PT_VALUE * (r.y10 - B["Y"])
            p_ief = B["n"] * B["ief_notional"] * (r.ief_cc - rf_d)
            B["Y"] = r.y10; B["g_tnx"] += p_tnx; B["g_ief"] += p_ief
            pnl = p_tnx if b_model == "tnx" else p_ief
            b_pnl += pnl; cash += pnl
            if r.rank_bwd == 1:
                fee = B["n"] * c["y10_fee"]; slip = B["n"] * c["y10_slip_ticks"] * Y10_TICK * Y10_PT_VALUE
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B["fee"] += fee; B["slip"] += slip
                B.update(exit_date=t, exit_time=r.close_time, exit_y=r.y10, closed=True)
        # --- leg A (intraday on the last session)
        if r.rank_bwd == 1 and np.isfinite(r.gap) and r.gap != 0 and np.isfinite(r.vol20):
            dirn = -np.sign(r.gap)
            px_prev = dd.gspc_close.iloc[i - 1] if i > 0 else r.gspc_close
            want = LEV_A * r.sizeA * nl_prev / (S.MES_MULT * px_prev)
            if today is not None:
                n = today["levA"][r.sizeA] * nl_prev / (S.MES_MULT * px_prev)
            elif a_round == "floor":
                n = math.floor(want + 1e-9)
            else:
                n = int(math.floor(want + 0.5))
                while n > 0 and n * S.MES_MULT * px_prev / nl_prev > MAX_LEV_A + 1e-9:
                    n -= 1
            p_in = r.gspc_close / (1 + r.spy_oc); p_out = r.gspc_close
            gross = dirn * n * S.MES_MULT * (p_out - p_in)
            fee = 2 * n * c["mes_fee"]; slip = 2 * n * c["mes_slip_ticks"] * S.MES_TICK * S.MES_MULT
            if today is not None:
                fee *= px_prev / today["spx_now"]; slip *= px_prev / today["spx_now"]
            net = gross - fee - slip
            cash += net; a_pnl += net; cost_day += fee + slip
            m_a = n * mes_margin_pct * S.MES_MULT * p_in
            m_b = B["n"] * y10_margin if B else 0.0             # B is still open during the last session
            trades.append(dict(
                leg="A", hypothetical=t < pd.Timestamp(MES_LAUNCH), entry_date=t.date(), entry_time="09:30",
                exit_date=t.date(), exit_time=r.close_time,
                direction=("long" if dirn > 0 else "short") if n > 0 else "skipped (0 contracts)",
                contracts=n, contracts_wanted=want, contract_notional=S.MES_MULT * p_in,
                position_notional=n * S.MES_MULT * p_in, lev_x_netliq=n * S.MES_MULT * p_in / nl_prev,
                entry_price=p_in, exit_price=p_out, gross_pnl=gross, gross_pnl_alt_model=np.nan,
                commissions=fee, slippage=slip, net_pnl=net, net_pct_account=100 * net / nl_prev,
                netliq_at_entry=nl_prev, margin_futures=m_a + m_b, margin_pct_netliq=100 * (m_a + m_b) / nl_prev,
                total_req_pct_netliq=100 * (m_a + m_b + SPY_MAINT * core) / nl_prev,
                margin_flag=(m_a + m_b) / nl_prev > MARGIN_FLAG, vol_tilt_size=r.sizeA,
                signal_gap_pct=100 * r.gap, ief_duration=np.nan, dv01_usd=np.nan))
        nl = core + cash
        if B is not None and B.get("closed"):
            g_main = B["g_tnx"] if b_model == "tnx" else B["g_ief"]
            g_alt = B["g_ief"] if b_model == "tnx" else B["g_tnx"]
            net = g_main - B["fee"] - B["slip"]
            trades.append(dict(
                leg="B", hypothetical=B["entry_date"] < pd.Timestamp(Y10_LAUNCH), entry_date=B["entry_date"].date(),
                entry_time=B["entry_time"], exit_date=t.date(), exit_time=B["exit_time"],
                direction="short yield (long bonds)", contracts=B["n"], contracts_wanted=B["want"],
                contract_notional=B["ief_notional"], position_notional=B["n"] * B["ief_notional"],
                lev_x_netliq=B["n"] * B["ief_notional"] / B["nl0"], entry_price=B["Y0"], exit_price=B["exit_y"],
                gross_pnl=g_main, gross_pnl_alt_model=g_alt, commissions=B["fee"], slippage=B["slip"], net_pnl=net,
                net_pct_account=100 * net / B["nl0"], netliq_at_entry=B["nl0"], margin_futures=B["n"] * y10_margin,
                margin_pct_netliq=100 * B["n"] * y10_margin / B["nl0"],
                total_req_pct_netliq=100 * (B["n"] * y10_margin + SPY_MAINT * B["core0"]) / B["nl0"],
                margin_flag=B["n"] * y10_margin / B["nl0"] > MARGIN_FLAG, vol_tilt_size=np.nan,
                signal_gap_pct=np.nan, ief_duration=B["dur"], dv01_usd=B["n"] * Y10_DV01))
            B = None
        if r.rank_bwd == S.B_HOLD_RANK and t < idx[-1]:
            dur = r.dur
            want = LEV_B * nl * dur * 1e-4 / Y10_DV01
            n = math.floor(want + 1e-9) if today is None else today["b_per_dollar"] * nl
            if n > 0:
                fee = n * c["y10_fee"]; slip = n * c["y10_slip_ticks"] * Y10_TICK * Y10_PT_VALUE
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B = dict(n=n, want=want, Y=r.y10, Y0=r.y10, dur=dur, ief_notional=Y10_DV01 * 1e4 / dur,
                         g_tnx=0.0, g_ief=0.0, fee=fee, slip=slip, entry_date=t, entry_time=r.close_time, nl0=nl,
                         core0=core)
            else:
                trades.append(dict(
                    leg="B", hypothetical=t < pd.Timestamp(Y10_LAUNCH), entry_date=t.date(), entry_time=r.close_time,
                    exit_date=None, exit_time=None, direction="skipped (0 contracts)", contracts=0,
                    contracts_wanted=want, contract_notional=Y10_DV01 * 1e4 / dur, position_notional=0,
                    lev_x_netliq=0, entry_price=r.y10, exit_price=np.nan, gross_pnl=0, gross_pnl_alt_model=0,
                    commissions=0, slippage=0, net_pnl=0, net_pct_account=0, netliq_at_entry=nl, margin_futures=0,
                    margin_pct_netliq=0, total_req_pct_netliq=100 * SPY_MAINT * core / nl, margin_flag=False,
                    vol_tilt_size=np.nan, signal_gap_pct=np.nan, ief_duration=dur, dv01_usd=0, equity_after=nl))
        # --- core rebalance (10% band)
        nl = core + cash
        reb = 0.0
        if S.CORE_BAND is not None and abs(core / nl - 1) > S.CORE_BAND:
            sh = (nl - core) / r.spy_close
            reb = S.spy_trade_cost(sh, r.spy_close)
            core, cash = nl - reb, 0.0
            cost_day += reb
        nl = core + cash
        for tr in trades[::-1]:
            if tr["exit_date"] != t.date():
                break
            tr["equity_after"] = nl
        rows.append(dict(date=t, netliq=nl, core=core, cash=cash, core_pnl=core_ret, a_pnl=a_pnl, b_pnl=b_pnl,
                         interest=interest, costs=cost_day, rebalance_cost=reb, rf_d=rf_d))
        nl_prev = nl
        rf_prev = r.irx
    eq = pd.DataFrame(rows).set_index("date")
    eq.attrs["e0"] = equity0
    return eq, pd.DataFrame(trades)


def today_sizing(d, e0, a_round):
    last = d.iloc[-1]
    mn = S.MES_MULT * last.gspc_close
    lev = {}
    for tilt in (1.0, 0.5):
        w = LEV_A * tilt * e0 / mn
        n = math.floor(w + 1e-9) if a_round == "floor" else int(math.floor(w + 0.5))
        while a_round != "floor" and n > 0 and n * mn / e0 > MAX_LEV_A + 1e-9:
            n -= 1
        lev[tilt] = n * mn / e0
    nb = math.floor(LEV_B * e0 * last.dur * 1e-4 / Y10_DV01)
    return dict(levA=lev, b_per_dollar=nb / e0, spx_now=last.gspc_close, nb=nb)


def perf(eq, tr, bench):
    p = S.perf(eq, tr, bench)
    for leg in ["A", "B"]:
        t = tr[(tr.leg == leg) & (tr.contracts > 0)]
        p[f"{leg} avg net $"] = t.net_pnl.mean() if len(t) else np.nan
        p[f"{leg} avg contracts"] = t.contracts.mean() if len(t) else np.nan
    if "margin_pct_netliq" in tr:
        p["max futures margin % netliq"] = tr.margin_pct_netliq.max()
        p["trades margin > 60%"] = int(tr.margin_flag.sum())
        p["min netliq $"] = eq.netliq.min()
    return p


def run_zn(d, e0, cost="base"):
    eq, tr = S.simulate(d, e0, cost=cost)
    eq.attrs["e0"] = e0
    return eq, tr


# =============================================================== charts
def chart(eqs, bench, e0):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mt
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": S.INK2, "axes.labelcolor": S.INK, "xtick.color": S.INK2,
                         "ytick.color": S.INK2, "axes.spines.top": False, "axes.spines.right": False,
                         "figure.facecolor": S.SURF, "axes.facecolor": S.SURF})
    first = next(iter(eqs.values()))
    b = bench.loc[first.index].copy(); b.iloc[0] = 0
    beq = e0 * (1 + b).cumprod()
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(11, 7.5), sharex=True, gridspec_kw={"height_ratios": [3, 1.2]})
    styles = [(S.C1, "-", 2.0), (S.C3, "-", 1.4), (S.C4, "-", 1.4)]
    ends = []
    for (lbl, e), (col, ls, lw) in zip(eqs.items(), styles):
        ax.plot(e.index, e.netliq, color=col, ls=ls, lw=lw, label=lbl)
        v = e.netliq.iloc[-1]
        dy = -9 if any(abs(np.log(v / u)) < 0.08 and v < u for u in ends) else \
            (9 if any(abs(np.log(v / u)) < 0.08 for u in ends) else 0)
        ends.append(v)
        ax.annotate(f"${v/1e6:.2f}M", (e.index[-1], v), xytext=(4, dy),
                    textcoords="offset points", va="center", fontsize=9, color=S.INK)
    ax.plot(beq.index, beq, color=S.C2, lw=2, label="S&P 500 total return (SPY, dividends reinvested)")
    ax.annotate(f"${beq.iloc[-1]/1e3:.0f}k", (beq.index[-1], beq.iloc[-1]), xytext=(4, 0), textcoords="offset points",
                va="center", fontsize=9, color=S.INK)
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}k"))
    ax.yaxis.set_minor_formatter(mt.NullFormatter())
    ax.set_yticks([1e4, 2e4, 5e4, 1e5, 2e5, 5e5, 1e6, 2e6, 5e6])
    lo = min(e.netliq.min() for e in eqs.values()); hi = max(max(e.netliq.max() for e in eqs.values()), beq.max())
    ax.set_ylim(lo * 0.85, hi * 1.3)
    ax.set_ylabel("Account equity (log scale)")
    ax.grid(True, color=S.GRID, lw=0.8); ax.set_axisbelow(True)
    for x_, txt in [(Y10_LAUNCH, "10Y micro launched"), ("2023-01-01", "holdout 2023+"), (MES_LAUNCH, "MES launched")]:
        ax.axvline(pd.Timestamp(x_), color=S.INK2, lw=0.8, ls=":")
        ax.text(pd.Timestamp(x_) + pd.Timedelta(days=20), lo * 0.95, txt, color=S.INK2, fontsize=8, rotation=90,
                va="bottom")
    ax.legend(loc="upper left", frameon=False)
    ax.set_title(f"Month-end trader from ${e0/1e3:.0f}k, {first.index[0]:%b %Y} - {first.index[-1]:%b %Y} "
                 "(whole contracts, IBKR costs; before the launch lines the contracts are hypothetical)",
                 loc="left", color=S.INK, fontsize=10.5)
    main = first
    ddS = 100 * (main.netliq / main.netliq.cummax() - 1); ddB = 100 * (beq / beq.cummax() - 1)
    ax2.fill_between(ddB.index, ddB, 0, color=S.C2, alpha=0.25, lw=0, label="S&P 500 TR")
    ax2.plot(ddS.index, ddS, color=S.C1, lw=1.2, label=list(eqs)[0].split(":")[0])
    ax2.set_ylabel("Drawdown from peak (%)"); ax2.set_xlabel("Date")
    ax2.grid(True, color=S.GRID, lw=0.8); ax2.set_axisbelow(True)
    ax2.legend(loc="lower left", frameon=False, ncol=2)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "equity_18k.png"), dpi=130); plt.close(fig)


# =============================================================== spot checks (independent recomputation)
def spot_check(tr, d):
    """Recompute one A and one B trade straight from the raw Yahoo files, not from the simulation's frame."""
    out = []
    gspc, spy, tnx = lib.get("^GSPC"), lib.get("SPY"), lib.get("^TNX")
    a = tr[(tr.leg == "A") & (tr.contracts > 0) & (pd.to_datetime(tr.entry_date.astype(str)) >= "2003-01-01")].iloc[0]
    t = pd.Timestamp(str(a.entry_date))
    close = gspc.Close.loc[t]; oc = spy.Close.loc[t] / spy.Open.loc[t] - 1
    p_in = close / (1 + oc)
    sgn = 1 if a.direction == "long" else -1
    gross = sgn * a.contracts * 5 * (close - p_in)
    cst = a.contracts * 2 * (COSTS["base"]["mes_fee"] + COSTS["base"]["mes_slip_ticks"] * 1.25)
    out.append(dict(trade=f"A {t.date()} {a.direction} {a.contracts} MES",
                    hand=f"SPX close {close:.2f}; SPY open->close {100*oc:+.3f}% so entry = {close:.2f}/(1{oc:+.5f}) "
                         f"= {p_in:.2f}; gross = {sgn:+d} x {a.contracts} x $5 x ({close:.2f} - {p_in:.2f}) = "
                         f"${gross:,.2f}; costs = {a.contracts} x 2 x ($0.62 + $1.25) = ${cst:.2f}; "
                         f"net ${gross - cst:,.2f}",
                    sim=f"gross ${a.gross_pnl:,.2f}, costs ${a.commissions + a.slippage:.2f}, net ${a.net_pnl:,.2f}",
                    match=abs(gross - cst - a.net_pnl) < 0.01))
    b = tr[(tr.leg == "B") & (tr.contracts > 0) & (pd.to_datetime(tr.entry_date.astype(str)) >= Y10_LAUNCH)].iloc[0]
    t0, t1 = pd.Timestamp(str(b.entry_date)), pd.Timestamp(str(b.exit_date))
    sc = 10.0 if tnx.Close.loc["2003":].median() > 20 else 1.0
    y0, y1 = tnx.Close.loc[t0] / sc, tnx.Close.loc[t1] / sc
    gross = -b.contracts * 1000 * (y1 - y0)
    cst = b.contracts * 2 * (COSTS["base"]["y10_fee"] + COSTS["base"]["y10_slip_ticks"] * 1.0)
    need = LEV_B * b.netliq_at_entry * b.ief_duration * 1e-4 / 10
    out.append(dict(trade=f"B {t0.date()} -> {t1.date()} short {b.contracts} x 10Y",
                    hand=f"size = floor(2.81 x ${b.netliq_at_entry:,.0f} x {b.ief_duration:.2f} x 0.0001 / $10) = "
                         f"floor({need:.2f}) = {math.floor(need)}; ^TNX {y0:.3f} -> {y1:.3f} "
                         f"({100*(y1-y0):+.1f}bp); gross = -{b.contracts} x 1000 x ({y1:.3f} - {y0:.3f}) = "
                         f"${gross:,.2f}; costs = {b.contracts} x 2 x ($0.85 + $1) = ${cst:.2f}; net ${gross - cst:,.2f}",
                    sim=f"gross ${b.gross_pnl:,.2f}, costs ${b.commissions + b.slippage:.2f}, net ${b.net_pnl:,.2f}",
                    match=abs(gross - cst - b.net_pnl) < 0.01 and math.floor(need) == b.contracts))
    return pd.DataFrame(out)


# =============================================================== main
TCOLS = ["leg", "hypothetical", "entry_date", "entry_time", "exit_date", "exit_time", "direction", "contracts",
         "contracts_wanted", "contract_notional", "position_notional", "lev_x_netliq", "entry_price", "exit_price",
         "gross_pnl", "gross_pnl_alt_model", "commissions", "slippage", "net_pnl", "net_pct_account",
         "netliq_at_entry", "equity_after", "margin_futures", "margin_pct_netliq", "total_req_pct_netliq",
         "margin_flag", "vol_tilt_size", "signal_gap_pct", "ief_duration", "dv01_usd"]


def main():
    d, info = load()
    bench = d.spy_cc
    scen, store = [], {}

    def add(key, label, e0, eq, tr):
        store[key] = (eq, tr)
        scen.append({"account": e0, "case": label, **perf(eq, tr, bench)})

    for e0 in ACCOUNT_SIZES:
        for ar in ["floor", "nearest"]:
            add((e0, ar, "tnx", "base"), f"10Y, A {ar}", e0, *simulate(d, e0, "base", ar, "tnx"))
    E = HEADLINE_EQUITY
    for ar in ["floor", "nearest"]:
        add((E, ar, "tnx", "stress"), f"10Y, A {ar}, stress costs", E, *simulate(d, E, "stress", ar, "tnx"))
        add((E, ar, "ief", "base"), f"10Y, A {ar}, B P&L = IEF-minus-bills model", E,
            *simulate(d, E, "base", ar, "ief"))
    add((E, "floor", "zn", "base"), "ZN (../simulate.py), A floor", E, *run_zn(d, E))
    for e0 in ACCOUNT_SIZES:
        for ar in ["floor", "nearest"]:
            add((e0, ar, "today", "base"), f"today's rounding fixed, A {ar}", e0,
                *simulate(d, e0, "base", ar, "tnx", today=today_sizing(d, e0, ar)))
    hi_eq, hi_tr = simulate(d, E, "base", "floor", "tnx", MES_MARGIN_PCT * MARGIN_STRESS_MULT,
                            Y10_MARGIN * MARGIN_STRESS_MULT)
    bs = S.bench_stats(store[(E, "floor", "tnx", "base")][0], bench)
    bs["final equity $"] *= E / S.START_EQUITY
    scen.append({"account": E, "case": "S&P 500 TR benchmark", **bs})
    SC = pd.DataFrame(scen)
    SC.round(3).to_csv(os.path.join(OUT, "scenarios_small.csv"), index=False)

    eq, tr = store[(E, "floor", "tnx", "base")]
    eqn, trn = store[(E, "nearest", "tnx", "base")]
    sortk = lambda T: T[TCOLS].sort_values(["entry_date", "leg"], key=lambda s: s.astype(str))
    sortk(tr).round(4).to_csv(os.path.join(OUT, "trades_18k.csv"), index=False)
    sortk(trn).round(4).to_csv(os.path.join(OUT, "trades_18k_nearest.csv"), index=False)
    Y = S.yearly(eq, tr, bench)
    Yn = S.yearly(eqn, trn, bench)
    Y.insert(1, "A-nearest variant %", Yn["strategy %"])
    Y.round(2).to_csv(os.path.join(OUT, "yearly_18k.csv"))
    eqz = store[(E, "floor", "zn", "base")][0]
    eqt = store[(E, "floor", "today", "base")][0]
    chart({"Strategy, B in 10Y micros, A floor (headline; A nearest is visually identical)": eq,
           "Same, with today's $18k contract counts held fixed (MES 2.11x / 0x, 3 x 10Y)": eqt,
           "Strategy, B in ZN priced on IEF-minus-bills (no B until ~$40k), A floor": eqz}, bench, E)

    # periods
    split = []
    for key, lab in [((E, "floor", "tnx", "base"), "floor"), ((E, "nearest", "tnx", "base"), "nearest")]:
        e_, t_ = store[key]
        for lbl, (a, b) in {"2003-2022 in-sample": ("2003", "2022"), "2023+ holdout": ("2023", None),
                            "2021-09+ (10Y micro live)": ("2021-09", None)}.items():
            e = e_.loc[a:b].copy()
            e.attrs["e0"] = e_.netliq.shift(1).loc[a:b].iloc[0] if a != "2003" else E
            ed = pd.to_datetime(t_.entry_date.astype(str))
            t = t_[(ed >= pd.Timestamp(a)) & (ed <= pd.Timestamp(f"{b or 2100}-12-31"))]
            p = perf(e, t, bench)
            bb = bench.loc[e.index]; bcagr = 100 * ((1 + bb).prod() ** (365.25 / (e.index[-1] - e.index[0]).days) - 1)
            split.append({"variant": lab, "period": lbl, "CAGR %": p["CAGR %"], "S&P CAGR %": bcagr,
                          "Sharpe": p["Sharpe"], "max DD %": p["max DD %"], "alpha %/yr": p["alpha %/yr"],
                          "beta": p["beta"], "A win %": p.get("A win %"), "B win %": p.get("B win %"),
                          "A avg % acct": p.get("A avg % acct"), "B avg % acct": p.get("B avg % acct"),
                          "A skipped": p["A skipped"], "B skipped": p["B skipped"]})
    SP = pd.DataFrame(split)
    SP.round(3).to_csv(os.path.join(OUT, "periods_18k.csv"), index=False)

    # leg B: TNX model vs IEF-minus-bills model, per trade (headline run)
    b = tr[(tr.leg == "B") & (tr.contracts > 0)].copy()
    rfm = d.irx
    b["carry_est"] = [n * N * (y0 / 100 - rfm.loc[str(e)]) * (pd.Timestamp(str(x)) - pd.Timestamp(str(e))).days / 365
                      for n, N, y0, e, x in zip(b.contracts, b.contract_notional, b.entry_price, b.entry_date,
                                                b.exit_date)]
    b["diff"] = b.gross_pnl_alt_model - b.gross_pnl                     # IEF model minus TNX model, $
    b["diff_bp"] = 1e4 * b["diff"] / b.position_notional
    b["carry_bp"] = 1e4 * b.carry_est / b.position_notional
    b["yr"] = pd.to_datetime(b.entry_date.astype(str)).dt.year
    bc = {"corr": np.corrcoef(b.gross_pnl, b.gross_pnl_alt_model)[0, 1],
          "sign_agree": 100 * (np.sign(b.gross_pnl) == np.sign(b.gross_pnl_alt_model)).mean(),
          "diff_bp_mean": b.diff_bp.mean(), "carry_bp_mean": b.carry_bp.mean(),
          "diff_bp_te": b.diff_bp.std(), "corr_diff_carry": np.corrcoef(b.diff_bp, b.carry_bp)[0, 1],
          "pre23_diff": b[b.yr < 2023].diff_bp.mean(), "post23_diff": b[b.yr >= 2023].diff_bp.mean(),
          "pre23_carry": b[b.yr < 2023].carry_bp.mean(), "post23_carry": b[b.yr >= 2023].carry_bp.mean(),
          "tnx_gross": b.gross_pnl.sum(), "ief_gross": b.gross_pnl_alt_model.sum()}
    info["b_check"] = bc
    # where does the (a)-(b) gap come from? daily gap between the two models vs carry, by session type
    dd_ = d.loc[START:]
    rf1 = dd_.irx.shift(1) * dd_.days / 360
    gap = 1e4 * ((dd_.ief_cc - rf1) + (dd_.y10.diff() / 100) * dd_.dur.shift(1))
    car = 1e4 * (dd_.y10.shift(1) / 100 - dd_.irx.shift(1)) * dd_.days / 365
    info["gap_diag"] = {lab: (gap[m].mean(), car[m].mean()) for lab, m in
                        [("other days", dd_.rank_bwd > 3), ("B hold days (last 3 sessions)", dd_.rank_bwd <= 3)]}

    # contract counts for a fresh account at today's levels
    last = d.iloc[-1]
    rows = []
    for e0 in ACCOUNT_SIZES:
        mn = S.MES_MULT * last.gspc_close
        r_ = {"account $": e0}
        for lbl, tilt in [("full", 1.0), ("half", 0.5)]:
            w = LEV_A * tilt * e0 / mn
            fl = math.floor(w); ne = int(math.floor(w + 0.5))
            while ne > 0 and ne * mn / e0 > MAX_LEV_A:
                ne -= 1
            r_[f"A {lbl} wanted"] = w
            r_[f"A {lbl} floor (lev x)"] = f"{fl} ({fl*mn/e0:.2f}x)"
            r_[f"A {lbl} nearest (lev x)"] = f"{ne} ({ne*mn/e0:.2f}x)"
        nb = math.floor(LEV_B * e0 * last.dur * 1e-4 / Y10_DV01)
        r_["B 10Y contracts (IEF-equiv lev x)"] = f"{nb} ({nb*Y10_DV01*1e4/last.dur/e0:.2f}x)"
        r_["B ZN contracts"] = math.floor(LEV_B * e0 * last.beta / (S.ZN_MULT * last.zn_close))
        rows.append(r_)
    CN = pd.DataFrame(rows)
    info["now"] = dict(spx=last.gspc_close, y10=last.y10, dur=last.dur, mes_notional=S.MES_MULT * last.gspc_close,
                       min_1_10y=Y10_DV01 / (LEV_B * last.dur * 1e-4), min_1_mes_full=S.MES_MULT * last.gspc_close / LEV_A,
                       min_1_mes_half_floor=S.MES_MULT * last.gspc_close / (LEV_A / 2),
                       min_1_mes_half_nearest=S.MES_MULT * last.gspc_close / MAX_LEV_A)
    info["dur_trades"] = b.ief_duration.describe().to_dict()
    info["margin_hi"] = dict(max=hi_tr.margin_pct_netliq.max(), flags=int(hi_tr.margin_flag.sum()),
                             cagr=perf(hi_eq, hi_tr, bench)["CAGR %"])
    info["margin_base"] = {k: (store[k][1].margin_pct_netliq.max(), int(store[k][1].margin_flag.sum()),
                               store[k][1].total_req_pct_netliq.max())
                           for k in store if k[2] == "tnx"}
    SPOT = spot_check(tr, d)

    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 60)
    print({k: v for k, v in info.items() if k not in ("now",)})
    print(info["now"])
    print(SC.set_index(["account", "case"]).round(2).T.to_string())
    print(Y.round(1).to_string()); print(SP.round(2).to_string()); print(CN.to_string())
    print(SPOT.to_string())
    write_summary(info, SC, Y, SP, CN, SPOT, eq, tr, trn, d)


def write_summary(info, SC, Y, SP, CN, SPOT, eq, tr, trn, d):
    E = HEADLINE_EQUITY
    f = lambda v, n=1: f"{v:,.{n}f}"
    g = lambda acct, case, k: SC[(SC.account == acct) & (SC.case == case)][k].iloc[0]
    keys = ["CAGR %", "vol %", "Sharpe", "max DD %", "beta", "alpha %/yr", "alpha t", "A trades", "A skipped",
            "A win %", "A avg % acct", "A avg net $", "A avg contracts", "A avg lev x", "B trades", "B skipped",
            "B win %", "B avg % acct", "B avg net $", "B avg contracts", "B avg lev x", "max futures margin % netliq",
            "trades margin > 60%", "costs $", "final equity $"]
    H = SC.copy()
    H["scenario"] = [f"${a/1e3:,.0f}k {c}" for a, c in zip(H.account, H.case)]
    H = H.set_index("scenario").reindex(columns=keys).T
    H.insert(0, "metric", H.index)
    c_sizes = ["metric"] + [c for c in H.columns if c.endswith(("10Y, A floor", "10Y, A nearest"))]
    c18 = ["metric"] + [c for c in H.columns if c.startswith("$18k") and "today" not in c]
    ctd = ["metric"] + [c for c in H.columns if "today" in c]
    td = lambda acct, ar, k: g(acct, f"today's rounding fixed, A {ar}", k)
    gd = info["gap_diag"]
    bm = SC[SC.case.str.contains("benchmark")].iloc[0]
    hf = lambda k: g(E, "10Y, A floor", k)
    hn = lambda k: g(E, "10Y, A nearest", k)
    bc, now, dt_ = info["b_check"], info["now"], info["dur_trades"]
    yt = Y.reset_index(); yt["year"] = yt["year"].astype(str)
    a18 = tr[tr.leg == "A"]; b18 = tr[tr.leg == "B"]
    a_sk = a18[a18.contracts == 0]; a_skn = trn[(trn.leg == "A") & (trn.contracts == 0)]
    a_sk_y = a_sk.entry_date.astype(str).str[:4].value_counts().sort_index()
    ah = a18[(a18.contracts > 0)]
    early = ah[ah.entry_date.astype(str) < "2013"].contracts
    flags = info["margin_base"]
    mb = flags[(E, "floor", "tnx", "base")]; mbn = flags[(E, "nearest", "tnx", "base")]
    worst_flag = max(v[0] for v in flags.values())
    zn = lambda k: g(E, "ZN (../simulate.py), A floor", k)
    spot = "\n".join(f"- **{r.trade}** (by hand from raw Yahoo files): {r.hand}. Simulation: {r.sim}. "
                     f"{'Matches.' if r.match else '**MISMATCH**'}" for r in SPOT.itertuples())
    md = f"""# Month-end trader, small account: $18k with leg B in CME Micro 10-Year Yield futures

Generated by `simulate_small.py` (imports `../simulate.py` for data, the leg-A signal, the SPY core, cash interest and
stats; nothing there is modified). Period **{eq.index[0].date()} to {eq.index[-1].date()}**, start equity
**${E:,.0f}** unless stated. Daily Yahoo data, whole contracts, IBKR-style costs. A simulation, not live fills.

Files: `trades_18k.csv` (every trade, headline run: A floor rounding, B in 10Y with the ^TNX P&L model),
`trades_18k_nearest.csv` (same, A nearest rounding), `yearly_18k.csv`, `periods_18k.csv`, `scenarios_small.csv`
(all sizes and variants), `equity_18k.png`.

**Hypothetical periods.** MES exists since {MES_LAUNCH} and the Micro 10Y Yield future since {Y10_LAUNCH}. Every leg-A trade
before May 2019 and every leg-B trade before Aug 2021 is a *hypothetical* contract with today's specs (flagged in the
`hypothetical` column). Only Sep 2021 onward could actually have been traded this way.

## Headline: $18,000, base costs

| | A floor (headline) | A nearest | B in ZN instead (A floor) | S&P 500 TR |
|---|---|---|---|---|
| CAGR | **{f(hf("CAGR %"))}%** | {f(hn("CAGR %"))}% | {f(zn("CAGR %"))}% | {f(bm["CAGR %"])}% |
| Final equity | ${hf("final equity $"):,.0f} | ${hn("final equity $"):,.0f} | ${zn("final equity $"):,.0f} | ${bm["final equity $"]:,.0f} |
| Volatility | {f(hf("vol %"))}% | {f(hn("vol %"))}% | {f(zn("vol %"))}% | {f(bm["vol %"])}% |
| Sharpe (excess of T-bills) | {f(hf("Sharpe"), 2)} | {f(hn("Sharpe"), 2)} | {f(zn("Sharpe"), 2)} | {f(bm["Sharpe"], 2)} |
| Max drawdown | {f(hf("max DD %"))}% | {f(hn("max DD %"))}% | {f(zn("max DD %"))}% | {f(bm["max DD %"])}% |
| Beta / alpha vs S&P (daily OLS) | {f(hf("beta"), 2)} / {f(hf("alpha %/yr"))}%/yr (t={f(hf("alpha t"))}) | {f(hn("beta"), 2)} / {f(hn("alpha %/yr"))}%/yr (t={f(hn("alpha t"))}) | {f(zn("beta"), 2)} / {f(zn("alpha %/yr"))}%/yr | 1.00 / 0 |
| Leg A trades / skipped (0 MES) | {hf("A trades"):.0f} / {hf("A skipped"):.0f} | {hn("A trades"):.0f} / {hn("A skipped"):.0f} | {zn("A trades"):.0f} / {zn("A skipped"):.0f} | |
| Leg A win rate, avg net per trade | {f(hf("A win %"))}%, {f(hf("A avg % acct"), 2)}% (${hf("A avg net $"):,.0f}) | {f(hn("A win %"))}%, {f(hn("A avg % acct"), 2)}% (${hn("A avg net $"):,.0f}) | {f(zn("A win %"))}%, {f(zn("A avg % acct"), 2)}% | |
| Leg B trades / skipped | {hf("B trades"):.0f} / {hf("B skipped"):.0f} | {hn("B trades"):.0f} / {hn("B skipped"):.0f} | {zn("B trades"):.0f} / {zn("B skipped"):.0f} | |
| Leg B win rate, avg net per trade | {f(hf("B win %"))}%, {f(hf("B avg % acct"), 2)}% (${hf("B avg net $"):,.0f}) | {f(hn("B win %"))}%, {f(hn("B avg % acct"), 2)}% | {f(zn("B win %"))}%, {f(zn("B avg % acct"), 2)}% | |
| Total costs (fees + slippage + SPY rebalancing) | ${hf("costs $"):,.0f} | ${hn("costs $"):,.0f} | ${zn("costs $"):,.0f} | |

Stress costs (2 ticks per side on MES and 10Y): A floor CAGR **{f(g(E, "10Y, A floor, stress costs", "CAGR %"))}%**
(Sharpe {f(g(E, "10Y, A floor, stress costs", "Sharpe"), 2)}), A nearest {f(g(E, "10Y, A nearest, stress costs", "CAGR %"))}%.
Leg B priced with the IEF-minus-bills model instead of ^TNX: A floor CAGR {f(g(E, "10Y, A floor, B P&L = IEF-minus-bills model", "CAGR %"))}%,
A nearest {f(g(E, "10Y, A nearest, B P&L = IEF-minus-bills model", "CAGR %"))}%.

The ZN contrast uses `../simulate.py` unchanged, whose leg B is priced on the IEF-minus-bills model; compare it with
the 10Y IEF-model row ({f(g(E, "10Y, A floor, B P&L = IEF-minus-bills model", "CAGR %"))}%), not with the ^TNX headline.
It is not "B never trades": a fresh $18k account cannot buy one ZN (needs ~$37-45k), but the account
grows, so the ZN run trades B once it gets there ({zn("B skipped"):.0f} months skipped, mostly early).

**Read this with "Starting today" below.** Started in 2003, the $18k account held ~10 MES from the first trade (the S&P was
near 900) and compounded past the point where rounding matters, so 0 months were skipped at any size. With today's
contract counts held fixed, $18k earns **19.8% (floor) / 21.2% (nearest)** CAGR, Sharpe 0.95 / 1.01.

![equity](equity_18k.png)

## What changes versus `../simulate.py`

- **Leg B = short the Micro 10-Year Yield future** (short yield = long bonds) from the close of the 4th-last session to the
  month-end close. Price = 10y yield in index points (4.250 = 4.25%), 0.001 pt = $1, DV01 = $10 per contract.
  Contracts = floor({LEV_B} x net liq x IEF duration x 0.0001 / $10). One contract therefore stands for $100,000 / duration
  of IEF (~$14k at duration 7.1), so a fresh $18k account gets 3 contracts, ~2.4x IEF-equivalent instead of 2.81x.
- **IEF duration** (the sensitivity of IEF to the 10y yield): regression of IEF daily total returns on -(daily change in
  the 10y yield in decimal). Full sample 2003-2026: **{info["dur_full"]:.2f}** (corr {info["dur_corr"]:.3f}); weekly returns
  give {info["dur_weekly"]:.2f} (the daily figure is slightly attenuated, most likely because ^TNX's daily close is not
  taken at IEF's 16:00 close);
  by period: {", ".join(f"{k} {v:.2f}" for k, v in info["dur_by_period"].items())}. Sizing uses the trailing
  {DUR_WIN}-session daily estimate (min {DUR_MIN}); at the trades it ranged {dt_["min"]:.2f}-{dt_["max"]:.2f} (mean {dt_["mean"]:.2f}).
  Note: the ^TNX file in this data cache is already in percent (4.912, not 49.12), so no /10 was needed
  (the script detects the scale; detected factor {info["tnx_scale"]:.0f}). {info["tnx_missing"]} sessions with no ^TNX print were forward-filled.
- **Two leg-B P&L models, both computed for every trade** (`gross_pnl` = headline model, `gross_pnl_alt_model` = the other):
  (b) **headline:** -contracts x (^TNX exit - ^TNX entry) x $1,000, marked daily; (a) contracts x $100,000/duration x
  (IEF total return - T-bill accrual), marked daily. On the {hf("B trades"):.0f} B trades of the headline run they agree
  closely: P&L correlation **{bc["corr"]:.3f}**, same sign on {bc["sign_agree"]:.0f}% of trades, tracking difference
  {bc["diff_bp_te"]:.0f}bp of notional per trade. Model (a) earns on average **{bc["diff_bp_mean"]:+.1f}bp** of notional per trade
  more than (b) ({bc["pre23_diff"]:+.1f}bp before 2023, {bc["post23_diff"]:+.1f}bp in 2023+). Gross over the whole run: (b)
  ${bc["tnx_gross"]:,.0f} vs (a) ${bc["ief_gross"]:,.0f}.
- **Why the sign: carry.** A yield future pays only the change in the yield; it has no coupon income and no financing.
  IEF minus bills earns that same yield change x duration *plus* carry = (bond yield - bill rate) x time (+ roll-down).
  With an upward-sloping curve (most of 2003-2022) carry is positive, so the IEF model beats the yield future; with the
  curve inverted (2023-2024, bills above 10s) the sign flips. Estimated carry over the 3-night hold,
  (y10 - ^IRX) x days/365: {bc["carry_bp_mean"]:+.1f}bp of notional on average ({bc["pre23_carry"]:+.1f}bp pre-2023,
  {bc["post23_carry"]:+.1f}bp in 2023+); correlation of the per-trade model gap with that carry estimate
  {bc["corr_diff_carry"]:.2f}. **So carry explains only about a third of the gap, and the 2023+ gap stayed positive
  although carry turned negative.** Day by day, IEF-minus-bills beats the duration-scaled ^TNX move by
  {gd["other days"][0]:.2f}bp/day on ordinary days (carry {gd["other days"][1]:.2f}bp/day: there the gap *is* carry) but by
  {gd["B hold days (last 3 sessions)"][0]:.2f}bp/day during the B hold (carry {gd["B hold days (last 3 sessions)"][1]:.2f}bp/day).
  The extra ~1bp/day is month-end specific: index-extension buying lifts IEF's 7-10y basket (and the 15:00-16:00 hour,
  which ^TNX's earlier close misses) more than the on-the-run 10y yield. A yield future is tied to the on-the-run 10y, so
  it likely does **not** capture that part; that is the main reason the 10Y leg earns less than the ZN/IEF model. (Strictly, a futures price is a *forward* yield which converges to spot, so a real 10Y future
  recovers a little of the carry through convergence; real fills should land between (a) and (b), close to (b).)
  ^TNX is the Cboe cash 10y index and the futures settle to BrokerTec on-the-run yields, so the two track closely but
  not exactly; I could not get usable Micro 10Y price history (Yahoo `10Y=F` returns a single bar), so (b) uses ^TNX.
  Model (b) is clean enough to be the headline.
- **Leg A rounding:** contracts = S&P notional {LEV_A}x (full) or {LEV_A/2}x (half) of net liq(T-1) / ($5 x S&P close T-1),
  whole MES. *floor* skips the month when that rounds to 0; *nearest* rounds half up but never above {MAX_LEV_A:.0f}x net liq
  (so today half-size 0.68 MES -> 1 MES at ~2.1x is allowed). Uses the actual S&P level at each trade: in 2003-2012 one
  MES was ~$4-10k of notional, so the $18k account held {early.min():.0f}-{early.max():.0f} MES (mean {early.mean():.1f}) on traded months before 2013.
- Everything else (signal, entry/exit times, 10% SPY drift band, cash interest, SPY rebalancing costs) is identical to
  `../simulate.py`. SPY is held in fractional dollars (IBKR offers fractional shares; at $18k that is ~25 SPY shares).

## Costs and margin assumptions

| Item | Base | Stress |
|---|---|---|
| MES fees (IBKR ~$0.25 + CME ~$0.35 + NFA ~$0.02) | $0.62 / contract / side | same |
| MES slippage | 1 tick = $1.25 / side | 2 ticks = $2.50 |
| Micro 10Y fees: my estimate, IBKR commission ~$0.25 + CME exchange ~$0.35-0.60 (I use the upper end) + NFA $0.02 | **$0.85 / contract / side** | same |
| Micro 10Y slippage | 1 tick = 0.001 = $1 / side | 2 ticks = $2 |
| MES initial margin | {MES_MARGIN_PCT:.1%} of notional (~${now["mes_notional"]*MES_MARGIN_PCT:,.0f} today at S&P {now["spx"]:,.0f}; scales with the index back in time) | x{MARGIN_STRESS_MULT:.0f} |
| Micro 10Y initial margin | ${Y10_MARGIN:,.0f} / contract (CME has been ~$300-500) | x{MARGIN_STRESS_MULT:.0f} |
| Cash | positive earns max(^IRX - 0.5%, 0); negative (after funding the 10Y overnight margin) pays ^IRX + 1.5% | same |

A 10Y round trip is $3.70 per contract (~$11 for the 3 contracts a fresh $18k account trades), about 0.09bp of
IEF-equivalent notional per side, vs ~0.9bp for one ZN at 1/2 tick + $1.70; the micro is cheap per unit of risk.
MES round trip $3.74 per contract.

**Liquidity.** The Micro 10Y Yield future trades far less than ZN (Yahoo's last bar shows ~900 contracts); the book is
thinner and can be several ticks wide outside the busiest hours, including around the 16:00 close. 1-3 contracts should be fillable, but use limit orders near the
mid rather than market orders, and expect occasional misses. The contracts are **monthly**; check CME's termination rule
for the front month (I believe it terminates on the last business day of the contract month) and hold the *next* month's
contract over month-end so the position is not forced out at the final settlement.

## Small-account realities

- **IBKR minimum** for a margin account ($2,000) is met at every size; the lowest equity reached in the $18k headline run was
  ${hf("min netliq $"):,.0f}. Futures are not subject to the pattern-day-trader rule.
- **Margin.** Futures margin is funded by IBKR moving excess liquidity from the securities side (the 100% SPY core has
  ~75% loan value under Reg T maintenance), so it is a borrowing on top of the stock position, not separate cash.
  Highest futures initial margin at any trade (A + open B on the last day), $18k base: **{mb[0]:.1f}% of net liq** (A floor),
  {mbn[0]:.1f}% (A nearest); trades above {MARGIN_FLAG:.0%}: **{mb[1]} / {mbn[1]}**. Across all sizes and variants the
  maximum was {worst_flag:.1f}%. With SPY's 25% maintenance added the worst total requirement was {mb[2]:.1f}% / {mbn[2]:.1f}%
  of net liq. With margins doubled (crisis) the headline run peaks at {info["margin_hi"]["max"]:.1f}%
  ({info["margin_hi"]["flags"]} trades above {MARGIN_FLAG:.0%}). The binding constraint is leverage, not margin.
- **Interest on negative cash** is charged when overlay losses or the 10Y margin take cash below zero (between drift-band
  rebalances); it is small: net interest over the run is ${g(E, "10Y, A floor", "interest $"):+,.0f} (credit interest on
  positive cash outweighs the debit interest).
- **Rounding at today's levels** (S&P {now["spx"]:,.0f}, one MES = ${now["mes_notional"]:,.0f}, 10y yield {now["y10"]:.3f}%,
  duration {now["dur"]:.2f}): one MES at full size needs ${now["min_1_mes_full"]:,.0f} of equity; at half size
  ${now["min_1_mes_half_floor"]:,.0f} with floor rounding, or ${now["min_1_mes_half_nearest"]:,.0f} with nearest (3x cap).
  One 10Y contract needs only ${now["min_1_10y"]:,.0f}.

{S.md_table(CN)}

So today a fresh $18k account trades leg A only in high-vol (full-size) months with floor rounding, and every month with
nearest rounding (then at ~2.1x instead of 1.4x in calm months: nearest *over*-sizes the calm months); leg B trades 3
contracts every month.

**Months skipped by rounding at $18k.** Leg A floor: **{len(a_sk)}** of {len(a18)} months
({", ".join(f"{k}: {v}" for k, v in a_sk_y.items()) if len(a_sk) else "none"}); leg A nearest: **{len(a_skn)}**; leg B:
**{int((b18.contracts == 0).sum())}**. Historically the S&P was much lower, so MES rounding did not bind until the index
rose and only bites when the account is small relative to one MES; by the time the index was high the account had grown.

## Starting today: today's rounding held fixed

The compounding backtest above flatters small accounts: starting at $18k in 2003 with the S&P near 900, one MES was ~$4.5k,
so the account held ~10 MES from the first trade and rounding never bit (0 months skipped at any size). A trader
starting **today** gets today's contract counts. This scenario freezes them: MES leverage and 10Y contracts per $ of
equity are what a fresh account of that size gets at today's prices (table above), scaled with equity as it grows,
MES costs at today's cost in bp of notional. It answers "what return stream does today's rounding buy?".

| account | A floor: CAGR / Sharpe / max DD / alpha | A nearest: CAGR / Sharpe / max DD / alpha | A skipped (floor / nearest) |
|---|---|---|---|
""" + "\n".join(f"| ${a/1e3:,.0f}k | {td(a, 'floor', 'CAGR %'):.1f}% / {td(a, 'floor', 'Sharpe'):.2f} / {td(a, 'floor', 'max DD %'):.1f}% / {td(a, 'floor', 'alpha %/yr'):.1f}% "
              f"| {td(a, 'nearest', 'CAGR %'):.1f}% / {td(a, 'nearest', 'Sharpe'):.2f} / {td(a, 'nearest', 'max DD %'):.1f}% / {td(a, 'nearest', 'alpha %/yr'):.1f}% "
              f"| {td(a, 'floor', 'A skipped'):.0f} / {td(a, 'nearest', 'A skipped'):.0f} |" for a in ACCOUNT_SIZES) + f"""

At $18k today: floor = A only in high-vol months (1 MES, 2.11x) and nothing in calm months; nearest = 1 MES (2.11x) in
every month, i.e. calm months are traded at ~1.5x the intended half size. Full detail in the table below
(rows "today's rounding fixed").

{S.md_table(H[ctd].reset_index(drop=True))}

## All account sizes (base costs, B in 10Y micros, ^TNX P&L)

{S.md_table(H[c_sizes].reset_index(drop=True))}

## $18k variants

{S.md_table(H[c18].reset_index(drop=True))}

## Yearly, $18k headline (A floor, base costs)

Strategy % and the A-nearest variant's % side by side; A/B P&L are for the headline run. Max DD within the calendar year.

{S.md_table(yt)}

## In-sample vs holdout ($18k)

{S.md_table(SP)}

## Spot checks (two trades recomputed by hand)

{spot}

## Caveats

- **In-sample.** Signals, the vol tilt and leverages (2.85x / 2.81x) were chosen on 2003-2022; only 2023+ is out of
  sample (~4 years, ~46 trades per leg). Expect live results below the backtest.
- **Hypothetical instruments.** MES before May 2019 and the Micro 10Y before Aug 2021 did not exist; those years use
  today's contract specs on index/yield data. An $18k trader in 2003-2019 would have had ES (10x coarser) and ZN (which
  this account could not afford at first).
- **Starting from $18k the account compounds** to six/seven figures in the backtest, so "small account" rounding effects
  matter only in the first years (0 months skipped at any size from $10k up). A trader starting today faces today's
  rounding; see "Starting today" (at $18k: 19.8% floor / 21.2% nearest CAGR vs 22.1% compounding; $10k cannot trade
  leg A at all, because 1 MES = 3.8x net liq exceeds even the 3x cap).
- **Leg-B model risk.** ^TNX is the cash 10y yield at Cboe's close (before IEF's 16:00), not the futures settlement; the
  real 10Y future has a small forward/convergence term. The IEF-minus-bills model earns ~4.4bp more per hold (about 1.5bp carry, the rest a
  month-end effect in IEF's basket and closing time that a yield future probably does not capture; see above).
- **Leverage and gap risk.** On the last day of the month gross exposure is SPY 1x + MES up to 2.85x (3x with nearest) + the
  bond leg ~2.3-2.8x IEF-equivalent. No stops modelled.
- **Liquidity of the Micro 10Y** (thin, few ticks wide near the close) is the main execution risk; the stress case only
  doubles slippage to 2 ticks.
- **Taxes ignored** (futures 60/40 Section 1256; SPY rebalancing realises gains).
"""
    open(os.path.join(OUT, "summary.md"), "w").write(md)


if __name__ == "__main__":
    main()
