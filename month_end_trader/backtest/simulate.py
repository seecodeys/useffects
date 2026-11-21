"""Full-timeline, trade-by-trade backtest of the month_end_trader account as it would run at IBKR.

Account = SPY core (100% of net liq, dividends reinvested) + two futures overlays:
  Leg A (research 013 + 168 vol tilt): last NYSE session of each month, MES open->close,
        direction -sign(SPY MTD - TLT MTD) through T-1 (dividend-adjusted closes),
        notional LEV_A x net liq at full size if SPY 20d vol (T-1) > 2002-08..2014 median, else half.
  Leg B (research 276): long ZN from the 4th-last session to the last session of each month.
        Notional LEV_B x net liq x hedge ratio. Return modelled as (IEF total return - T-bill) / beta per
        contract (futures earn excess return), or on Yahoo ZN=F closes (b_src="znf").

BOT_FAITHFUL=True (default) runs the account the way the live bot (../met, config.example.yaml defaults) trades it:
  * bot leverages: A 2.8x full size (vol tilt 0.5), B 2.8 x fixed hedge ratio 1.14 (ZN notional 3.19x);
  * caps: A <= guardrails.max_contracts (40) and max_leverage 3.0x; B <= bond.max_contracts (20 ZN) and
    bond.max_leverage 4.5x; A is skipped if A + open B notional > guardrails.max_total_leverage (7x).
    Contract caps are applied in today's dollars by default (CAP_MODE, see below);
  * disaster stops: A flattens when the MES overlay loses 6% of equity intraday (^GSPC high/low, sanity-filtered,
    SPY as fallback), B when the ZN overlay loses 4% of equity at any time during the hold (ZN=F daily lows
    scaled to the model's price path); the stop fills at the stop level plus slippage;
  * bot times for leg B: exit 15:55 / entry 15:58 ET (daily closes stand in for both), but on 12:50 days (NYSE half
    days and SIFMA early closes: 31 Dec, the sessions before Good Friday and Memorial Day, met.bond.rates_early)
    the PREVIOUS session's close is used as the trade price (daily-resolution approximation).
BOT_FAITHFUL=False reproduces the earlier "research" run exactly (no caps, no stops, 2.85x / 2.81x x trailing beta,
16:00 closes on every day).

Run:  python3 simulate.py          (writes trades.csv, yearly.csv, scenarios.csv, *.png, summary.md here)
"""
import os, sys, math, warnings
warnings.filterwarnings("ignore")
sys.dont_write_bytecode = True          # do not leave __pycache__ in ../met (imported, read-only)
import numpy as np
import pandas as pd

# =============================================================== PARAMETERS
LIB_DIR = "/tmp/claude-0/-home-user-useffects/bdec50ca-f87a-5f19-aaff-f84bd7c2facb/scratchpad/ar/alpha_research"
OUT = os.path.dirname(os.path.abspath(__file__))

START = "2003-01-02"            # first simulated session (TLT starts 2002-07-30; vol window needs ~1 month)
END = None                      # None = last session of the latest COMPLETE month in the data
START_EQUITY = 100_000.0
ACCOUNT_SIZES = [25_000, 100_000, 250_000, 1_000_000]

# ---- bot-faithful switches (all follow BOT_FAITHFUL unless set individually)
BOT_FAITHFUL = True
BOT_LEVERAGE = BOT_FAITHFUL     # True: bot leverages (2.8 / 2.8 x fixed 1.14); False: research 2.85 / 2.81 x trailing beta
BOT_TIMES = BOT_FAITHFUL        # True: leg B trades at the previous close on 12:50 days; False: 16:00 closes always
BOT_CAPS = "default" if BOT_FAITHFUL else None   # None | "lev" (leverage caps only) | "default" (40 MES / 20 ZN) | "hard" (200 / 50)
BOT_STOPS = BOT_FAITHFUL        # disaster stops (A 6% of equity intraday, B 4% during the hold)
CAP_MODE = "today_usd"          # "today_usd": cap = N contracts at TODAY's contract value, i.e. a fixed $ notional
                                #   (floor(N x value_today / value_t) contracts at date t); "contracts": literal N
                                #   contracts at every date (with the S&P at 900 in 2003, 40 MES would be $180k)
STOP_EXTRA_SLIP_TICKS = 1.0     # stop exits fill this many ticks beyond the stop level (on top of normal slippage)
A_STOP_SOURCE = "gspc"          # "gspc": ^GSPC high/low, sanity-filtered, SPY fallback; "spy_raw": raw SPY high/low
GAP_FILTER = 0.01               # SPY extreme rejected if beyond the prior close against an opening gap > 1% (bad print)
ZN_SPIKE_MAX = 0.01             # ZN=F low more than 1% below min(prior close, close) treated as a bad print

LEV_A_RESEARCH, LEV_B_RESEARCH = 2.85, 2.81      # research 013/168 and 276 (bot values: see LEV_A_BOT / LEV_B_BOT below)
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
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # month_end_trader/ -> met
from met.config import (BOND_SPECS, BondConfig, GuardrailConfig, StrategyConfig, HARD_MAX_BOND_LEVERAGE,  # noqa: E402
                        HARD_MAX_CONTRACTS, HARD_MAX_LEVERAGE)
from met.bond import rates_early                    # noqa: E402  (NYSE half days + SIFMA early closes)
from met.guardrails import size_contracts           # noqa: E402  (the bot's sizing function)

_G, _BC, _SC = GuardrailConfig(), BondConfig(), StrategyConfig()
LEV_A_BOT = 2.8                                     # config.example.yaml strategy.leverage (vol_tilt: true, 0.5)
LEV_B_BOT = _BC.leverage                            # 2.8
HEDGE_BOT = BOND_SPECS["ZN"]["hedge"]               # 1.14
A_MAX_LEV, B_MAX_LEV, TOTAL_MAX_LEV = _G.max_leverage, _BC.max_leverage, _G.max_total_leverage   # 3.0, 4.5, 7.0
A_CAPS = {"default": _G.max_contracts, "hard": HARD_MAX_CONTRACTS}                    # 40 / 200 MES
B_CAPS = {"default": BOND_SPECS["ZN"]["default_contracts"], "hard": BOND_SPECS["ZN"]["max_contracts"]}   # 20 / 50 ZN
A_STOP, B_STOP = _G.disaster_stop_pct_equity, _BC.disaster_stop_pct_equity               # 0.06 / 0.04
VOL_THRESHOLD_BOT = _SC.vol_threshold               # 0.008369 (the bot's fixed copy of the 2002-08..2014 median)
LEV_A = LEV_A_BOT if BOT_LEVERAGE else LEV_A_RESEARCH   # MES notional / net liq at full size (half = LEV_A / 2)
LEV_B = LEV_B_BOT if BOT_LEVERAGE else LEV_B_RESEARCH   # IEF-equivalent notional / net liq


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
    # intraday extremes for the disaster-stop approximations
    d["gspc_low"], d["gspc_high"] = gspc.Low.reindex(idx), gspc.High.reindex(idx)
    d["spy_low"], d["spy_high"] = spy.Low.reindex(idx), spy.High.reindex(idx)
    d["spy_prev_close"] = spy.Close.shift(1).reindex(idx)
    zl = zn.Low.reindex(idx); zc = zn.Close.reindex(idx)
    zprev = d.zn_close.shift(1)
    floor_ = np.minimum(zprev, d.zn_close)
    lo = np.minimum(zl, d.zn_close)                                       # a low above the close is a bad print
    bad = lo.isna() | (lo < floor_ * (1 - ZN_SPIKE_MAX))                 # spike far below both closes: bad print
    d["zn_low_bad"] = bad & zl.notna()
    d["zn_low_r"] = (lo.where(~bad, floor_) / zprev - 1).fillna(0).clip(upper=0)   # worst move vs the prior close
    d["b_early"] = [rates_early(t.date()) for t in idx]                    # 12:50 ET leg-B trade days
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
    # ZN=F returns with Yahoo's quarterly roll days neutralised (replaced by the IEF model): the roll day is the
    # largest |ZN=F - (IEF - bills)/beta| residual in the 12th-25th of Mar/Jun/Sep/Dec, if above 3 robust sd
    ief_model = (d.ief_cc - d.irx.shift(1) * d.days / 360) / d.beta
    res = d.zn_cc_f - ief_model
    rw = d.index.month.isin([3, 6, 9, 12]) & (d.index.day >= 12) & (d.index.day <= 25)
    base = res[~rw].dropna(); sd = 1.4826 * (base - base.median()).abs().median()
    flag = pd.Series(False, index=d.index)
    rr = res[rw].dropna()
    for _, g in rr.groupby([rr.index.year, rr.index.month]):
        t = g.abs().idxmax()
        if abs(g[t]) > 3 * sd:
            flag[t] = True
    d["zn_roll"] = flag
    d["zn_ret_n"] = d.zn_cc_f.where(~flag, ief_model).fillna(0)
    xs, ys = x.loc[START:].dropna(), y.loc[START:].dropna()
    full_beta = np.polyfit(xs, ys, 1)[0]
    wk = d[["ief_aC"]].assign(zn=d.zn_close)[~roll].resample("W-FRI").last().pct_change().dropna()  # rough weekly check
    info = {"vol_median": med, "beta_full": full_beta, "beta_corr": np.corrcoef(xs, ys)[0, 1],
            "beta_weekly": np.polyfit(wk.zn, wk.ief_aC, 1)[0], "n_roll_excluded": int(roll.sum()),
            "zn_roll_days": int(flag.sum()), "zn_roll_in_hold": int((flag & (d.rank_bwd <= B_HOLD_RANK)).sum()),
            "zn_low_bad": int(d.zn_low_bad.sum())}
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


def cap_contracts(kind, leg, d, t, cap_mode=None):
    """Contract cap for leg A ("A", MES) or B ("B", ZN) at date t. kind: None/"lev" -> no contract cap."""
    if kind in (None, "lev"):
        return 10 ** 9
    n = (A_CAPS if leg == "A" else B_CAPS)[kind]
    if (cap_mode or CAP_MODE) == "contracts":
        return n
    px_now, px_t = (d.gspc_close.iloc[-1], d.gspc_close.loc[t]) if leg == "A" else (d.zn_close.iloc[-1], d.zn_close.loc[t])
    return int(math.floor(n * px_now / px_t + 1e-9))          # today's $ notional of n contracts, in contracts at t


def size(nl, price, mult, lev, max_lev, max_n, hard_lev, rnd_frac):
    """The bot's sizing (met.guardrails.size_contracts) for whole contracts; the same caps on fractional ones."""
    if not rnd_frac:
        n, _ = size_contracts(nl, price, mult, lev, 1.0, max_lev, max_n, "floor", hard_max_leverage=hard_lev,
                              hard_max_contracts=max_n)
        return n
    return max(min(lev * nl / (price * mult), max_n, min(max_lev, hard_lev) * nl / (price * mult)), 0.0)


def a_adverse(r, dirn, p_in, p_out, source=None):
    """Worst intraday move against leg A from the 09:30 entry, as a positive fraction of the entry price, and its source.
    ^GSPC high/low is preferred; it is rejected when the extreme IS the index's opening print (computed from stale
    component prices: e.g. 2008-09-30, 2011-11-30, 2012-06-29). SPY's extreme (relative to its open) is the
    fallback, rejected when it lies beyond the prior close against an opening gap > GAP_FILTER (the bad-print
    pattern of SPY's Yahoo lows on 2008-09-30 and 2011-11-30). The close-to-close loss always counts."""
    source = source or A_STOP_SOURCE
    close_adv = max(0.0, -dirn * (p_out / p_in - 1))
    s_ext = r.spy_low if dirn > 0 else r.spy_high
    s_adv = max(0.0, -dirn * (s_ext / r.spy_open - 1)) if np.isfinite(s_ext) else np.nan
    if source == "spy_raw":
        return max(close_adv, s_adv if np.isfinite(s_adv) else 0.0), "SPY raw"
    g_ext = r.gspc_low if dirn > 0 else r.gspc_high
    if np.isfinite(g_ext) and not (np.isfinite(r.gspc_open_raw) and abs(g_ext - r.gspc_open_raw) < 0.05):
        return max(close_adv, -dirn * (g_ext / p_in - 1), 0.0), "^GSPC"
    gap = r.spy_open / r.spy_prev_close - 1
    s_bad = (dirn > 0 and gap > GAP_FILTER and s_ext < r.spy_prev_close) or \
            (dirn < 0 and gap < -GAP_FILTER and s_ext > r.spy_prev_close)
    if np.isfinite(s_adv) and not s_bad:
        return max(close_adv, s_adv), "SPY (^GSPC extreme = open print)"
    return close_adv, "close only (^GSPC open print, SPY bad print)"


def simulate(d, equity0=START_EQUITY, cost="base", fractional=False, research=False, core_band="param", b_src="ief",
             bot_lev="param", bot_times="param", caps="param", stops="param", a_stop_source=None, cap_mode=None):
    """research=True: mimic the research P&L (fractional contracts, no costs, core rebalanced daily,
    B earns IEF total return rather than excess, no cash interest) and switch every bot feature off.
    bot_lev / bot_times / caps / stops default to the module switches (BOT_LEVERAGE, BOT_TIMES, BOT_CAPS, BOT_STOPS).
    b_src: "ief" (IEF total return - bills, / trailing beta) or "znf" (Yahoo ZN=F closes, roll days neutralised)."""
    P = lambda v, dflt: dflt if (isinstance(v, str) and v == "param") else v
    bot_lev, bot_times, caps, stops = P(bot_lev, BOT_LEVERAGE), P(bot_times, BOT_TIMES), P(caps, BOT_CAPS), P(stops, BOT_STOPS)
    if research:
        bot_lev, bot_times, caps, stops = False, False, None, False
    lev_a = LEV_A_BOT if bot_lev else LEV_A_RESEARCH
    lev_b = LEV_B_BOT if bot_lev else LEV_B_RESEARCH
    c = COSTS["zero" if research else cost]
    band = CORE_BAND if core_band == "param" else core_band
    frac = fractional or research
    rnd = (lambda v: v) if frac else (lambda v: math.floor(max(v, 0.0) + 1e-9))
    dd = d.loc[START:]
    idx = dd.index
    core, cash = equity0, 0.0
    nl_prev = equity0
    B = None                      # open leg-B position
    rows, trades = [], []
    rf_prev = dd.irx.iloc[0]
    zn_slip = c["zn_slip_ticks"] * ZN_TICK * ZN_MULT
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
        # --- leg B mark-to-market / disaster stop / exit
        if B is not None:
            early_exit = bot_times and r.rank_bwd == 1 and r.b_early    # exit 12:50: price = previous close
            ief_ret = r.ief_cc if research else r.ief_cc - rf_d
            ret = 0.0 if early_exit else (r.zn_ret_n if b_src == "znf" else ief_ret / B["beta"])
            P_new = B["P"] * (1 + ret)
            stopped = False
            if stops and not early_exit:
                worst = min(B["P"] * (1 + r.zn_low_r), P_new)            # ZN=F low vs prior close, on the model path
                P_stop = B["P0"] - B_STOP * B["nl0"] / (B["n"] * ZN_MULT)
                if worst <= P_stop + 1e-12:
                    P_new = P_stop - STOP_EXTRA_SLIP_TICKS * ZN_TICK  # out at the stop level, extra ticks worse
                    stopped = True
            pnl = B["n"] * ZN_MULT * (P_new - B["P"])
            B["P"] = P_new; B["gross"] += pnl; b_pnl += pnl; cash += pnl
            if r.rank_bwd == 1 or stopped:
                fee = B["n"] * c["zn_fee"]; slip = B["n"] * zn_slip
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B["fee"] += fee; B["slip"] += slip
                xt = "disaster stop" if stopped else ("12:50" if early_exit else ("15:55" if bot_times else r.close_time))
                B.update(exit_date=t, exit_time=xt, exit_price=P_new, closed=True,
                         exit_reason="disaster stop (4% of equity)" if stopped else
                         ("12:50 exit (previous close)" if early_exit else "month-end close"))
        # --- leg A (intraday, last session of the month)
        if r.rank_bwd == 1 and np.isfinite(r.gap) and r.gap != 0 and np.isfinite(r.vol20):
            dirn = -np.sign(r.gap)
            px_prev = dd.gspc_close.iloc[i - 1] if i > 0 else r.gspc_close
            want_a = rnd(lev_a * r.sizeA * nl_prev / (MES_MULT * px_prev))
            if caps is None:
                n = want_a
            else:
                n = size(nl_prev, px_prev, MES_MULT, lev_a * r.sizeA, A_MAX_LEV, cap_contracts(caps, "A", d, t, cap_mode),
                         HARD_MAX_LEVERAGE, frac)
            note = ""
            if caps is not None and n > 0:                             # total futures exposure incl. open leg B
                b_not = B["n"] * ZN_MULT * B["P"] if (B is not None and not B.get("closed")) else 0.0
                if (n * MES_MULT * px_prev + b_not) / nl_prev > TOTAL_MAX_LEV + 1e-9:
                    n, note = 0, "blocked: total leverage > 7x"
            # entry = S&P level implied by SPY's 09:30 open (Yahoo's ^GSPC opens are stale pre-2014)
            p_in = r.gspc_close / (1 + r.spy_oc); p_out = r.gspc_close
            reason, adv, src = "close", np.nan, ""
            if stops and n > 0:
                adv, src = a_adverse(r, dirn, p_in, p_out, a_stop_source)
                if n * MES_MULT * p_in * adv >= A_STOP * nl_prev:
                    p_out = p_in - dirn * A_STOP * nl_prev / (n * MES_MULT) - dirn * STOP_EXTRA_SLIP_TICKS * MES_TICK
                    reason = "disaster stop (6% of equity)"
            gross = dirn * n * MES_MULT * (p_out - p_in)
            fee = 2 * n * c["mes_fee"]; slip = 2 * n * c["mes_slip_ticks"] * MES_TICK * MES_MULT
            net = gross - fee - slip
            cash += net; a_pnl += net; cost_day += fee + slip
            trades.append(dict(leg="A", entry_date=t.date(), entry_time="09:30", exit_date=t.date(),
                               exit_time="disaster stop" if reason != "close" else r.close_time,
                               direction=("long" if dirn > 0 else "short") if (n > 0 or not note) else note,
                               contracts=n,
                               contract_notional=MES_MULT * p_in, position_notional=n * MES_MULT * p_in,
                               lev_x_netliq=n * MES_MULT * p_in / nl_prev,
                               entry_price=p_in, exit_price=p_out, gross_pnl=gross, commissions=fee,
                               slippage=slip, net_pnl=net, net_pct_account=100 * net / nl_prev,
                               netliq_at_entry=nl_prev, vol_tilt_size=r.sizeA, signal_gap_pct=100 * r.gap,
                               hedge_ratio=np.nan, exit_reason=reason if n > 0 else (note or "0 contracts"),
                               capped=bool(n < want_a - 1e-9),
                               adverse_pct=100 * adv, adverse_source=src,
                               close_pnl_pct_if_held=100 * (dirn * n * MES_MULT * (r.gspc_close - p_in) - fee - slip) / nl_prev))
        nl = core + cash
        # --- leg B record exit / entry
        if B is not None and B.get("closed"):
            net = B["gross"] - B["fee"] - B["slip"]
            trades.append(dict(leg="B", entry_date=B["entry_date"].date(), entry_time=B["entry_time"],
                               exit_date=t.date(), exit_time=B["exit_time"], direction="long", contracts=B["n"],
                               contract_notional=ZN_MULT * B["P0"], position_notional=B["n"] * ZN_MULT * B["P0"],
                               lev_x_netliq=B["n"] * ZN_MULT * B["P0"] / B["nl0"],
                               entry_price=B["P0"], exit_price=B["exit_price"], gross_pnl=B["gross"],
                               commissions=B["fee"], slippage=B["slip"], net_pnl=net,
                               net_pct_account=100 * net / B["nl0"], netliq_at_entry=B["nl0"],
                               vol_tilt_size=np.nan, signal_gap_pct=np.nan, hedge_ratio=B["hedge"],
                               exit_reason=B["exit_reason"], model_beta=B["beta"], capped=B["capped"]))
            B = None
        if r.rank_bwd == B_HOLD_RANK and t < idx[-1]:
            beta = r.beta                                    # trailing beta: maps IEF returns to ZN (P&L model)
            hedge = HEDGE_BOT if bot_lev else beta           # sizing hedge ratio: bot's fixed 1.14 or the trailing beta
            early_entry = bot_times and r.b_early            # enter 12:50: price = previous close, earn today's move
            P0 = dd.zn_close.iloc[i - 1] if early_entry else r.zn_close
            nl_s = nl_prev if early_entry else nl
            want_b = rnd(lev_b * nl_s * hedge / (ZN_MULT * P0))
            if caps is None:
                n = want_b
            else:
                n = size(nl_s, P0, ZN_MULT, lev_b * hedge, B_MAX_LEV, cap_contracts(caps, "B", d, t, cap_mode),
                         HARD_MAX_BOND_LEVERAGE, frac)
            if n > 0:
                fee = n * c["zn_fee"]; slip = n * zn_slip
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B = dict(n=n, P=P0, P0=P0, beta=beta, hedge=hedge, capped=bool(n < want_b - 1e-9), gross=0.0, fee=fee, slip=slip, entry_date=t,
                         entry_time="12:50" if early_entry else ("15:58" if bot_times else r.close_time), nl0=nl_s)
                if early_entry:                               # today's move from the previous close
                    ief_ret = r.ief_cc if research else r.ief_cc - rf_d
                    P_new = P0 * (1 + (r.zn_ret_n if b_src == "znf" else ief_ret / beta))
                    pnl = n * ZN_MULT * (P_new - P0)
                    B["P"] = P_new; B["gross"] += pnl; b_pnl += pnl; cash += pnl
            else:
                trades.append(dict(leg="B", entry_date=t.date(), entry_time=r.close_time, exit_date=None,
                                   exit_time=None, direction="skipped (0 contracts)", contracts=0,
                                   contract_notional=ZN_MULT * P0, position_notional=0, lev_x_netliq=0,
                                   entry_price=P0, exit_price=np.nan, gross_pnl=0, commissions=0,
                                   slippage=0, net_pnl=0, net_pct_account=0, netliq_at_entry=nl_s,
                                   vol_tilt_size=np.nan, signal_gap_pct=np.nan, hedge_ratio=hedge,
                                   exit_reason="0 contracts", model_beta=beta,
                                   _need=lev_b * nl_s * hedge / (ZN_MULT * P0)))
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


def charts(eq, eq_stress, bench_ret, eq_research=None):
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
    ax.plot(eq.index, eq.netliq, color=C1, lw=2, label="Strategy, bot-faithful (caps, stops, bot leverages/times), base costs")
    ax.plot(eq_stress.index, eq_stress.netliq, color=C1, lw=1, ls="--", alpha=0.8, label="Same, stress costs")
    if eq_research is not None:
        ax.plot(eq_research.index, eq_research.netliq, color=C4, lw=1.2, label="Research run (no caps or stops), base costs")
    ax.plot(beq.index, beq, color=C2, lw=2, label="S&P 500 total return (SPY, dividends reinvested)")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mt.FuncFormatter(lambda v, _: f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}k"))
    ax.yaxis.set_minor_formatter(mt.NullFormatter())
    ax.set_yticks([1e5, 2e5, 5e5, 1e6, 2e6, 5e6, 1e7, 2e7, 5e7])
    top = max(eq.netliq.max(), beq.max(), eq_research.netliq.max() if eq_research is not None else 0)
    ax.set_ylim(eq.netliq.min() * 0.85, top * 1.2)
    ax.set_ylabel("Account equity (log scale)")
    ax.grid(True, color=GRID, lw=0.8); ax.set_axisbelow(True)
    ends = [(eq.netliq, f"${eq.netliq.iloc[-1]/1e6:.1f}M", C1), (beq, f"${beq.iloc[-1]/1e6:.2f}M", C2)]
    if eq_research is not None:
        ends.append((eq_research.netliq, f"${eq_research.netliq.iloc[-1]/1e6:.1f}M", C4))
    for s, lbl, col in ends:
        ax.annotate(lbl, (s.index[-1], s.iloc[-1]), xytext=(4, 0), textcoords="offset points", va="center",
                    color=INK, fontsize=9)
    ax.axvline(pd.Timestamp("2023-01-01"), color=INK2, lw=0.8, ls=":")
    ax.text(pd.Timestamp("2023-02-01"), ax.get_ylim()[0] * 1.15, "holdout 2023+", color=INK2, fontsize=8)
    ax.legend(loc="upper left", frameon=False)
    ax.set_title(f"Month-end trader account from ${START_EQUITY/1e3:.0f}k, {eq.index[0]:%b %Y} - {eq.index[-1]:%b %Y} "
                 "(whole contracts, IBKR costs, as the bot trades it)", loc="left", color=INK, fontsize=12)
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


RESEARCH_KW = dict(bot_lev=False, bot_times=False, caps=None, stops=False)   # = BOT_FAITHFUL False
HEADLINE = "bot-faithful (default caps, stops, bot leverages and times)"
HARD = "hard caps (200 MES / 50 ZN), else bot-faithful"
NOCAP = "no contract caps, else bot-faithful"
RESEARCH = "no caps (research run: BOT_FAITHFUL=False)"
ZNF = "leg B priced on ZN=F (roll days neutralised), else bot-faithful"
VARIANTS = [   # $100k, base costs
    (HEADLINE, {}), (HARD, dict(caps="hard")), (NOCAP, dict(caps="lev")), (RESEARCH, RESEARCH_KW), (ZNF, dict(b_src="znf")),
    # bridge research -> bot-faithful
    ("research + bot leverages (2.8 / 2.8 x 1.14)", dict(bot_lev=True, bot_times=False, caps=None, stops=False)),
    ("  + bot leg-B times", dict(bot_lev=True, bot_times=True, caps=None, stops=False)),
    ("  + default caps (40 MES / 20 ZN, 3x / 4.5x / 7x)", dict(stops=False)),
    # sensitivities around the headline
    ("bot-faithful, caps as literal contract counts", dict(cap_mode="contracts")),
    ("bot-faithful, A stop on raw SPY highs/lows", dict(a_stop_source="spy_raw")),
    ("bot-faithful, ZN=F pricing and hard caps", dict(b_src="znf", caps="hard")),
    ("bot-faithful, never rebalance core", dict(core_band=None)),
    # research-settings steps for the reconciliation with research 350
    ("research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", dict(research=True)),
    ("research settings, fractional contracts, zero costs", dict(fractional=True, cost="zero", **RESEARCH_KW)),
    ("research settings, fractional contracts, base costs", dict(fractional=True, cost="base", **RESEARCH_KW)),
]


def spot_check_stops(tr, n=2):
    """Recompute the first n leg-B disaster stops straight from the raw Yahoo files (ZN=F, IEF, ^IRX)."""
    zn, ief, irx = lib.get("ZN=F"), lib.get("IEF"), lib.get("^IRX")
    rf = irx.Close.reindex(ief.index).ffill() / 100
    out = []
    for b in tr[(tr.leg == "B") & tr.exit_reason.astype(str).str.startswith("disaster")].head(n).itertuples():
        t0, t1 = pd.Timestamp(str(b.entry_date)), pd.Timestamp(str(b.exit_date))
        P0 = zn.Close.loc[t0]; P = P0; beta = b.model_beta
        stop = P0 - B_STOP * b.netliq_at_entry / (b.contracts * ZN_MULT)
        steps, hit = [], None
        for u, v in zip(ief.loc[t0:t1].index[:-1], ief.loc[t0:t1].index[1:]):
            lo = min(zn.Low.loc[v], zn.Close.loc[v]) / zn.Close.loc[u] - 1
            worst = P * (1 + lo)
            ret = (ief["Adj Close"].loc[v] / ief["Adj Close"].loc[u] - 1 - rf.loc[u] * (v - u).days / 360) / beta
            steps.append(f"{v:%m-%d}: ZN=F low {zn.Low.loc[v]:.3f} vs prior close {zn.Close.loc[u]:.3f} ({100*lo:+.2f}%) "
                         f"-> worst model price {P:.3f} x (1{lo:+.4f}) = {worst:.3f}")
            if worst <= stop:
                hit = v; break
            P *= 1 + ret
            steps.append(f"close (IEF {100*(ief['Adj Close'].loc[v]/ief['Adj Close'].loc[u]-1):+.3f}% - bills, / {beta:.3f}) = {P:.3f}")
        px = stop - STOP_EXTRA_SLIP_TICKS * ZN_TICK
        gross = b.contracts * ZN_MULT * (px - P0)
        cost = b.contracts * 2 * (COSTS["base"]["zn_fee"] + COSTS["base"]["zn_slip_ticks"] * ZN_TICK * ZN_MULT)
        out.append(dict(trade=f"B {t0.date()} -> stopped {hit.date() if hit is not None else '?'}: long {b.contracts} ZN",
                        hand=f"entry ZN=F close {P0:.4f}, net liq ${b.netliq_at_entry:,.0f}; stop level = {P0:.4f} - 4% x "
                             f"{b.netliq_at_entry:,.0f} / ({b.contracts} x 1000) = {stop:.4f}; " + "; ".join(steps) +
                             f"; fill {stop:.4f} - 1/64 = {px:.4f}; gross {b.contracts} x 1000 x ({px:.4f} - {P0:.4f}) = "
                             f"${gross:,.2f}; costs ${cost:,.2f}; net ${gross - cost:,.2f} = {100*(gross-cost)/b.netliq_at_entry:.2f}% of equity",
                        sim=f"exit {b.exit_date}, net ${b.net_pnl:,.2f} ({b.net_pct_account:.2f}%)",
                        match=hit is not None and str(hit.date()) == str(b.exit_date) and abs(gross - cost - b.net_pnl) < 0.05))
    return pd.DataFrame(out)


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
    for name, kw in VARIANTS:
        if name == HEADLINE:
            store[name] = store[(START_EQUITY, "base")]
        else:
            store[name] = run(d, START_EQUITY, **kw)
        scen.append({"account": START_EQUITY, "case": name, **perf(*store[name], bench)})
    eq_r, tr_r = store[RESEARCH]
    bs = bench_stats(store[(START_EQUITY, "base")][0], bench)
    scen.append({"account": START_EQUITY, "case": "S&P 500 TR (SPY) benchmark", **bs})
    S = pd.DataFrame(scen)
    for k in ["A stops", "B stops"]:
        S[k] = [np.nan] * len(S)
    for i, (a, c) in enumerate(zip(S.account, S.case)):
        key = (a, c) if (a, c) in store else c
        if key in store:
            t = store[key][1]
            S.loc[i, "A stops"] = int(((t.leg == "A") & t.exit_reason.astype(str).str.startswith("disaster")).sum())
            S.loc[i, "B stops"] = int(((t.leg == "B") & t.exit_reason.astype(str).str.startswith("disaster")).sum())
    S.round(3).to_csv(os.path.join(OUT, "scenarios.csv"), index=False)

    eq, tr = store[(START_EQUITY, "base")]
    cols = ["leg", "entry_date", "entry_time", "exit_date", "exit_time", "direction", "contracts", "contract_notional",
            "position_notional", "lev_x_netliq", "entry_price", "exit_price", "gross_pnl", "commissions", "slippage",
            "net_pnl", "net_pct_account", "netliq_at_entry", "equity_after", "vol_tilt_size", "signal_gap_pct",
            "hedge_ratio", "model_beta", "capped", "exit_reason", "adverse_pct", "adverse_source", "close_pnl_pct_if_held"]
    T = tr[cols].sort_values(["exit_date", "leg"], na_position="last")
    T.round(4).to_csv(os.path.join(OUT, "trades.csv"), index=False)
    Y = yearly(eq, tr, bench)
    Y.round(2).to_csv(os.path.join(OUT, "yearly.csv"))
    eq.round(2).to_csv(os.path.join(OUT, "equity_daily.csv"))
    charts(eq, store[(START_EQUITY, "stress")][0], bench, eq_r)

    # holdout split stats for the $100k headline (bot-faithful, base costs)
    split = []
    for lbl, (a, b) in {"2003-2014": ("2003", "2014"), "2015-2022": ("2015", "2022"), "2023+ holdout": ("2023", None),
                        "2003-2022 in-sample": ("2003", "2022")}.items():
        e = eq.loc[a:b].copy(); e.attrs["e0"] = eq.netliq.shift(1).loc[a:b].iloc[0] if a != "2003" else START_EQUITY
        t = tr[(pd.to_datetime(tr.exit_date.astype(str), errors="coerce") >= pd.Timestamp(a)) &
               (pd.to_datetime(tr.exit_date.astype(str), errors="coerce") <= pd.Timestamp(f"{b or 2100}-12-31"))]
        p = perf(e, t, bench)
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
    info["beta_trades"] = tr_r[tr_r.leg == "B"].hedge_ratio.describe().to_dict()
    info["model_beta"] = tr[tr.leg == "B"].model_beta.describe().to_dict()
    info["n_rebalances"] = int((eq.rebalance_cost > 0).sum())
    info["cash_wt"] = (eq.cash / eq.netliq).describe().to_dict()
    # leg-B model vs Yahoo ZN=F over the same holds (research-settings run: plain 16:00 -> 16:00 holds, no stops)
    b = tr_r[(tr_r.leg == "B") & (tr_r.contracts > 0)].copy()
    zc = d.zn_close
    b["zn_f_ret"] = [zc.loc[str(x)] / zc.loc[str(y)] - 1 for x, y in zip(b.exit_date, b.entry_date)]
    b["model_ret"] = b.exit_price / b.entry_price - 1
    b["zn_f_pnl"] = b.contracts * ZN_MULT * b.entry_price * b.zn_f_ret - b.commissions - b.slippage
    info["b_check"] = {"corr": np.corrcoef(b.zn_f_ret, b.model_ret)[0, 1],
                       "model_mean_bp": 1e4 * b.model_ret.mean(), "znf_mean_bp": 1e4 * b.zn_f_ret.mean(),
                       "model_win": 100 * (b.net_pnl > 0).mean(), "znf_win": 100 * (b.zn_f_pnl > 0).mean(),
                       "model_pnl": b.net_pnl.sum(), "znf_pnl": b.zn_f_pnl.sum(),
                       "te_bp": 1e4 * (b.zn_f_ret - b.model_ret).std(), "n": len(b)}
    # contract counts a fresh account gets at the latest prices (bot sizing, default caps)
    last = d.iloc[-1]
    rows = []
    for e0 in ACCOUNT_SIZES:
        af = size(e0, last.gspc_close, MES_MULT, LEV_A_BOT, A_MAX_LEV, A_CAPS["default"], HARD_MAX_LEVERAGE, False)
        ah = size(e0, last.gspc_close, MES_MULT, LEV_A_BOT * 0.5, A_MAX_LEV, A_CAPS["default"], HARD_MAX_LEVERAGE, False)
        bn = size(e0, last.zn_close, ZN_MULT, LEV_B_BOT * HEDGE_BOT, B_MAX_LEV, B_CAPS["default"], HARD_MAX_BOND_LEVERAGE, False)
        rows.append({"account $": e0, "SPX": last.gspc_close, "ZN": last.zn_close,
                     "A full (2.8x)": af, "A full lev x": af * MES_MULT * last.gspc_close / e0,
                     "A half (1.4x)": ah, "A half lev x": ah * MES_MULT * last.gspc_close / e0,
                     "B ZN contracts": bn, "B ZN notional x": bn * ZN_MULT * last.zn_close / e0,
                     "min acct for 1 ZN $": ZN_MULT * last.zn_close / (LEV_B_BOT * HEDGE_BOT)})
    info["counts_now"] = pd.DataFrame(rows)
    info["cap_bind"] = (A_CAPS["default"] * MES_MULT * last.gspc_close / LEV_A_BOT,
                        B_CAPS["default"] * ZN_MULT * last.zn_close / (LEV_B_BOT * HEDGE_BOT))
    print(info["b_check"]); print(info["counts_now"].round(2).to_string()); print(info["n_rebalances"], info["cash_wt"])
    s25 = store[(25_000, "base")][1]
    sk = s25[(s25.leg == "B") & (s25.contracts == 0)]
    info["skip25"] = (str(sk.entry_date.min()), str(sk.entry_date.max()), len(sk))
    print(info["skip25"])
    info["spot"] = spot_check_stops(tr)
    print(info["spot"].to_string())
    write_summary(d, info, S, Y, SP, eq, tr, store)


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


def write_summary(d, info, S, Y, SP, eq, tr, store):
    f = lambda v, n=1: "n/a" if v is None or (isinstance(v, float) and not np.isfinite(v)) else f"{v:,.{n}f}"
    get = lambda acct, case, k: S[(S.account == acct) & (S.case == case)][k].iloc[0]
    base = lambda k: get(START_EQUITY, "base", k)
    v = lambda case, k: get(START_EQUITY, case, k)
    keys = ["CAGR %", "vol %", "Sharpe", "Sortino", "max DD %", "longest DD days", "beta", "alpha %/yr", "alpha t",
            "A win %", "B win %", "A avg % acct", "A median % acct", "B avg % acct", "B median % acct",
            "A profit factor", "B profit factor", "A best % acct", "A worst % acct", "B best % acct", "B worst % acct",
            "A avg lev x", "B avg lev x", "B skipped", "A stops", "B stops", "costs $", "final equity $"]
    H = S.copy()
    H["scenario"] = [f"${a/1e3:,.0f}k {c}" for a, c in zip(H.account, H.case)]
    H = H.set_index("scenario")[keys].T
    H.insert(0, "metric", H.index)
    main_cols = ["metric"] + [c for c in H.columns if c.endswith(("base", "stress"))] + \
                [c for c in H.columns if "benchmark" in c]
    other_cols = ["metric"] + [c for c in H.columns if c not in main_cols]
    bc = info["b_check"]; bt = info["beta_trades"]; cw = info["cash_wt"]; mbeta = info["model_beta"]
    y_tab = Y.reset_index(); y_tab["year"] = y_tab["year"].astype(str)
    split = SP.copy()
    counts = info["counts_now"].copy()
    for c_ in ["A full (2.8x)", "A half (1.4x)", "B ZN contracts"]:
        counts[c_] = counts[c_].astype(int).astype(str)
    research_eq = v("research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "CAGR %")
    frac0 = v("research settings, fractional contracts, zero costs", "CAGR %")
    fracb = v("research settings, fractional contracts, base costs", "CAGR %")
    never = S[S.case.str.contains("never rebalance")].iloc[0]
    bench = S[S.case.str.contains("benchmark")].iloc[0]
    sk = info["skip25"]
    yrs_cost = 100 * (eq.costs / eq.netliq.shift(1).fillna(START_EQUITY)).groupby(eq.index.year).sum()
    cols5 = [HEADLINE, HARD, RESEARCH, ZNF, NOCAP]

    def row(label, fn):
        return f"| {label} | " + " | ".join(("**" + fn(c) + "**") if c == HEADLINE else fn(c) for c in cols5) + " |"
    pct = lambda k, n=1: (lambda c: f"{f(v(c, k), n)}%")
    num = lambda k, n=2: (lambda c: f(v(c, k), n))
    head_rows = "\n".join([
        row("CAGR", pct("CAGR %")) + f" {f(bench['CAGR %'])}% |",
        row("Final equity", lambda c: f"${v(c, 'final equity $'):,.0f}") + f" ${bench['final equity $']:,.0f} |",
        row("Volatility", pct("vol %")) + f" {f(bench['vol %'])}% |",
        row("Sharpe (excess of T-bills)", num("Sharpe")) + f" {f(bench['Sharpe'], 2)} |",
        row("Sortino", num("Sortino")) + f" {f(bench['Sortino'], 2)} |",
        row("Max drawdown", pct("max DD %")) + f" {f(bench['max DD %'])}% |",
        row("Longest drawdown (calendar days)", lambda c: f"{v(c, 'longest DD days'):.0f}") + f" {bench['longest DD days']:.0f} |",
        row("Beta / alpha vs S&P (%/yr, t)", lambda c: f"{f(v(c, 'beta'), 2)} / {f(v(c, 'alpha %/yr'))} (t={f(v(c, 'alpha t'))})") + " 1.00 / 0 |",
        row("Leg A trades, win rate", lambda c: f"{v(c, 'A trades'):.0f}, {f(v(c, 'A win %'))}%") + " |",
        row("Leg A avg / median net per trade (% of acct)", lambda c: f"{f(v(c, 'A avg % acct'), 2)}% / {f(v(c, 'A median % acct'), 2)}%") + " |",
        row("Leg B trades, win rate", lambda c: f"{v(c, 'B trades'):.0f}, {f(v(c, 'B win %'))}%") + " |",
        row("Leg B avg / median net per trade (% of acct)", lambda c: f"{f(v(c, 'B avg % acct'), 2)}% / {f(v(c, 'B median % acct'), 2)}%") + " |",
        row("Profit factor A / B", lambda c: f"{f(v(c, 'A profit factor'), 2)} / {f(v(c, 'B profit factor'), 2)}") + " |",
        row("Worst trade A / B (% of acct)", lambda c: f"{f(v(c, 'A worst % acct'), 2)}% / {f(v(c, 'B worst % acct'), 2)}%") + " |",
        row("Avg leverage A / B (notional / net liq)", lambda c: f"{f(v(c, 'A avg lev x'), 2)}x / {f(v(c, 'B avg lev x'), 2)}x") + " |",
        row("Disaster stops fired A / B", lambda c: f"{v(c, 'A stops'):.0f} / {v(c, 'B stops'):.0f}") + " |",
        row("Trading costs", lambda c: f"${v(c, 'costs $'):,.0f}") + " |",
    ])
    # stops
    nostop = store["  + default caps (40 MES / 20 ZN, 3x / 4.5x / 7x)"][1]
    ns = nostop.set_index(["leg", nostop.entry_date.astype(str)])
    st = tr[tr.exit_reason.astype(str).str.startswith("disaster")]
    stop_lines = []
    for x in st.itertuples():
        held = ns.loc[(x.leg, str(x.entry_date))]
        stop_lines.append(f"| {x.leg} | {x.entry_date} | {x.exit_date} | {x.contracts} | {x.net_pct_account:.2f}% | "
                          f"{held.net_pct_account:.2f}% ({held.exit_date}) |")
    stop_tab = "\n".join(stop_lines) if stop_lines else "| - | none | | | | |"
    a_tr = tr[(tr.leg == "A") & (tr.contracts > 0)].copy()
    a_tr["stop_use"] = a_tr.adverse_pct * a_tr.lev_x_netliq / (100 * A_STOP)       # 1.0 = stop level
    near = a_tr[a_tr.stop_use >= 0.7].sort_values("stop_use", ascending=False)
    near_txt = "; ".join(f"{x.entry_date} {x.direction} {x.lev_x_netliq:.2f}x: adverse {x.adverse_pct:.2f}% "
                         f"({x.stop_use:.0%} of the stop distance, {x.adverse_source})" for x in near.itertuples()) or "none"
    raw = store["bot-faithful, A stop on raw SPY highs/lows"][1]
    raw_st = raw[(raw.leg == "A") & raw.exit_reason.astype(str).str.startswith("disaster")]
    raw_txt = ", ".join(f"{x.entry_date} ({x.net_pct_account:.2f}%, {x.close_pnl_pct_if_held:+.2f}% if held)" for x in raw_st.itertuples()) or "none"
    src_counts = a_tr.adverse_source.value_counts().to_dict()
    n_early = int(tr[(tr.leg == "B")].entry_time.eq("12:50").sum()), int(tr[tr.leg == "B"].exit_time.eq("12:50").sum())
    n_block = int(tr.direction.astype(str).str.startswith("blocked").sum())
    capA = tr[(tr.leg == "A") & (tr.capped == True)]; capB = tr[(tr.leg == "B") & (tr.capped == True)]   # noqa: E712
    bind_a_first = f"{capA.entry_date.min()} ({len(capA)} A trades capped)" if len(capA) else "never (A)"
    bind_b_first = f"{capB.entry_date.min()} ({len(capB)} B trades capped)" if len(capB) else "never (B)"
    spot = "\n".join(f"- **{r.trade}** (by hand from the raw Yahoo files): {r.hand}. Simulation: {r.sim}. "
                     f"{'Matches.' if r.match else '**MISMATCH**'}" for r in info["spot"].itertuples())
    bridge = [("Research run (BOT_FAITHFUL=False): no caps or stops, 2.85x / 2.81x x trailing beta, 16:00 closes", RESEARCH),
              ("+ bot leverages: A 2.8x (vol tilt 0.5), B 2.8 x fixed 1.14", "research + bot leverages (2.8 / 2.8 x 1.14)"),
              ("+ bot leg-B times (12:50 days at the previous close)", "  + bot leg-B times"),
              ("+ default caps: 40 MES / 20 ZN (today's $), 3x A / 4.5x B / 7x total", "  + default caps (40 MES / 20 ZN, 3x / 4.5x / 7x)"),
              ("+ disaster stops (6% A / 4% B) = **bot-faithful headline**", HEADLINE)]
    bridge_tab = "\n".join(f"| {lab} | {f(v(c, 'CAGR %'), 2)}% | {f(v(c, 'Sharpe'), 2)} | {f(v(c, 'max DD %'))}% | "
                           f"{v(c, 'A stops'):.0f} / {v(c, 'B stops'):.0f} |" for lab, c in bridge)
    sens = [("Caps as literal contract counts (40 MES / 20 ZN at every date)", "bot-faithful, caps as literal contract counts"),
            ("A stop on raw SPY highs/lows (no ^GSPC, no bad-print filter)", "bot-faithful, A stop on raw SPY highs/lows"),
            ("ZN=F pricing and hard caps", "bot-faithful, ZN=F pricing and hard caps"),
            ("Never rebalance the core", "bot-faithful, never rebalance core")]
    sens_tab = "\n".join(f"| {lab} | {f(v(c, 'CAGR %'), 2)}% | {f(v(c, 'Sharpe'), 2)} | {f(v(c, 'max DD %'))}% | "
                         f"{v(c, 'A stops'):.0f} / {v(c, 'B stops'):.0f} |" for lab, c in sens)
    md = f"""# Month-end trader: full-timeline IBKR backtest (SPY core + MES leg A + ZN leg B)

Generated by `simulate.py` (re-run it to regenerate every file here). Period **{eq.index[0].date()} to {eq.index[-1].date()}**
(the last complete month; November 2026 is excluded because its month-end has not happened). Start equity
**${START_EQUITY:,.0f}** unless stated. All numbers are a simulation on daily Yahoo data, not live fills.

**The headline is the bot-faithful run** (`BOT_FAITHFUL = True` at the top of `simulate.py`): the live bot's default
leverages, contract and leverage caps, disaster stops and leg-B trade times (`../met`, `config.example.yaml`). This
replaces the earlier headline (24.3% CAGR), which had no caps or stops and research leverages; that run is kept as the
"no caps (research)" column and is reproduced exactly with `BOT_FAITHFUL = False`.

Files: `trades.csv` (one row per trade, $100k bot-faithful base case; `exit_reason` marks disaster stops),
`yearly.csv`, `periods.csv`, `scenarios.csv` (all account sizes, cost cases and variants), `equity_daily.csv`,
`equity.png`, `legs_contribution.png`.

## Headline ($100k, base costs)

| | Bot-faithful (default caps, stops, bot leverages and times) | Hard caps (200 MES / 50 ZN) | No caps (research run) | Leg B on ZN=F prices (bot-faithful) | No contract caps (else bot-faithful) | S&P 500 TR (SPY) |
|---|---|---|---|---|---|---|
{head_rows}

Columns: *hard caps* = bot-faithful with `guardrails.max_contracts` / `bond.max_contracts` raised to the code's hard
limits (200 MES, 50 ZN); *no caps (research)* = the previous headline (no caps, no stops, 2.85x / 2.81x x trailing beta,
16:00 closes); *ZN=F* = bot-faithful with leg B priced on Yahoo ZN=F closes (roll days neutralised) instead of IEF minus
bills; *no contract caps* = bot-faithful with the 3x / 4.5x / 7x leverage caps but no contract caps.
Stress costs (2 MES ticks, 1 ZN tick per side), bot-faithful $100k: CAGR **{f(get(START_EQUITY, "stress", "CAGR %"))}%**,
Sharpe {f(get(START_EQUITY, "stress", "Sharpe"), 2)}, max DD {f(get(START_EQUITY, "stress", "max DD %"))}%.

### From the research run to the bot ($100k, base costs)

| Step | CAGR | Sharpe | Max DD | Stops A / B |
|---|---|---|---|---|
{bridge_tab}

Sensitivities around the headline:

| Variant | CAGR | Sharpe | Max DD | Stops A / B |
|---|---|---|---|---|
{sens_tab}

![equity](equity.png)

![legs](legs_contribution.png)

## What "bot-faithful" means here

- **Leverages** (`config.example.yaml`): leg A `strategy.leverage` {LEV_A_BOT} at full size, x{_SC.low_vol_scale} when SPY's 20-day vol
  (T-1) is at or below {VOL_THRESHOLD_BOT} (the bot's fixed copy of the 2002-08..2014 median; recomputed here:
  {info["vol_median"]:.6f}). Leg B `bond.leverage` {LEV_B_BOT} x the fixed hedge ratio {HEDGE_BOT} = ZN notional
  {LEV_B_BOT * HEDGE_BOT:.2f}x net liq (the research run used 2.81 x the trailing beta, mean {bt["mean"]:.2f}).
  Leg B's *P&L* is still modelled with the trailing beta (IEF excess return / beta, mean {mbeta["mean"]:.2f} at the trades),
  so the fixed 1.14 changes only the size.
- **Caps**, with the bot's own sizing function (`met.guardrails.size_contracts`, imported): A <= {A_CAPS["default"]} MES and <=
  {A_MAX_LEV}x net liq; B <= {B_CAPS["default"]} ZN and <= {B_MAX_LEV}x; A is skipped if A + open B notional > {TOTAL_MAX_LEV}x
  (`guardrails.max_total_leverage`; it never bound: {n_block} months blocked). **Contract caps are applied in today's
  dollars** (`CAP_MODE = "today_usd"`): N contracts at today's prices (S&P {d.gspc_close.iloc[-1]:,.0f}, ZN
  {d.zn_close.iloc[-1]:.2f}), converted to contracts at each date's price, so a 2003 account is not capped at 40 x $4.5k
  MES. At today's prices the default caps bind above about **${info["cap_bind"][0]:,.0f}** of equity (A, full size) and
  **${info["cap_bind"][1]:,.0f}** (B). In this run they first bound on {bind_a_first} and {bind_b_first}. Literal
  contract counts at every date are a sensitivity above ({f(v("bot-faithful, caps as literal contract counts", "CAGR %"))}% CAGR).
- **Disaster stops.** Leg A flattens when the MES overlay is down {A_STOP:.0%} of the morning's net liq
  (`guardrails.disaster_stop_pct_equity`, checked every 15 s); leg B when the ZN overlay is down {B_STOP:.0%} of the entry net liq
  (`bond.disaster_stop_pct_equity`, whenever CBOT trades during the hold). Approximation at daily resolution: A's worst
  intraday move from the 09:30 entry = ^GSPC low (long) / high (short) vs the entry level; **sanity filter:** an
  ^GSPC extreme equal to the index's own opening print is ignored (the official open is computed from stale component
  prices: 2008-09-30, 2011-11-30, 2012-06-29 and many 2002-2013 days), then SPY's extreme vs its open is used, unless it
  lies beyond the prior close against an opening gap above {GAP_FILTER:.0%} (the bad-print pattern on 2008-09-30, SPY low
  110.53 vs open 113.51 on a +1.9% gap day that closed at 115.99, and 2011-11-30, low 120.00 vs open 123.49, close 124.99);
  if both are rejected only the close counts. Sources used over the {len(a_tr)} A trades: {src_counts}.
  B's worst price each hold day = the model's prior close x (ZN=F low / ZN=F prior close), and the close itself;
  ZN=F lows above the close or more than {ZN_SPIKE_MAX:.0%} below both closes are treated as bad prints ({info["zn_low_bad"]} days in
  the whole file). The entry day (B enters 2 minutes before the close) and 12:50 exit days are not checked. **A stop fills
  at the stop level plus {STOP_EXTRA_SLIP_TICKS:.0f} extra tick**, plus the normal exit slippage and fees.
- **Leg-B times.** The bot enters at 15:58 and exits at 15:55 ET; the daily close stands in for both. On 12:50 ET days
  (NYSE half days and SIFMA early closes: 31 Dec, the sessions before Good Friday and Memorial Day; `met.bond.rates_early`)
  the trade price is approximated by the **previous session's close**: an early entry earns that day's move (sized on the
  previous close's net liq), an early exit forgoes it. {n_early[0]} entries and {n_early[1]} exits fell on such days.

## Disaster stops in the headline run

**{len(st[st.leg == "A"])} leg-A and {len(st[st.leg == "B"])} leg-B stops fired.** Each stopped trade lost the stop amount plus costs
(~{B_STOP:.0%} / {A_STOP:.0%} of equity); the last column is what the same trade returned in the run without stops (held to the
normal exit):

| Leg | Entry | Stopped | Contracts | Net (% of acct) | Without the stop (exit) |
|---|---|---|---|---|---|
{stop_tab}

Leg A: no stop fired. Closest calls (>= 70% of the way to the stop): {near_txt}. With raw SPY highs/lows instead
(no ^GSPC, no filter) the A stops would have been: {raw_txt} (CAGR {f(v("bot-faithful, A stop on raw SPY highs/lows", "CAGR %"))}%).

### Spot checks (stop trades recomputed by hand)

{spot}

## Strategy as simulated

- **Core:** 100% of net liquidation in SPY, dividends reinvested on the ex-date (DRIP, modelled with Yahoo adjusted closes).
  Futures P&L settles to cash. **Rebalancing choice: drift band.** When the SPY core drifts more than
  {CORE_BAND:.0%} from 100% of net liq (i.e. accumulated overlay cash > +/-{CORE_BAND:.0%}), SPY is bought/sold at the close
  back to 100%. This happened **{info["n_rebalances"]} times** in {len(Y)} years. Cash weight averaged {100*cw["mean"]:.1f}%
  (range {100*cw["min"]:.1f}% to {100*cw["max"]:.1f}%). "Never rebalance" is a sensitivity above
  (beta {never["beta"]:.2f}, CAGR {f(never["CAGR %"])}%).
- **Leg A (013 + 168 tilt):** on the last NYSE session of each month, direction = -sign(SPY MTD - TLT MTD), MTD from the prior
  month's last adjusted close to the T-1 adjusted close. Size = the bot's sizing of {LEV_A_BOT} x tilt x net liq(T-1) at the
  S&P 500 close(T-1), floor, capped as above. Entry 09:30, exit at the close (16:00, or 13:00 on half days per the XNYS
  calendar) unless stopped.
  **Prices:** exit = ^GSPC close; entry = ^GSPC close / (1 + SPY open-to-close return). Yahoo's ^GSPC *opens* are stale (equal
  to the prior close) on most days in 2002-2007 and many in 2011-2013, and the index's own opening print is computed from
  stale component prices anyway, so SPY's 09:30 print is the better proxy for where MES trades at the open. Futures basis
  is assumed constant intraday. MES only exists since May 2019; earlier trades are hypothetical MES at $5 x index.
- **Leg B (276):** long ZN from the 4th-last NYSE session to the last session (3 overnight holds), times as above.
  Contracts = floor({LEV_B_BOT} x {HEDGE_BOT} x net liq / (1000 x ZN=F close)), capped. Per contract the price path is modelled as
  ZN_t = ZN_(t-1) x (1 + (IEF total return_t - T-bill accrual_t) / trailing beta): futures earn the excess return. T-bill
  accrual = ^IRX x calendar days / 360. P&L is marked daily (variation margin to cash).
- **Trailing beta** = beta of IEF daily returns on ZN=F daily returns, trailing {BETA_WIN} sessions (min {BETA_MIN}), excluding
  Yahoo's continuous-contract roll windows (day 15-25 of Mar/Jun/Sep/Dec, {info["n_roll_excluded"]} days). Full-sample
  estimate: **{info["beta_full"]:.3f}** (corr {info["beta_corr"]:.3f}; weekly returns {info["beta_weekly"]:.3f}).
- Both legs overlap on the last session of the month, as intended (unless B was stopped earlier or exits at 12:50).

## Costs (IBKR Pro tiered, US)

| Item | Base | Stress |
|---|---|---|
| MES all-in fees (IBKR commission ~$0.25 + CME exchange ~$0.35 + NFA/regulatory ~$0.02) | $0.62 / contract / side | same |
| MES slippage (market orders at open and close) | 1 tick = $1.25 / contract / side | 2 ticks = $2.50 |
| ZN all-in fees (IBKR ~$0.85 + CBOT exchange ~$0.80 + NFA/clearing ~$0.05; my estimate) | $1.70 / contract / side | same |
| ZN slippage | half a tick (tick = 1/64 pt = $15.625) = $7.81 / contract / side | 1 tick = $15.63 |
| Disaster-stop fills | stop level + {STOP_EXTRA_SLIP_TICKS:.0f} tick, then the normal slippage | same |
| SPY rebalancing | $0.0035/share (min $0.35, max 1% of value) + $0.0005/share fees + $0.005/share spread | same |
| Cash | positive cash earns max(^IRX - 0.5%, 0); negative cash (after funding ZN overnight margin of {ZN_OVERNIGHT_MARGIN:.0%} of notional) pays ^IRX + 1.5% | same |

No financing on futures notional (futures P&L is an excess return). MES is never held overnight. Per-year costs as a % of
equity: ~{yrs_cost.loc[2024:].mean():.2f}% recently, max {yrs_cost.max():.1f}% in {yrs_cost.idxmax()}.

## All account sizes and cost cases (bot-faithful)

{md_table(H[main_cols].reset_index(drop=True))}

All $100k variants:

{md_table(H[other_cols].reset_index(drop=True))}

**Rounding and caps.** Accounts compound, so whole-contract rounding only bites while the account is small, and the
caps only once it is large. The $25k account could not trade leg B (one ZN needs ~{LEV_B_BOT * HEDGE_BOT:.2f}x net liq of
notional) in **{sk[2]} months**, on and off between {sk[0]} and {sk[1]}; those are logged as "skipped". Contract counts a
*fresh* account gets **today** (S&P {counts["SPX"].iloc[0]:,.0f}, ZN {counts["ZN"].iloc[0]:.2f}, bot sizing, default caps):

{md_table(counts.drop(columns=["SPX", "ZN"]))}

## Reconciliation with research 350

350 reports CAGR 26.2% (SPY + A+B, 2003-01 to 2026-11-20, 0.5bp/1bp per side costs, A 62.2% / B 62.2% win).

| Step ($100k) | CAGR % |
|---|---|
| Research-equivalent here (fractional, zero costs, B = IEF *total* return, core rebalanced daily) | {f(research_eq, 2)} |
| ... minus 350's 0.5bp (A) / 1bp (B) per-side costs (~0.9 pp/yr) | ~{f(research_eq - 0.9)} (350: 26.2; the rest is 350's window running to 2026-11-20) |
| B earns IEF *minus T-bills* (correct for futures), core on a 10% band, cash interest, fractional, no costs | {f(frac0, 2)} |
| + IBKR fees and slippage (base) | {f(fracb, 2)} |
| + whole contracts = the research run (previous headline) | {f(v(RESEARCH, "CAGR %"), 2)} |
| + bot leverages, leg-B times, caps, stops = **bot-faithful headline** (bridge above) | **{f(base("CAGR %"), 2)}** |
| + stress slippage | {f(get(START_EQUITY, "stress", "CAGR %"), 2)} |

The two legs reproduce 350 (A win {f(v("research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "A win %"))}%,
B win {f(v("research-equivalent (fractional, no costs, B=IEF TR, daily core rebal)", "B win %"))}% before costs).
**350 credits leg B with IEF's total return**, but with SPY already 100% invested the bond exposure has to come from
futures, which earn only the excess over bills (~{f(research_eq - frac0, 1)} pp/yr here).

## Leg B model check against actual ZN=F prices

Over the {bc["n"]} B holds of the research run (plain close-to-close holds, no stops), the IEF-minus-bills model and
Yahoo's ZN=F closes agree in sign on most trades (return correlation {bc["corr"]:.2f}, tracking error {bc["te_bp"]:.0f}bp per
trade) but **ZN=F made less**: mean {bc["znf_mean_bp"]:.1f}bp per trade vs {bc["model_mean_bp"]:.1f}bp modelled. Pricing leg B on
ZN=F in the bot-faithful run gives CAGR **{f(v(ZNF, "CAGR %"), 2)}%** instead of {f(base("CAGR %"), 2)}%
({f(v(ZNF, "CAGR %") - base("CAGR %"), 2)} pp). Yahoo roll days were detected and neutralised ({info["zn_roll_days"]} found,
{info["zn_roll_in_hold"]} inside a hold window). Causes: ZN=F's close is the ~15:00 ET settlement, an hour before IEF's
close (the bot's 15:55 exit is in between), IEF's cash basket differs from the futures CTD, and Yahoo's continuous ZN=F
holds the expiring contract through first-notice day in Feb/May/Aug/Nov (the bot holds the next quarterly). **Treat leg
B's true P&L as somewhere between the two.**

## Yearly ($100k, bot-faithful, base costs)

Costs = futures commissions + slippage + SPY rebalancing. Max DD is within the calendar year.

{md_table(y_tab)}

## In-sample vs holdout (bot-faithful)

{md_table(split)}

The sizing rule, leverages and the vol median come from 2003-2014 data and the legs were selected on 2003-2022, so
**only 2023+ is out of sample** (about four years, 46 trades per leg).

## Caveats

- **In-sample.** 2003-2022 was used to discover and select both signals, the vol tilt and the leverage. Expect live
  results below the backtest.
- **Caps in today's dollars** are a modelling choice: they answer "what would the bot as configured today have done".
  The bot's caps are contract counts, so a live account that grows past ~${info["cap_bind"][0]/1e3:,.0f}k stops compounding
  leg A unless `guardrails.max_contracts` is raised (hard limit 200; 50 ZN for leg B).
- **Stops at daily resolution.** The stop is approximated from daily highs/lows; the real check is a 15 s (A) / 30 s (B)
  poll on the futures price, fills can be worse in a fast market, and a ZN=F daily bar runs 18:00-17:00 ET, so it
  includes the hour after the bot's 15:55 exit. ^GSPC/SPY lows are a proxy for MES (futures basis, overnight session not
  relevant because A is intraday). The A-stop result hinges on two Yahoo prints judged bad (2008-09-30, 2011-11-30); with
  raw SPY extremes the CAGR is {f(v("bot-faithful, A stop on raw SPY highs/lows", "CAGR %"))}%.
- **12:50 days** use the previous session's close: a daily-resolution approximation of a 12:50 trade.
- **Leverage and gap risk.** Gross exposure on the last day of the month is up to SPY 1x + MES {LEV_A_BOT}x + ZN
  {LEV_B_BOT * HEDGE_BOT:.2f}x. The worst single A day here cost {f(-base("A worst % acct"), 1)}% of the account and the worst B
  hold {f(-base("B worst % acct"), 1)}%. A gap through the stop (e.g. a halt) is not modelled.
- **Fills.** Open/close fills are modelled from daily bars plus fixed tick slippage; month-end closes are crowded.
- **Data limits.** Yahoo daily data; ^IRX as the bills/benchmark rate; ZN=F is a non-back-adjusted continuous contract.
  MES did not exist before 2019. No survivorship issue (index products).
- **Margin.** MES intraday margin and ZN overnight margin (~2% of notional) are assumed always available against a 100% SPY
  core. A margin call or IBKR raising margins in a crisis could force a smaller size.
- **Not modelled:** the SPRT decay monitors (`auto_derisk`, can halve or stop either leg), signal-source disagreement
  skips, missed trades from outages. Taxes ignored.
"""
    open(os.path.join(OUT, "summary.md"), "w").write(md)


if __name__ == "__main__":
    main()
