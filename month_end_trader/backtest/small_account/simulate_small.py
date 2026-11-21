"""Small-account (~$18k) variant of ../simulate.py: leg B in CME Micro Ultra 10-Year T-Note futures (MTN).

Reuses ../simulate.py (imported, not edited) for data loading, the leg-A signal, the SPY core, cash interest,
the stats and the yearly table, and ../../met (imported, not edited) for the live bot's MTN contract choice
(met.bond.contract_month_for / last_safe_day), its sizing function (met.guardrails.size_contracts) and BOND_SPECS.
What is new here:
  * Leg B = LONG MTN ($100 x price, cash settled, quarterly) from the close of the 4th-last session to the
    month-end close, in the contract the bot would pick: the first quarterly contract whose last trade day is more
    than 5 sessions after the month-end exit date (so Feb/May/Aug/Nov windows hold the contract after next).
    Contracts = floor(2.8 x 0.885 x net liq / (price x 100)), max 400, notional <= 4.5x net liq (bot defaults).
    P&L model "tn" (primary, 2016-01-11+): TN=F (Yahoo, Ultra 10-Year continuous front) daily close-to-close
    returns x MTN notional, Yahoo roll days neutralised (replaced by the IEF model's return for that day).
    P&L model "ief" (before 2016 and as a check): MTN notional x (IEF total return - T-bills) / 0.885.
  * Leg A rounding variants: "floor" (skip when 0 MES) or "nearest" (round half up, capped at MAX_LEV_A).
  * Margin per trade (MES and MTN % of notional) vs net liq, flagged above MARGIN_FLAG.
  * BOT_FAITHFUL=True (default): the live bot's defaults, as in ../simulate.py: A at 2.8x (vol tilt 0.5) capped at
    40 MES and 3x, B capped at bond.max_contracts = 200 MTN (default for MTN; hard limit 400), both caps in today's
    dollars (S.CAP_MODE), 7x total; disaster stops (A 6% of equity intraday from ^GSPC/SPY extremes, B 4% from
    ZN=F daily lows scaled to MTN by trailing beta / 0.885); leg B on 12:50 days at the previous session's close.
    BOT_FAITHFUL=False reproduces the earlier research run exactly (A 2.85x uncapped, B 400 MTN, no stops, 16:00).
Run:  python3 simulate_small.py      (writes trades_18k*.csv, yearly_18k.csv, periods_18k.csv, scenarios_small.csv,
                                      equity_18k.png, summary.md in this directory)
"""
import os, sys, math, datetime as dt, warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

sys.dont_write_bytecode = True          # do not leave __pycache__ in ../ or ../../met (imported modules live there)
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))                       # backtest/  -> simulate
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))      # month_end_trader/ -> met
import simulate as S          # noqa: E402  (the full-size simulation; imported only)
import lib                    # noqa: E402  (on sys.path via simulate.LIB_DIR)
from met.bond import contract_month_for, last_safe_day          # noqa: E402  (the live bot's contract choice)
from met.config import BOND_SPECS, BondConfig, HARD_MAX_BOND_LEVERAGE   # noqa: E402
from met.guardrails import size_contracts                       # noqa: E402  (the live bot's sizing)

# =============================================================== PARAMETERS
OUT = HERE
HEADLINE_EQUITY = 18_000.0
ACCOUNT_SIZES = [10_000, 18_000, 25_000, 50_000, 100_000]
BOT_FAITHFUL = True                      # see the docstring; False = the earlier research run
BOT_LEVERAGE = BOT_FAITHFUL              # A 2.8x (bot) vs 2.85x (research); leg B is 2.8 x 0.885 either way
BOT_TIMES = BOT_FAITHFUL                 # leg B at the previous close on 12:50 days
BOT_CAPS = "default" if BOT_FAITHFUL else None   # None: research (A uncapped, B 400 MTN literal) | "lev" | "default" | "hard"
BOT_STOPS = BOT_FAITHFUL
LEV_A = S.LEV_A_BOT if BOT_LEVERAGE else S.LEV_A_RESEARCH   # MES notional / net liq at full size
MAX_LEV_A = S.A_MAX_LEV                  # 3.0: "nearest" rounding (and the bot) may not exceed this MES notional / net liq
START = S.START

# Micro Ultra 10-Year T-Note futures, exactly as the bot uses them
SPEC = BOND_SPECS["MTN"]
MTN_MULT = SPEC["multiplier"]            # $100 x price
HEDGE = SPEC["hedge"]                    # 0.885 = beta of IEF daily returns on TN futures returns
_BC = BondConfig()
LEV_B = _BC.leverage                     # 2.8 IEF-equivalent notional / net liq (bot default; research 2.81)
B_MAX_LEV = _BC.max_leverage             # 4.5 futures notional / net liq ceiling
B_MAX_CONTRACTS = SPEC["max_contracts"]  # 400 = MTN hard limit (the research run's cap)
B_CAPS = {"default": SPEC["default_contracts"], "hard": SPEC["max_contracts"]}   # 200 / 400 MTN
ROLL_BUFFER = _BC.roll_buffer_days       # 5 sessions
MTN_TICK = 1 / 32                        # points; the bot's FakeBroker uses 1/32 for MTN = $3.125 per contract
MTN_LAUNCH = "2024-01-01"                # CME launched MTN in 2024; earlier trades are hypothetical
TN_START = "2016-01-11"                  # Ultra 10-Year (TN) futures launch; Yahoo's earlier TN=F rows are not TN
MES_LAUNCH = "2019-05-06"
B_MODEL = "auto"                         # "auto": TN=F where available, else IEF model; "ief": IEF model throughout
ROLL_SD = 3.0                            # Yahoo roll day = largest |TN - IEF/hedge| residual of the roll window > 3 sd

COSTS = {   # per contract per side
    #           MES all-in fee, MES slippage ticks ($1.25), MTN all-in fee, MTN slippage ticks of 1/32 ($3.125)
    "base":    {"mes_fee": 0.62, "mes_slip_ticks": 1.0, "mtn_fee": SPEC["cost"], "mtn_slip_ticks": 1.0},
    "stress":  {"mes_fee": 0.62, "mes_slip_ticks": 2.0, "mtn_fee": SPEC["cost"], "mtn_slip_ticks": 2.0},
    "halftick": {"mes_fee": 0.62, "mes_slip_ticks": 1.0, "mtn_fee": SPEC["cost"], "mtn_slip_ticks": 0.5},
    "zero":    {"mes_fee": 0.0,  "mes_slip_ticks": 0.0, "mtn_fee": 0.0,  "mtn_slip_ticks": 0.0},
}
# margin assumptions (initial, per contract, as % of notional)
MES_MARGIN_PCT = 0.075                   # ~$2,840 at S&P 7,580
MTN_MARGIN_PCT = 0.03                    # ~$330 per MTN at 108 (TN ~$3k-4k per $100k face)
MARGIN_STRESS_MULT = 2.0                 # crisis check: exchanges/IBKR double margins
MARGIN_FLAG = 0.60                       # flag trades where futures margin > 60% of net liq
SPY_MAINT = 0.25                         # Reg T maintenance on the SPY core (for the total-requirement check)
IBKR_MIN_EQUITY = 2_000.0

# Previous run of this script (leg B in the CME Micro 10-Year Yield future "10Y", ^TNX P&L, sized by IEF duration,
# fee $0.85 + 1 tick $1 per side), kept for comparison. Copied from its scenarios_small.csv / periods_18k.csv.
nan = float("nan")
PREV_10Y = {
    (10000, 'A floor'): {'CAGR %': 22.009, 'Sharpe': 1.01, 'max DD %': -41.548, 'alpha %/yr': 9.783, 'beta': 0.961, 'B win %': 58.392, 'B avg % acct': 0.321, 'B avg net $': 372.361, 'final equity $': 1143663.36},
    (10000, 'A nearest'): {'CAGR %': 22.043, 'Sharpe': 1.004, 'max DD %': -41.498, 'alpha %/yr': 9.789, 'beta': 0.966, 'B win %': 58.392, 'B avg % acct': 0.328, 'B avg net $': 375.507, 'final equity $': 1151210.365},
    (18000, 'A floor'): {'CAGR %': 22.08, 'Sharpe': 1.005, 'max DD %': -43.174, 'alpha %/yr': 9.826, 'beta': 0.966, 'A win %': 60.839, 'B win %': 58.392, 'A avg % acct': 0.49, 'B avg % acct': 0.336, 'B avg net $': 690.991, 'B avg contracts': 69.577, 'B skipped': 0, 'A skipped': 0, 'vol %': 20.163, 'final equity $': 2087063.534},
    (18000, 'A nearest'): {'CAGR %': 22.143, 'Sharpe': 1.004, 'max DD %': -42.513, 'alpha %/yr': 9.87, 'beta': 0.968, 'A win %': 60.839, 'B win %': 58.392, 'A avg % acct': 0.503, 'B avg % acct': 0.335, 'B avg net $': 695.461, 'final equity $': 2113036.749},
    (25000, 'A floor'): {'CAGR %': 22.116, 'Sharpe': 1.004, 'max DD %': -42.812, 'alpha %/yr': 9.852, 'beta': 0.967, 'B win %': 58.392, 'B avg % acct': 0.339, 'final equity $': 2919010.229},
    (25000, 'A nearest'): {'CAGR %': 22.199, 'Sharpe': 1.006, 'max DD %': -42.474, 'alpha %/yr': 9.92, 'beta': 0.968, 'B win %': 58.392, 'B avg % acct': 0.34, 'final equity $': 2967096.237},
    (50000, 'A floor'): {'CAGR %': 22.322, 'Sharpe': 1.017, 'max DD %': -41.244, 'alpha %/yr': 10.102, 'beta': 0.957, 'B win %': 58.392, 'B avg % acct': 0.342, 'final equity $': 6077657.16},
    (50000, 'A nearest'): {'CAGR %': 22.366, 'Sharpe': 1.018, 'max DD %': -41.072, 'alpha %/yr': 10.142, 'beta': 0.957, 'B win %': 58.392, 'B avg % acct': 0.342, 'final equity $': 6129967.761},
    (100000, 'A floor'): {'CAGR %': 22.363, 'Sharpe': 1.018, 'max DD %': -41.297, 'alpha %/yr': 10.141, 'beta': 0.957, 'B win %': 58.392, 'B avg % acct': 0.343, 'final equity $': 12252437.081},
    (100000, 'A nearest'): {'CAGR %': 22.379, 'Sharpe': 1.018, 'max DD %': -41.381, 'alpha %/yr': 10.155, 'beta': 0.957, 'B win %': 58.392, 'B avg % acct': 0.343, 'final equity $': 12290264.792},
    (18000, 'A floor, stress costs'): {'CAGR %': 20.928, 'Sharpe': 0.963, 'max DD %': -43.162, 'alpha %/yr': 8.922, 'beta': 0.959, 'B win %': 58.042, 'B avg % acct': 0.293, 'final equity $': 1665105.534},
    (18000, 'A nearest, stress costs'): {'CAGR %': 21.007, 'Sharpe': 0.964, 'max DD %': -42.395, 'alpha %/yr': 8.975, 'beta': 0.961, 'B win %': 58.042, 'B avg % acct': 0.291, 'final equity $': 1691153.821},
    (18000, 'A floor, B P&L = IEF-minus-bills model'): {'CAGR %': 23.771, 'Sharpe': 1.076, 'max DD %': -39.684, 'alpha %/yr': 11.313, 'beta': 0.954, 'B win %': 60.14, 'B avg % acct': 0.458, 'final equity $': 2896758.577},
    (18000, 'A nearest, B P&L = IEF-minus-bills model'): {'CAGR %': 24.037, 'Sharpe': 1.083, 'max DD %': -39.325, 'alpha %/yr': 11.521, 'beta': 0.956, 'B win %': 60.14, 'B avg % acct': 0.456, 'final equity $': 3048762.776},
    (10000, "today's rounding fixed, A floor"): {'CAGR %': 13.551, 'Sharpe': 0.702, 'max DD %': -53.374, 'alpha %/yr': 2.307, 'beta': 0.952, 'B win %': 58.392, 'B avg % acct': 0.169, 'final equity $': 206516.647},
    (10000, "today's rounding fixed, A nearest"): {'CAGR %': 13.551, 'Sharpe': 0.702, 'max DD %': -53.374, 'alpha %/yr': 2.307, 'beta': 0.952, 'B win %': 58.392, 'B avg % acct': 0.169, 'final equity $': 206516.647},
    (18000, "today's rounding fixed, A floor"): {'CAGR %': 19.828, 'Sharpe': 0.949, 'max DD %': -44.085, 'alpha %/yr': 7.9, 'beta': 0.954, 'B win %': 58.392, 'B avg % acct': 0.282, 'final equity $': 1339498.724},
    (18000, "today's rounding fixed, A nearest"): {'CAGR %': 21.248, 'Sharpe': 1.005, 'max DD %': -44.084, 'alpha %/yr': 9.147, 'beta': 0.95, 'B win %': 58.392, 'B avg % acct': 0.282, 'final equity $': 1773429.572},
    (25000, "today's rounding fixed, A floor"): {'CAGR %': 17.997, 'Sharpe': 0.891, 'max DD %': -45.809, 'alpha %/yr': 6.378, 'beta': 0.941, 'B win %': 58.392, 'B avg % acct': 0.271, 'final equity $': 1289037.373},
    (25000, "today's rounding fixed, A nearest"): {'CAGR %': 19.214, 'Sharpe': 0.931, 'max DD %': -48.351, 'alpha %/yr': 7.288, 'beta': 0.959, 'B win %': 58.392, 'B avg % acct': 0.271, 'final equity $': 1645829.507},
    (50000, "today's rounding fixed, A floor"): {'CAGR %': 21.006, 'Sharpe': 0.985, 'max DD %': -44.455, 'alpha %/yr': 8.875, 'beta': 0.961, 'B win %': 58.392, 'B avg % acct': 0.304, 'final equity $': 4696951.904},
    (50000, "today's rounding fixed, A nearest"): {'CAGR %': 21.522, 'Sharpe': 1.01, 'max DD %': -43.493, 'alpha %/yr': 9.376, 'beta': 0.953, 'B win %': 58.392, 'B avg % acct': 0.304, 'final equity $': 5198294.231},
    (100000, "today's rounding fixed, A floor"): {'CAGR %': 22.54, 'Sharpe': 1.032, 'max DD %': -41.98, 'alpha %/yr': 10.205, 'beta': 0.962, 'B win %': 58.392, 'B avg % acct': 0.321, 'final equity $': 12683498.109},
    (100000, "today's rounding fixed, A nearest"): {'CAGR %': 22.696, 'Sharpe': 1.033, 'max DD %': -43.169, 'alpha %/yr': 10.305, 'beta': 0.967, 'B win %': 58.392, 'B avg % acct': 0.321, 'final equity $': 13071897.617},
}
PREV_10Y_PERIODS = {
    ('floor', '2003-2022 in-sample'): {'CAGR %': 19.691, 'Sharpe': 0.913, 'max DD %': -43.174, 'alpha %/yr': 9.405, 'B win %': 58.75, 'B avg % acct': 0.369},
    ('floor', '2023+ holdout'): {'CAGR %': 35.45, 'Sharpe': 1.608, 'max DD %': -11.091, 'alpha %/yr': 11.782, 'B win %': 56.522, 'B avg % acct': 0.164},
    ('nearest', '2003-2022 in-sample'): {'CAGR %': 19.754, 'Sharpe': 0.912, 'max DD %': -42.513, 'alpha %/yr': 9.451, 'B win %': 58.75, 'B avg % acct': 0.368},
    ('nearest', '2023+ holdout'): {'CAGR %': 35.518, 'Sharpe': 1.611, 'max DD %': -11.089, 'alpha %/yr': 11.838, 'B win %': 56.522, 'B avg % acct': 0.164},
}
PREV_10Y_TODAY_18K = "3 x 10Y (2.42x IEF-equivalent)"
PREV_10Y_B_GROSS_BP, PREV_10Y_B_COSTS = 14.906, 73626.3   # leg-B gross bp of notional per trade; fees + slippage $
# =============================================================================


def theo_price(y, n=19, c=0.06):
    """Price of a 6% semi-annual coupon bond with n half-years left at yield y (decimal): the Ultra 10-Year's
    conversion-factor basis, used only to size hypothetical pre-2016 trades (calibrated to TN=F below)."""
    v = 1 / (1 + y / 2)
    return 100 * (c / 2 * (1 - v ** n) / (y / 2) + v ** n)


def load():
    d, info = S.load()
    idx = d.index
    rf_d = d.irx.shift(1) * d.days / 360
    d["ief_ex"] = d.ief_cc - rf_d                                         # IEF total return minus T-bill accrual
    d["ief_model_ret"] = d.ief_ex / HEDGE                                 # implied MTN return, IEF model
    tn = lib.get("TN=F")
    info["tn_raw_first"] = tn.index[0].date()
    info["tn_rows_rejected"] = int((tn.index < TN_START).sum())
    tn = tn[tn.index >= TN_START]
    tc = tn.Close.reindex(idx)
    info["tn_missing"] = int(tc.loc[TN_START:].isna().sum())
    tcf = tc.ffill(limit=2)
    d["tn_close"] = tc
    r = tcf / tcf.shift(1) - 1
    r[idx <= pd.Timestamp(TN_START)] = np.nan
    res = r - d.ief_model_ret
    roll_win = idx.month.isin([3, 6, 9, 12]) & (idx.day >= 12) & (idx.day <= 25)
    base = res[~roll_win].dropna()
    sd = 1.4826 * (base - base.median()).abs().median()                   # robust sd of the daily residual
    flag = pd.Series(False, index=idx)
    for _, g in res[roll_win].dropna().groupby([idx[roll_win & res.notna()].year, idx[roll_win & res.notna()].month]):
        t = g.abs().idxmax()
        if abs(g[t]) > ROLL_SD * sd:
            flag[t] = True
    d["tn_roll"] = flag
    d["tn_ret_raw"] = r
    d["tn_ret"] = r.where(~flag, d.ief_model_ret)                         # roll days neutralised
    d["tn_resid"] = res
    hold = d.rank_bwd <= S.B_HOLD_RANK - 1
    info["roll"] = dict(sd=sd, n_flag=int(flag.sum()), dates=[t.date() for t in flag[flag].index],
                        sizes=[100 * res[t] for t in flag[flag].index], on_hold=int((flag & hold).sum()),
                        hold_big=[(t.date(), 100 * res[t]) for t in res[hold & (res.abs() > 5 * sd)].index],
                        hold_max=100 * res[hold].abs().max(), nonhold_max_nonroll=100 * res[~hold & ~flag].abs().max())
    ok = r.notna() & d.ief_ex.notna() & ~roll_win
    for a in ["2016", "2017"]:
        m = ok & (idx >= a)
        info[f"hedge_{a}"] = (np.polyfit(r[m], d.ief_ex[m], 1)[0], np.corrcoef(r[m], d.ief_ex[m])[0, 1])
    # sizing price: TN=F close when available, else the 6% / 9.5y theoretical price at ^TNX x calibration ratio
    tnx = lib.get("^TNX").Close
    scale = 10.0 if tnx.loc["2003":].median() > 20 else 1.0
    y = (tnx / scale / 100).reindex(idx).ffill()
    th = theo_price(y)
    ratio = (tc / th).dropna()
    info["theo_ratio"] = ratio.median(); info["theo_ratio_rng"] = (ratio.quantile(0.05), ratio.quantile(0.95))
    d["mtn_px"] = tcf.where(tcf.notna() & (idx >= pd.Timestamp(TN_START)), th * info["theo_ratio"])
    d["znf_model_ret"] = d.zn_ret_n * d.beta / HEDGE                       # ZN=F return scaled to MTN (TN per ZN)
    d["mtn_px_src"] = np.where(tcf.notna() & (idx >= pd.Timestamp(TN_START)), "TN=F", "synthetic")
    return d, info


def simulate(d, equity0=HEADLINE_EQUITY, cost="base", a_round="floor", b_model=B_MODEL, mes_margin_pct=MES_MARGIN_PCT,
             mtn_margin_pct=MTN_MARGIN_PCT, today=None, bot_lev="param", bot_times="param", caps="param", stops="param"):
    """today: None, or dict(levA={1.0: x, 0.5: y}, levB=z, spx_now=...) = "today's rounding held fixed":
    MES leverage and MTN notional per $ of equity frozen at what a fresh account of this size gets at today's
    prices (fractional contracts scale with equity, MES costs scaled to today's cost in bp of notional).
    b_model: "auto" (TN=F 2016+, IEF model before), "ief" (IEF model throughout), "znf" (TN=F 2016+, ZN=F x
    trailing beta / 0.885 before: real futures prices throughout). bot_lev / bot_times / caps / stops default to
    the module switches (BOT_LEVERAGE, BOT_TIMES, BOT_CAPS, BOT_STOPS)."""
    P_ = lambda v, dflt: dflt if (isinstance(v, str) and v == "param") else v
    bot_lev, bot_times, caps, stops = P_(bot_lev, BOT_LEVERAGE), P_(bot_times, BOT_TIMES), P_(caps, BOT_CAPS), P_(stops, BOT_STOPS)
    lev_a = S.LEV_A_BOT if bot_lev else S.LEV_A_RESEARCH
    c = COSTS[cost]
    dd = d.loc[START:]
    idx = dd.index
    core, cash, nl_prev = equity0, 0.0, equity0
    B = None
    rows, trades = [], []
    rf_prev = dd.irx.iloc[0]
    tick_usd = MTN_TICK * MTN_MULT
    mtn_now = d.mtn_px.iloc[-1]

    def b_cap(t, P0):
        if caps is None:
            return B_MAX_CONTRACTS                                         # research run: 400 MTN, literal
        if caps == "lev":
            return 10 ** 9
        n = B_CAPS[caps]
        return n if S.CAP_MODE == "contracts" else int(math.floor(n * mtn_now / P0 + 1e-9))
    alt = lambda r: r.znf_model_ret if b_model == "znf" else r.ief_model_ret   # pre-2016 return of the main path
    for i, t in enumerate(idx):
        r = dd.loc[t]
        core_ret = core * r.spy_cc if i > 0 else 0.0
        core += core_ret
        interest = 0.0
        if i > 0:
            margin = B["n"] * mtn_margin_pct * MTN_MULT * B["P_tn"] if B else 0.0   # MTN overnight margin from cash
            bal = cash - margin
            rate = max(rf_prev + S.CREDIT_SPREAD, 0.0) if bal > 0 else rf_prev + S.DEBIT_SPREAD
            interest = bal * rate * r.days / 360
            cash += interest
        rf_d = rf_prev * r.days / 360
        a_pnl = b_pnl = cost_day = 0.0
        # --- leg B mark / disaster stop / exit
        if B is not None:
            early_exit = bot_times and r.rank_bwd == 1 and r.b_early      # 12:50 exit: previous close
            r_tn = 0.0 if early_exit else (r.tn_ret if B["model"] == "tn" else alt(r))
            r_ief = 0.0 if early_exit else r.ief_model_ret
            main_ief = b_model == "ief"
            Pm = B["P_ief"] if main_ief else B["P_tn"]
            Pm_new = Pm * (1 + (r_ief if main_ief else r_tn))
            stopped = False
            if stops and not early_exit:
                worst = min(Pm * (1 + B["k"] * r.zn_low_r), Pm_new)        # ZN=F low, scaled to MTN
                P_stop = B["P0"] - S.B_STOP * B["nl0"] / (B["n"] * MTN_MULT)
                if worst <= P_stop + 1e-12:
                    Pm_new = P_stop - S.STOP_EXTRA_SLIP_TICKS * MTN_TICK
                    stopped = True
            p_tn = B["n"] * MTN_MULT * B["P_tn"] * r_tn
            p_ief = B["n"] * MTN_MULT * B["P_ief"] * r_ief
            if stopped:                                                      # the main path exits at the stop
                if main_ief:
                    p_ief = B["n"] * MTN_MULT * (Pm_new - B["P_ief"]); B["P_ief"] = Pm_new; B["P_tn"] *= 1 + r_tn
                else:
                    p_tn = B["n"] * MTN_MULT * (Pm_new - B["P_tn"]); B["P_tn"] = Pm_new; B["P_ief"] *= 1 + r_ief
            else:
                B["P_tn"] *= 1 + r_tn
                B["P_ief"] *= 1 + r_ief
            B["g_tn"] += p_tn; B["g_ief"] += p_ief; B["roll_days"] += int(r.tn_roll)
            pnl = p_ief if main_ief else p_tn
            b_pnl += pnl; cash += pnl
            if r.rank_bwd == 1 or stopped:
                fee = B["n"] * c["mtn_fee"]; slip = B["n"] * c["mtn_slip_ticks"] * tick_usd
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B["fee"] += fee; B["slip"] += slip
                B.update(exit_date=t, closed=True,
                         exit_time="disaster stop" if stopped else ("12:50" if early_exit else ("15:55" if bot_times else r.close_time)),
                         exit_reason="disaster stop (4% of equity)" if stopped else
                         ("12:50 exit (previous close)" if early_exit else "month-end close"))
        # --- leg A (intraday on the last session)
        if r.rank_bwd == 1 and np.isfinite(r.gap) and r.gap != 0 and np.isfinite(r.vol20):
            dirn = -np.sign(r.gap)
            px_prev = dd.gspc_close.iloc[i - 1] if i > 0 else r.gspc_close
            want = lev_a * r.sizeA * nl_prev / (S.MES_MULT * px_prev)
            capn = S.cap_contracts(caps, "A", d, t) if caps is not None else 10 ** 9
            if today is not None:
                n = min(today["levA"][r.sizeA] * nl_prev / (S.MES_MULT * px_prev), capn)
            elif caps is None and a_round == "floor":
                n = math.floor(want + 1e-9)
            elif caps is None:
                n = int(math.floor(want + 0.5))
                while n > 0 and n * S.MES_MULT * px_prev / nl_prev > MAX_LEV_A + 1e-9:
                    n -= 1
            else:
                n, _ = size_contracts(nl_prev, px_prev, S.MES_MULT, lev_a * r.sizeA, 1.0, MAX_LEV_A, capn, a_round,
                                      hard_max_contracts=capn)
            note = ""
            if caps is not None and n > 0:
                b_not = B["n"] * MTN_MULT * B["P_tn"] if (B is not None and not B.get("closed")) else 0.0
                if (n * S.MES_MULT * px_prev + b_not) / nl_prev > S.TOTAL_MAX_LEV + 1e-9:
                    n, note = 0, "blocked: total leverage > 7x"
            p_in = r.gspc_close / (1 + r.spy_oc); p_out = r.gspc_close
            reason, adv, src = "close", np.nan, ""
            if stops and n > 0:
                adv, src = S.a_adverse(r, dirn, p_in, p_out)
                if n * S.MES_MULT * p_in * adv >= S.A_STOP * nl_prev:
                    p_out = p_in - dirn * S.A_STOP * nl_prev / (n * S.MES_MULT) - dirn * S.STOP_EXTRA_SLIP_TICKS * S.MES_TICK
                    reason = "disaster stop (6% of equity)"
            gross = dirn * n * S.MES_MULT * (p_out - p_in)
            fee = 2 * n * c["mes_fee"]; slip = 2 * n * c["mes_slip_ticks"] * S.MES_TICK * S.MES_MULT
            if today is not None:
                fee *= px_prev / today["spx_now"]; slip *= px_prev / today["spx_now"]
            net = gross - fee - slip
            cash += net; a_pnl += net; cost_day += fee + slip
            m_a = n * mes_margin_pct * S.MES_MULT * p_in
            m_b = B["n"] * mtn_margin_pct * MTN_MULT * B["P0"] if B else 0.0   # B is still open during the last session
            trades.append(dict(
                leg="A", hypothetical=t < pd.Timestamp(MES_LAUNCH), entry_date=t.date(), entry_time="09:30",
                exit_date=t.date(), exit_time="disaster stop" if reason != "close" else r.close_time,
                direction=("long" if dirn > 0 else "short") if n > 0 else (note or "skipped (0 contracts)"),
                contracts=n, contracts_wanted=want, contract_notional=S.MES_MULT * p_in,
                position_notional=n * S.MES_MULT * p_in, lev_x_netliq=n * S.MES_MULT * p_in / nl_prev,
                entry_price=p_in, exit_price=p_out, gross_pnl=gross, gross_pnl_alt_model=np.nan,
                commissions=fee, slippage=slip, net_pnl=net, net_pct_account=100 * net / nl_prev,
                netliq_at_entry=nl_prev, margin_futures=m_a + m_b, margin_pct_netliq=100 * (m_a + m_b) / nl_prev,
                total_req_pct_netliq=100 * (m_a + m_b + SPY_MAINT * core) / nl_prev,
                margin_flag=(m_a + m_b) / nl_prev > MARGIN_FLAG, vol_tilt_size=r.sizeA,
                signal_gap_pct=100 * r.gap, capped=bool(n < math.floor(want + 1e-9)),
                exit_reason=reason if n > 0 else (note or "0 contracts"), adverse_pct=100 * adv, adverse_source=src))
        nl = core + cash
        if B is not None and B.get("closed"):
            main_ief = b_model == "ief"
            g_main = B["g_ief"] if main_ief else B["g_tn"]
            g_alt = B["g_tn"] if main_ief else B["g_ief"]
            used = "ief" if main_ief else (B["model"] if b_model == "auto" or B["model"] == "tn" else "znf")
            net = g_main - B["fee"] - B["slip"]
            m = B["n"] * mtn_margin_pct * MTN_MULT * B["P0"]
            trades.append(dict(
                leg="B", hypothetical=B["entry_date"] < pd.Timestamp(MTN_LAUNCH), entry_date=B["entry_date"].date(),
                entry_time=B["entry_time"], exit_date=t.date(), exit_time=B["exit_time"],
                direction="long MTN", contract=B["sym"], contract_last_trade=B["lsd"], contracts=B["n"],
                contracts_wanted=B["want"], contract_notional=MTN_MULT * B["P0"],
                position_notional=B["n"] * MTN_MULT * B["P0"], lev_x_netliq=B["n"] * MTN_MULT * B["P0"] / B["nl0"],
                ief_equiv_lev=B["n"] * MTN_MULT * B["P0"] / B["nl0"] / HEDGE,
                entry_price=B["P0"], exit_price=B["P_ief"] if main_ief else B["P_tn"], price_source=B["src"],
                b_model=used, roll_days_neutralised=B["roll_days"],
                gross_pnl=g_main, gross_pnl_alt_model=g_alt if B["model"] == "tn" else np.nan,
                gross_pnl_tn=B["g_tn"] if B["model"] == "tn" else np.nan, gross_pnl_ief=B["g_ief"],
                commissions=B["fee"], slippage=B["slip"], net_pnl=net,
                net_pct_account=100 * net / B["nl0"], netliq_at_entry=B["nl0"], margin_futures=m,
                margin_pct_netliq=100 * m / B["nl0"],
                total_req_pct_netliq=100 * (m + SPY_MAINT * B["core0"]) / B["nl0"],
                margin_flag=m / B["nl0"] > MARGIN_FLAG, capped=B["capped"], exit_reason=B["exit_reason"]))
            B = None
        if r.rank_bwd == S.B_HOLD_RANK and t < idx[-1]:
            X = idx[min(i + S.B_HOLD_RANK - 1, len(idx) - 1)]                  # month-end exit session
            y_, m_, lsd = contract_month_for(X.date(), ROLL_BUFFER, "MTN")
            sym = "MTN" + "FGHJKMNQUVXZ"[m_ - 1] + str(y_)[-1]
            early_entry = bot_times and r.b_early                             # 12:50 entry: previous close
            P0 = dd.mtn_px.iloc[i - 1] if early_entry else r.mtn_px
            nl_s = nl_prev if early_entry else nl
            want = LEV_B * HEDGE * nl_s / (P0 * MTN_MULT)
            capb = b_cap(t, P0)
            if today is None:
                n, _ = size_contracts(nl_s, P0, MTN_MULT, LEV_B * HEDGE, 1.0, B_MAX_LEV, capb, "floor",
                                      hard_max_leverage=HARD_MAX_BOND_LEVERAGE, hard_max_contracts=capb)
            else:
                n = min(today["levB"] * nl_s / (P0 * MTN_MULT), capb)
            hold = dd.iloc[(i if early_entry else i + 1):i + S.B_HOLD_RANK]
            model = "tn" if (r.mtn_px_src == "TN=F" and hold.tn_ret.notna().all()
                             and len(hold) >= S.B_HOLD_RANK - 1) else "ief"
            if n > 0:
                fee = n * c["mtn_fee"]; slip = n * c["mtn_slip_ticks"] * tick_usd
                cash -= fee + slip; b_pnl -= fee + slip; cost_day += fee + slip
                B = dict(n=n, want=want, P0=P0, P_tn=P0, P_ief=P0, model=model, src=r.mtn_px_src, sym=sym, lsd=lsd,
                         g_tn=0.0, g_ief=0.0, fee=fee, slip=slip, entry_date=t, nl0=nl_s, core0=core, roll_days=0,
                         entry_time="12:50" if early_entry else ("15:58" if bot_times else r.close_time),
                         k=r.beta / HEDGE, capped=bool(n < math.floor(want + 1e-9)))
                if early_entry:                                              # today's move from the previous close
                    r_tn = r.tn_ret if model == "tn" else alt(r)
                    p_tn = n * MTN_MULT * P0 * r_tn; p_ief = n * MTN_MULT * P0 * r.ief_model_ret
                    B["P_tn"] *= 1 + r_tn; B["P_ief"] *= 1 + r.ief_model_ret
                    B["g_tn"] += p_tn; B["g_ief"] += p_ief
                    pnl = p_ief if b_model == "ief" else p_tn
                    b_pnl += pnl; cash += pnl
            else:
                trades.append(dict(
                    leg="B", hypothetical=t < pd.Timestamp(MTN_LAUNCH), entry_date=t.date(), entry_time=r.close_time,
                    exit_date=None, exit_time=None, direction="skipped (0 contracts)", contract=sym,
                    contract_last_trade=lsd, contracts=0, contracts_wanted=want, contract_notional=MTN_MULT * P0,
                    position_notional=0, lev_x_netliq=0, ief_equiv_lev=0, entry_price=P0, exit_price=np.nan,
                    price_source=r.mtn_px_src, b_model=model, gross_pnl=0, gross_pnl_alt_model=np.nan,
                    commissions=0, slippage=0, net_pnl=0, net_pct_account=0, netliq_at_entry=nl_s, margin_futures=0,
                    margin_pct_netliq=0, total_req_pct_netliq=100 * SPY_MAINT * core / nl_s, margin_flag=False,
                    equity_after=nl, capped=False, exit_reason="0 contracts"))
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
    nb, levb = size_contracts(e0, last.mtn_px, MTN_MULT, LEV_B * HEDGE, 1.0, B_MAX_LEV, B_MAX_CONTRACTS, "floor",
                              hard_max_leverage=HARD_MAX_BOND_LEVERAGE, hard_max_contracts=B_MAX_CONTRACTS)
    return dict(levA=lev, levB=levb, spx_now=last.gspc_close, nb=nb)


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
    for x_, txt in [(TN_START, "TN=F prices from here"), (MTN_LAUNCH, "MTN launched"), ("2023-01-01", "holdout 2023+"),
                    (MES_LAUNCH, "MES launched")]:
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
    ax2.plot(ddS.index, ddS, color=S.C1, lw=1.2, label=list(eqs)[0].split(",")[0])
    ax2.set_ylabel("Drawdown from peak (%)"); ax2.set_xlabel("Date")
    ax2.grid(True, color=S.GRID, lw=0.8); ax2.set_axisbelow(True)
    ax2.legend(loc="lower left", frameon=False, ncol=2)
    fig.tight_layout(); fig.savefig(os.path.join(OUT, "equity_18k.png"), dpi=130); plt.close(fig)


# =============================================================== spot checks (independent recomputation)
def spot_check(tr):
    """Recompute one A and two B trades straight from the raw Yahoo files, not from the simulation's frame."""
    out = []
    gspc, spy, tn, ief, irx = lib.get("^GSPC"), lib.get("SPY"), lib.get("TN=F"), lib.get("IEF"), lib.get("^IRX")
    a = tr[(tr.leg == "A") & (tr.contracts > 0)].iloc[0]
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
    per = COSTS["base"]["mtn_fee"] + COSTS["base"]["mtn_slip_ticks"] * MTN_TICK * MTN_MULT
    bt = tr[(tr.leg == "B") & (tr.contracts > 0)]
    # (1) a real-MTN-era trade priced on TN=F: price change x $100 x contracts
    b = bt[(bt.b_model == "tn") & (bt.roll_days_neutralised == 0) & (bt.entry_date.astype(str) >= MTN_LAUNCH)].iloc[0]
    t0, t1 = pd.Timestamp(str(b.entry_date)), pd.Timestamp(str(b.exit_date))
    p0, p1 = tn.Close.loc[t0], tn.Close.loc[t1]
    need = LEV_B * HEDGE * b.netliq_at_entry / (p0 * 100)
    gross = b.contracts * 100 * (p1 - p0); cst = b.contracts * 2 * per
    out.append(dict(trade=f"B {t0.date()} -> {t1.date()} long {b.contracts} {b.contract} (TN=F model)",
                    hand=f"contract: first quarterly MTN whose last trade day is > 5 sessions after {t1.date()} = "
                         f"{b.contract} (last trade {b.contract_last_trade}); size = floor(2.8 x 0.885 x "
                         f"${b.netliq_at_entry:,.0f} / ({p0:.6f} x 100)) = floor({need:.2f}) = {math.floor(need)}; "
                         f"TN=F {p0:.6f} -> {p1:.6f}; gross = {b.contracts} x 100 x ({p1:.6f} - {p0:.6f}) = "
                         f"${gross:,.2f}; costs = {b.contracts} x 2 x ($0.60 + $3.125) = ${cst:.2f}; "
                         f"net ${gross - cst:,.2f}",
                    sim=f"gross ${b.gross_pnl:,.2f}, costs ${b.commissions + b.slippage:.2f}, net ${b.net_pnl:,.2f}",
                    match=abs(gross - cst - b.net_pnl) < 0.01 and math.floor(need) == b.contracts))
    # (2) a hypothetical pre-2016 trade priced on IEF minus bills / 0.885
    b = bt[(bt.b_model == "ief") & (bt.entry_date.astype(str) >= "2008-01-01")].iloc[0]
    t0, t1 = pd.Timestamp(str(b.entry_date)), pd.Timestamp(str(b.exit_date))
    days = ief.loc[t0:t1].index
    P = b.entry_price; steps = []
    for k in range(1, len(days)):
        u, v = days[k - 1], days[k]
        ret = ief["Adj Close"].loc[v] / ief["Adj Close"].loc[u] - 1
        rf = irx.Close.reindex(ief.index).ffill().loc[u] / 100 * (v - u).days / 360
        steps.append(f"{v:%m-%d} IEF {100*ret:+.3f}% - bill {100*rf:.4f}%")
        P *= 1 + (ret - rf) / HEDGE
    gross = b.contracts * 100 * (P - b.entry_price); cst = b.contracts * 2 * per
    out.append(dict(trade=f"B {t0.date()} -> {t1.date()} long {b.contracts} {b.contract} (hypothetical, IEF model)",
                    hand=f"sizing price {b.entry_price:.3f} (synthetic: 6%/9.5y bond at ^TNX x calibration); "
                         f"size floor(2.8 x 0.885 x ${b.netliq_at_entry:,.0f} / ({b.entry_price:.3f} x 100)) = "
                         f"{math.floor(LEV_B * HEDGE * b.netliq_at_entry / (b.entry_price * 100))}; "
                         f"{'; '.join(steps)}; price x prod(1 + excess/0.885) = {P:.4f}; gross = {b.contracts} x 100 x "
                         f"({P:.4f} - {b.entry_price:.4f}) = ${gross:,.2f}; costs ${cst:.2f}; net ${gross - cst:,.2f}",
                    sim=f"gross ${b.gross_pnl:,.2f}, costs ${b.commissions + b.slippage:.2f}, net ${b.net_pnl:,.2f}",
                    match=abs(gross - cst - b.net_pnl) < 0.05))
    return pd.DataFrame(out)


# =============================================================== main
TCOLS = ["leg", "hypothetical", "entry_date", "entry_time", "exit_date", "exit_time", "direction", "contract",
         "contract_last_trade", "contracts", "contracts_wanted", "contract_notional", "position_notional",
         "lev_x_netliq", "ief_equiv_lev", "entry_price", "exit_price", "price_source", "b_model",
         "roll_days_neutralised", "gross_pnl", "gross_pnl_alt_model", "gross_pnl_tn", "gross_pnl_ief", "commissions",
         "slippage", "net_pnl", "net_pct_account", "netliq_at_entry", "equity_after", "margin_futures",
         "margin_pct_netliq", "total_req_pct_netliq", "margin_flag", "vol_tilt_size", "signal_gap_pct", "capped",
         "exit_reason", "adverse_pct", "adverse_source"]


def main():
    d, info = load()
    bench = d.spy_cc
    scen, store = [], {}

    def add(key, label, e0, eq, tr):
        store[key] = (eq, tr)
        scen.append({"account": e0, "case": label, **perf(eq, tr, bench)})

    for e0 in ACCOUNT_SIZES:
        for ar in ["floor", "nearest"]:
            add((e0, ar, "auto", "base"), f"MTN, A {ar}", e0, *simulate(d, e0, "base", ar, "auto"))
    E = HEADLINE_EQUITY
    for ar in ["floor", "nearest"]:
        add((E, ar, "auto", "stress"), f"MTN, A {ar}, stress costs", E, *simulate(d, E, "stress", ar, "auto"))
        add((E, ar, "auto", "halftick"), f"MTN, A {ar}, 1/64 slippage", E, *simulate(d, E, "halftick", ar, "auto"))
        add((E, ar, "ief", "base"), f"MTN, A {ar}, B P&L = IEF-minus-bills model throughout", E,
            *simulate(d, E, "base", ar, "ief"))
    add((E, "floor", "zn", "base"), "ZN (../simulate.py), A floor", E, *run_zn(d, E))
    for e0 in ACCOUNT_SIZES:
        for ar in ["floor", "nearest"]:
            add((e0, ar, "today", "base"), f"today's rounding fixed, A {ar}", e0,
                *simulate(d, e0, "base", ar, "auto", today=today_sizing(d, e0, ar)))
    hi_eq, hi_tr = simulate(d, E, "base", "floor", "auto", MES_MARGIN_PCT * MARGIN_STRESS_MULT,
                            MTN_MARGIN_PCT * MARGIN_STRESS_MULT)
    bs = S.bench_stats(store[(E, "floor", "auto", "base")][0], bench)
    bs["final equity $"] *= E / S.START_EQUITY
    scen.append({"account": E, "case": "S&P 500 TR benchmark", **bs})
    SC = pd.DataFrame(scen)
    SC.round(3).to_csv(os.path.join(OUT, "scenarios_small.csv"), index=False)

    eq, tr = store[(E, "floor", "auto", "base")]
    eqn, trn = store[(E, "nearest", "auto", "base")]
    sortk = lambda T: T.reindex(columns=TCOLS).sort_values(["entry_date", "leg"], key=lambda s: s.astype(str))
    sortk(tr).round(4).to_csv(os.path.join(OUT, "trades_18k.csv"), index=False)
    sortk(trn).round(4).to_csv(os.path.join(OUT, "trades_18k_nearest.csv"), index=False)
    Y = S.yearly(eq, tr, bench)
    Yn = S.yearly(eqn, trn, bench)
    Y.insert(1, "A-nearest variant %", Yn["strategy %"])
    Y.round(2).to_csv(os.path.join(OUT, "yearly_18k.csv"))
    eqz = store[(E, "floor", "zn", "base")][0]
    eqt = store[(E, "floor", "today", "base")][0]
    td18 = today_sizing(d, E, "floor")
    chart({"Strategy, B in MTN, A floor (headline; A nearest is visually identical)": eq,
           f"Same, with today's $18k contract counts held fixed (MES {td18['levA'][1.0]:.2f}x / "
           f"{td18['levA'][0.5]:.0f}x, {td18['nb']} MTN)": eqt,
           "Strategy, B in ZN priced on IEF-minus-bills (no B until ~$40k), A floor": eqz}, bench, E)

    # periods
    split = []
    for key, lab in [((E, "floor", "auto", "base"), "floor"), ((E, "nearest", "auto", "base"), "nearest")]:
        e_, t_ = store[key]
        for lbl, (a, b) in {"2003-2022 in-sample": ("2003", "2022"), "2023+ holdout": ("2023", None),
                            "2016+ (B on TN=F prices)": ("2016", None), "2024+ (MTN live)": ("2024", None)}.items():
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

    # leg B: TN=F model vs IEF-minus-bills model, per trade (headline run, trades priced on TN=F)
    b = tr[(tr.leg == "B") & (tr.contracts > 0) & (tr.b_model == "tn")].copy()
    b["diff_bp"] = 1e4 * (b.gross_pnl_ief - b.gross_pnl_tn) / b.position_notional
    b["tn_bp"] = 1e4 * b.gross_pnl_tn / b.position_notional; b["ief_bp"] = 1e4 * b.gross_pnl_ief / b.position_notional
    b["yr"] = pd.to_datetime(b.entry_date.astype(str)).dt.year
    b["qroll"] = pd.to_datetime(b.exit_date.astype(str)).dt.month.isin([2, 5, 8, 11])
    ballB = tr[(tr.leg == "B") & (tr.contracts > 0)]
    allb = 1e4 * ballB.gross_pnl_ief / ballB.position_notional
    info["b_check"] = {
        "n": len(b), "corr": np.corrcoef(b.gross_pnl_tn, b.gross_pnl_ief)[0, 1],
        "corr_bp": np.corrcoef(b.tn_bp, b.ief_bp)[0, 1],
        "sign_agree": 100 * (np.sign(b.gross_pnl_tn) == np.sign(b.gross_pnl_ief)).mean(),
        "diff_bp_mean": b.diff_bp.mean(), "diff_bp_se": b.diff_bp.std() / np.sqrt(len(b)), "diff_bp_te": b.diff_bp.std(),
        "diff_usd_mean": (b.gross_pnl_ief - b.gross_pnl_tn).mean(),
        "tn_bp_mean": b.tn_bp.mean(), "ief_bp_mean": b.ief_bp.mean(),
        "pre23_diff": b[b.yr < 2023].diff_bp.mean(), "post23_diff": b[b.yr >= 2023].diff_bp.mean(),
        "qroll_diff": b[b.qroll].diff_bp.mean(), "other_diff": b[~b.qroll].diff_bp.mean(),
        "qroll_corr": np.corrcoef(b[b.qroll].tn_bp, b[b.qroll].ief_bp)[0, 1],
        "other_corr": np.corrcoef(b[~b.qroll].tn_bp, b[~b.qroll].ief_bp)[0, 1],
        "tn_gross": b.gross_pnl_tn.sum(), "ief_gross": b.gross_pnl_ief.sum(),
        "ief_bp_pre16": allb[pd.to_datetime(ballB.entry_date.astype(str)) < TN_START].mean(),
        "n_ief_trades": int((tr[(tr.leg == "B") & (tr.contracts > 0)].b_model == "ief").sum()),
        "n_ief_post16": int(((ballB.b_model == "ief") & (ballB.entry_date.astype(str) >= TN_START)).sum()),
        "n_roll_neutralised": int(ballB.roll_days_neutralised.sum())}
    bth = b[["entry_date", "contract", "tn_bp", "ief_bp", "diff_bp"]]
    info["b_worst_gap"] = bth.reindex(bth.diff_bp.abs().sort_values(ascending=False).index).head(5)

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
        nb, lv = size_contracts(e0, last.mtn_px, MTN_MULT, LEV_B * HEDGE, 1.0, B_MAX_LEV, B_MAX_CONTRACTS, "floor",
                                hard_max_leverage=HARD_MAX_BOND_LEVERAGE, hard_max_contracts=B_MAX_CONTRACTS)
        r_["B MTN wanted"] = LEV_B * HEDGE * e0 / (last.mtn_px * MTN_MULT)
        r_["B MTN contracts (notional x / IEF-equiv x)"] = f"{nb} ({lv:.2f}x / {lv/HEDGE:.2f}x)"
        r_["B ZN contracts"] = math.floor(LEV_B * e0 * last.beta / (S.ZN_MULT * last.zn_close))
        rows.append(r_)
    CN = pd.DataFrame(rows)
    X_now = last.name.date()
    info["now"] = dict(date=X_now, spx=last.gspc_close, px=last.mtn_px, px_src=last.mtn_px_src,
                       mtn_notional=MTN_MULT * last.mtn_px, mes_notional=S.MES_MULT * last.gspc_close,
                       min_1_mtn=MTN_MULT * last.mtn_px / (LEV_B * HEDGE),
                       min_1_mes_full=S.MES_MULT * last.gspc_close / LEV_A,
                       min_1_mes_half_floor=S.MES_MULT * last.gspc_close / (LEV_A / 2),
                       min_1_mes_half_nearest=S.MES_MULT * last.gspc_close / MAX_LEV_A,
                       next_X=(dt.date(2026, 11, 30)), next_contract=contract_month_for(dt.date(2026, 11, 30), 5, "MTN"))
    info["margin_hi"] = dict(max=hi_tr.margin_pct_netliq.max(), flags=int(hi_tr.margin_flag.sum()),
                             cagr=perf(hi_eq, hi_tr, bench)["CAGR %"])
    info["margin_base"] = {k: (store[k][1].margin_pct_netliq.max(), int(store[k][1].margin_flag.sum()),
                               store[k][1].total_req_pct_netliq.max())
                           for k in store if k[2] == "auto"}
    bb = tr[(tr.leg == "B") & (tr.contracts > 0)]
    info["contracts_by_month"] = bb.contract.str[3].map({"H": "Mar", "M": "Jun", "U": "Sep", "Z": "Dec"}).value_counts().to_dict()
    info["b_lev"] = (bb.lev_x_netliq.mean(), bb.lev_x_netliq.min(), bb.ief_equiv_lev.mean())
    cap = bb[bb.contracts >= B_MAX_CONTRACTS]
    info["n_capped"] = len(cap); info["first_capped"] = str(cap.entry_date.iloc[0]) if len(cap) else "-"
    info["b_gross_bp"] = (1e4 * bb.gross_pnl / bb.position_notional).mean()
    info["b_costs"] = (bb.commissions + bb.slippage).sum()
    info["prev_b_gross_bp"], info["prev_b_costs"] = PREV_10Y_B_GROSS_BP, PREV_10Y_B_COSTS
    SPOT = spot_check(tr)

    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 60)
    print({k: v for k, v in info.items() if k not in ("now", "b_worst_gap")})
    print(info["now"]); print(info["b_worst_gap"])
    print(SC.set_index(["account", "case"]).round(2).T.to_string())
    print(Y.round(1).to_string()); print(SP.round(2).to_string()); print(CN.to_string())
    print(SPOT.to_string())
    write_summary(info, SC, Y, SP, CN, SPOT, eq, tr, trn, d)


def write_summary(info, SC, Y, SP, CN, SPOT, eq, tr, trn, d):
    E = HEADLINE_EQUITY
    f = lambda v, n=1: "n/a" if v is None or (isinstance(v, float) and not np.isfinite(v)) else f"{v:,.{n}f}"
    g = lambda acct, case, k: SC[(SC.account == acct) & (SC.case == case)][k].iloc[0]
    pv = lambda acct, case, k: PREV_10Y.get((acct, case), {}).get(k, nan)
    keys = ["CAGR %", "vol %", "Sharpe", "max DD %", "beta", "alpha %/yr", "alpha t", "A trades", "A skipped",
            "A win %", "A avg % acct", "A avg net $", "A avg contracts", "A avg lev x", "B trades", "B skipped",
            "B win %", "B avg % acct", "B avg net $", "B avg contracts", "B avg lev x", "max futures margin % netliq",
            "trades margin > 60%", "costs $", "final equity $"]
    H = SC.copy()
    H["scenario"] = [f"${a/1e3:,.0f}k {c}" for a, c in zip(H.account, H.case)]
    H = H.set_index("scenario").reindex(columns=keys).T
    H.insert(0, "metric", H.index)
    c_sizes = ["metric"] + [c for c in H.columns if c.endswith(("MTN, A floor", "MTN, A nearest"))]
    c18 = ["metric"] + [c for c in H.columns if c.startswith("$18k") and "today" not in c]
    ctd = ["metric"] + [c for c in H.columns if "today" in c]
    td = lambda acct, ar, k: g(acct, f"today's rounding fixed, A {ar}", k)
    tdp = lambda acct, ar, k: pv(acct, f"today's rounding fixed, A {ar}", k)
    bm = SC[SC.case.str.contains("benchmark")].iloc[0]
    hf = lambda k: g(E, "MTN, A floor", k)
    hn = lambda k: g(E, "MTN, A nearest", k)
    of = lambda k: pv(E, "A floor", k)
    zn = lambda k: g(E, "ZN (../simulate.py), A floor", k)
    bc, now, rl = info["b_check"], info["now"], info["roll"]
    yt = Y.reset_index(); yt["year"] = yt["year"].astype(str)
    a18 = tr[tr.leg == "A"]; b18 = tr[tr.leg == "B"]
    a_sk = a18[a18.contracts == 0]; a_skn = trn[(trn.leg == "A") & (trn.contracts == 0)]
    a_sk_y = a_sk.entry_date.astype(str).str[:4].value_counts().sort_index()
    bt = b18[b18.contracts > 0]
    early = bt[bt.entry_date.astype(str) < "2006"].contracts
    flags = info["margin_base"]
    mb = flags[(E, "floor", "auto", "base")]; mbn = flags[(E, "nearest", "auto", "base")]
    worst_flag = max(v[0] for v in flags.values())
    nb18 = int(CN[CN["account $"] == E]["B MTN contracts (notional x / IEF-equiv x)"].iloc[0].split()[0])
    spot = "\n".join(f"- **{r.trade}** (by hand from raw Yahoo files): {r.hand}. Simulation: {r.sim}. "
                     f"{'Matches.' if r.match else '**MISMATCH**'}" for r in SPOT.itertuples())
    cmp_rows = [("CAGR %", "CAGR", 1, "%"), ("Sharpe", "Sharpe", 2, ""), ("max DD %", "Max drawdown", 1, "%"),
                ("alpha %/yr", "Alpha vs S&P (%/yr)", 1, ""), ("beta", "Beta", 2, ""), ("B win %", "Leg B win rate", 1, "%"),
                ("B avg % acct", "Leg B avg net per trade (% of acct)", 3, "%"), ("final equity $", "Final equity", 0, "")]
    cmp_tab = "\n".join(
        f"| {lab} | **{f(hf(k), n)}{u}** | {f(hn(k), n)}{u} | {f(of(k), n)}{u} | {f(pv(E, 'A nearest', k), n)}{u} |"
        for k, lab, n, u in cmp_rows)
    rolls = ", ".join(f"{t} ({s:+.2f}%)" for t, s in zip(rl["dates"], rl["sizes"]))
    wg = "; ".join(f"{r.entry_date} {r.contract}: TN {r.tn_bp:+.0f}bp vs IEF {r.ief_bp:+.0f}bp"
                   for r in info["b_worst_gap"].itertuples())
    md = f"""# Month-end trader, small account: $18k with leg B in CME Micro Ultra 10-Year T-Note futures (MTN)

Generated by `simulate_small.py` (imports `../simulate.py` for data, the leg-A signal, the SPY core, cash interest and
stats, and `../../met` for the live bot's MTN contract choice, sizing function and `BOND_SPECS`; nothing there is
modified). Period **{eq.index[0].date()} to {eq.index[-1].date()}**, start equity **${E:,.0f}** unless stated. Daily
Yahoo data, whole contracts, IBKR-style costs. A simulation, not live fills. Replaces the previous run, which put leg B
in the Micro 10-Year *Yield* future (10Y); its results are kept below as a comparison column.

Files: `trades_18k.csv` (every trade, headline run: A floor rounding, B in MTN), `trades_18k_nearest.csv` (same, A
nearest), `yearly_18k.csv`, `periods_18k.csv`, `scenarios_small.csv` (all sizes and variants), `equity_18k.png`.

**Hypothetical periods.** MES exists since {MES_LAUNCH}, the Ultra 10-Year future (TN, MTN's underlying) since
{TN_START} and MTN since 2024 (entries before {MTN_LAUNCH} flagged `hypothetical`; I did not verify the exact 2024
launch day). Every leg-B trade before 2024 is a hypothetical MTN with today's specs, and every one before 2016 is also
priced without any Ultra 10-Year data (IEF model, synthetic price for sizing). Only 2024 onward could actually have
been traded this way.

## Headline: $18,000, base costs

| | MTN, A floor (headline) | MTN, A nearest | B in ZN instead (A floor) | S&P 500 TR |
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
| Leg B avg contracts, avg notional / net liq | {hf("B avg contracts"):.0f}, {hf("B avg lev x"):.2f}x | {hn("B avg contracts"):.0f}, {hn("B avg lev x"):.2f}x | | |
| Total costs (fees + slippage + SPY rebalancing) | ${hf("costs $"):,.0f} | ${hn("costs $"):,.0f} | ${zn("costs $"):,.0f} | |
| Contracts a fresh $18k account trades today | A: {CN[CN["account $"] == E]["A full floor (lev x)"].iloc[0]} full / {CN[CN["account $"] == E]["A half floor (lev x)"].iloc[0]} half size; B: **{nb18} MTN** | A: {CN[CN["account $"] == E]["A full nearest (lev x)"].iloc[0]} / {CN[CN["account $"] == E]["A half nearest (lev x)"].iloc[0]}; B: {nb18} MTN | B: 0 ZN | |

Stress costs (2 ticks per side on MES and 2/32 on MTN): A floor CAGR **{f(g(E, "MTN, A floor, stress costs", "CAGR %"))}%**
(Sharpe {f(g(E, "MTN, A floor, stress costs", "Sharpe"), 2)}), A nearest {f(g(E, "MTN, A nearest, stress costs", "CAGR %"))}%.
With 1/64 slippage instead of 1/32: {f(g(E, "MTN, A floor, 1/64 slippage", "CAGR %"))}% / {f(g(E, "MTN, A nearest, 1/64 slippage", "CAGR %"))}%.
Leg B priced on the IEF-minus-bills model for every trade (no TN=F): A floor {f(g(E, "MTN, A floor, B P&L = IEF-minus-bills model throughout", "CAGR %"))}%,
A nearest {f(g(E, "MTN, A nearest, B P&L = IEF-minus-bills model throughout", "CAGR %"))}%.

### Versus the previous run (leg B in the Micro 10-Year Yield future)

| $18k, base costs | MTN, A floor | MTN, A nearest | 10Y yield future, A floor (previous) | 10Y yield future, A nearest (previous) |
|---|---|---|---|---|
{cmp_tab}
| Leg B priced on | TN=F 2016+, IEF model before | same | ^TNX yield changes x DV01 | same |
| Leg B contracts for a fresh $18k account today | {nb18} MTN | {nb18} MTN | {PREV_10Y_TODAY_18K} | same |

Stress costs: {f(g(E, "MTN, A floor, stress costs", "CAGR %"))}% now vs {f(pv(E, "A floor, stress costs", "CAGR %"))}% before;
B on the IEF model throughout: {f(g(E, "MTN, A floor, B P&L = IEF-minus-bills model throughout", "CAGR %"))}% now vs
{f(pv(E, "A floor, B P&L = IEF-minus-bills model", "CAGR %"))}% before (the 10Y used duration-based sizing at 2.81x, MTN uses
2.8 x 0.885 and whole $10.8k contracts, so the two IEF-model runs are not identical). Why MTN beats the previous
headline: leg B earned {info["b_gross_bp"]:.1f}bp of notional gross per trade vs {info["prev_b_gross_bp"]:.1f}bp in the yield-future run
(^TNX missed carry and IEF's month-end basket effect; a *price* future is an excess-return bond position and tracks
the IEF model, see "Leg-B models" below). Against that, MTN costs more: leg-B fees + slippage ${info["b_costs"]:,.0f} vs
${info["prev_b_costs"]:,.0f}, and floor rounding plus the 400-contract cap ({info["n_capped"]} trades capped, all from
{info["first_capped"]}) leave {info["b_lev"][2]:.2f}x IEF-equivalent on average instead of 2.81x. Net: B averages
{f(hf("B avg % acct"), 3)}% of the account per trade vs {f(of("B avg % acct"), 3)}% before.

![equity](equity_18k.png)

## What changes versus `../simulate.py`

- **Leg B = long the Micro Ultra 10-Year T-Note future (MTN)**, $100 x price (~${now["mtn_notional"]:,.0f} per
  contract at {now["px"]:.3f} on {now["date"]}), cash settled, quarterly. Held from the close of the 4th-last session
  (bot: 15:58 ET) to the month-end close (bot: 15:55 ET, 12:50 on half days); at daily resolution both are closes.
- **Contract choice exactly as the bot** (`met.bond.contract_month_for(X, 5, "MTN")`, imported): the first quarterly
  contract whose last trade day (2 NYSE sessions before the first business day of the contract month) is more than 5
  sessions after the month-end exit date X. So Feb/May/Aug/Nov windows hold the contract *after* next (e.g. Nov 2026:
  {"MTN" + "FGHJKMNQUVXZ"[now["next_contract"][1] - 1] + str(now["next_contract"][0])[-1]}, last trade {now["next_contract"][2]}), all other months the next quarterly. Contracts held over
  the run: {", ".join(f"{k} {v}" for k, v in sorted(info["contracts_by_month"].items()))}. Contract and last trade day are in `trades_18k.csv`.
- **Sizing exactly as the bot** (`met.guardrails.size_contracts`, imported): contracts = floor({LEV_B} x {HEDGE} x
  net liq / (price x 100)), max {B_MAX_CONTRACTS}, notional <= {B_MAX_LEV}x net liq. Net liq at the entry close.
  Average {info["b_lev"][0]:.2f}x notional (min {info["b_lev"][1]:.2f}x) = {info["b_lev"][2]:.2f}x IEF-equivalent
  (target 2.80x). In 2003-2005 the $18k account held {early.min():.0f}-{early.max():.0f} MTN.
  The bot's config default `bond.max_contracts` is 20 (the MTN hard cap is 400); this backtest uses 400 as asked,
  so a live account above ~$90k must raise `bond.max_contracts`.
- **Price used for sizing:** TN=F close from {TN_START}; before that a synthetic price = price of a 6% coupon 9.5-year
  bond at the ^TNX yield x {info["theo_ratio"]:.3f} (median TN=F / theoretical ratio 2016-2026, 5-95% range
  {info["theo_ratio_rng"][0]:.3f}-{info["theo_ratio_rng"][1]:.3f}). It only sets the contract count (a few % error at most).
- **Leg A** and everything else (signal, rounding variants, 10% SPY drift band, cash interest, SPY rebalancing costs)
  is unchanged from the previous run.

## Leg-B models, rolls and data

- **Primary: TN=F.** Daily close-to-close returns of Yahoo's continuous Ultra 10-Year front month x MTN notional
  (position marked daily, P&L = contracts x $100 x price x return). Used for {bc["n"]} of {hf("B trades"):.0f} B trades
  (all whole-hold windows with TN=F data from {TN_START}; {bc["n_ief_post16"]} later windows fell back to IEF because of a missing TN=F close).
- **TN=F history:** Yahoo's file starts {info["tn_raw_first"]}, but the Ultra 10-Year contract only launched in
  January 2016; the {info["tn_rows_rejected"]} rows before that (2010-10 to 2012, with gaps of up to 3.7 years before 2016)
  price ~13% below what an Ultra 10 would have been (TN=F / theoretical ratio ~0.87 vs ~1.00-1.06 from 2016), so they are
  some other series and are **rejected**. Before {TN_START} leg B uses the IEF model only.
  {info["tn_missing"]} NYSE sessions from 2016 have no TN=F close (forward-filled up to 2 sessions).
- **Rolls.** Yahoo's TN=F rolls from the expiring to the next contract around the 15th-25th of Mar/Jun/Sep/Dec. A roll
  day is detected as the single largest |TN=F return - (IEF - bills)/0.885| residual in the 12th-25th of each contract
  month, if above {ROLL_SD:.0f} robust sd ({rl["sd"]*100:.3f}% here): {rl["n_flag"]} days flagged ({rolls}).
  On a flagged day the TN=F return is replaced by the IEF model's return (neutralised). **None of these falls inside a
  B hold** ({rl["on_hold"]} hold days flagged; trades with a neutralised day: {bc["n_roll_neutralised"]}): the hold is the last 3
  sessions of the month, while Yahoo rolls 1-2 weeks before contract-month end. Largest |residual| on any hold day
  {rl["hold_max"]:.2f}% (hold days with residual above 5 sd: {len(rl["hold_big"])}{": " + ", ".join(f"{t} {v:+.2f}%" for t, v in rl["hold_big"]) if rl["hold_big"] else ""}).
- **Contract mismatch.** In Feb/May/Aug/Nov the bot holds the contract after next while Yahoo's front is the
  nearby (about to go into delivery). Returns of adjacent TN contracts differ only by the change in the calendar spread
  over 3 days; on those windows the TN vs IEF per-trade correlation is {bc["qroll_corr"]:.3f} vs {bc["other_corr"]:.3f} in other
  months, mean gap {bc["qroll_diff"]:+.1f}bp vs {bc["other_diff"]:+.1f}bp. No contract-level history was available to do better.
- **Cross-check: IEF total return minus T-bills, / 0.885** (`gross_pnl_ief` for every trade). On the {bc["n"]} trades
  priced on TN=F: per-trade P&L correlation **{bc["corr"]:.3f}** (in bp of notional {bc["corr_bp"]:.3f}), same sign on
  {bc["sign_agree"]:.0f}%, tracking difference {bc["diff_bp_te"]:.1f}bp of notional per trade. Mean per trade: TN=F
  {bc["tn_bp_mean"]:+.1f}bp, IEF model {bc["ief_bp_mean"]:+.1f}bp, so the IEF model earns **{bc["diff_bp_mean"]:+.1f}bp**
  (s.e. {bc["diff_bp_se"]:.1f}bp, ${bc["diff_usd_mean"]:+,.0f} per trade at the run's sizes) more per hold
  ({bc["pre23_diff"]:+.1f}bp 2016-2022, {bc["post23_diff"]:+.1f}bp 2023+). Gross over these trades: TN=F
  ${bc["tn_gross"]:,.0f} vs IEF ${bc["ief_gross"]:,.0f}. Largest gaps: {wg}.
  Pre-2016 trades (IEF model only) averaged {bc["ief_bp_pre16"]:+.1f}bp of notional gross.
- **Why they differ:** TN=F's close is the CBOT settlement (~15:00 ET) while IEF closes at 16:00 and the bot trades at
  15:55-15:58, so each model misses a different last hour; IEF holds a 7-10y basket, TN's cheapest-to-deliver is a
  ~9.5-10y note; and 0.885 is a 2017-2026 average beta (re-estimated here on non-roll days: 2016+
  {info["hedge_2016"][0]:.3f} (corr {info["hedge_2016"][1]:.3f}), 2017+ {info["hedge_2017"][0]:.3f}). Unlike the yield
  future, a price future is an excess-return bond position, so carry is not a systematic difference here.

## Costs and margin assumptions

| Item | Base | Stress |
|---|---|---|
| MES fees (IBKR ~$0.25 + CME ~$0.35 + NFA ~$0.02) | $0.62 / contract / side | same |
| MES slippage | 1 tick = $1.25 / side | 2 ticks = $2.50 |
| MTN fees (bot's `BOND_SPECS` / IBKR all-in estimate) | **$0.60 / contract / side** | same |
| MTN slippage | 1 x 1/32 pt = **$3.125 / side** | 2/32 = $6.25 |
| MES initial margin | {MES_MARGIN_PCT:.1%} of notional (~${now["mes_notional"]*MES_MARGIN_PCT:,.0f} today) | x{MARGIN_STRESS_MULT:.0f} |
| MTN initial margin | {MTN_MARGIN_PCT:.0%} of notional (~${now["mtn_notional"]*MTN_MARGIN_PCT:,.0f} per contract today; my assumption) | x{MARGIN_STRESS_MULT:.0f} |
| Cash | positive earns max(^IRX - 0.5%, 0); negative (after funding the MTN overnight margin) pays ^IRX + 1.5% | same |

**Tick size.** The bot's FakeBroker uses 1/32 for MTN, so 1/32 is the base here. CME's Ultra 10-Year (TN) trades in
half-32nds (1/64 point); if MTN has the same minimum tick (I believe so but did not verify it), 1 x 1/32 is two real
ticks, and the "1/64 slippage" row is the one-real-tick case. An MTN round trip at base costs is
${2 * (SPEC["cost"] + MTN_TICK * MTN_MULT):.2f} per contract, ~{1e4 * 2 * (SPEC["cost"] + MTN_TICK * MTN_MULT) / now["mtn_notional"]:.1f}bp of notional
(vs ~0.9bp per side for ZN at 1/2 tick + $1.70 and ~0.09bp for the 10Y yield future): the micro is expensive per unit of
risk, and slippage, not the fee, is most of it. Total B costs therefore matter: compare the stress and 1/64 rows.

**Liquidity.** MTN is new and thin; at the close a few contracts should be fillable with limit orders near the mid,
but the 1-2 x 1/32 assumptions are untested against real MTN fills.

## Small-account realities

- **IBKR minimum** ($2,000) is met at every size; lowest equity in the $18k headline run ${hf("min netliq $"):,.0f}.
- **Margin.** Highest futures initial margin at any trade (A + open B on the last day), $18k base: **{mb[0]:.1f}% of net
  liq** (A floor), {mbn[0]:.1f}% (A nearest); trades above {MARGIN_FLAG:.0%}: **{mb[1]} / {mbn[1]}**. Across all sizes and
  variants the maximum was {worst_flag:.1f}%. With SPY's 25% maintenance added the worst total requirement was
  {mb[2]:.1f}% / {mbn[2]:.1f}%. With margins doubled the headline run peaks at {info["margin_hi"]["max"]:.1f}%
  ({info["margin_hi"]["flags"]} trades above {MARGIN_FLAG:.0%}). Net interest over the run ${g(E, "MTN, A floor", "interest $"):+,.0f}.
- **Rounding at today's levels** ({now["date"]}: S&P {now["spx"]:,.0f}, one MES = ${now["mes_notional"]:,.0f}, one MTN =
  ${now["mtn_notional"]:,.0f}): one MES at full size needs ${now["min_1_mes_full"]:,.0f} of equity, at half size
  ${now["min_1_mes_half_floor"]:,.0f} (floor) or ${now["min_1_mes_half_nearest"]:,.0f} (nearest, 3x cap); one MTN needs
  ${now["min_1_mtn"]:,.0f}.

{S.md_table(CN)}

**Months skipped by rounding at $18k.** Leg A floor: **{len(a_sk)}** of {len(a18)}
({", ".join(f"{k}: {v}" for k, v in a_sk_y.items()) if len(a_sk) else "none"}); leg A nearest: **{len(a_skn)}**;
leg B: **{int((b18.contracts == 0).sum())}**.

## Starting today: today's rounding held fixed

Started in 2003 the account compounds past the point where rounding matters. This scenario freezes today's contract
counts instead: MES leverage and MTN notional per $ of equity are what a fresh account of that size gets at today's
prices (table above), scaled with equity as it grows. Previous (10Y yield future) values in brackets.

| account | A floor: CAGR / Sharpe / max DD / alpha | A nearest: CAGR / Sharpe / max DD / alpha | A skipped (floor / nearest) |
|---|---|---|---|
""" + "\n".join(f"| ${a/1e3:,.0f}k | {td(a, 'floor', 'CAGR %'):.1f}% [{tdp(a, 'floor', 'CAGR %'):.1f}%] / {td(a, 'floor', 'Sharpe'):.2f} [{tdp(a, 'floor', 'Sharpe'):.2f}] / {td(a, 'floor', 'max DD %'):.1f}% / {td(a, 'floor', 'alpha %/yr'):.1f}% "
              f"| {td(a, 'nearest', 'CAGR %'):.1f}% [{tdp(a, 'nearest', 'CAGR %'):.1f}%] / {td(a, 'nearest', 'Sharpe'):.2f} [{tdp(a, 'nearest', 'Sharpe'):.2f}] / {td(a, 'nearest', 'max DD %'):.1f}% / {td(a, 'nearest', 'alpha %/yr'):.1f}% "
              f"| {td(a, 'floor', 'A skipped'):.0f} / {td(a, 'nearest', 'A skipped'):.0f} |" for a in ACCOUNT_SIZES) + f"""

{S.md_table(H[ctd].reset_index(drop=True))}

## All account sizes (base costs, B in MTN), started in 2003

Previous 10Y-yield-future CAGR, floor / nearest: """ + ", ".join(
        f"${a/1e3:,.0f}k {pv(a, 'A floor', 'CAGR %'):.1f}% / {pv(a, 'A nearest', 'CAGR %'):.1f}%" for a in ACCOUNT_SIZES) + f"""

{S.md_table(H[c_sizes].reset_index(drop=True))}

## $18k variants

{S.md_table(H[c18].reset_index(drop=True))}

## Yearly, $18k headline (A floor, base costs)

{S.md_table(yt)}

## In-sample vs holdout ($18k)

Previous run (10Y yield future), A floor: in-sample {PREV_10Y_PERIODS[("floor", "2003-2022 in-sample")]["CAGR %"]:.1f}% CAGR /
Sharpe {PREV_10Y_PERIODS[("floor", "2003-2022 in-sample")]["Sharpe"]:.2f}; 2023+ holdout
{PREV_10Y_PERIODS[("floor", "2023+ holdout")]["CAGR %"]:.1f}% / {PREV_10Y_PERIODS[("floor", "2023+ holdout")]["Sharpe"]:.2f}
(B avg {PREV_10Y_PERIODS[("floor", "2023+ holdout")]["B avg % acct"]:.3f}% of account per trade).

{S.md_table(SP)}

## Spot checks (trades recomputed by hand)

{spot}

## Caveats

- **In-sample.** Signals, the vol tilt and leverages were chosen on 2003-2022; only 2023+ is out of sample.
- **Hypothetical instruments.** MES before May 2019 and MTN before 2024 did not exist (TN itself not before 2016); those
  years use today's contract specs on index/ETF data.
- **Leg-B model risk.** TN=F settles ~15:00 ET, IEF closes 16:00, the bot trades 15:55-15:58; Yahoo's front contract is
  not the bot's contract in Feb/May/Aug/Nov; the IEF model / 0.885 is a fixed-beta proxy. The two models differ by
  {bc["diff_bp_te"]:.0f}bp of notional per trade (1 sd).
- **Costs.** 1/32 slippage on a ~$10.8k contract is ~2.9bp per side, large relative to a few-bp-per-day edge.
- **Leverage and gap risk.** Gross exposure on the last day: SPY 1x + MES up to 2.85x (3x nearest) + MTN ~2.5x notional.
  The bot's bond disaster stop (4% of equity) and decay monitor (de-risk/stop) are not modelled.
- **Taxes ignored.**
"""
    open(os.path.join(OUT, "summary.md"), "w").write(md)


if __name__ == "__main__":
    main()
