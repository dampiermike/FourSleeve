#!/usr/bin/env python3
"""
Three-Sleeve backtest:  QQQ + SPY + XLK + GLD  (30/30/30/10)
Each equity sleeve uses its per-sleeve-optimum WMA/SMA:
  QQQ -> TQQQ : WMA=10 SMA=175
  SPY -> SPXL : WMA= 5 SMA=200
  DIA -> UDOW : WMA=50 SMA=200   (synthetic pre-2010-02-11)

Compared to:
  Two-Sleeve Opt (no Dow)        $35,448,247  +25.10%  -37.99%  Sharpe 0.8373
  TwoSleeves 20/200 target       $33,858,674  +24.88%  -39.65%  Sharpe 0.8209
  FourSleeve original 20/200     $16,227,399  +21.73%  -35.57%  Sharpe 0.7570
"""

import json
import math
from datetime import date
from pathlib import Path

WORKSPACE   = Path(__file__).resolve().parent
DATA_DIR    = WORKSPACE / "json" / "history"
SPLICED_DIR = WORKSPACE / "json" / "spliced"

TOTAL_CAPITAL    = 100_000.0
SAFETY_TICKER    = "GLD"
SAFETY_ALLOC     = 0.10
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC          # $10,000
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 3     # $30,000 each
BACKTEST_START   = date(2000, 1, 1)

VOL_PERIOD       = 20
VOL_ENTRY_MAX    = 16.0
VOL_EXIT_THRESH  = 30.0
TAKE_PROFIT_PCT  = 200.0
STOP_LOSS_PCT    = 12.0
DEF_STOP_PCT     = 18.0
COOLDOWN_DAYS    = 30

EQUITY_CONFIGS = [
    # (signal, vehicle, defensive, wma, sma)
    ("QQQ", "TQQQ", "QQQ", 10, 175),
    ("SPY", "SPXL", "SPY",  5, 200),
    ("XLK", "TECL", "XLK",  5, 250),
]


def compute_hvol(closes, window):
    n, out = len(closes), [None] * len(closes)
    for i in range(window, n):
        lr   = [math.log(closes[j] / closes[j-1]) for j in range(i - window + 1, i + 1)]
        mean = sum(lr) / window
        var  = sum((r - mean)**2 for r in lr) / (window - 1)
        out[i] = math.sqrt(var * 252) * 100.0
    return out

def compute_wma(closes, period):
    n, out = len(closes), [None] * len(closes)
    denom  = period * (period + 1) / 2
    for i in range(period - 1, n):
        out[i] = sum(closes[i - period + 1 + j] * (j + 1) for j in range(period)) / denom
    return out

def compute_sma(closes, period):
    n, out = len(closes), [None] * len(closes)
    s = sum(closes[:period]); out[period - 1] = s / period
    for i in range(period, n):
        s += closes[i] - closes[i - period]; out[i] = s / period
    return out


def load_ticker(ticker):
    spliced_path = SPLICED_DIR / f"{ticker}_US.json"
    history_path = DATA_DIR    / f"{ticker}_US.json"
    path = spliced_path if spliced_path.exists() else history_path
    raw  = json.load(open(path))
    raw  = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


all_tickers = {SAFETY_TICKER}
for s, v, d, _, _ in EQUITY_CONFIGS:
    all_tickers |= {s, v, d}

print("Loading data...", flush=True)
raw_data = {t: load_ticker(t) for t in all_tickers}
common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))
n = len(common)
print(f"  {n} bars  {common[0]} -> {common[-1]}\n")

arrays = {}
for ticker, d in raw_data.items():
    closes = [d[day]["close"]          for day in common]
    adjs   = [d[day]["adjusted_close"] for day in common]
    opens  = [d[day]["open"]           for day in common]
    ratios = [a / c if c else 1.0 for a, c in zip(adjs, closes)]
    arrays[ticker] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

for s, v, d, wma, sma in EQUITY_CONFIGS:
    c = arrays[s]["closes"]
    arrays[s]["wma"]  = compute_wma(c, wma)
    arrays[s]["sma"]  = compute_sma(c, sma)
    arrays[s]["hvol"] = compute_hvol(c, VOL_PERIOD)


def make_sleeve(signal, vehicle, defensive, wma, sma):
    return dict(
        signal=signal, vehicle=vehicle, defensive=defensive,
        wma_period=wma, sma_period=sma, label=f"{signal}->{vehicle}",
        state="cash", next_state=None,
        v_shares=0.0, v_entry=0.0, v_entry_date="", v_exit_rsn="",
        d_shares=0.0, d_entry=0.0, d_entry_date="", d_exit_rsn="",
        cash=EQ_ALLOC_EACH, equity=EQ_ALLOC_EACH,
        wma_was_below=True, entry_eligible=False, cooldown=0,
        trade_count=0,
    )

eq_sleeves = [make_sleeve(*cfg) for cfg in EQUITY_CONFIGS]
gld_adj0   = arrays[SAFETY_TICKER]["adj"][0]
gld_shares = SAFETY_INIT / gld_adj0
gld_equity = SAFETY_INIT
portfolio_curve  = []
sleeve_eq_curves = {sl["label"]: [] for sl in eq_sleeves}
gld_curve        = []
prev_year = int(common[0][:4])

min_idx = max(VOL_PERIOD,
              max(sl["wma_period"] for sl in eq_sleeves),
              max(sl["sma_period"] for sl in eq_sleeves))

for i in range(n):
    day = common[i]

    for sl in eq_sleeves:
        if sl["next_state"] is None:
            continue
        veh = sl["vehicle"]; dfn = sl["defensive"]
        vo = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
        do = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]

        if sl["state"] == "vehicle":
            sl["cash"] = sl["v_shares"] * vo
            sl["trade_count"] += 1
            sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
        elif sl["state"] == "defensive":
            sl["cash"] = sl["d_shares"] * do
            sl["trade_count"] += 1
            sl["d_shares"] = 0.0; sl["d_entry"] = 0.0

        if sl["next_state"] == "vehicle":
            sl["v_shares"] = sl["cash"] / vo; sl["v_entry"] = vo
            sl["v_entry_date"] = day; sl["cash"] = 0.0
        elif sl["next_state"] == "defensive":
            sl["d_shares"] = sl["cash"] / do; sl["d_entry"] = do
            sl["d_entry_date"] = day; sl["cash"] = 0.0

        sl["state"] = sl["next_state"]; sl["next_state"] = None

    for sl in eq_sleeves:
        if sl["cooldown"] > 0:
            sl["cooldown"] -= 1

    for sl in eq_sleeves:
        if sl["state"] == "vehicle":
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i]
        elif sl["state"] == "defensive":
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i]
        else:
            sl["equity"] = sl["cash"]

    gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][i]

    cur_year = int(day[:4])
    if cur_year > prev_year:
        total_eq   = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        eq_target  = total_eq * (EQ_ALLOC_EACH / TOTAL_CAPITAL)
        gld_target = total_eq * (SAFETY_INIT   / TOTAL_CAPITAL)

        for sl in eq_sleeves:
            if sl["state"] == "vehicle":
                sl["v_shares"] = eq_target / arrays[sl["vehicle"]]["adj"][i]
                sl["equity"]   = eq_target
            elif sl["state"] == "defensive":
                sl["d_shares"] = eq_target / arrays[sl["defensive"]]["adj"][i]
                sl["equity"]   = eq_target
            else:
                sl["cash"]   = eq_target
                sl["equity"] = eq_target

        gld_shares = gld_target / arrays[SAFETY_TICKER]["adj"][i]
        gld_equity = gld_target

    prev_year = cur_year

    port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
    portfolio_curve.append(port_eq)
    for sl in eq_sleeves:
        sleeve_eq_curves[sl["label"]].append(sl["equity"])
    gld_curve.append(gld_equity)

    if i < min_idx:
        continue

    for sl in eq_sleeves:
        sig = sl["signal"]; veh = sl["vehicle"]
        wa  = arrays[sig]["wma"]; sa = arrays[sig]["sma"]; hva = arrays[sig]["hvol"]
        if any(v is None for v in [wa[i], sa[i], wa[i-1], sa[i-1]]):
            continue

        w, wp = wa[i], wa[i-1]; s, sp = sa[i], sa[i-1]
        hv    = hva[i] if hva[i] is not None else 0.0
        cab   = wp <= sp and w > s
        cbl   = wp >= sp and w < s

        if sl["state"] == "vehicle" and sl["next_state"] is None:
            vad   = arrays[veh]["adj"][i]
            do_tp = vad >= sl["v_entry"] * (1 + TAKE_PROFIT_PCT / 100)
            do_sl = vad <= sl["v_entry"] * (1 - STOP_LOSS_PCT   / 100)
            do_v  = hv >= VOL_EXIT_THRESH
            do_w  = cbl
            if do_tp or do_sl or do_v or do_w:
                if do_sl:
                    sl["cooldown"] = COOLDOWN_DAYS
                sl["wma_was_below"] = False
                sl["next_state"]    = "defensive"

        if sl["state"] == "defensive" and sl["next_state"] is None:
            dad = arrays[sl["defensive"]]["adj"][i]
            if sl["d_entry"] > 0 and dad <= sl["d_entry"] * (1 - DEF_STOP_PCT / 100):
                sl["cooldown"]   = COOLDOWN_DAYS
                sl["next_state"] = "cash"

        if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
            if w < s: sl["wma_was_below"] = True; sl["entry_eligible"] = False
            if cab and sl["wma_was_below"]: sl["entry_eligible"] = True; sl["wma_was_below"] = False
            if sl["entry_eligible"] and w < s: sl["entry_eligible"] = False; sl["wma_was_below"] = True

        if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
            if (sl["entry_eligible"] and hv <= VOL_ENTRY_MAX
                    and w > s and i + 1 < n and sl["cooldown"] == 0):
                sl["next_state"] = "vehicle"
                sl["entry_eligible"] = False; sl["wma_was_below"] = False


# Final mark-to-market
for sl in eq_sleeves:
    if sl["state"] == "vehicle":
        sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][-1]
    elif sl["state"] == "defensive":
        sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][-1]
gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][-1]
portfolio_curve[-1] = sum(sl["equity"] for sl in eq_sleeves) + gld_equity


# Metrics
final_eq = portfolio_curve[-1]
years = (date.fromisoformat(common[-1]) - date.fromisoformat(common[0])).days / 365.25
cagr  = ((final_eq / TOTAL_CAPITAL) ** (1.0 / years) - 1) * 100.0

peak = TOTAL_CAPITAL; max_dd = 0.0
for eq in portfolio_curve:
    if eq > peak: peak = eq
    dd = (eq - peak) / peak * 100.0
    if dd < max_dd: max_dd = dd

dr = [(portfolio_curve[i] - portfolio_curve[i-1]) / portfolio_curve[i-1]
       for i in range(1, len(portfolio_curve)) if portfolio_curve[i-1]]
mu  = sum(dr) / len(dr)
sig = (sum((r - mu)**2 for r in dr) / (len(dr) - 1)) ** 0.5
sharpe = mu / sig * 252**0.5 if sig > 0 else 0.0

trades = sum(sl["trade_count"] for sl in eq_sleeves)


# Print results
print("=" * 92)
print("  THREE-SLEEVE (QQQ + SPY + XLK + GLD)  with per-sleeve optimum WMA/SMA")
print("=" * 92)
for sl in eq_sleeves:
    print(f"  {sl['label']:<14} WMA={sl['wma_period']:>2} / SMA={sl['sma_period']:>3}   "
          f"alloc ${EQ_ALLOC_EACH:>10,.0f} (30%)")
print(f"  GLD            buy & hold          alloc ${SAFETY_INIT:>10,.0f} (10%)")
print()
print(f"  Period:   {common[0]} -> {common[-1]} ({years:.2f} years, {n} bars)")
print(f"  Trades:   {trades}")
print()

print("─" * 92)
print(f"  {'Metric':<22} {'Three-Sleeve':>14} {'Two-Sleeve Opt':>16} {'FourSleeve 20/200':>20}")
print("─" * 92)
TS_OPT = dict(final_eq=35_448_247, cagr=25.10, max_dd=-37.99, sharpe=0.8373)
FS_BL  = dict(final_eq=16_227_399, cagr=21.73, max_dd=-35.57, sharpe=0.757)

print(f"  {'Final equity':<22} ${final_eq:>13,.0f} ${TS_OPT['final_eq']:>15,.0f} ${FS_BL['final_eq']:>19,.0f}")
print(f"  {'CAGR':<22} {cagr:>+13.2f}% {TS_OPT['cagr']:>+15.2f}% {FS_BL['cagr']:>+19.2f}%")
print(f"  {'Max Drawdown':<22} {max_dd:>+13.2f}% {TS_OPT['max_dd']:>+15.2f}% {FS_BL['max_dd']:>+19.2f}%")
print(f"  {'Sharpe':<22} {sharpe:>14.4f} {TS_OPT['sharpe']:>16.4f} {FS_BL['sharpe']:>20.3f}")
print()

# Per-sleeve final equity contribution
print("─" * 92)
print("  PER-SLEEVE FINAL EQUITY (after annual rebalance reshuffles)")
print("─" * 92)
for sl in eq_sleeves:
    pct = sl["equity"] / final_eq * 100
    print(f"  {sl['label']:<14}  ${sl['equity']:>13,.0f}  ({pct:>5.1f}% of portfolio)")
print(f"  {'GLD':<14}  ${gld_equity:>13,.0f}  ({gld_equity/final_eq*100:>5.1f}% of portfolio)")
print(f"  {'TOTAL':<14}  ${final_eq:>13,.0f}")
