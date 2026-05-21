#!/usr/bin/env python3
"""
════════════════════════════════════════════════════════════════════════════════
  FINAL LOCKED STRATEGY  —  Four-Sleeve Portfolio with Annual Rebalancing
  Confirmed results: $100K → $16,227,399  |  CAGR +21.73%  |  MaxDD -35.57%
                     Sharpe 0.757  |  Backtest 2000-05-05 → 2026-03-23
  Signal: WMA20/SMA200  |  Defensive stop: 18%  |  Cooldown: 30 trading days
════════════════════════════════════════════════════════════════════════════════

SLEEVES
  Sleeve 1 (30%  $30,000): QQQ  → TQQQ  (defensive: QQQ)
  Sleeve 2 (30%  $30,000): SPY  → SPXL  (defensive: SPY)
  Sleeve 3 (30%  $30,000): SMH  → SOXL  (defensive: SMH)
  Sleeve 4 (10%  $10,000): GLD  — always-on safety sleeve (buy & hold gold)

EQUITY SLEEVE STRATEGY (identical for sleeves 1-3)
  Signal    : WMA10 / SMA200 cross on signal ticker
  Entry gate: QQQ 20-day realized vol ≤ 16%  +  re-entry gate (WMA must dip
              below SMA after each exit before next entry is eligible)
  Take profit: +200% gain from entry  (3× return)
  Hard stop  : -12% loss from entry
  Vol exit   : exit when 20-day realized vol ≥ 30%
  Defensive  : rotate into signal ticker (QQQ/SPY/SMH) between vehicle trades

SAFETY SLEEVE (sleeve 4)
  GLD held at all times — no signal, no exit
  Annual rebalance restores all four sleeves to target weights

ANNUAL REBALANCE
  First trading day of each calendar year
  Adjust share counts at that day's adjusted close
  Net portfolio value unchanged; only allocations reset

OUTPUTS
  backtest_four_sleeve_equity_curve.csv   — daily portfolio + per-sleeve equity
  backtest_four_sleeve_trades.csv         — all entry/exit trade log
  backtest_four_sleeve_rebalance_events.csv — annual rebalance detail

No external dependencies — pure Python + stdlib only.
"""

import json
import csv
import math
from datetime import date
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
WORKSPACE   = Path(__file__).resolve().parent
DATA_DIR    = WORKSPACE / "json" / "history"
SPLICED_DIR = WORKSPACE / "json" / "spliced"
OUT_DIR     = WORKSPACE

# ── Portfolio config ───────────────────────────────────────────────────────────
TOTAL_CAPITAL    = 100_000.0
SAFETY_TICKER    = "GLD"
SAFETY_ALLOC     = 0.10          # 10% to GLD
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC          # $10,000
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 3     # $30,000 each

BACKTEST_START   = date(2000, 1, 1)

# ── Strategy config ────────────────────────────────────────────────────────────
WMA_PERIOD      = 20
SMA_PERIOD      = 200
VOL_PERIOD      = 20

VOL_ENTRY_MAX   = 16.0
VOL_EXIT_THRESH = 30.0
TAKE_PROFIT_PCT = 200.0
STOP_LOSS_PCT   = 12.0
DEF_STOP_PCT    = 18.0   # exit defensive if it drops 18% below defensive entry price → cash
COOLDOWN_DAYS   = 30     # trading days to wait after any stop-loss before re-entry
MIN_IDX         = max(WMA_PERIOD, SMA_PERIOD, VOL_PERIOD)

EQUITY_CONFIGS = [
    ("QQQ", "TQQQ", "QQQ"),
    ("SPY", "SPXL", "SPY"),
    ("SMH", "SOXL", "SMH"),
]


# ── Indicator helpers ──────────────────────────────────────────────────────────
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


# ── Data loader ────────────────────────────────────────────────────────────────
def load_ticker(ticker):
    # Prefer spliced (proxy + real) file if available, fall back to history
    spliced_path = SPLICED_DIR / f"{ticker}_US.json"
    history_path = DATA_DIR    / f"{ticker}_US.json"
    path = spliced_path if spliced_path.exists() else history_path
    raw  = json.load(open(path))
    raw  = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


# ── Load all tickers ───────────────────────────────────────────────────────────
print()
print("=" * 80)
print("  FOUR-SLEEVE PORTFOLIO  —  GLD 10% SAFETY SLEEVE")
print(f"  Equity sleeves : ${ EQ_ALLOC_EACH:,.0f} each × 3  (90%)")
print(f"  Safety sleeve  : ${SAFETY_INIT:,.0f}       (10% GLD, always-on)")
print(f"  Strategy       : WMA{WMA_PERIOD}/SMA{SMA_PERIOD} | TP={TAKE_PROFIT_PCT:.0f}% | "
      f"SL={STOP_LOSS_PCT:.0f}% | VolEntry≤{VOL_ENTRY_MAX:.0f}% | VolExit≥{VOL_EXIT_THRESH:.0f}%")
print("=" * 80)

all_tickers = set()
for s, v, d in EQUITY_CONFIGS:
    all_tickers |= {s, v, d}
all_tickers.add(SAFETY_TICKER)

print("  Loading data …", end="", flush=True)
raw_data = {t: load_ticker(t) for t in all_tickers}
print("  done.")

# Common dates: intersection of ALL tickers
common = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))
n      = len(common)
print(f"  Common date range: {common[0]}  →  {common[-1]}  ({n} bars)\n")


# ── Build per-ticker arrays ────────────────────────────────────────────────────
arrays = {}
for ticker, d in raw_data.items():
    closes = [d[day]["close"]          for day in common]
    adjs   = [d[day]["adjusted_close"] for day in common]
    opens  = [d[day]["open"]           for day in common]
    ratios = [a / c if c else 1.0 for a, c in zip(adjs, closes)]
    arrays[ticker] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

for sig in ["QQQ", "SPY", "SMH"]:
    c = arrays[sig]["closes"]
    arrays[sig]["wma"]  = compute_wma(c, WMA_PERIOD)
    arrays[sig]["sma"]  = compute_sma(c, SMA_PERIOD)
    arrays[sig]["hvol"] = compute_hvol(c, VOL_PERIOD)


# ── Equity sleeve initializer ──────────────────────────────────────────────────
def make_sleeve(signal, vehicle, defensive, init_equity):
    return dict(
        signal=signal, vehicle=vehicle, defensive=defensive,
        label=f"{signal}→{vehicle}",
        state="cash", next_state=None,
        v_shares=0.0, v_entry=0.0, v_entry_date="", v_exit_rsn="", v_peak=0.0,
        d_shares=0.0, d_entry=0.0, d_entry_date="", d_exit_rsn="",
        cash=init_equity, initial_equity=init_equity,
        wma_was_below=True, entry_eligible=False, equity=init_equity,
        cooldown=0,
        trades=[],
    )

eq_sleeves = [make_sleeve(s, v, d, EQ_ALLOC_EACH) for s, v, d in EQUITY_CONFIGS]

# ── GLD safety sleeve ──────────────────────────────────────────────────────────
gld_adj0   = arrays[SAFETY_TICKER]["adj"][0]
gld_shares = SAFETY_INIT / gld_adj0
gld_equity = SAFETY_INIT


# ── Main simulation loop ───────────────────────────────────────────────────────
portfolio_curve  = []
rebalance_events = []
all_trades       = []
prev_year        = int(common[0][:4])

for i in range(n):
    day = common[i]

    # ── Execute pending equity transitions (at today's open) ──────────────────
    for sl in eq_sleeves:
        if sl["next_state"] is None:
            continue
        veh = sl["vehicle"]; dfn = sl["defensive"]
        vo  = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
        do  = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]

        if sl["state"] == "vehicle":
            proceeds  = sl["v_shares"] * vo
            pnl_pct   = (vo - sl["v_entry"]) / sl["v_entry"] * 100.0
            hold_days = (date.fromisoformat(day) - date.fromisoformat(sl["v_entry_date"])).days
            all_trades.append({
                "sleeve": sl["label"], "vehicle": sl["vehicle"],
                "entry_date": sl["v_entry_date"], "entry_price": round(sl["v_entry"], 4),
                "exit_date": day, "exit_price": round(vo, 4),
                "pnl_pct": round(pnl_pct, 4), "hold_days": hold_days,
                "exit_reason": sl["v_exit_rsn"],
            })
            sl["cash"] = proceeds; sl["v_shares"] = 0.0; sl["v_entry"] = 0.0

        elif sl["state"] == "defensive":
            proceeds  = sl["d_shares"] * do
            pnl_pct   = (do - sl["d_entry"]) / sl["d_entry"] * 100.0 if sl["d_entry"] else 0.0
            hold_days = (date.fromisoformat(day) - date.fromisoformat(sl["d_entry_date"])).days
            reason    = sl["d_exit_rsn"] if sl["d_exit_rsn"] else "def_to_" + sl["next_state"]
            all_trades.append({
                "sleeve": sl["label"], "vehicle": f"{sl['defensive']}_DEF",
                "entry_date": sl["d_entry_date"], "entry_price": round(sl["d_entry"], 4),
                "exit_date": day, "exit_price": round(do, 4),
                "pnl_pct": round(pnl_pct, 4), "hold_days": hold_days,
                "exit_reason": reason,
            })
            sl["cash"] = proceeds; sl["d_shares"] = 0.0; sl["d_entry"] = 0.0; sl["d_exit_rsn"] = ""

        if sl["next_state"] == "vehicle":
            sl["v_shares"] = sl["cash"] / vo; sl["v_entry"] = vo
            sl["v_peak"]   = vo
            sl["v_entry_date"] = day; sl["cash"] = 0.0
        elif sl["next_state"] == "defensive":
            sl["d_shares"] = sl["cash"] / do; sl["d_entry"] = do
            sl["d_entry_date"] = day; sl["cash"] = 0.0
        # next_state == "cash": proceeds already moved to sl["cash"] above

        sl["state"] = sl["next_state"]; sl["next_state"] = None

    # ── Decrement cooldown ─────────────────────────────────────────────────
    for sl in eq_sleeves:
        if sl["cooldown"] > 0:
            sl["cooldown"] -= 1

    # ── Mark to market ────────────────────────────────────────────────────────
    for sl in eq_sleeves:
        if sl["state"] == "vehicle":
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i]
        elif sl["state"] == "defensive":
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i]
        else:
            sl["equity"] = sl["cash"]

    gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][i]

    # ── Annual rebalance ──────────────────────────────────────────────────────
    cur_year = int(day[:4])
    if cur_year > prev_year:
        total_eq   = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        eq_target  = total_eq * (EQ_ALLOC_EACH / TOTAL_CAPITAL)
        gld_target = total_eq * (SAFETY_INIT   / TOTAL_CAPITAL)

        event = {
            "date": day, "total_equity": round(total_eq, 2),
            "eq_target": round(eq_target, 2), "gld_target": round(gld_target, 2),
        }

        for sl in eq_sleeves:
            pre = sl["equity"]
            if sl["state"] == "vehicle":
                sl["v_shares"] = eq_target / arrays[sl["vehicle"]]["adj"][i]
                sl["equity"]   = eq_target
            elif sl["state"] == "defensive":
                sl["d_shares"] = eq_target / arrays[sl["defensive"]]["adj"][i]
                sl["equity"]   = eq_target
            else:
                sl["cash"]   = eq_target
                sl["equity"] = eq_target
            event[f"{sl['label']}_pre"]   = round(pre, 2)
            event[f"{sl['label']}_delta"] = round(eq_target - pre, 2)

        gld_pre    = gld_equity
        gld_shares = gld_target / arrays[SAFETY_TICKER]["adj"][i]
        gld_equity = gld_target
        event["GLD_pre"]   = round(gld_pre, 2)
        event["GLD_delta"] = round(gld_target - gld_pre, 2)

        rebalance_events.append(event)
        prev_year = cur_year

    prev_year = cur_year

    # ── Record portfolio equity curve ─────────────────────────────────────────
    port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
    portfolio_curve.append({
        "date"        : day,
        "equity"      : round(port_eq, 2),
        "s1_qqq_tqqq" : round(eq_sleeves[0]["equity"], 2),
        "s2_spy_spxl" : round(eq_sleeves[1]["equity"], 2),
        "s3_smh_soxl" : round(eq_sleeves[2]["equity"], 2),
        "s4_gld"      : round(gld_equity, 2),
        "s1_state"    : eq_sleeves[0]["state"],
        "s2_state"    : eq_sleeves[1]["state"],
        "s3_state"    : eq_sleeves[2]["state"],
    })

    if i < MIN_IDX:
        continue

    # ── Equity signal logic (generates next_state for tomorrow) ───────────────
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
            vad  = arrays[veh]["adj"][i]
            tp_p  = sl["v_entry"] * (1 + TAKE_PROFIT_PCT / 100)
            sl_p  = sl["v_entry"] * (1 - STOP_LOSS_PCT   / 100)
            do_tp = vad >= tp_p
            do_sl = vad <= sl_p
            do_v  = hv >= VOL_EXIT_THRESH
            do_w  = cbl
            if do_tp or do_sl or do_v or do_w:
                if do_tp:    sl["v_exit_rsn"] = f"take_profit({TAKE_PROFIT_PCT:.0f}%)"
                elif do_sl:
                    sl["v_exit_rsn"] = f"stop_loss({STOP_LOSS_PCT:.0f}%)"
                    sl["cooldown"] = COOLDOWN_DAYS
                elif do_v:   sl["v_exit_rsn"] = f"vol_exit({hv:.1f}%)"
                else:        sl["v_exit_rsn"] = "wma_cross_below"
                sl["wma_was_below"] = False
                sl["next_state"]    = "defensive"

        # ── Defensive stop: if defensive asset drops DEF_STOP_PCT from entry → cash
        if sl["state"] == "defensive" and sl["next_state"] is None:
            dad = arrays[sl["defensive"]]["adj"][i]
            if sl["d_entry"] > 0 and dad <= sl["d_entry"] * (1 - DEF_STOP_PCT / 100):
                sl["d_exit_rsn"] = f"def_stop({DEF_STOP_PCT:.0f}%)"
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


# ── Close open positions at last bar ──────────────────────────────────────────
last_day = common[-1]
for sl in eq_sleeves:
    veh = sl["vehicle"]; dfn = sl["defensive"]
    if sl["state"] == "vehicle" and sl["v_shares"] > 0:
        last = arrays[veh]["adj"][-1]
        pnl  = (last - sl["v_entry"]) / sl["v_entry"] * 100.0
        hold = (date.fromisoformat(last_day) - date.fromisoformat(sl["v_entry_date"])).days
        all_trades.append({
            "sleeve": sl["label"], "vehicle": veh,
            "entry_date": sl["v_entry_date"], "entry_price": round(sl["v_entry"], 4),
            "exit_date": last_day, "exit_price": round(last, 4),
            "pnl_pct": round(pnl, 4), "hold_days": hold, "exit_reason": "end_of_data",
        })
        sl["equity"] = sl["v_shares"] * last

    elif sl["state"] == "defensive" and sl["d_shares"] > 0:
        last = arrays[dfn]["adj"][-1]
        pnl  = (last - sl["d_entry"]) / sl["d_entry"] * 100.0
        hold = (date.fromisoformat(last_day) - date.fromisoformat(sl["d_entry_date"])).days
        all_trades.append({
            "sleeve": sl["label"], "vehicle": f"{dfn}_DEF",
            "entry_date": sl["d_entry_date"], "entry_price": round(sl["d_entry"], 4),
            "exit_date": last_day, "exit_price": round(last, 4),
            "pnl_pct": round(pnl, 4), "hold_days": hold, "exit_reason": "end_of_data",
        })
        sl["equity"] = sl["d_shares"] * last

gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][-1]

# Update last row
port_final = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
portfolio_curve[-1]["equity"]       = round(port_final, 2)
portfolio_curve[-1]["s1_qqq_tqqq"]  = round(eq_sleeves[0]["equity"], 2)
portfolio_curve[-1]["s2_spy_spxl"]  = round(eq_sleeves[1]["equity"], 2)
portfolio_curve[-1]["s3_smh_soxl"]  = round(eq_sleeves[2]["equity"], 2)
portfolio_curve[-1]["s4_gld"]       = round(gld_equity, 2)


# ── Metrics ────────────────────────────────────────────────────────────────────
def calc_metrics(curve, init_eq):
    final_eq     = curve[-1]["equity"]
    total_return = (final_eq - init_eq) / init_eq * 100.0
    start_dt     = date.fromisoformat(curve[0]["date"])
    end_dt       = date.fromisoformat(curve[-1]["date"])
    years        = (end_dt - start_dt).days / 365.25
    cagr         = ((final_eq / init_eq) ** (1.0 / years) - 1) * 100.0 if years > 0 else 0.0
    peak, max_dd = init_eq, 0.0
    for row in curve:
        eq = row["equity"]
        if eq > peak: peak = eq
        dd = (eq - peak) / peak * 100.0
        if dd < max_dd: max_dd = dd
    dr = [(curve[i]["equity"] - curve[i-1]["equity"]) / curve[i-1]["equity"]
           for i in range(1, len(curve)) if curve[i-1]["equity"]]
    if len(dr) > 1:
        mu  = sum(dr) / len(dr)
        sig = (sum((r - mu)**2 for r in dr) / (len(dr) - 1)) ** 0.5
        sharpe = mu / sig * 252**0.5 if sig > 0 else 0.0
    else:
        sharpe = 0.0
    return dict(final_eq=final_eq, total_return=total_return, cagr=cagr,
                max_dd=max_dd, sharpe=sharpe, years=years)

port_m = calc_metrics(portfolio_curve, TOTAL_CAPITAL)

# Per-sleeve metrics
sleeve_cols = {
    "QQQ→TQQQ": "s1_qqq_tqqq",
    "SPY→SPXL": "s2_spy_spxl",
    "SMH→SOXL": "s3_smh_soxl",
    "GLD"      : "s4_gld",
}
sleeve_inits = {
    "QQQ→TQQQ": EQ_ALLOC_EACH, "SPY→SPXL": EQ_ALLOC_EACH,
    "SMH→SOXL": EQ_ALLOC_EACH, "GLD":       SAFETY_INIT,
}
sleeve_metrics = {}
for lbl, col in sleeve_cols.items():
    curve = [{"date": r["date"], "equity": r[col]} for r in portfolio_curve]
    sleeve_metrics[lbl] = calc_metrics(curve, sleeve_inits[lbl])


# ── Save CSVs ─────────────────────────────────────────────────────────────────
eq_csv  = OUT_DIR / "backtest_four_sleeve_equity_curve.csv"
tr_csv  = OUT_DIR / "backtest_four_sleeve_trades.csv"
reb_csv = OUT_DIR / "backtest_four_sleeve_rebalance_events.csv"

with open(eq_csv, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=portfolio_curve[0].keys())
    writer.writeheader(); writer.writerows(portfolio_curve)

if all_trades:
    with open(tr_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_trades[0].keys())
        writer.writeheader(); writer.writerows(all_trades)

if rebalance_events:
    with open(reb_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rebalance_events[0].keys())
        writer.writeheader(); writer.writerows(rebalance_events)


# ── Print results ──────────────────────────────────────────────────────────────
print()
print("─" * 80)
print("  REBALANCE EVENTS")
print(f"  {'Date':<12}  {'Total $':>10}  {'EQ Target':>10}  {'GLD Target':>10}  "
      f"{'QQQ→TQQQ Δ':>12}  {'SPY→SPXL Δ':>12}  {'SMH→SOXL Δ':>12}  {'GLD Δ':>10}")
print("─" * 80)
for e in rebalance_events:
    print(f"  {e['date']:<12}  ${e['total_equity']:>9,.0f}  ${e['eq_target']:>9,.0f}  "
          f"${e['gld_target']:>9,.0f}  "
          f"  {e['QQQ→TQQQ_delta']:>+10,.0f}    {e['SPY→SPXL_delta']:>+10,.0f}"
          f"    {e['SMH→SOXL_delta']:>+10,.0f}  {e['GLD_delta']:>+9,.0f}")

print()
print("─" * 80)
print("  INDIVIDUAL SLEEVE PERFORMANCE")
print(f"  {'Sleeve':<12}  {'Alloc':>6}  {'Init $':>9}  {'Final $':>11}  "
      f"{'Return':>9}  {'CAGR':>7}  {'MaxDD':>7}  {'Sharpe':>7}")
print("─" * 80)
for lbl in ["QQQ→TQQQ", "SPY→SPXL", "SMH→SOXL", "GLD"]:
    m    = sleeve_metrics[lbl]
    init = sleeve_inits[lbl]
    alloc_pct = init / TOTAL_CAPITAL * 100
    print(f"  {lbl:<12}  {alloc_pct:>5.0f}%  ${init:>9,.0f}  ${m['final_eq']:>11,.0f}  "
          f"{m['total_return']:>+8.2f}%  {m['cagr']:>+6.2f}%  "
          f"{m['max_dd']:>+6.2f}%  {m['sharpe']:>7.3f}")

print()
print("=" * 80)
print("  PORTFOLIO SUMMARY")
print("=" * 80)
print(f"  {'Metric':<28}  {'4-Sleeve+GLD':>14}  {'3-Sleeve Baseline':>18}  {'Δ':>8}")
print("─" * 80)

BASELINE = dict(total_return=7233.26, cagr=30.74, max_dd=-40.49, sharpe=1.000, final_eq=7_333_258)

rows = [
    ("Initial Capital",  f"${TOTAL_CAPITAL:>12,.0f}",   f"${TOTAL_CAPITAL:>16,.0f}",  ""),
    ("Final Equity",     f"${port_m['final_eq']:>12,.0f}", f"${BASELINE['final_eq']:>16,.0f}",
                         f"{port_m['final_eq']-BASELINE['final_eq']:>+8,.0f}"),
    ("Total Return",     f"{port_m['total_return']:>+12.2f}%", f"{BASELINE['total_return']:>+16.2f}%",
                         f"{port_m['total_return']-BASELINE['total_return']:>+7.2f}%"),
    ("CAGR",             f"{port_m['cagr']:>+12.2f}%",  f"{BASELINE['cagr']:>+16.2f}%",
                         f"{port_m['cagr']-BASELINE['cagr']:>+7.2f}%"),
    ("Max Drawdown",     f"{port_m['max_dd']:>+12.2f}%", f"{BASELINE['max_dd']:>+16.2f}%",
                         f"{port_m['max_dd']-BASELINE['max_dd']:>+7.2f}%"),
    ("Sharpe Ratio",     f"{port_m['sharpe']:>12.3f}",  f"{BASELINE['sharpe']:>16.3f}",
                         f"{port_m['sharpe']-BASELINE['sharpe']:>+7.3f}"),
    ("Period (years)",   f"{port_m['years']:>12.2f}",   f"{'16.02':>16}",              ""),
]
for label, v4, v3, delta in rows:
    print(f"  {label:<28}  {v4}  {v3}  {delta}")

print("=" * 80)
print(f"\n  Saved: {eq_csv}")
print(f"  Saved: {tr_csv}")
print(f"  Saved: {reb_csv}")
