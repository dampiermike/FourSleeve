#!/usr/bin/env python3
"""
WMA/SMA parameter sweep for the FourSleeve strategy.

Loads data once, then runs the full backtest for every (WMA, SMA) combo
in the grid below. Holds all other parameters constant. Writes
sweep_results.csv with metrics for each combo and prints a top-10 table
sorted by CAGR / |MaxDD| (risk-adjusted return).
"""

import json
import csv
import math
import time
from datetime import date
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
WORKSPACE   = Path(__file__).resolve().parent
DATA_DIR    = WORKSPACE / "json" / "history"
SPLICED_DIR = WORKSPACE / "json" / "spliced"

# ── Held-constant parameters ──────────────────────────────────────────────────
TOTAL_CAPITAL    = 100_000.0
SAFETY_TICKER    = "GLD"
SAFETY_ALLOC     = 0.10
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 3
BACKTEST_START   = date(2000, 1, 1)

VOL_PERIOD       = 20
VOL_ENTRY_MAX    = 16.0
VOL_EXIT_THRESH  = 30.0
TAKE_PROFIT_PCT  = 200.0
STOP_LOSS_PCT    = 12.0
DEF_STOP_PCT     = 18.0
COOLDOWN_DAYS    = 30

EQUITY_CONFIGS = [
    ("QQQ", "TQQQ", "QQQ"),
    ("SPY", "SPXL", "SPY"),
    ("SMH", "SOXL", "SMH"),
]

# ── Sweep grid ────────────────────────────────────────────────────────────────
WMA_GRID = [5, 10, 15, 20, 25, 30, 40, 50]
SMA_GRID = [100, 125, 150, 175, 200, 225, 250]


# ── Indicator helpers ─────────────────────────────────────────────────────────
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


# ── Data loading (once) ───────────────────────────────────────────────────────
def load_ticker(ticker):
    spliced_path = SPLICED_DIR / f"{ticker}_US.json"
    history_path = DATA_DIR    / f"{ticker}_US.json"
    path = spliced_path if spliced_path.exists() else history_path
    raw  = json.load(open(path))
    raw  = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


def build_arrays():
    all_tickers = set()
    for s, v, d in EQUITY_CONFIGS:
        all_tickers |= {s, v, d}
    all_tickers.add(SAFETY_TICKER)

    raw_data = {t: load_ticker(t) for t in all_tickers}
    common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))

    arrays = {}
    for ticker, d in raw_data.items():
        closes = [d[day]["close"]          for day in common]
        adjs   = [d[day]["adjusted_close"] for day in common]
        opens  = [d[day]["open"]           for day in common]
        ratios = [a / c if c else 1.0 for a, c in zip(adjs, closes)]
        arrays[ticker] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

    # HVol is independent of WMA/SMA, compute once
    for sig in ["QQQ", "SPY", "SMH"]:
        arrays[sig]["hvol"] = compute_hvol(arrays[sig]["closes"], VOL_PERIOD)

    return arrays, common


# ── Simulation (one run) ──────────────────────────────────────────────────────
def make_sleeve(signal, vehicle, defensive, init_equity):
    return dict(
        signal=signal, vehicle=vehicle, defensive=defensive,
        label=f"{signal}->{vehicle}",
        state="cash", next_state=None,
        v_shares=0.0, v_entry=0.0, v_entry_date="", v_exit_rsn="",
        d_shares=0.0, d_entry=0.0, d_entry_date="", d_exit_rsn="",
        cash=init_equity, initial_equity=init_equity,
        wma_was_below=True, entry_eligible=False, equity=init_equity,
        cooldown=0,
    )


def run_backtest(arrays, common, wma_period, sma_period):
    n = len(common)
    min_idx = max(wma_period, sma_period, VOL_PERIOD)

    # Compute WMA/SMA for this combo
    for sig in ["QQQ", "SPY", "SMH"]:
        c = arrays[sig]["closes"]
        arrays[sig]["wma"] = compute_wma(c, wma_period)
        arrays[sig]["sma"] = compute_sma(c, sma_period)

    eq_sleeves = [make_sleeve(s, v, d, EQ_ALLOC_EACH) for s, v, d in EQUITY_CONFIGS]
    gld_adj0   = arrays[SAFETY_TICKER]["adj"][0]
    gld_shares = SAFETY_INIT / gld_adj0
    gld_equity = SAFETY_INIT

    portfolio_curve = []
    trade_count     = 0
    prev_year       = int(common[0][:4])

    for i in range(n):
        day = common[i]

        # ── Execute pending equity transitions (at today's open) ──────────────
        for sl in eq_sleeves:
            if sl["next_state"] is None:
                continue
            veh = sl["vehicle"]; dfn = sl["defensive"]
            vo  = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
            do  = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]

            if sl["state"] == "vehicle":
                proceeds = sl["v_shares"] * vo
                trade_count += 1
                sl["cash"] = proceeds; sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
            elif sl["state"] == "defensive":
                proceeds = sl["d_shares"] * do
                trade_count += 1
                sl["cash"] = proceeds; sl["d_shares"] = 0.0; sl["d_entry"] = 0.0
                sl["d_exit_rsn"] = ""

            if sl["next_state"] == "vehicle":
                sl["v_shares"] = sl["cash"] / vo; sl["v_entry"] = vo
                sl["v_entry_date"] = day; sl["cash"] = 0.0
            elif sl["next_state"] == "defensive":
                sl["d_shares"] = sl["cash"] / do; sl["d_entry"] = do
                sl["d_entry_date"] = day; sl["cash"] = 0.0

            sl["state"] = sl["next_state"]; sl["next_state"] = None

        # ── Cooldown decrement ────────────────────────────────────────────────
        for sl in eq_sleeves:
            if sl["cooldown"] > 0:
                sl["cooldown"] -= 1

        # ── Mark to market ────────────────────────────────────────────────────
        for sl in eq_sleeves:
            if sl["state"] == "vehicle":
                sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i]
            elif sl["state"] == "defensive":
                sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i]
            else:
                sl["equity"] = sl["cash"]

        gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][i]

        # ── Annual rebalance ──────────────────────────────────────────────────
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

        # ── Record portfolio equity ───────────────────────────────────────────
        port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        portfolio_curve.append(port_eq)

        if i < min_idx:
            continue

        # ── Signal logic ──────────────────────────────────────────────────────
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
                tp_p  = sl["v_entry"] * (1 + TAKE_PROFIT_PCT / 100)
                sl_p  = sl["v_entry"] * (1 - STOP_LOSS_PCT   / 100)
                do_tp = vad >= tp_p
                do_sl = vad <= sl_p
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

    # Close open positions at last bar (mark-to-market only)
    for sl in eq_sleeves:
        if sl["state"] == "vehicle" and sl["v_shares"] > 0:
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][-1]
        elif sl["state"] == "defensive" and sl["d_shares"] > 0:
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][-1]
    gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][-1]
    portfolio_curve[-1] = sum(sl["equity"] for sl in eq_sleeves) + gld_equity

    # Metrics
    final_eq = portfolio_curve[-1]
    start_dt = date.fromisoformat(common[0])
    end_dt   = date.fromisoformat(common[-1])
    years    = (end_dt - start_dt).days / 365.25
    cagr     = ((final_eq / TOTAL_CAPITAL) ** (1.0 / years) - 1) * 100.0 if years > 0 else 0.0

    peak = TOTAL_CAPITAL; max_dd = 0.0
    for eq in portfolio_curve:
        if eq > peak: peak = eq
        dd = (eq - peak) / peak * 100.0
        if dd < max_dd: max_dd = dd

    dr = [(portfolio_curve[i] - portfolio_curve[i-1]) / portfolio_curve[i-1]
           for i in range(1, len(portfolio_curve)) if portfolio_curve[i-1]]
    if len(dr) > 1:
        mu  = sum(dr) / len(dr)
        sig = (sum((r - mu)**2 for r in dr) / (len(dr) - 1)) ** 0.5
        sharpe = mu / sig * 252**0.5 if sig > 0 else 0.0
    else:
        sharpe = 0.0

    return dict(final_eq=final_eq, cagr=cagr, max_dd=max_dd, sharpe=sharpe,
                trades=trade_count, years=years)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Loading data...", flush=True)
    arrays, common = build_arrays()
    print(f"  {len(common)} bars  {common[0]} -> {common[-1]}")

    combos = [(w, s) for w in WMA_GRID for s in SMA_GRID if w < s]
    print(f"\nRunning {len(combos)} backtests "
          f"(WMA in {WMA_GRID}, SMA in {SMA_GRID}, WMA<SMA)...")

    results = []
    t0 = time.time()
    for idx, (wma, sma) in enumerate(combos, 1):
        t1 = time.time()
        m  = run_backtest(arrays, common, wma, sma)
        score = m["cagr"] / abs(m["max_dd"]) if m["max_dd"] else 0.0
        results.append({
            "wma": wma, "sma": sma,
            "final_eq": m["final_eq"], "cagr": m["cagr"], "max_dd": m["max_dd"],
            "sharpe": m["sharpe"], "trades": m["trades"],
            "score": score,
        })
        print(f"  [{idx:2d}/{len(combos)}] WMA={wma:>2} SMA={sma:>3}  "
              f"final=${m['final_eq']:>14,.0f}  CAGR={m['cagr']:>6.2f}%  "
              f"MaxDD={m['max_dd']:>7.2f}%  Sharpe={m['sharpe']:.3f}  "
              f"score={score:.4f}  ({time.time()-t1:.1f}s)")

    print(f"\nTotal sweep time: {time.time()-t0:.1f}s")

    out = WORKSPACE / "sweep_results.csv"
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader(); writer.writerows(results)
    print(f"Saved: {out}")

    results.sort(key=lambda r: r["score"], reverse=True)
    print("\n" + "=" * 90)
    print("  TOP 10 BY CAGR / |MaxDD|  (risk-adjusted return)")
    print("=" * 90)
    print(f"  {'Rank':<5} {'WMA':>4} {'SMA':>5} {'Final $':>14} {'CAGR':>8} "
          f"{'MaxDD':>9} {'Sharpe':>8} {'Trades':>7} {'Score':>8}")
    print("-" * 90)
    for rank, r in enumerate(results[:10], 1):
        print(f"  {rank:<5} {r['wma']:>4} {r['sma']:>5} ${r['final_eq']:>13,.0f} "
              f"{r['cagr']:>+7.2f}% {r['max_dd']:>+8.2f}% {r['sharpe']:>8.3f} "
              f"{r['trades']:>7} {r['score']:>8.4f}")

    # Locate baseline 20/200 in the results
    baseline = next((r for r in results if r["wma"] == 20 and r["sma"] == 200), None)
    if baseline:
        rank = results.index(baseline) + 1
        print(f"\n  Baseline WMA=20 SMA=200 ranks #{rank} of {len(results)} by score.")
