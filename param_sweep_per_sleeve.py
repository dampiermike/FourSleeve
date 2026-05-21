#!/usr/bin/env python3
"""
Per-sleeve WMA/SMA parameter sweep.

Runs each (signal, vehicle, defensive) pair in isolation — no GLD, no
rebalance, no cross-sleeve interaction. Uses each pair's natural date
intersection (no forced common start date). All other strategy
parameters held constant.

Writes one CSV per sleeve and prints top 10 by CAGR / |MaxDD| for each.
"""

import json
import csv
import math
import time
from datetime import date
from pathlib import Path

WORKSPACE   = Path(__file__).resolve().parent
DATA_DIR    = WORKSPACE / "json" / "history"
SPLICED_DIR = WORKSPACE / "json" / "spliced"

INIT_EQUITY      = 100_000.0
BACKTEST_START   = date(2000, 1, 1)

VOL_PERIOD       = 20
VOL_ENTRY_MAX    = 16.0
VOL_EXIT_THRESH  = 30.0
TAKE_PROFIT_PCT  = 200.0
STOP_LOSS_PCT    = 12.0
DEF_STOP_PCT     = 18.0
COOLDOWN_DAYS    = 30

SLEEVES = [
    ("QQQ", "TQQQ", "QQQ"),
    ("SPY", "SPXL", "SPY"),
    ("SMH", "SOXL", "SMH"),
]

WMA_GRID = [5, 10, 15, 20, 25, 30, 40, 50]
SMA_GRID = [100, 125, 150, 175, 200, 225, 250]


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


def build_arrays_for_pair(signal, vehicle, defensive):
    tickers = {signal, vehicle, defensive}
    raw_data = {t: load_ticker(t) for t in tickers}
    common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in tickers]))

    arrays = {}
    for ticker, d in raw_data.items():
        closes = [d[day]["close"]          for day in common]
        adjs   = [d[day]["adjusted_close"] for day in common]
        opens  = [d[day]["open"]           for day in common]
        ratios = [a / c if c else 1.0 for a, c in zip(adjs, closes)]
        arrays[ticker] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

    arrays[signal]["hvol"] = compute_hvol(arrays[signal]["closes"], VOL_PERIOD)
    return arrays, common


def run_single_sleeve(arrays, common, signal, vehicle, defensive, wma_period, sma_period):
    n = len(common)
    min_idx = max(wma_period, sma_period, VOL_PERIOD)

    c = arrays[signal]["closes"]
    arrays[signal]["wma"] = compute_wma(c, wma_period)
    arrays[signal]["sma"] = compute_sma(c, sma_period)

    state = "cash"; next_state = None
    v_shares = 0.0; v_entry = 0.0; v_entry_date = ""
    d_shares = 0.0; d_entry = 0.0
    cash = INIT_EQUITY
    wma_was_below = True; entry_eligible = False
    cooldown = 0
    equity = INIT_EQUITY
    trade_count = 0

    portfolio_curve = []

    for i in range(n):
        day = common[i]

        if next_state is not None:
            vo = arrays[vehicle]["opens"][i] * arrays[vehicle]["ratio"][i]
            do = arrays[defensive]["opens"][i] * arrays[defensive]["ratio"][i]

            if state == "vehicle":
                cash = v_shares * vo
                trade_count += 1
                v_shares = 0.0; v_entry = 0.0
            elif state == "defensive":
                cash = d_shares * do
                trade_count += 1
                d_shares = 0.0; d_entry = 0.0

            if next_state == "vehicle":
                v_shares = cash / vo; v_entry = vo; v_entry_date = day; cash = 0.0
            elif next_state == "defensive":
                d_shares = cash / do; d_entry = do; cash = 0.0

            state = next_state; next_state = None

        if cooldown > 0:
            cooldown -= 1

        if state == "vehicle":
            equity = v_shares * arrays[vehicle]["adj"][i]
        elif state == "defensive":
            equity = d_shares * arrays[defensive]["adj"][i]
        else:
            equity = cash

        portfolio_curve.append(equity)

        if i < min_idx:
            continue

        wa  = arrays[signal]["wma"]; sa = arrays[signal]["sma"]; hva = arrays[signal]["hvol"]
        if any(v is None for v in [wa[i], sa[i], wa[i-1], sa[i-1]]):
            continue

        w, wp = wa[i], wa[i-1]; s, sp = sa[i], sa[i-1]
        hv    = hva[i] if hva[i] is not None else 0.0
        cab   = wp <= sp and w > s
        cbl   = wp >= sp and w < s

        if state == "vehicle" and next_state is None:
            vad   = arrays[vehicle]["adj"][i]
            do_tp = vad >= v_entry * (1 + TAKE_PROFIT_PCT / 100)
            do_sl = vad <= v_entry * (1 - STOP_LOSS_PCT   / 100)
            do_v  = hv >= VOL_EXIT_THRESH
            do_w  = cbl
            if do_tp or do_sl or do_v or do_w:
                if do_sl:
                    cooldown = COOLDOWN_DAYS
                wma_was_below = False
                next_state    = "defensive"

        if state == "defensive" and next_state is None:
            dad = arrays[defensive]["adj"][i]
            if d_entry > 0 and dad <= d_entry * (1 - DEF_STOP_PCT / 100):
                cooldown   = COOLDOWN_DAYS
                next_state = "cash"

        if state in ("cash", "defensive") and next_state is None:
            if w < s: wma_was_below = True; entry_eligible = False
            if cab and wma_was_below: entry_eligible = True; wma_was_below = False
            if entry_eligible and w < s: entry_eligible = False; wma_was_below = True

        if state in ("cash", "defensive") and next_state is None:
            if (entry_eligible and hv <= VOL_ENTRY_MAX
                    and w > s and i + 1 < n and cooldown == 0):
                next_state = "vehicle"
                entry_eligible = False; wma_was_below = False

    # Mark-to-market at last bar
    if state == "vehicle" and v_shares > 0:
        equity = v_shares * arrays[vehicle]["adj"][-1]
    elif state == "defensive" and d_shares > 0:
        equity = d_shares * arrays[defensive]["adj"][-1]
    portfolio_curve[-1] = equity

    final_eq = portfolio_curve[-1]
    start_dt = date.fromisoformat(common[0])
    end_dt   = date.fromisoformat(common[-1])
    years    = (end_dt - start_dt).days / 365.25
    cagr     = ((final_eq / INIT_EQUITY) ** (1.0 / years) - 1) * 100.0 if years > 0 else 0.0

    peak = INIT_EQUITY; max_dd = 0.0
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


if __name__ == "__main__":
    combos = [(w, s) for w in WMA_GRID for s in SMA_GRID if w < s]
    print(f"Per-sleeve sweep: {len(combos)} combos per sleeve × {len(SLEEVES)} sleeves\n")

    all_sleeve_results = {}

    for signal, vehicle, defensive in SLEEVES:
        label = f"{signal}->{vehicle}"
        print(f"━━━ {label} ━━━")
        print("Loading data...", flush=True)
        arrays, common = build_arrays_for_pair(signal, vehicle, defensive)
        print(f"  {len(common)} bars  {common[0]} -> {common[-1]}")
        print()

        results = []
        t0 = time.time()
        for idx, (wma, sma) in enumerate(combos, 1):
            m = run_single_sleeve(arrays, common, signal, vehicle, defensive, wma, sma)
            score = m["cagr"] / abs(m["max_dd"]) if m["max_dd"] else 0.0
            results.append({
                "wma": wma, "sma": sma,
                "final_eq": m["final_eq"], "cagr": m["cagr"], "max_dd": m["max_dd"],
                "sharpe": m["sharpe"], "trades": m["trades"], "score": score,
            })

        print(f"  Done in {time.time()-t0:.1f}s")

        out = WORKSPACE / f"sweep_results_{signal}_{vehicle}.csv"
        with open(out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader(); writer.writerows(results)
        print(f"  Saved: {out.name}")

        results.sort(key=lambda r: r["score"], reverse=True)
        all_sleeve_results[label] = results

        print(f"\n  TOP 10 — {label}")
        print(f"  {'Rank':<5} {'WMA':>4} {'SMA':>5} {'Final $':>14} {'CAGR':>8} "
              f"{'MaxDD':>9} {'Sharpe':>8} {'Trades':>7} {'Score':>8}")
        print("  " + "-" * 88)
        for rank, r in enumerate(results[:10], 1):
            print(f"  {rank:<5} {r['wma']:>4} {r['sma']:>5} ${r['final_eq']:>13,.0f} "
                  f"{r['cagr']:>+7.2f}% {r['max_dd']:>+8.2f}% {r['sharpe']:>8.3f} "
                  f"{r['trades']:>7} {r['score']:>8.4f}")

        baseline = next((r for r in results if r["wma"] == 20 and r["sma"] == 200), None)
        if baseline:
            rank = results.index(baseline) + 1
            print(f"\n  Baseline WMA=20 SMA=200 ranks #{rank} of {len(results)} "
                  f"(CAGR {baseline['cagr']:.2f}% / MaxDD {baseline['max_dd']:.2f}% / "
                  f"score {baseline['score']:.4f})")
        print()

    # Cross-sleeve summary
    print("=" * 90)
    print("  BEST WMA/SMA PER SLEEVE")
    print("=" * 90)
    print(f"  {'Sleeve':<12} {'WMA':>4} {'SMA':>5} {'CAGR':>8} {'MaxDD':>9} "
          f"{'Sharpe':>8} {'Score':>8}")
    print("  " + "-" * 60)
    for label, results in all_sleeve_results.items():
        top = results[0]
        print(f"  {label:<12} {top['wma']:>4} {top['sma']:>5} "
              f"{top['cagr']:>+7.2f}% {top['max_dd']:>+8.2f}% "
              f"{top['sharpe']:>8.3f} {top['score']:>8.4f}")
