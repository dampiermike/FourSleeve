#!/usr/bin/env python3
"""
Two-Sleeve (QQQ + SPY + GLD) backtest with per-sleeve optimal WMA/SMA
PLUS VIX-based position sizing on vehicle entries.

Sizing rule: locked at vehicle entry, never changes mid-trade.
    size_fraction = min(1.0, VIX_REF / vix_at_entry)

Sweep VIX_REF to see the CAGR/MaxDD tradeoff.
- VIX_REF = 1e9 reproduces the unsized baseline ($35.4M, -37.99% DD)
- VIX_REF = 15:  size <= 1.0 if VIX < 15, halves at VIX=30, quarters at VIX=60
- VIX_REF = 20:  size <= 1.0 if VIX < 20, halves at VIX=40

The locked size_fraction is preserved across annual rebalances. Only
on a fresh vehicle entry (cash->vehicle or defensive->vehicle) is the
VIX read and a new size_fraction stored.

Diagnostic columns:
  AvgSize : average size_fraction across all entries
  %Full   : pct of entries that fired at full (1.0) size
  Skipped : entries blocked entirely (size_fraction < SKIP_MIN)
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
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 2
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
]

# Linear cap sweep: size = min(1.0, REF/VIX)
LINEAR_SWEEP = [12.0, 15.0, 18.0, 20.0, 25.0, 30.0, 1e9]

# Banded sweep (mimics common VIX-tier sizing schemes)
BAND_SWEEP = {
    "bands_AA":  [(15, 1.00), (20, 0.85), (30, 0.60), (40, 0.40), (1e9, 0.20)],
    "bands_B":   [(20, 1.00), (30, 0.70), (40, 0.40), (1e9, 0.20)],
    "bands_C":   [(20, 1.00), (25, 0.75), (30, 0.50), (1e9, 0.25)],
    "skip_30":   [(30, 1.00), (1e9, 0.0)],   # full below VIX 30, skip above
    "skip_25":   [(25, 1.00), (1e9, 0.0)],
}


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


def load_vix():
    path = DATA_DIR / "VIX_INDX.json"
    raw = json.load(open(path))
    raw = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    return {r["date"]: r["close"] for r in raw}


# Build arrays once
all_tickers = {SAFETY_TICKER}
for s, v, d, _, _ in EQUITY_CONFIGS:
    all_tickers |= {s, v, d}

print("Loading data...", flush=True)
raw_data = {t: load_ticker(t) for t in all_tickers}
vix_data = load_vix()
common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))
n = len(common)
print(f"  {n} bars  {common[0]} -> {common[-1]}")

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

# Forward-fill VIX onto common_dates (default 20.0 if no prior VIX value)
vix_arr = []
last = 20.0
for day in common:
    if day in vix_data:
        last = vix_data[day]
    vix_arr.append(last)

vix_min = min(vix_arr); vix_max = max(vix_arr)
vix_mean = sum(vix_arr) / len(vix_arr)
print(f"  VIX:  min={vix_min:.2f}  mean={vix_mean:.2f}  max={vix_max:.2f}\n")


def linear_size(vix, ref):
    return min(1.0, ref / max(vix, 1e-9))

def band_size(vix, bands):
    """bands = [(vix_upper_inclusive, size), ...] sorted by vix_upper"""
    for ub, sz in bands:
        if vix < ub:
            return sz
    return bands[-1][1]


def run(size_fn, label):
    def make_sleeve(signal, vehicle, defensive, wma, sma):
        return dict(
            signal=signal, vehicle=vehicle, defensive=defensive,
            wma_period=wma, sma_period=sma,
            state="cash", next_state=None,
            v_shares=0.0, v_entry=0.0, v_entry_date="",
            d_shares=0.0, d_entry=0.0, d_entry_date="",
            cash=EQ_ALLOC_EACH, equity=EQ_ALLOC_EACH,
            wma_was_below=True, entry_eligible=False, cooldown=0,
            trade_count=0,
            locked_size=1.0,  # size_fraction locked at entry
            pending_size=None,  # computed at signal time, applied at fill
        )

    eq_sleeves = [make_sleeve(*cfg) for cfg in EQUITY_CONFIGS]
    gld_adj0   = arrays[SAFETY_TICKER]["adj"][0]
    gld_shares = SAFETY_INIT / gld_adj0
    gld_equity = SAFETY_INIT
    portfolio_curve = []
    prev_year = int(common[0][:4])
    entry_sizes = []
    skip_count  = 0

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
                sl["cash"] += sl["v_shares"] * vo
                sl["trade_count"] += 1
                sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
            elif sl["state"] == "defensive":
                sl["cash"] += sl["d_shares"] * do
                sl["trade_count"] += 1
                sl["d_shares"] = 0.0; sl["d_entry"] = 0.0

            if sl["next_state"] == "vehicle":
                size_frac = sl["pending_size"] if sl["pending_size"] is not None else 1.0
                sl["locked_size"] = size_frac
                invest = sl["cash"] * size_frac
                if invest > 0 and vo > 0:
                    sl["v_shares"] = invest / vo
                    sl["v_entry"]  = vo
                    sl["v_entry_date"] = day
                    sl["cash"] -= invest
                else:
                    # Size 0 -> stay in cash, never entered vehicle
                    sl["state"] = "cash"; sl["next_state"] = None
                    sl["pending_size"] = None
                    continue
            elif sl["next_state"] == "defensive":
                sl["d_shares"] = sl["cash"] / do
                sl["d_entry"]  = do
                sl["d_entry_date"] = day
                sl["cash"] = 0.0
                sl["locked_size"] = 1.0  # defensive is unsized
            elif sl["next_state"] == "cash":
                sl["locked_size"] = 1.0

            sl["state"] = sl["next_state"]; sl["next_state"] = None
            sl["pending_size"] = None

        for sl in eq_sleeves:
            if sl["cooldown"] > 0:
                sl["cooldown"] -= 1

        for sl in eq_sleeves:
            if sl["state"] == "vehicle":
                sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i] + sl["cash"]
            elif sl["state"] == "defensive":
                sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i] + sl["cash"]
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
                    # Preserve LOCKED entry size_fraction across rebalance
                    invest = eq_target * sl["locked_size"]
                    sl["v_shares"] = invest / arrays[sl["vehicle"]]["adj"][i]
                    sl["cash"]     = eq_target - invest
                elif sl["state"] == "defensive":
                    sl["d_shares"] = eq_target / arrays[sl["defensive"]]["adj"][i]
                    sl["cash"]     = 0.0
                else:
                    sl["cash"]     = eq_target
                sl["equity"] = eq_target

            gld_shares = gld_target / arrays[SAFETY_TICKER]["adj"][i]
            gld_equity = gld_target

        prev_year = cur_year

        port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        portfolio_curve.append(port_eq)

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
                    # Compute and lock size_fraction at signal bar
                    vix = vix_arr[i]
                    size_frac = size_fn(vix)
                    entry_sizes.append((size_frac, vix))
                    if size_frac > 0:
                        sl["next_state"]   = "vehicle"
                        sl["pending_size"] = size_frac
                        sl["entry_eligible"] = False; sl["wma_was_below"] = False
                    else:
                        # Skipped: arm latch back so future cross fires again
                        sl["entry_eligible"] = False; sl["wma_was_below"] = True

    # Final mark-to-market
    for sl in eq_sleeves:
        if sl["state"] == "vehicle":
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][-1] + sl["cash"]
        elif sl["state"] == "defensive":
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][-1] + sl["cash"]
    gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][-1]
    portfolio_curve[-1] = sum(sl["equity"] for sl in eq_sleeves) + gld_equity

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

    total_trades = sum(sl["trade_count"] for sl in eq_sleeves)
    n_entries = len(entry_sizes)
    avg_size  = sum(s for s, _ in entry_sizes) / n_entries if n_entries else 0.0
    pct_full  = sum(1 for s, _ in entry_sizes if s >= 0.999) / n_entries * 100 if n_entries else 0.0
    skipped   = sum(1 for s, _ in entry_sizes if s <= 1e-9)
    avg_vix   = sum(v for _, v in entry_sizes) / n_entries if n_entries else 0.0

    return dict(label=label, final_eq=final_eq, cagr=cagr, max_dd=max_dd,
                sharpe=sharpe, trades=total_trades, n_entries=n_entries,
                avg_size=avg_size, pct_full=pct_full, skipped=skipped,
                avg_vix=avg_vix)


# Run linear cap sweep
print("=" * 100)
print("  VIX-BASED ENTRY SIZING  —  Locked at entry, never changes mid-trade")
print("  Base: QQQ@10/175 + SPY@5/200 + GLD 10%")
print("=" * 100)
print()
print("Linear cap:  size = min(1.0, VIX_REF / vix_at_entry)")
print("-" * 100)
print(f"  {'VIX_REF':>9} {'Final $':>14} {'CAGR':>8} {'MaxDD':>9} {'Sharpe':>8} "
      f"{'Trades':>7} {'Entries':>8} {'AvgSize':>8} {'%Full':>7} {'AvgVIX':>7}")
print("-" * 100)

linear_results = []
for ref in LINEAR_SWEEP:
    label = "no cap" if ref >= 1e8 else f"{ref:.0f}"
    m = run(lambda v, r=ref: linear_size(v, r), label)
    linear_results.append(m)
    print(f"  {label:>9} ${m['final_eq']:>13,.0f} {m['cagr']:>+7.2f}% "
          f"{m['max_dd']:>+8.2f}% {m['sharpe']:>8.4f} {m['trades']:>7} "
          f"{m['n_entries']:>8} {m['avg_size']:>7.3f} {m['pct_full']:>6.1f}% "
          f"{m['avg_vix']:>7.2f}")

print()
print("Banded schemes:  size depends on VIX band (vix_upper, size)")
print("-" * 100)
print(f"  {'Scheme':>9} {'Final $':>14} {'CAGR':>8} {'MaxDD':>9} {'Sharpe':>8} "
      f"{'Trades':>7} {'Entries':>8} {'AvgSize':>8} {'%Full':>7} {'Skipped':>8}")
print("-" * 100)
for name, bands in BAND_SWEEP.items():
    m = run(lambda v, b=bands: band_size(v, b), name)
    print(f"  {name:>9} ${m['final_eq']:>13,.0f} {m['cagr']:>+7.2f}% "
          f"{m['max_dd']:>+8.2f}% {m['sharpe']:>8.4f} {m['trades']:>7} "
          f"{m['n_entries']:>8} {m['avg_size']:>7.3f} {m['pct_full']:>6.1f}% "
          f"{m['skipped']:>8}")

print()
print("Reference benchmarks:")
print(f"  TwoSleeve Opt (no sizing)      $35,448,247  +25.10%  -37.99%  Sharpe 0.8373")
print(f"  TwoSleeves 20/200 (target)     $33,858,674  +24.88%  -39.65%  Sharpe 0.8209")
print(f"  FourSleeve original            $16,227,399  +21.73%  -35.57%  Sharpe 0.7570")

print()
print("Band definitions (vix_upper_exclusive, size_fraction):")
for name, bands in BAND_SWEEP.items():
    parts = []
    for ub, sz in bands:
        ubs = "inf" if ub > 1e8 else f"{ub:.0f}"
        parts.append(f"VIX<{ubs}: {sz:.2f}")
    print(f"  {name}:  " + ", ".join(parts))
