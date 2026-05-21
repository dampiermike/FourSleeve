#!/usr/bin/env python3
"""
Sweep WMA/SMA for XLK->TECL within the full two-sleeve portfolio
(XLK->TECL + SPY->SPXL@5/200 + GLD 10%).

Different from the isolation sweep — this one captures portfolio-level
interactions (annual rebalance, paired drawdowns, etc.).
"""

import json, math
from datetime import date
from pathlib import Path

WORKSPACE   = Path(__file__).resolve().parent
DATA_DIR    = WORKSPACE / "json" / "history"
SPLICED_DIR = WORKSPACE / "json" / "spliced"

TOTAL_CAPITAL    = 100_000.0
SAFETY_ALLOC     = 0.10
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 2

VOL_PERIOD       = 20
VOL_ENTRY_MAX    = 16.0
VOL_EXIT_THRESH  = 30.0
TAKE_PROFIT_PCT  = 200.0
STOP_LOSS_PCT    = 12.0
DEF_STOP_PCT     = 18.0
COOLDOWN_DAYS    = 30

WMA_GRID = [5, 10, 15, 20, 25, 30, 40, 50]
SMA_GRID = [100, 125, 150, 175, 200, 225, 250]


def compute_hvol(c, w):
    n, out = len(c), [None]*len(c)
    for i in range(w, n):
        lr = [math.log(c[j]/c[j-1]) for j in range(i-w+1, i+1)]
        m = sum(lr)/w; v = sum((r-m)**2 for r in lr)/(w-1)
        out[i] = math.sqrt(v*252)*100.0
    return out
def compute_wma(c, p):
    n, out = len(c), [None]*len(c); denom = p*(p+1)/2
    for i in range(p-1, n):
        out[i] = sum(c[i-p+1+j]*(j+1) for j in range(p)) / denom
    return out
def compute_sma(c, p):
    n, out = len(c), [None]*len(c); s = sum(c[:p]); out[p-1] = s/p
    for i in range(p, n):
        s += c[i] - c[i-p]; out[i] = s/p
    return out

def load_ticker(t):
    sp = SPLICED_DIR / f"{t}_US.json"; hp = DATA_DIR / f"{t}_US.json"
    path = sp if sp.exists() else hp
    raw = json.load(open(path))
    raw = [r for r in raw if date.fromisoformat(r["date"]) >= date(2000, 1, 1)]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


# Load once
print("Loading data...", flush=True)
tickers = ["XLK", "TECL", "SPY", "SPXL", "GLD"]
raw_data = {t: load_ticker(t) for t in tickers}
common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in tickers]))
n = len(common)
print(f"  {n} bars  {common[0]} -> {common[-1]}")

arrays = {}
for tk, d in raw_data.items():
    closes = [d[day]["close"]          for day in common]
    adjs   = [d[day]["adjusted_close"] for day in common]
    opens  = [d[day]["open"]           for day in common]
    ratios = [a/c if c else 1.0 for a, c in zip(adjs, closes)]
    arrays[tk] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

# Pre-compute SPY indicators (fixed)
SPY_WMA, SPY_SMA = 5, 200
arrays["SPY"]["wma"]  = compute_wma(arrays["SPY"]["closes"], SPY_WMA)
arrays["SPY"]["sma"]  = compute_sma(arrays["SPY"]["closes"], SPY_SMA)
arrays["SPY"]["hvol"] = compute_hvol(arrays["SPY"]["closes"], VOL_PERIOD)
# Pre-compute XLK HVol (doesn't depend on WMA/SMA)
arrays["XLK"]["hvol"] = compute_hvol(arrays["XLK"]["closes"], VOL_PERIOD)


def make_sleeve(signal, vehicle, defensive, wma, sma):
    return dict(
        signal=signal, vehicle=vehicle, defensive=defensive,
        wma_period=wma, sma_period=sma,
        state="cash", next_state=None,
        v_shares=0.0, v_entry=0.0,
        d_shares=0.0, d_entry=0.0,
        cash=EQ_ALLOC_EACH, equity=EQ_ALLOC_EACH,
        wma_was_below=True, entry_eligible=False, cooldown=0,
        trades=0,
    )


def run(xlk_wma, xlk_sma):
    arrays["XLK"]["wma"] = compute_wma(arrays["XLK"]["closes"], xlk_wma)
    arrays["XLK"]["sma"] = compute_sma(arrays["XLK"]["closes"], xlk_sma)

    eq_sleeves = [
        make_sleeve("XLK", "TECL", "XLK", xlk_wma, xlk_sma),
        make_sleeve("SPY", "SPXL", "SPY", SPY_WMA, SPY_SMA),
    ]
    gld_shares = SAFETY_INIT / arrays["GLD"]["adj"][0]
    gld_equity = SAFETY_INIT
    portfolio = []
    prev_year = int(common[0][:4])
    min_idx = max(VOL_PERIOD,
                  max(sl["wma_period"] for sl in eq_sleeves),
                  max(sl["sma_period"] for sl in eq_sleeves))

    for i in range(n):
        day = common[i]

        for sl in eq_sleeves:
            if sl["next_state"] is None: continue
            veh = sl["vehicle"]; dfn = sl["defensive"]
            vo = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
            do = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]
            if sl["state"] == "vehicle":
                sl["cash"] = sl["v_shares"] * vo; sl["trades"] += 1
                sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
            elif sl["state"] == "defensive":
                sl["cash"] = sl["d_shares"] * do; sl["trades"] += 1
                sl["d_shares"] = 0.0; sl["d_entry"] = 0.0
            if sl["next_state"] == "vehicle":
                sl["v_shares"] = sl["cash"]/vo; sl["v_entry"] = vo; sl["cash"] = 0.0
            elif sl["next_state"] == "defensive":
                sl["d_shares"] = sl["cash"]/do; sl["d_entry"] = do; sl["cash"] = 0.0
            sl["state"] = sl["next_state"]; sl["next_state"] = None

        for sl in eq_sleeves:
            if sl["cooldown"] > 0: sl["cooldown"] -= 1

        for sl in eq_sleeves:
            if sl["state"] == "vehicle":
                sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i]
            elif sl["state"] == "defensive":
                sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i]
            else:
                sl["equity"] = sl["cash"]
        gld_equity = gld_shares * arrays["GLD"]["adj"][i]

        cur_year = int(day[:4])
        if cur_year > prev_year:
            total_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
            eq_t = total_eq * (EQ_ALLOC_EACH/TOTAL_CAPITAL)
            gld_t = total_eq * (SAFETY_INIT/TOTAL_CAPITAL)
            for sl in eq_sleeves:
                if sl["state"] == "vehicle":
                    sl["v_shares"] = eq_t / arrays[sl["vehicle"]]["adj"][i]; sl["equity"] = eq_t
                elif sl["state"] == "defensive":
                    sl["d_shares"] = eq_t / arrays[sl["defensive"]]["adj"][i]; sl["equity"] = eq_t
                else:
                    sl["cash"] = eq_t; sl["equity"] = eq_t
            gld_shares = gld_t / arrays["GLD"]["adj"][i]
            gld_equity = gld_t
        prev_year = cur_year

        port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        portfolio.append(port_eq)

        if i < min_idx: continue

        for sl in eq_sleeves:
            sig = sl["signal"]; veh = sl["vehicle"]
            wa = arrays[sig]["wma"]; sa = arrays[sig]["sma"]; hva = arrays[sig]["hvol"]
            if any(v is None for v in [wa[i], sa[i], wa[i-1], sa[i-1]]): continue
            w, wp = wa[i], wa[i-1]; s, sp = sa[i], sa[i-1]
            hv = hva[i] if hva[i] is not None else 0.0
            cab = wp <= sp and w > s; cbl = wp >= sp and w < s

            if sl["state"] == "vehicle" and sl["next_state"] is None:
                vad = arrays[veh]["adj"][i]
                do_tp = vad >= sl["v_entry"] * (1 + TAKE_PROFIT_PCT/100)
                do_sl = vad <= sl["v_entry"] * (1 - STOP_LOSS_PCT/100)
                do_v  = hv >= VOL_EXIT_THRESH
                do_w  = cbl
                if do_tp or do_sl or do_v or do_w:
                    if do_sl: sl["cooldown"] = COOLDOWN_DAYS
                    sl["wma_was_below"] = False; sl["next_state"] = "defensive"

            if sl["state"] == "defensive" and sl["next_state"] is None:
                dad = arrays[sl["defensive"]]["adj"][i]
                if sl["d_entry"] > 0 and dad <= sl["d_entry"] * (1 - DEF_STOP_PCT/100):
                    sl["cooldown"] = COOLDOWN_DAYS; sl["next_state"] = "cash"

            if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
                if w < s: sl["wma_was_below"] = True; sl["entry_eligible"] = False
                if cab and sl["wma_was_below"]: sl["entry_eligible"] = True; sl["wma_was_below"] = False
                if sl["entry_eligible"] and w < s: sl["entry_eligible"] = False; sl["wma_was_below"] = True
            if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
                if (sl["entry_eligible"] and hv <= VOL_ENTRY_MAX
                        and w > s and i + 1 < n and sl["cooldown"] == 0):
                    sl["next_state"] = "vehicle"
                    sl["entry_eligible"] = False; sl["wma_was_below"] = False

    for sl in eq_sleeves:
        if sl["state"] == "vehicle":
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][-1]
        elif sl["state"] == "defensive":
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][-1]
    gld_equity = gld_shares * arrays["GLD"]["adj"][-1]
    portfolio[-1] = sum(sl["equity"] for sl in eq_sleeves) + gld_equity

    final_eq = portfolio[-1]
    years = (date.fromisoformat(common[-1]) - date.fromisoformat(common[0])).days / 365.25
    cagr = ((final_eq/TOTAL_CAPITAL)**(1/years) - 1)*100
    peak = TOTAL_CAPITAL; max_dd = 0.0
    for eq in portfolio:
        if eq > peak: peak = eq
        dd = (eq - peak)/peak * 100
        if dd < max_dd: max_dd = dd
    dr = [(portfolio[i]-portfolio[i-1])/portfolio[i-1] for i in range(1, len(portfolio)) if portfolio[i-1]]
    mu = sum(dr)/len(dr); sig = (sum((r-mu)**2 for r in dr)/(len(dr)-1))**0.5
    sharpe = mu/sig * 252**0.5 if sig else 0.0
    return dict(final_eq=final_eq, cagr=cagr, max_dd=max_dd, sharpe=sharpe,
                trades=sum(sl["trades"] for sl in eq_sleeves))


combos = [(w, s) for w in WMA_GRID for s in SMA_GRID if w < s]
print(f"Sweeping XLK->TECL in portfolio context (SPY@5/200 + GLD 10% fixed): {len(combos)} combos\n")

results = []
for wma, sma in combos:
    m = run(wma, sma)
    score = m["cagr"]/abs(m["max_dd"]) if m["max_dd"] else 0.0
    results.append({"wma": wma, "sma": sma, **m, "score": score})

results.sort(key=lambda r: r["score"], reverse=True)

print(f"  {'Rank':<5} {'WMA':>4} {'SMA':>5} {'Final $':>14} {'CAGR':>8} {'MaxDD':>9} {'Sharpe':>8} {'Trades':>7} {'Score':>8}")
print("  " + "-"*88)
for rank, r in enumerate(results[:15], 1):
    marker = ""
    if r["final_eq"] > 35_448_247:
        marker = "  ★ beats v1"
    elif r["final_eq"] > 33_858_674:
        marker = "  beats v3 baseline"
    print(f"  {rank:<5} {r['wma']:>4} {r['sma']:>5} ${r['final_eq']:>13,.0f} "
          f"{r['cagr']:>+7.2f}% {r['max_dd']:>+8.2f}% {r['sharpe']:>8.4f} "
          f"{r['trades']:>7} {r['score']:>8.4f}{marker}")

print()
print("Reference benchmarks:")
print(f"  v1 (QQQ+SPY+GLD, optimized)    $35,448,247  +25.10%  -37.99%  Sharpe 0.8373")
print(f"  v3 baseline (20/200)           $33,858,674  +24.88%  -39.65%  Sharpe 0.8209")
print(f"  XLK swap at 5/250              $20,515,345  +22.51%  -34.79%  Sharpe 0.7692  (previous test)")
print()
print("Best XLK params (isolation): WMA=5/SMA=250 = score 0.4343")
