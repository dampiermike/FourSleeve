#!/usr/bin/env python3
"""
DD-reduction variants on the two-sleeve optimized base (QQQ@10/175 + SPY@5/200 + GLD).

Variants tested:

  baseline          — unchanged reference
  regime            — block ALL vehicle entries when SPY < its own SMA200
                      (bear-regime filter on the broad market)
  circuit_25_10     — force all sleeves to cash when portfolio DD <= -25%;
                      re-enable entries when DD >= -10%
  circuit_20_15     — same idea, tighter trigger & later release
  cooldown_all      — apply COOLDOWN_DAYS (30 bars) after ALL vehicle exits
                      (currently only hard_stop and def_stop trigger cooldown)
  regime+circuit_25_10
  regime+cooldown_all
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
    ("QQQ", "TQQQ", "QQQ", 10, 175),
    ("SPY", "SPXL", "SPY",  5, 200),
]

# Regime filter uses SPY's SMA200 (which is already computed for the SPY sleeve)
REGIME_SIGNAL_TICKER = "SPY"
REGIME_SMA_PERIOD    = 200


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
    raw = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


# Load
all_tickers = {SAFETY_TICKER, REGIME_SIGNAL_TICKER}
for s, v, d, _, _ in EQUITY_CONFIGS: all_tickers |= {s, v, d}
raw_data = {t: load_ticker(t) for t in all_tickers}
common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))
n = len(common)
print(f"Loaded {n} bars  {common[0]} -> {common[-1]}\n")

arrays = {}
for tk, d in raw_data.items():
    closes = [d[day]["close"]          for day in common]
    adjs   = [d[day]["adjusted_close"] for day in common]
    opens  = [d[day]["open"]           for day in common]
    ratios = [a/c if c else 1.0 for a, c in zip(adjs, closes)]
    arrays[tk] = dict(closes=closes, adj=adjs, opens=opens, ratio=ratios)

for s, v, d, wma, sma in EQUITY_CONFIGS:
    c = arrays[s]["closes"]
    arrays[s]["wma"]  = compute_wma(c, wma)
    arrays[s]["sma"]  = compute_sma(c, sma)
    arrays[s]["hvol"] = compute_hvol(c, VOL_PERIOD)

# Regime indicator: SPY close vs SPY SMA(REGIME_SMA_PERIOD)
# (SPY's per-sleeve sma is already SMA=200 so we can reuse it)
regime_sma = arrays[REGIME_SIGNAL_TICKER]["sma"]
regime_close = arrays[REGIME_SIGNAL_TICKER]["closes"]


def run(label, regime=False, circuit=None, cooldown_all=False):
    """
    circuit = None or dict(trigger_pct, release_pct)
    """
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
    eq_sleeves = [make_sleeve(*cfg) for cfg in EQUITY_CONFIGS]
    gld_shares = SAFETY_INIT / arrays[SAFETY_TICKER]["adj"][0]
    gld_equity = SAFETY_INIT
    portfolio = []
    prev_year = int(common[0][:4])

    port_peak     = TOTAL_CAPITAL
    circuit_active = False
    circuit_trips  = 0
    regime_blocks  = 0
    cooldown_extras = 0

    min_idx = max(VOL_PERIOD, REGIME_SMA_PERIOD,
                  max(sl["wma_period"] for sl in eq_sleeves),
                  max(sl["sma_period"] for sl in eq_sleeves))

    for i in range(n):
        day = common[i]

        # Phase 1: execute pending transitions
        for sl in eq_sleeves:
            if sl["next_state"] is None: continue
            veh = sl["vehicle"]; dfn = sl["defensive"]
            vo = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
            do = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]
            if sl["state"] == "vehicle":
                sl["cash"] = sl["v_shares"] * vo
                sl["trades"] += 1
                sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
            elif sl["state"] == "defensive":
                sl["cash"] = sl["d_shares"] * do
                sl["trades"] += 1
                sl["d_shares"] = 0.0; sl["d_entry"] = 0.0
            if sl["next_state"] == "vehicle":
                sl["v_shares"] = sl["cash"]/vo; sl["v_entry"] = vo; sl["cash"] = 0.0
            elif sl["next_state"] == "defensive":
                sl["d_shares"] = sl["cash"]/do; sl["d_entry"] = do; sl["cash"] = 0.0
            sl["state"] = sl["next_state"]; sl["next_state"] = None

        # Cooldown decrement
        for sl in eq_sleeves:
            if sl["cooldown"] > 0: sl["cooldown"] -= 1

        # Mark to market
        for sl in eq_sleeves:
            if sl["state"] == "vehicle":
                sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][i]
            elif sl["state"] == "defensive":
                sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][i]
            else:
                sl["equity"] = sl["cash"]
        gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][i]

        # Annual rebalance
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
            gld_shares = gld_t / arrays[SAFETY_TICKER]["adj"][i]
            gld_equity = gld_t
        prev_year = cur_year

        port_eq = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
        portfolio.append(port_eq)
        if port_eq > port_peak: port_peak = port_eq
        port_dd = (port_eq - port_peak) / port_peak * 100

        # Circuit breaker
        if circuit is not None:
            if not circuit_active and port_dd <= -circuit["trigger_pct"]:
                circuit_active = True; circuit_trips += 1
                # Force all sleeves to cash on next bar
                for sl in eq_sleeves:
                    if sl["state"] != "cash" and sl["next_state"] is None:
                        sl["next_state"] = "cash"
                        sl["cooldown"] = COOLDOWN_DAYS
            elif circuit_active and port_dd >= -circuit["release_pct"]:
                circuit_active = False

        if i < min_idx: continue

        # Signal logic
        regime_ok = True
        if regime:
            rc = regime_close[i]; rs = regime_sma[i]
            regime_ok = (rs is not None and rc > rs)

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
                    if do_sl:
                        sl["cooldown"] = COOLDOWN_DAYS
                    elif cooldown_all:
                        # Apply cooldown to non-stop exits too
                        sl["cooldown"] = COOLDOWN_DAYS; cooldown_extras += 1
                    sl["wma_was_below"] = False
                    sl["next_state"] = "defensive"

            if sl["state"] == "defensive" and sl["next_state"] is None:
                dad = arrays[sl["defensive"]]["adj"][i]
                if sl["d_entry"] > 0 and dad <= sl["d_entry"] * (1 - DEF_STOP_PCT/100):
                    sl["cooldown"] = COOLDOWN_DAYS
                    sl["next_state"] = "cash"

            if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
                if w < s: sl["wma_was_below"] = True; sl["entry_eligible"] = False
                if cab and sl["wma_was_below"]: sl["entry_eligible"] = True; sl["wma_was_below"] = False
                if sl["entry_eligible"] and w < s: sl["entry_eligible"] = False; sl["wma_was_below"] = True

            if sl["state"] in ("cash", "defensive") and sl["next_state"] is None:
                if (sl["entry_eligible"] and hv <= VOL_ENTRY_MAX
                        and w > s and i + 1 < n and sl["cooldown"] == 0
                        and not circuit_active):
                    if regime and not regime_ok:
                        regime_blocks += 1
                        # Latch is consumed once and waits for a new cross
                        sl["entry_eligible"] = False; sl["wma_was_below"] = False
                    else:
                        sl["next_state"] = "vehicle"
                        sl["entry_eligible"] = False; sl["wma_was_below"] = False

    # Final mark-to-market
    for sl in eq_sleeves:
        if sl["state"] == "vehicle":
            sl["equity"] = sl["v_shares"] * arrays[sl["vehicle"]]["adj"][-1]
        elif sl["state"] == "defensive":
            sl["equity"] = sl["d_shares"] * arrays[sl["defensive"]]["adj"][-1]
    gld_equity = gld_shares * arrays[SAFETY_TICKER]["adj"][-1]
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
    trades = sum(sl["trades"] for sl in eq_sleeves)

    return dict(label=label, final_eq=final_eq, cagr=cagr, max_dd=max_dd, sharpe=sharpe,
                trades=trades, circuit_trips=circuit_trips, regime_blocks=regime_blocks,
                cooldown_extras=cooldown_extras)


VARIANTS = [
    ("baseline",              dict()),
    ("regime",                dict(regime=True)),
    ("circuit_25_10",         dict(circuit=dict(trigger_pct=25, release_pct=10))),
    ("circuit_20_15",         dict(circuit=dict(trigger_pct=20, release_pct=15))),
    ("cooldown_all",          dict(cooldown_all=True)),
    ("regime+circuit_25_10",  dict(regime=True, circuit=dict(trigger_pct=25, release_pct=10))),
    ("regime+cooldown_all",   dict(regime=True, cooldown_all=True)),
]

print(f"{'Variant':<24} {'Final $':>14} {'CAGR':>8} {'MaxDD':>9} {'Sharpe':>8} {'Trades':>7} {'ΔFinal':>11} {'ΔDD':>7} {'Notes':<20}")
print("-" * 120)
baseline_eq = baseline_dd = None
for name, kwargs in VARIANTS:
    m = run(name, **kwargs)
    if name == "baseline":
        baseline_eq = m["final_eq"]; baseline_dd = m["max_dd"]
    notes = []
    if m["regime_blocks"]:  notes.append(f"regime_blk={m['regime_blocks']}")
    if m["circuit_trips"]:  notes.append(f"circuit_trips={m['circuit_trips']}")
    if m["cooldown_extras"]: notes.append(f"cooldown_extra={m['cooldown_extras']}")
    notes_s = ", ".join(notes)
    deq = m["final_eq"] - baseline_eq
    ddd = m["max_dd"] - baseline_dd
    print(f"{name:<24} ${m['final_eq']:>13,.0f} {m['cagr']:>+7.2f}% {m['max_dd']:>+8.2f}% {m['sharpe']:>8.4f} "
          f"{m['trades']:>7} ${deq:>+10,.0f} {ddd:>+6.2f}% {notes_s:<20}")
