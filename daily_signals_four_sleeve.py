#!/usr/bin/env python3
"""
Daily signal generator for the Four-Sleeve Portfolio strategy.

Reconstructs the full simulation state through today and reports:
  - Current position in each sleeve (vehicle / defensive / cash)
  - Pending trades to execute at tomorrow's open
  - Entry price, current price, and unrealised P&L for open positions
"""
from __future__ import annotations

import json
import math
import os
import smtplib
import subprocess
import sys
from datetime import date, timedelta
from email.mime.text import MIMEText
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
WORKSPACE    = Path(__file__).resolve().parent
DATA_DIR     = WORKSPACE / "json" / "history"
SYMBOLS_FILE = WORKSPACE / "current_tickers.json"

# ── Portfolio / strategy config (must match backtest_four_sleeve.py) ──────────
TOTAL_CAPITAL    = 100_000.0
SAFETY_TICKER    = "GLD"
SAFETY_ALLOC     = 0.10
SAFETY_INIT      = TOTAL_CAPITAL * SAFETY_ALLOC
EQ_ALLOC_EACH    = (TOTAL_CAPITAL - SAFETY_INIT) / 3

BACKTEST_START   = date(2000, 1, 1)

WMA_PERIOD      = 20
SMA_PERIOD      = 200
VOL_PERIOD      = 20
VOL_ENTRY_MAX   = 16.0
VOL_EXIT_THRESH = 30.0
TAKE_PROFIT_PCT = 200.0
STOP_LOSS_PCT   = 12.0
DEF_STOP_PCT    = 18.0   # exit defensive if drops 18% from entry → cash
COOLDOWN_DAYS   = 30     # trading days to wait after any stop-loss
MIN_IDX         = max(WMA_PERIOD, SMA_PERIOD, VOL_PERIOD)

EQUITY_CONFIGS = [
    ("QQQ", "TQQQ", "QQQ"),
    ("SPY", "SPXL", "SPY"),
    ("SMH", "SOXL", "SMH"),
]


# ── Step 1: Refresh data ───────────────────────────────────────────────────────
def refresh_data() -> None:
    print("── Refreshing market data …")
    result = subprocess.run(
        [sys.executable, str(WORKSPACE / "incremental_download_eodhd_history.py"),
         "--symbols-file", str(SYMBOLS_FILE)],
        capture_output=True, text=True, cwd=str(WORKSPACE)
    )
    for line in result.stdout.strip().splitlines():
        print(" ", line)
    if result.returncode != 0:
        print("  WARNING: download exited with errors:", result.stderr.strip())


# ── Indicator helpers (identical to backtest) ─────────────────────────────────
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
    path = DATA_DIR / f"{ticker}_US.json"
    raw  = json.load(open(path))
    raw  = [r for r in raw if date.fromisoformat(r["date"]) >= BACKTEST_START]
    raw.sort(key=lambda r: r["date"])
    return {r["date"]: r for r in raw}


# ── Simulation ────────────────────────────────────────────────────────────────
def run_simulation():
    all_tickers = set()
    for s, v, d in EQUITY_CONFIGS:
        all_tickers |= {s, v, d}
    all_tickers.add(SAFETY_TICKER)

    raw_data = {t: load_ticker(t) for t in all_tickers}
    common   = sorted(set.intersection(*[set(raw_data[t].keys()) for t in all_tickers]))
    n        = len(common)

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

    def make_sleeve(signal, vehicle, defensive, init_equity):
        return dict(
            signal=signal, vehicle=vehicle, defensive=defensive,
            label=f"{signal}→{vehicle}",
            state="cash", next_state=None,
            v_shares=0.0, v_entry=0.0, v_entry_date="", v_exit_rsn="",
            d_shares=0.0, d_entry=0.0, d_entry_date="", d_exit_rsn="",
            cash=init_equity,
            wma_was_below=True, entry_eligible=False, equity=init_equity,
            cooldown=0,
        )

    eq_sleeves   = [make_sleeve(s, v, d, EQ_ALLOC_EACH) for s, v, d in EQUITY_CONFIGS]
    gld_shares   = SAFETY_INIT / arrays[SAFETY_TICKER]["adj"][0]
    prev_year    = int(common[0][:4])

    for i in range(n):
        day = common[i]

        # Execute pending transitions
        for sl in eq_sleeves:
            if sl["next_state"] is None:
                continue
            veh = sl["vehicle"]; dfn = sl["defensive"]
            vo  = arrays[veh]["opens"][i] * arrays[veh]["ratio"][i]
            do  = arrays[dfn]["opens"][i] * arrays[dfn]["ratio"][i]

            if sl["state"] == "vehicle":
                sl["cash"] = sl["v_shares"] * vo; sl["v_shares"] = 0.0; sl["v_entry"] = 0.0
            elif sl["state"] == "defensive":
                proceeds = sl["d_shares"] * do if do else sl["cash"]
                sl["cash"] = proceeds; sl["d_shares"] = 0.0; sl["d_entry"] = 0.0; sl["d_exit_rsn"] = ""

            if sl["next_state"] == "vehicle":
                sl["v_shares"] = sl["cash"] / vo; sl["v_entry"] = vo
                sl["v_entry_date"] = day; sl["cash"] = 0.0
            elif sl["next_state"] == "defensive":
                sl["d_shares"] = sl["cash"] / do; sl["d_entry"] = do
                sl["d_entry_date"] = day; sl["cash"] = 0.0
            # next_state == "cash": proceeds already in sl["cash"]

            sl["state"] = sl["next_state"]; sl["next_state"] = None

        # Decrement cooldown
        for sl in eq_sleeves:
            if sl["cooldown"] > 0:
                sl["cooldown"] -= 1

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
            total_eq   = sum(sl["equity"] for sl in eq_sleeves) + gld_equity
            eq_target  = total_eq * (EQ_ALLOC_EACH / TOTAL_CAPITAL)
            gld_target = total_eq * (SAFETY_INIT   / TOTAL_CAPITAL)
            for sl in eq_sleeves:
                if sl["state"] == "vehicle":
                    sl["v_shares"] = eq_target / arrays[sl["vehicle"]]["adj"][i]; sl["equity"] = eq_target
                elif sl["state"] == "defensive":
                    sl["d_shares"] = eq_target / arrays[sl["defensive"]]["adj"][i]; sl["equity"] = eq_target
                else:
                    sl["cash"] = eq_target; sl["equity"] = eq_target
            gld_shares_new = gld_target / arrays[SAFETY_TICKER]["adj"][i]
            gld_shares = gld_shares_new
            gld_equity = gld_target
        prev_year = cur_year

        if i < MIN_IDX:
            continue

        # Signal logic
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
                tp_p = sl["v_entry"] * (1 + TAKE_PROFIT_PCT / 100)
                sl_p = sl["v_entry"] * (1 - STOP_LOSS_PCT   / 100)
                do_tp = vad >= tp_p; do_sl = vad <= sl_p
                do_v  = hv >= VOL_EXIT_THRESH; do_w = cbl
                if do_tp or do_sl or do_v or do_w:
                    if do_tp:   sl["v_exit_rsn"] = f"take_profit({TAKE_PROFIT_PCT:.0f}%)"
                    elif do_sl:
                        sl["v_exit_rsn"] = f"stop_loss({STOP_LOSS_PCT:.0f}%)"
                        sl["cooldown"] = COOLDOWN_DAYS
                    elif do_v:  sl["v_exit_rsn"] = f"vol_exit({hv:.1f}%)"
                    else:       sl["v_exit_rsn"] = "wma_cross_below"
                    sl["wma_was_below"] = False
                    sl["next_state"]    = "defensive"

            # Defensive stop: if defensive drops DEF_STOP_PCT from entry → cash
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

    # Last bar prices for reporting
    last_adjs = {t: arrays[t]["adj"][-1] for t in all_tickers}
    last_day  = common[-1]

    return eq_sleeves, gld_shares, last_adjs, last_day, arrays, common


# ── Report ─────────────────────────────────────────────────────────────────────
def build_report(eq_sleeves, gld_shares, last_adjs, last_day, arrays, common):
    today = date.today().isoformat()
    lines = []
    lines += ["", "=" * 70,
              f"  FOUR-SLEEVE DAILY SIGNAL REPORT  —  {today}",
              f"  Last data bar : {last_day}",
              "=" * 70, "",
              "  PENDING TRADES  (execute at tomorrow's open)",
              "─" * 70]

    has_action = False
    for sl in eq_sleeves:
        ns = sl["next_state"]
        if ns == "vehicle":
            has_action = True
            cur = last_adjs[sl["vehicle"]]
            sl_price = cur * (1 - STOP_LOSS_PCT / 100)
            tp_price = cur * (1 + TAKE_PROFIT_PCT / 100)
            sig      = sl["signal"]
            last_i   = len(common) - 1
            hv       = arrays[sig]["hvol"][last_i] or 0.0
            lines.append(f"  BUY  {sl['vehicle']:<6}  [{sl['label']}]  "
                         f"entry price ≈ ${cur:,.2f}  (exit {sl['defensive']})")
            lines.append(f"    STOPS TO SET AT FILL:")
            lines.append(f"      Hard stop   : ${sl_price:>10,.2f}  ({STOP_LOSS_PCT:.0f}% below entry — set immediately after fill)")
            lines.append(f"      Take-profit : ${tp_price:>10,.2f}  ({TAKE_PROFIT_PCT:.0f}% above entry)")
            lines.append(f"      Vol exit    :  HVol currently {hv:.1f}% — exit if >= {VOL_EXIT_THRESH:.0f}% (check daily)")
            lines.append(f"      WMA exit    :  exit if {sig} WMA{WMA_PERIOD} crosses below SMA{SMA_PERIOD} (check daily)")
        elif ns == "defensive":
            has_action = True
            cur = last_adjs[sl["vehicle"]]
            lines.append(f"  SELL {sl['vehicle']:<6}  [{sl['label']}]  "
                         f"current price ≈ ${cur:,.2f}  reason: {sl['v_exit_rsn']}  "
                         f"→ rotate into {sl['defensive']}")
    if not has_action:
        lines.append("  No trades — hold current positions.")

    lines += ["", "  CURRENT POSITIONS", "─" * 70]
    total_equity = 0.0
    last_i = len(common) - 1
    for sl in eq_sleeves:
        state = sl["state"]
        sig   = sl["signal"]
        if state == "vehicle":
            cur  = last_adjs[sl["vehicle"]]
            val  = sl["v_shares"] * cur
            pnl  = (cur - sl["v_entry"]) / sl["v_entry"] * 100.0
            held = (date.fromisoformat(last_day) - date.fromisoformat(sl["v_entry_date"])).days
            lines.append(f"  {sl['label']:<12}  VEHICLE   {sl['vehicle']:<5}  "
                         f"${val:>12,.2f}  entry ${sl['v_entry']:,.2f}  "
                         f"P&L {pnl:>+.1f}%  held {held}d")
            # ── Exit criteria ──
            sl_price = sl["v_entry"] * (1 - STOP_LOSS_PCT   / 100)
            tp_price = sl["v_entry"] * (1 + TAKE_PROFIT_PCT / 100)
            wma_v    = arrays[sig]["wma"][last_i]
            sma_v    = arrays[sig]["sma"][last_i]
            hv       = arrays[sig]["hvol"][last_i] or 0.0
            buf_pts  = (wma_v - sma_v) if (wma_v and sma_v) else 0.0
            buf_pct  = buf_pts / sma_v * 100 if sma_v else 0.0
            lines.append(f"    EXIT CRITERIA:")
            lines.append(f"      Stop-loss   : ${sl_price:>10,.2f}  ({STOP_LOSS_PCT:.0f}% below entry)")
            lines.append(f"      Take-profit : ${tp_price:>10,.2f}  ({TAKE_PROFIT_PCT:.0f}% above entry)")
            lines.append(f"      Vol exit    :  HVol {hv:.1f}% — exits if >= {VOL_EXIT_THRESH:.0f}%  "
                         f"({'⚠ NEAR THRESHOLD' if hv >= VOL_EXIT_THRESH * 0.8 else 'OK'})")
            lines.append(f"      WMA exit    :  {sig} WMA{WMA_PERIOD} {wma_v:.2f} vs SMA{SMA_PERIOD} {sma_v:.2f}  "
                         f"(buffer {buf_pts:+.2f} pts / {buf_pct:+.1f}%  "
                         f"{'⚠ NARROW' if abs(buf_pct) < 1.0 else 'OK'})")
        elif state == "defensive":
            cur  = last_adjs[sl["defensive"]]
            val  = sl["d_shares"] * cur
            pnl  = (cur - sl["d_entry"]) / sl["d_entry"] * 100.0
            held = (date.fromisoformat(last_day) - date.fromisoformat(sl["d_entry_date"])).days
            def_stop_price = sl["d_entry"] * (1 - DEF_STOP_PCT / 100)
            wma_v = arrays[sig]["wma"][last_i]
            sma_v = arrays[sig]["sma"][last_i]
            hv    = arrays[sig]["hvol"][last_i] or 0.0
            buf_pts = (wma_v - sma_v) if (wma_v and sma_v) else 0.0
            buf_pct = buf_pts / sma_v * 100 if sma_v else 0.0
            lines.append(f"  {sl['label']:<12}  DEFENSIVE {sl['defensive']:<5}  "
                         f"${val:>12,.2f}  entry ${sl['d_entry']:,.2f}  "
                         f"P&L {pnl:>+.1f}%  held {held}d")
            lines.append(f"    EXIT CRITERIA:")
            lines.append(f"      Def stop    : ${def_stop_price:>10,.2f}  "
                         f"({DEF_STOP_PCT:.0f}% below entry — exits to cash)")
            lines.append(f"      Re-entry    :  {sig} WMA{WMA_PERIOD} {wma_v:.2f} vs SMA{SMA_PERIOD} {sma_v:.2f}  "
                         f"(buffer {buf_pts:+.2f} pts / {buf_pct:+.1f}%  "
                         f"{'⚠ BULL — watch for re-entry' if buf_pct > 0 else 'BEAR — staying defensive'})")
            lines.append(f"      HVol        :  {hv:.1f}%  (entry gate <= {VOL_ENTRY_MAX:.0f}%  "
                         f"{'⚠ TOO HIGH TO RE-ENTER' if hv > VOL_ENTRY_MAX else 'OK to re-enter'})")
            if sl["cooldown"] > 0:
                lines.append(f"      Cooldown    :  {sl['cooldown']} trading days remaining before re-entry allowed")
        else:
            cd_note = f"  (cooldown: {sl['cooldown']}d remaining)" if sl["cooldown"] > 0 else ""
            lines.append(f"  {sl['label']:<12}  CASH              "
                         f"${sl['cash']:>12,.2f}{cd_note}")
        total_equity += sl["equity"]

    gld_val = gld_shares * last_adjs[SAFETY_TICKER]
    lines.append(f"  {'GLD':<12}  HOLD      GLD    ${gld_val:>12,.2f}")
    total_equity += gld_val

    lines += ["", f"  Total Portfolio Value : ${total_equity:>12,.2f}",
              "", "  KEY INDICATORS (last bar)", "─" * 70]

    n = len(common)
    i = n - 1
    for sig in ["QQQ", "SPY", "SMH"]:
        wma_v = arrays[sig]["wma"][i]
        sma_v = arrays[sig]["sma"][i]
        hv    = arrays[sig]["hvol"][i]
        trend = "BULL" if (wma_v and sma_v and wma_v > sma_v) else "BEAR"
        vol_s = f"{hv:.1f}%" if hv else "n/a"
        lines.append(f"  {sig}  WMA{WMA_PERIOD}={wma_v:>8.2f}  SMA{SMA_PERIOD}={sma_v:>8.2f}  "
                     f"HVol={vol_s:>6}  [{trend}]")

    lines += ["=" * 70, ""]
    return "\n".join(lines)


def send_email(subject: str, body: str) -> None:
    email    = os.environ["GOOGLE_EMAIL"]
    password = os.environ["GOOGLE_APP_PASSWORD"]
    recipients = [email, "2256144680@tmomail.net"]
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = email
    msg["To"]      = ", ".join(recipients)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(email, password)
        server.sendmail(email, recipients, msg.as_string())


def main():
    print("── Running simulation …")
    eq_sleeves, gld_shares, last_adjs, last_day, arrays, common = run_simulation()
    report = build_report(eq_sleeves, gld_shares, last_adjs, last_day, arrays, common)
    print(report)
    send_email("Four-Sleeve: Daily Trade Signals", report)
    print("── Email sent.")


if __name__ == "__main__":
    main()
