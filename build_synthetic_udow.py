#!/usr/bin/env python3
"""
Build a synthetic UDOW (3x Dow Jones) history pre-2010-02-11 and splice
it with the real UDOW history. Output: json/spliced/UDOW_US.json.

Method:
  - Use DIA raw close-to-close returns from 2000-01-03 to 2010-02-10 as
    the underlying source.
  - Apply 3x leverage with a 0.04%/day cost drag (~10% annual,
    representative of 3x ETF drag in the higher-rate 2000-2010 era).
  - For each pre-inception bar, OHLC is built by applying 3x to the
    corresponding DIA OHLC move-vs-previous-close (no cost on OHLC
    intra-bar, only on close-to-close).
  - Anchor: scale the synthetic series so the synthetic 2010-02-10 close
    matches an estimate consistent with real UDOW's 2010-02-11 open.
  - Mark every synthetic bar with "synthetic": true.
"""

import json
from datetime import date
from pathlib import Path

WORKSPACE   = Path(__file__).resolve().parent
DIA_PATH    = WORKSPACE / "json" / "history" / "DIA_US.json"
UDOW_REAL   = WORKSPACE / "json" / "history" / "UDOW_US.json"
OUT_PATH    = WORKSPACE / "json" / "spliced" / "UDOW_US.json"

DAILY_COST_DRAG = 0.0004   # 0.04%/day  ~= 10% annual
LEVERAGE        = 3.0

# Splice cutoff: synthetic dates strictly before this
SPLICE_DATE     = "2010-02-11"

# Copy UDOW data into FourSleeve from Oscillator if it's not there yet
import shutil
if not UDOW_REAL.exists():
    src = Path("/Users/mikedampier/Documents/Development/Oscillator/json/history/UDOW_US.json")
    shutil.copy(src, UDOW_REAL)
    print(f"Copied UDOW from Oscillator: {UDOW_REAL}")

with open(DIA_PATH)  as f: dia_data  = json.load(f)
with open(UDOW_REAL) as f: udow_real = json.load(f)

dia_data.sort(key=lambda r: r["date"])
udow_real.sort(key=lambda r: r["date"])

# Find real UDOW first bar
real_first = udow_real[0]
assert real_first["date"] == SPLICE_DATE, f"Expected {SPLICE_DATE}, got {real_first['date']}"
print(f"Real UDOW starts: {real_first['date']}  open={real_first['open']}  close={real_first['close']}")

# Filter DIA for pre-splice synthetic window
dia_pre = [r for r in dia_data if r["date"] < SPLICE_DATE]
dia_at_splice = next((r for r in dia_data if r["date"] == SPLICE_DATE), None)
assert dia_at_splice is not None, f"DIA must have {SPLICE_DATE}"
print(f"DIA pre-splice bars: {len(dia_pre)}  ({dia_pre[0]['date']} -> {dia_pre[-1]['date']})")

# Build synthetic 3x daily series, starting at arbitrary value 100, then scale
synth = []
prev_dia_close = dia_pre[0]["close"]
synth_close = 100.0

# First bar: synthesize OHLC from DIA's first bar
# (For the very first bar we use simple 3x scaling of relative day-1 moves)
first = dia_pre[0]
day1_open_ret  = (first["open"]  - prev_dia_close) / prev_dia_close
day1_high_ret  = (first["high"]  - prev_dia_close) / prev_dia_close
day1_low_ret   = (first["low"]   - prev_dia_close) / prev_dia_close
day1_close_ret = (first["close"] - prev_dia_close) / prev_dia_close
# This bar has no "previous close" so we set close to 100 and OHLC ratios consistent
day1_close = 100.0
day1_open  = day1_close * (1 + LEVERAGE * (day1_open_ret  - day1_close_ret))
day1_high  = day1_close * (1 + LEVERAGE * (day1_high_ret  - day1_close_ret))
day1_low   = day1_close * (1 + LEVERAGE * (day1_low_ret   - day1_close_ret))
synth.append(dict(date=first["date"], open=day1_open, high=day1_high, low=day1_low,
                  close=day1_close, adjusted_close=day1_close, volume=0, synthetic=True))
prev_dia_close = first["close"]

for r in dia_pre[1:]:
    if prev_dia_close <= 0:
        prev_dia_close = r["close"]; continue
    open_ret  = (r["open"]  - prev_dia_close) / prev_dia_close
    high_ret  = (r["high"]  - prev_dia_close) / prev_dia_close
    low_ret   = (r["low"]   - prev_dia_close) / prev_dia_close
    close_ret = (r["close"] - prev_dia_close) / prev_dia_close

    # 3x daily move with cost drag (close only — OHLC scaled by close basis)
    prior_close = synth[-1]["close"]
    new_close = prior_close * (1 + LEVERAGE * close_ret - DAILY_COST_DRAG)
    new_open  = prior_close * (1 + LEVERAGE * open_ret)
    new_high  = prior_close * (1 + LEVERAGE * high_ret)
    new_low   = prior_close * (1 + LEVERAGE * low_ret)
    new_open  = max(new_open, 0.01); new_high = max(new_high, 0.01)
    new_low   = max(new_low,  0.01); new_close = max(new_close, 0.01)
    # Ensure h>=max(o,c) and l<=min(o,c)
    new_high  = max(new_high, new_open, new_close)
    new_low   = min(new_low,  new_open, new_close)

    synth.append(dict(date=r["date"], open=new_open, high=new_high, low=new_low,
                      close=new_close, adjusted_close=new_close, volume=0, synthetic=True))
    prev_dia_close = r["close"]

# Anchor: target synthetic close on 2010-02-10 such that the implied
# overnight move synthetic->real on 2010-02-11 matches DIA's overnight move
dia_at_splice_open = dia_at_splice["open"]
dia_pre_last_close = dia_pre[-1]["close"]
implied_overnight  = (dia_at_splice_open - dia_pre_last_close) / dia_pre_last_close
# Want: real_open[splice] / synth_close[splice-1] = 1 + 3 * implied_overnight
target_synth_last = real_first["open"] / (1 + LEVERAGE * implied_overnight)

raw_synth_last = synth[-1]["close"]
scale = target_synth_last / raw_synth_last
print(f"\nSynthetic anchoring:")
print(f"  Raw synthetic last close (2010-02-10):  {raw_synth_last:.4f}")
print(f"  DIA implied overnight move:             {implied_overnight*100:+.3f}%")
print(f"  Target synthetic last close:            {target_synth_last:.4f}")
print(f"  Scale factor applied to entire synth:   {scale:.6f}")

for r in synth:
    r["open"]   = round(r["open"]   * scale, 4)
    r["high"]   = round(r["high"]   * scale, 4)
    r["low"]    = round(r["low"]    * scale, 4)
    r["close"]  = round(r["close"]  * scale, 4)
    r["adjusted_close"] = round(r["adjusted_close"] * scale, 4)

# Concatenate synthetic + real UDOW
combined = synth + udow_real
combined.sort(key=lambda r: r["date"])

# Sanity: continuous handoff
synth_last = combined[len(synth) - 1]
real_first_check = combined[len(synth)]
print(f"\nSplice handoff:")
print(f"  Synthetic last  ({synth_last['date']}):  close={synth_last['close']:.4f}")
print(f"  Real first      ({real_first_check['date']}):  open ={real_first_check['open']:.4f}  "
      f"close={real_first_check['close']:.4f}")
print(f"  Overnight gap:  open/synth_close = {real_first_check['open']/synth_last['close']:.4f}")

# Save
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUT_PATH, "w") as f:
    json.dump(combined, f, indent=2)
print(f"\nSaved: {OUT_PATH}  ({len(combined)} bars, {len(synth)} synthetic + {len(udow_real)} real)")

# Quick sanity: synthetic UDOW first bar
print(f"\nFirst synthetic UDOW bar (after scaling):")
print(f"  {combined[0]}")
print(f"Spot check: UDOW adj[0] = {combined[0]['adjusted_close']:.4f}")
