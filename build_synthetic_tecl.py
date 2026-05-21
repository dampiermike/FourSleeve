#!/usr/bin/env python3
"""
Build a synthetic TECL (3x Technology Sector) history pre-2008-12-17
and splice it with the real TECL history. Output: json/spliced/TECL_US.json.

Same methodology as build_synthetic_udow.py:
  - Source: XLK raw close-to-close returns (full history from 2000-01-03)
  - 3x daily leverage with 0.04%/day cost drag (~10% annual)
  - Anchor: synthetic 2008-12-16 close handed off to real TECL 2008-12-17 open
"""

import json
from pathlib import Path

WORKSPACE   = Path(__file__).resolve().parent
XLK_PATH    = WORKSPACE / "json" / "history" / "XLK_US.json"
TECL_REAL   = WORKSPACE / "json" / "history" / "TECL_US.json"
OUT_PATH    = WORKSPACE / "json" / "spliced" / "TECL_US.json"

DAILY_COST_DRAG = 0.0004
LEVERAGE        = 3.0
SPLICE_DATE     = "2008-12-17"

with open(XLK_PATH)  as f: xlk_data  = json.load(f)
with open(TECL_REAL) as f: tecl_real = json.load(f)

xlk_data.sort(key=lambda r: r["date"])
tecl_real.sort(key=lambda r: r["date"])

real_first = tecl_real[0]
assert real_first["date"] == SPLICE_DATE, f"Expected {SPLICE_DATE}, got {real_first['date']}"
print(f"Real TECL starts: {real_first['date']}  open={real_first['open']}  close={real_first['close']}")

xlk_pre = [r for r in xlk_data if r["date"] < SPLICE_DATE]
xlk_at_splice = next((r for r in xlk_data if r["date"] == SPLICE_DATE), None)
assert xlk_at_splice is not None
print(f"XLK pre-splice bars: {len(xlk_pre)}  ({xlk_pre[0]['date']} -> {xlk_pre[-1]['date']})")

# First bar
first = xlk_pre[0]
prev_xlk_close = first["close"]
synth = []
day1_open_ret  = (first["open"]  - prev_xlk_close) / prev_xlk_close
day1_high_ret  = (first["high"]  - prev_xlk_close) / prev_xlk_close
day1_low_ret   = (first["low"]   - prev_xlk_close) / prev_xlk_close
day1_close_ret = (first["close"] - prev_xlk_close) / prev_xlk_close
day1_close = 100.0
day1_open  = day1_close * (1 + LEVERAGE * (day1_open_ret  - day1_close_ret))
day1_high  = day1_close * (1 + LEVERAGE * (day1_high_ret  - day1_close_ret))
day1_low   = day1_close * (1 + LEVERAGE * (day1_low_ret   - day1_close_ret))
synth.append(dict(date=first["date"], open=day1_open, high=day1_high, low=day1_low,
                  close=day1_close, adjusted_close=day1_close, volume=0, synthetic=True))

for r in xlk_pre[1:]:
    if prev_xlk_close <= 0:
        prev_xlk_close = r["close"]; continue
    open_ret  = (r["open"]  - prev_xlk_close) / prev_xlk_close
    high_ret  = (r["high"]  - prev_xlk_close) / prev_xlk_close
    low_ret   = (r["low"]   - prev_xlk_close) / prev_xlk_close
    close_ret = (r["close"] - prev_xlk_close) / prev_xlk_close

    prior_close = synth[-1]["close"]
    new_close = prior_close * (1 + LEVERAGE * close_ret - DAILY_COST_DRAG)
    new_open  = prior_close * (1 + LEVERAGE * open_ret)
    new_high  = prior_close * (1 + LEVERAGE * high_ret)
    new_low   = prior_close * (1 + LEVERAGE * low_ret)
    new_open  = max(new_open,  0.01); new_high  = max(new_high,  0.01)
    new_low   = max(new_low,   0.01); new_close = max(new_close, 0.01)
    new_high  = max(new_high, new_open, new_close)
    new_low   = min(new_low,  new_open, new_close)

    synth.append(dict(date=r["date"], open=new_open, high=new_high, low=new_low,
                      close=new_close, adjusted_close=new_close, volume=0, synthetic=True))
    prev_xlk_close = r["close"]

# Anchor
xlk_at_splice_open = xlk_at_splice["open"]
xlk_pre_last_close = xlk_pre[-1]["close"]
implied_overnight  = (xlk_at_splice_open - xlk_pre_last_close) / xlk_pre_last_close
target_synth_last  = real_first["open"] / (1 + LEVERAGE * implied_overnight)

raw_synth_last = synth[-1]["close"]
scale = target_synth_last / raw_synth_last
print(f"\nAnchoring:")
print(f"  Raw synth last close (2008-12-16): {raw_synth_last:.4f}")
print(f"  XLK implied overnight: {implied_overnight*100:+.3f}%")
print(f"  Target synth last close: {target_synth_last:.4f}")
print(f"  Scale factor: {scale:.6f}")

for r in synth:
    r["open"]   = round(r["open"]   * scale, 4)
    r["high"]   = round(r["high"]   * scale, 4)
    r["low"]    = round(r["low"]    * scale, 4)
    r["close"]  = round(r["close"]  * scale, 4)
    r["adjusted_close"] = round(r["adjusted_close"] * scale, 4)

combined = synth + tecl_real
combined.sort(key=lambda r: r["date"])
synth_last = combined[len(synth) - 1]
real_first_check = combined[len(synth)]
print(f"\nSplice handoff:")
print(f"  Synthetic last ({synth_last['date']}):  close={synth_last['close']:.4f}")
print(f"  Real first     ({real_first_check['date']}):  open ={real_first_check['open']:.4f}  close={real_first_check['close']:.4f}")
print(f"  Overnight ratio open/synth_close = {real_first_check['open']/synth_last['close']:.4f}")

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(OUT_PATH, "w") as f:
    json.dump(combined, f, indent=2)
print(f"\nSaved: {OUT_PATH}  ({len(combined)} bars, {len(synth)} synthetic + {len(tecl_real)} real)")
print(f"First synthetic TECL bar: adj[0]={combined[0]['adjusted_close']:.4f}")
