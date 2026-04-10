#!/usr/bin/env python3
"""Incrementally update EODHD JSON history files.

This script inspects existing per-ticker JSON files (as produced by
``download_eodhd_history.py``) and only fetches the missing date range for each
ticker. It keeps prior rows (including any data prior to the requested
start-date) and deduplicates merged output by date.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from download_eodhd_history import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_START,
    DEFAULT_SYMBOLS_FILE,
    fetch_history,
    load_symbols,
    parse_date,
    sanitize,
    to_params,
)


def _parse_args() -> argparse.Namespace:
    today = date.today().isoformat()
    parser = argparse.ArgumentParser(
        description="Incrementally update EODHD JSON history files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS_FILE,
        help="JSON file produced by fetch_eodhd_symbols.py",
    )
    parser.add_argument(
        "--history-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory containing per-symbol JSON history files",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START.isoformat(),
        help="Earliest date to download when seeding or overwriting",
    )
    parser.add_argument(
        "--end-date",
        default=today,
        help="Last date (inclusive) to download",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("EODHD_API_TOKEN"),
        help="EODHD API token (or set EODHD_API_TOKEN)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep after each request (per worker)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of symbols (for testing)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-fetch data starting from --start-date even if files exist",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Number of concurrent download workers",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout for each request",
    )
    return parser.parse_args()


def _safe_parse_row_date(value: str | None) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _load_existing_history(path: Path) -> Tuple[List[Dict], Optional[date]]:
    if not path.exists():
        return [], None
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return [], None

    rows: List[Dict] = []
    latest: Optional[date] = None
    if isinstance(data, list):
        for entry in data:
            if not isinstance(entry, dict):
                continue
            row_date = _safe_parse_row_date(entry.get("date"))
            if row_date is None:
                continue
            rows.append(entry)
            if latest is None or row_date > latest:
                latest = row_date
    return rows, latest


def _merge_rows(existing: List[Dict], new_rows: Iterable[Dict]) -> List[Dict]:
    by_date: Dict[str, Dict] = {row["date"]: row for row in existing if "date" in row}
    for row in new_rows:
        date_key = row.get("date")
        if not date_key:
            continue
        by_date[date_key] = row
    merged_dates = sorted(by_date.keys())
    return [by_date[d] for d in merged_dates]


def _trim_before(rows: List[Dict], cutoff: date) -> List[Dict]:
    trimmed: List[Dict] = []
    for row in rows:
        row_date = _safe_parse_row_date(row.get("date"))
        if row_date is None:
            continue
        if row_date < cutoff:
            trimmed.append(row)
    return trimmed


def process_symbol(
    idx: int,
    total: int,
    symbol: str,
    history_dir: Path,
    token: str,
    base_start: date,
    end_date: date,
    overwrite: bool,
    timeout: float,
    sleep_seconds: float,
) -> str:
    output_file = history_dir / f"{sanitize(symbol)}.json"
    existing_rows, latest_date = _load_existing_history(output_file)
    if overwrite and existing_rows:
        existing_rows = _trim_before(existing_rows, base_start)
        latest_date = None

    if latest_date is None:
        start_date = base_start
    else:
        start_date = max(base_start, latest_date + timedelta(days=1))

    if start_date > end_date:
        return f"[{idx}/{total}] Skipping {symbol} (up-to-date)"

    params = to_params(start_date, end_date)

    try:
        new_rows = fetch_history(symbol, token, params, timeout)
    except Exception as exc:  # fetch_history already raises on HTTP failure
        return f"[{idx}/{total}] Error {symbol}: {exc}"

    if not new_rows:
        return f"[{idx}/{total}] Skipping {symbol} (no data from API)"

    before = len(existing_rows)
    merged = _merge_rows(existing_rows, new_rows)
    added = len(merged) - before
    if added == 0:
        message = f"[{idx}/{total}] Skipping {symbol} (no new rows)"
    else:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", encoding="utf-8") as fh:
            json.dump(merged, fh, ensure_ascii=False, indent=2)
        message = f"[{idx}/{total}] Updated {symbol} (added {added} rows)"

    if sleep_seconds:
        time.sleep(sleep_seconds)
    return message


def main() -> None:
    args = _parse_args()
    token = args.token
    if not token:
        print("Error: missing EODHD API token (use --token or set EODHD_API_TOKEN).", file=sys.stderr)
        sys.exit(1)

    try:
        base_start = parse_date(args.start_date)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"Invalid --start-date: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        end_date = parse_date(args.end_date)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"Invalid --end-date: {exc}", file=sys.stderr)
        sys.exit(1)

    if base_start > end_date:
        print("Error: --start-date must be on or before --end-date.", file=sys.stderr)
        sys.exit(1)

    symbols = load_symbols(args.symbols_file, args.limit)
    total = len(symbols)
    if total == 0:
        print("No symbols to process.")
        return

    history_dir = args.history_dir
    history_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                process_symbol,
                idx,
                total,
                symbol,
                history_dir,
                token,
                base_start,
                end_date,
                args.overwrite,
                args.timeout,
                args.sleep,
            ): symbol
            for idx, symbol in enumerate(symbols, start=1)
        }
        for future in as_completed(futures):
            try:
                print(future.result())
            except Exception as exc:
                symbol = futures[future]
                print(f"Error processing {symbol}: {exc}")


if __name__ == "__main__":
    main()
