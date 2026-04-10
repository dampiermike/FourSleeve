#!/usr/bin/env python3
"""Download historical OHLC data from EODHD for a list of tickers."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Iterable, List, Tuple

import requests


DEFAULT_SYMBOLS_FILE = Path("json/US_symbols_20260317.json")
DEFAULT_OUTPUT_DIR = Path("json/history")
DEFAULT_START = date(2000, 1, 3)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download EODHD history for a list of tickers.")
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS_FILE,
        help="Path to the JSON file containing the symbol list.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to store per-ticker JSON files.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START.isoformat(),
        help="Start date (YYYY-MM-DD). Default: 2000-01-03.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("EODHD_API_TOKEN"),
        help="API token (or set EODHD_API_TOKEN).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds each worker sleeps after a request (default: 0.5).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on number of symbols to download (for testing).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-download symbols even if the output file already exists.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Number of concurrent download threads (default: 8).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds (default: 60).",
    )
    return parser.parse_args()


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid date: {value}") from exc


def to_params(start: date, end: date) -> Tuple[int, int, int, int, int, int]:
    # EODHD's table.csv endpoint expects month values zero-indexed.
    return (
        start.month - 1,
        start.day,
        start.year,
        end.month - 1,
        end.day,
        end.year,
    )


def load_symbols(path: Path, limit: int | None) -> List[str]:
    with path.open() as fh:
        data = json.load(fh)
    symbols: List[str] = []
    for item in data:
        code = item.get("Code")
        if not code:
            continue
        symbols.append(f"{code}.US")
        if limit is not None and len(symbols) >= limit:
            break
    return symbols


def sanitize(symbol: str) -> str:
    return symbol.replace("/", "_").replace(".", "_")


def fetch_history(symbol: str, token: str, params: Tuple[int, int, int, int, int, int], timeout: float) -> list:
    a, b, c, d, e, f = params
    url = (
        "https://eodhd.com/api/table.csv"
        f"?s={symbol}&a={a}&b={b}&c={c}&d={d}&e={e}&f={f}&g=d&api_token={token}&fmt=json"
    )
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def process_symbol(
    idx: int,
    total: int,
    symbol: str,
    output_dir: Path,
    token: str,
    params: Tuple[int, int, int, int, int, int],
    overwrite: bool,
    timeout: float,
    sleep_seconds: float,
) -> str:
    output_file = output_dir / f"{sanitize(symbol)}.json"
    if output_file.exists() and not overwrite:
        return f"[{idx}/{total}] Skipping {symbol} (exists)"
    try:
        data = fetch_history(symbol, token, params, timeout)
    except requests.HTTPError as exc:
        return f"[{idx}/{total}] HTTP error for {symbol}: {exc.response.status_code}"
    except requests.RequestException as exc:
        return f"[{idx}/{total}] Request failed for {symbol}: {exc}"

    with output_file.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)
    if sleep_seconds:
        time.sleep(sleep_seconds)
    return f"[{idx}/{total}] Saved {len(data)} rows for {symbol} -> {output_file}"


def main() -> None:
    args = parse_args()
    if not args.token:
        print("Error: EODHD API token missing (set EODHD_API_TOKEN or use --token).", file=sys.stderr)
        sys.exit(1)

    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start > end:
        print("Error: start date must be before end date.", file=sys.stderr)
        sys.exit(1)

    params = to_params(start, end)
    symbols = load_symbols(args.symbols_file, args.limit)
    total = len(symbols)
    if total == 0:
        print("No symbols to process.")
        return

    args.output_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {
            executor.submit(
                process_symbol,
                idx,
                total,
                symbol,
                args.output_dir,
                args.token,
                params,
                args.overwrite,
                args.timeout,
                args.sleep,
            ): symbol
            for idx, symbol in enumerate(symbols, start=1)
        }
        for future in as_completed(futures):
            message = future.result()
            print(message)


if __name__ == "__main__":
    main()
