#!/bin/bash
# FourSleeve — Daily runner
# Incrementally updates EODHD history, then generates and emails the daily signal.

set -e

# Load credentials (EODHD_API_TOKEN, GOOGLE_EMAIL/APP_PASSWORD, STOCKS_DIR, etc.)
source "$HOME/.bash_profile"

# Project subdirectory under $STOCKS_DIR (local to this script)
PROJECT_SUBDIR="FourSleeve"

# Require STOCKS_DIR to be set (e.g. export STOCKS_DIR=/Users/mikedampier/Documents/Development)
if [ -z "$STOCKS_DIR" ]; then
    echo "ERROR: STOCKS_DIR is not set. Add 'export STOCKS_DIR=...' to ~/.bash_profile." >&2
    exit 1
fi

ROOT_DIR="$STOCKS_DIR/$PROJECT_SUBDIR"
if [ ! -d "$ROOT_DIR" ]; then
    echo "ERROR: project directory does not exist: $ROOT_DIR" >&2
    exit 1
fi
cd "$ROOT_DIR"

# Use project venv
PY=".venv/bin/python3"

echo "=========================================="
echo "  FourSleeve Daily Run — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

echo
echo "── Step 1/2: Incremental EODHD history download ──"
$PY incremental_download_eodhd_history.py --symbols-file current_tickers.json

echo
echo "── Step 2/2: Daily signal report ──"
$PY daily_signals_four_sleeve.py

echo
echo "=========================================="
echo "  Done — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
