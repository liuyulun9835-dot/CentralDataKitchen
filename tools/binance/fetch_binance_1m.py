#!/usr/bin/env python3
"""Fetch Binance Futures 1m klines into a CSV file."""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import List, Optional

import requests

from tools.binance.common import timeutils

BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
MAX_LIMIT = 1500
BACKOFF_SECONDS = 1.0
BACKOFF_CAP = 60.0


def read_last_timestamp(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    last_row: Optional[List[str]] = None
    with open(path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp)
        for row in reader:
            if row and row[0] != "timestamp":
                last_row = row
    if last_row:
        return last_row[0]
    return None


def write_rows(path: str, rows: List[List[str]]) -> None:
    if not rows:
        return
    file_exists = os.path.exists(path)
    write_header = not file_exists or os.path.getsize(path) == 0
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        if write_header:
            writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)


def fetch_klines(symbol: str, date: str, out_path: str) -> None:
    symbol = symbol.upper()
    minute_range = timeutils.build_minute_range(date)
    start_open_ms = minute_range.start_ms
    end_close_ms = minute_range.end_ms

    last_timestamp = read_last_timestamp(out_path)
    last_close_ms = None
    if last_timestamp:
        last_close_ms = timeutils.iso8601_to_ms(last_timestamp)
        timeutils.ensure_minute_alignment(last_close_ms)
        start_open_ms = max(start_open_ms, timeutils.minute_open_from_close(last_close_ms))

    session = requests.Session()
    backoff = BACKOFF_SECONDS
    has_progress = False

    while start_open_ms < end_close_ms:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": start_open_ms,
            "endTime": end_close_ms,
            "limit": MAX_LIMIT,
        }

        try:
            response = session.get(BASE_URL, params=params, timeout=10)
        except requests.RequestException as exc:
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP)
            print(f"network error: {exc}; retrying in {backoff:.1f}s", file=sys.stderr)
            continue

        if response.status_code == 429:
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP)
            print(f"HTTP 429 received; retrying in {backoff:.1f}s", file=sys.stderr)
            continue

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise SystemExit(f"HTTP error: {exc}") from exc

        data = response.json()
        backoff = BACKOFF_SECONDS
        if not data:
            break

        rows: List[List[str]] = []
        for entry in data:
            open_ms = int(entry[0])
            close_ms = timeutils.minute_close_from_open(open_ms)
            if close_ms > end_close_ms:
                continue
            if last_close_ms and close_ms <= last_close_ms:
                continue

            timestamp_iso = timeutils.ms_to_iso8601(close_ms)
            rows.append([
                timestamp_iso,
                entry[1],
                entry[2],
                entry[3],
                entry[4],
                entry[5],
            ])
            last_close_ms = close_ms
            has_progress = True

        if rows:
            write_rows(out_path, rows)

        last_open_ms = int(data[-1][0])
        next_open = last_open_ms + 60_000
        if next_open <= start_open_ms:
            next_open = start_open_ms + 60_000
        start_open_ms = next_open

    if not has_progress and last_timestamp:
        print("no new data fetched; CSV already up to date", file=sys.stderr)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Binance 1m klines into CSV")
    parser.add_argument("--symbol", required=True, help="Binance symbol, e.g. BTCUSDT")
    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD")
    parser.add_argument("--out", required=True, help="Output CSV path")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    fetch_klines(args.symbol, args.date, args.out)


if __name__ == "__main__":
    main()
