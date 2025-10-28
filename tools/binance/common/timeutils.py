"""Time utilities for Binance data ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Tuple

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_MINUTE_MS = 60_000


def parse_utc_date(date_str: str) -> datetime:
    """Parse a YYYY-MM-DD date string as a UTC datetime at midnight."""
    if not date_str:
        raise ValueError("date_str is required")
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def date_to_utc_range(date_str: str) -> Tuple[int, int]:
    """Return the start (inclusive) and end (exclusive) UTC epoch ms for the given date."""
    start = parse_utc_date(date_str)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def ms_to_iso8601(ms: int) -> str:
    """Convert epoch milliseconds to ISO8601 UTC string with millisecond precision."""
    seconds, remainder = divmod(ms, 1000)
    dt = datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(milliseconds=remainder)
    return dt.strftime(ISO_FORMAT)


def iso8601_to_ms(value: str) -> int:
    """Convert ISO8601 UTC string back to epoch milliseconds."""
    if not value:
        raise ValueError("value is required")
    dt = datetime.strptime(value, ISO_FORMAT)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def minute_close_from_open(open_ms: int) -> int:
    """Return the inclusive minute close (open + 60s) in epoch ms."""
    ensure_minute_alignment(open_ms)
    return open_ms + _MINUTE_MS


def minute_open_from_close(close_ms: int) -> int:
    """Return the minute open (close - 60s) in epoch ms."""
    ensure_minute_alignment(close_ms)
    return close_ms - _MINUTE_MS


def ensure_minute_alignment(ms_value: int) -> None:
    """Ensure the millisecond value lies on exact minute boundaries."""
    if ms_value % _MINUTE_MS != 0:
        raise ValueError(f"{ms_value} is not aligned to minute boundaries")


@dataclass(frozen=True)
class MinuteRange:
    start_ms: int
    end_ms: int

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    def iter_closes(self):
        """Yield minute close timestamps in epoch ms within the range."""
        current = self.start_ms + _MINUTE_MS
        while current <= self.end_ms:
            yield current
            current += _MINUTE_MS


def build_minute_range(date_str: str) -> MinuteRange:
    start, end = date_to_utc_range(date_str)
    return MinuteRange(start_ms=start, end_ms=end)
