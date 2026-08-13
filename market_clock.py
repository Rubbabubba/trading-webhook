"""Market clock/session helpers for the trading webhook.

The live Alpaca clock call still lives in app.py for now. This module provides
pure helpers so market-hour and session logic can be moved safely in later
cleanup patches.
"""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


MARKET_CLOCK_MODULE_VERSION = "patch-392-broker-market-module-split-prep"

NY_TZ = ZoneInfo("America/New_York")
REGULAR_MARKET_OPEN_NY = time(9, 30)
REGULAR_MARKET_CLOSE_NY = time(16, 0)


def now_ny() -> datetime:
    return datetime.now(tz=NY_TZ)


def parse_hhmm(value: str) -> time:
    parts = str(value or "").strip().split(":")
    return time(int(parts[0]), int(parts[1]))


def parse_session_window(raw: str) -> tuple[time, time] | None:
    text = str(raw or "").strip()
    if not text or "-" not in text:
        return None
    start_raw, end_raw = [part.strip() for part in text.split("-", 1)]
    return parse_hhmm(start_raw), parse_hhmm(end_raw)


def in_session(raw: str, current_time: time | None = None) -> bool:
    window = parse_session_window(raw)
    if not window:
        return True
    start, end = window
    current = current_time or now_ny().time()
    return start <= current <= end


def is_regular_market_time(dt_ny: datetime | None = None) -> bool:
    dt_ny = dt_ny or now_ny()
    current = dt_ny.time()
    return REGULAR_MARKET_OPEN_NY <= current <= REGULAR_MARKET_CLOSE_NY


def market_clock_module_status() -> dict:
    current = now_ny()
    return {
        "ok": True,
        "module": "market_clock",
        "module_version": MARKET_CLOCK_MODULE_VERSION,
        "now_ny": current.isoformat(),
        "regular_market_time": is_regular_market_time(current),
        "regular_open_ny": REGULAR_MARKET_OPEN_NY.isoformat(timespec="minutes"),
        "regular_close_ny": REGULAR_MARKET_CLOSE_NY.isoformat(timespec="minutes"),
    }