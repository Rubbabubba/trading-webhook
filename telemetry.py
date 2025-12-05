"""
telemetry.py — in-memory scheduler telemetry store.

Used by:
- app._scheduler_loop() to record the last Scheduler v2 scan + actions
- /debug/last_scan and /debug/last_actions endpoints to expose that state
"""

from __future__ import annotations

import copy
import datetime as dt
import threading
from typing import Any, Dict, Optional

_LOCK = threading.Lock()

_STATE: Dict[str, Any] = {
    "last_tick": None,       # int | None
    "last_ts_utc": None,     # ISO8601 str | None
    "scan": None,            # dict | None
    "actions": None,         # list|dict | None
    "telemetry": None,       # list|dict | None
    "meta": None,            # dict | None
}


def record_scheduler_state(
    *,
    tick: int,
    scan: Dict[str, Any],
    actions: Any,
    meta: Optional[Dict[str, Any]] = None,
    telemetry: Optional[Any] = None,
) -> None:
    """
    Record the latest scheduler v2 tick.
    """
    now = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with _LOCK:
        _STATE["last_tick"] = int(tick)
        _STATE["last_ts_utc"] = now
        _STATE["scan"] = copy.deepcopy(scan)
        _STATE["actions"] = copy.deepcopy(actions)
        _STATE["telemetry"] = copy.deepcopy(telemetry)
        _STATE["meta"] = copy.deepcopy(meta or {})


def get_scheduler_state() -> Dict[str, Any]:
    """
    Return a deep-copied snapshot of the last scheduler state.
    """
    with _LOCK:
        return copy.deepcopy(_STATE)
