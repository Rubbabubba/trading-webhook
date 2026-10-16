"""Give existing bounded markout windows priority over optional research reads.

This changes runner scheduling only. Signal decisions, timestamps, lateness
allowances, old observations, order authority and risk gates are unchanged.
"""
import math
import time

from .kalshi_high_edge_shadow import PROTOCOL as HIGH_EDGE
from .kalshi_timely_shadow_measurement import PROTOCOL as TIMELY

# Largest optional collector uses four reads. Each can consume its eight-second
# timeout plus a two-second rate wait. Leave another twenty seconds for the
# two-read measurement quote and a two-second loop wait. This is a scheduling
# reserve, not a guarantee of network service or a new lateness allowance.
RESERVE_SECONDS = 65


def next_window(db, now, *, due_only=False):
    if not math.isfinite(now):
        raise ValueError('measurement_scheduler_invalid_clock')
    windows = []
    for identity, ticker, at in db.execute(
            'SELECT id,ticker,at FROM high_edge_v15_signals WHERE at>=?',
            (now - 305,)):
        marked = {r[0] for r in db.execute(
            'SELECT horizon FROM high_edge_v15_marks WHERE signal_id=?', (identity,))}
        for horizon in HIGH_EDGE['horizons_seconds']:
            due = at + horizon
            deadline = due + HIGH_EDGE['maximum_markout_lateness_seconds']
            if horizon not in marked and now <= deadline and (not due_only or due <= now):
                windows.append((deadline, due, 'v15', str(identity), ticker))
    for strategy, identity, ticker, at in db.execute(
            'SELECT strategy,source_id,ticker,at FROM timely_shadow_signals WHERE at>=?',
            (now - 305,)):
        marked = {r[0] for r in db.execute(
            'SELECT horizon FROM timely_shadow_marks WHERE strategy=? AND source_id=?',
            (strategy, identity))}
        for horizon in TIMELY['horizons_seconds']:
            due = at + horizon
            deadline = due + TIMELY['maximum_lateness_seconds']
            if horizon not in marked and now <= deadline and (not due_only or due <= now):
                windows.append((deadline, due, strategy, str(identity), ticker))
    return min(windows) if windows else None


def protected(db, now):
    window = next_window(db, now)
    return window is not None and window[1] <= now + RESERVE_SECONDS


def due_ticker(db, now):
    window = next_window(db, now, due_only=True)
    return window[4] if window else None


def allow_optional(state, stage, *, clock=None):
    """Recheck before each collector, rather than once before a long batch."""
    now = (clock or time.time)()
    if not protected(state.db, now):
        return True
    receipt = state.load('measurement_scheduler', {})
    counts = receipt.get('deferred', {})
    counts[stage] = counts.get(stage, 0) + 1
    state.save('measurement_scheduler', {
        'schema': 'kalshi_measurement_scheduler_v1', 'reserve_seconds': RESERVE_SECONDS,
        'deferred': counts, 'last_deferred_at': now,
        'execution_enabled': False, 'lateness_allowance_changed': False,
    })
    return False
