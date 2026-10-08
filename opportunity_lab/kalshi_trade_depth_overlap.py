"""Read-only, no-lookahead overlap of sampled public trades with prior observations.

An overlap is a research lead, never an owned fill or a profitability result.
"""

from bisect import bisect_right
from collections import defaultdict


def _prior(rows, ticker, at, window):
    times = rows.get(ticker, ())
    index = bisect_right(times, at) - 1
    return index >= 0 and at - times[index] <= window


def report(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {'depth_cohort_trades', 'targeted_public_trades', 'depth_snapshots',
                'v12_shadow_signals'}
    if not required <= tables:
        return {'state': 'unavailable', 'missing_tables': sorted(required - tables),
                'execution_enabled': False, 'owned_fills': 0, 'promotion_ready': False}

    depth = defaultdict(list)
    for ticker, at in db.execute('SELECT ticker,observed_at FROM depth_snapshots'):
        depth[ticker].append(at)
    signals = defaultdict(list)
    for ticker, at in db.execute('SELECT ticker,observed_at FROM v12_shadow_signals'):
        signals[ticker].append(at)
    for rows in (depth, signals):
        for times in rows.values():
            times.sort()

    trades = {}
    for table in ('depth_cohort_trades', 'targeted_public_trades'):
        for identity, ticker, at in db.execute(f'SELECT id,ticker,executed_at FROM {table}'):
            trades.setdefault(identity, (ticker, at))

    depth_ids = set()
    signal_ids = set()
    combined_ids = set()
    tickers = set()
    for identity, (ticker, at) in trades.items():
        tickers.add(ticker)
        has_depth = _prior(depth, ticker, at, 30)
        has_signal = _prior(signals, ticker, at, 300)
        if has_depth:
            depth_ids.add(identity)
        if has_signal:
            signal_ids.add(identity)
        if has_depth and has_signal:
            combined_ids.add(identity)

    return {'state': 'observed', 'sampled_public_trades': len(trades),
            'distinct_trade_tickers': len(tickers),
            'prior_depth_within_30s': len(depth_ids),
            'prior_v12_signal_within_300s': len(signal_ids),
            'both_prior_observations': len(combined_ids),
            'both_prior_observation_tickers': len({trades[i][0] for i in combined_ids}),
            'sampled_only': True, 'price_side_and_queue_not_assessed': True,
            'execution_enabled': False, 'owned_fills': 0, 'promotion_ready': False}
