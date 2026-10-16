"""Strict public-tape contract for a future separately registered maker test.

No fill inference, network interface or order authority. Direction follows the
official canonical outcome fields, not the misleading idea that book_side
identifies the resting book level that was hit. Explicit YES/NO prices retain
their economic scales; the canonical direction bit never rewrites a price.
"""
from datetime import datetime
from decimal import Decimal


def validate(row, minimum, maximum):
    if not isinstance(row, dict) or row.get('is_block_trade') is not False:
        raise ValueError('non_block_trade_required')
    for name in ('trade_id', 'ticker'):
        if not isinstance(row.get(name), str) or not 1 <= len(row[name]) <= 160:
            raise ValueError('trade_identity_required')
    side = row.get('taker_outcome_side')
    if side not in ('yes', 'no') or row.get('taker_book_side') != ('bid' if side=='yes' else 'ask'):
        raise ValueError('canonical_direction_inconsistent')
    if row.get('taker_side') not in (None, side):
        raise ValueError('legacy_direction_conflict')
    values = []
    for name in ('count_fp', 'yes_price_dollars', 'no_price_dollars'):
        if not isinstance(row.get(name), str) or len(row[name])>40:
            raise ValueError('fixed_point_trade_value_required')
        value = Decimal(row[name])
        if not value.is_finite():
            raise ValueError('nonfinite_trade_value')
        values.append(value)
    count, yes, no = values
    if not 0<count<=1000000000 or not 0<=yes<=1 or not 0<=no<=1 or yes+no!=1:
        raise ValueError('invalid_trade_economic_values')
    stamp = datetime.fromisoformat(row['created_time'].replace('Z', '+00:00'))
    if stamp.tzinfo is None or not minimum<=stamp.timestamp()<=maximum:
        raise ValueError('trade_outside_requested_window')
    return dict(trade_id=row['trade_id'], ticker=row['ticker'], at=stamp.timestamp(),
        count=str(count), yes_price=str(yes), no_price=str(no),
        taker_outcome_side=side, taker_book_side=row['taker_book_side'],
        is_block_trade=False, owned_fill=False)
