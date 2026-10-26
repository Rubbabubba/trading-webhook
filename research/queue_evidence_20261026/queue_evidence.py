"""Offline evidence admission only. No transport, broker or strategy interface."""
from decimal import Decimal


def decimal(value):
    if isinstance(value, bool):
        raise ValueError('boolean_numeric')
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError('nonfinite_numeric')
    return value


def assess(plan, trades, *, tape_complete):
    """Conservative same-level queue depletion; never returns an owned fill.

    Caller supplies a separately verified side normalization and full tape window.
    Cancelled displayed depth never reduces the initial queue-ahead estimate.
    Trade-through at a different price is deliberately not an inferred fill.
    """
    out = dict(schema='causal_queue_evidence_v1_20261026', owned_fills=0,
               execution_enabled=False, promotion_ready=False, profitability_proven=False)
    def result(state, reason, **detail):
        return dict(out, state=state, reason=reason, **detail)
    if plan['environment'] != 'demo':
        raise ValueError('demo_only')
    arrival, cancel, ack, close = map(decimal, (
        plan['arrival_at'], plan['cancel_requested_at'], plan['cancel_ack_at'], plan['close_at']))
    price, ahead, quantity = map(decimal, (plan['price'], plan['queue_ahead'], plan['quantity']))
    if not arrival < cancel <= ack < close or not 0 < price < 1 or ahead < 0 or quantity != 1:
        raise ValueError('invalid_plan_or_lifecycle')
    if plan['side'] not in ('yes', 'no') or not plan['epoch'] or not plan['ticker']:
        raise ValueError('invalid_identity')
    if not all(plan.get(k) is True for k in (
        'metadata_current', 'rules_verified', 'fees_verified', 'activation_book_verified',
        'clock_bound_verified', 'side_mapping_verified')):
        return result('blocked', 'source_metadata_fee_clock_or_activation_proof_missing')
    margin = decimal(plan['clock_margin_seconds'])
    if margin < 1:
        raise ValueError('clock_margin_below_fixed_floor')
    seen, eligible, race, boundary = {}, Decimal(0), Decimal(0), False
    previous = None
    for row in trades:
        at, received, p, size = map(decimal, (
            row['occurred_at'], row['received_at'], row['price'], row['quantity']))
        if not row['id'] or not 0 < p < 1 or size <= 0:
            raise ValueError('invalid_trade')
        identity = (row['ticker'], row['epoch'], row['resting_side'], at, p, size)
        if row['id'] in seen:
            if seen[row['id']] != identity:
                return result('blocked', 'conflicting_trade_identity')
            continue
        seen[row['id']] = identity
        if previous is not None and received < previous:
            return result('blocked', 'receipt_order_regression')
        previous = received
        if row['ticker'] != plan['ticker'] or row['epoch'] != plan['epoch']:
            return result('blocked', 'ticker_or_epoch_mismatch')
        if row['resting_side'] not in ('yes', 'no'):
            return result('blocked', 'ambiguous_trade_direction')
        if at > received + margin:
            return result('blocked', 'trade_after_receipt_clock_bound')
        if row['resting_side'] != plan['side'] or p != price:
            continue
        if arrival - margin <= at <= arrival + margin or cancel - margin <= at <= ack + margin:
            boundary = True
        if arrival + margin < at < cancel - margin:
            eligible += size
        elif cancel <= at < ack:
            race += size
    if tape_complete is not True:
        return result('uncertain', 'incomplete_tape_not_no_fill')
    if boundary:
        return result('uncertain', 'activation_or_cancel_clock_band', cancel_race_volume=str(race))
    residual = max(Decimal(0), eligible - ahead)
    if residual >= quantity:
        return result('conditional_queue_depletion', 'public_tape_not_owned_acknowledgement',
                      eligible_volume=str(eligible), queue_ahead=str(ahead), conditional_quantity='1')
    if residual:
        return result('uncertain', 'partial_depletion_not_whole_fill', residual=str(residual))
    return result('no_evidence', 'queue_not_depleted_not_broker_no_fill', eligible_volume=str(eligible))
