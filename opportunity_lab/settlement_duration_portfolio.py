"""Pure shared research ranking primitive; no transport or order authority.

Inputs must come from separately validated source receipts. This function does
not authenticate receipts, infer probabilities from prices, or establish profit.
Frozen historical and prospective callers use the same decision-time cutoff.
"""
from decimal import Decimal, InvalidOperation
from statistics import median

CAPABILITY_ID = 'settlement_duration_portfolio_v1'

def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('invalid_number') from None
    if not result.is_finite():
        raise ValueError('nonfinite_number')
    return result

def rank(candidates, training, *, decision_at, fee_stress):
    """Rank one independent event per candidate by net return / dollar-day.

    Training observations require explicit resolution, credit and receipt times;
    only receipts strictly before the decision are admitted. Candidate expected
    payout is an externally frozen forecast, NOT the eventual settlement label.
    Missing forecasts, absent prior delays, stale books and partial available
    quantity fail closed. Dollar budget is identical per selected event; unused
    cash stays idle. No simulated fill is represented as a broker fill.
    """
    now, stress = number(decision_at), number(fee_stress)
    if stress < 0:
        raise ValueError('negative_fee_stress')
    delays, seen = [], set()
    for row in training:
        event = row['event_id']
        if not event or event in seen:
            raise ValueError('duplicate_training_event')
        seen.add(event)
        resolved, credited, observed = map(number, (row['resolved_at'], row['credited_at'], row['observed_at']))
        if not resolved <= credited <= observed:
            raise ValueError('invalid_training_chronology')
        if observed < now:
            delays.append(credited - resolved)
    if not delays:
        return []
    delay = median(delays)
    ranked, events = [], set()
    for row in candidates:
        event = row['event_id']
        if not event or event in events or event in seen:
            raise ValueError('nonindependent_event')
        events.add(event)
        quote, expires, forecast_at, resolves, ask, payout, fee, depth, quantity, budget = map(number, (
            row['quote_at'], row['quote_expires_at'], row['forecast_at'], row['expected_resolution_at'],
            row['ask'], row['expected_payout'], row['fee'], row['available_quantity'], row['quantity'], row['capital_budget']))
        if not quote <= now <= expires or forecast_at >= now:
            raise ValueError('stale_or_future_input')
        if not 0 < ask < 1 or not 0 <= payout <= 1 or fee < 0 or quantity <= 0 or quantity != quantity.to_integral_value() or depth < quantity or budget <= 0:
            raise ValueError('unsupported_fill_or_amount')
        duration = resolves + delay - now
        debit = quantity * ask + fee + stress
        if duration <= 0 or debit > budget:
            raise ValueError('duration_or_budget')
        net = quantity * payout - debit
        score = net / (debit * duration / Decimal(86400))
        ranked.append({'event_id': event, 'score': str(score), 'expected_net': str(net),
                       'locked_debit': str(debit), 'expected_locked_seconds': str(duration),
                       'simulation_only': True, 'execution_enabled': False})
    return sorted(ranked, key=lambda row: (-number(row['score']), row['event_id']))
