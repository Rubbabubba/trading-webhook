"""Shadow-only sports challenger that emits passive, persistent entry signals.

The module deliberately has no order API.  It converts already validated sports
observations into preregistered shadow signals that can be scored prospectively.
"""
from datetime import datetime
import math


def initial():
    return {'candidate': None, 'signaled': False, 'signal': None, 'rejections': {}}


def _reject(state, reason):
    state['rejections'][reason] = state['rejections'].get(reason, 0) + 1
    return {'action': 'wait', 'reason': reason}


def _maker_fee_cents(price, count, coefficient):
    return math.ceil(coefficient * count * price * (1 - price) * 100 - 1e-9)


def evaluate(state, observation, now, config):
    """Evaluate one observation and mutate ``state``; never place an order.

    A signal requires the same side to retain a cost-stressed edge across two
    distinct play states separated in time.  The proposed price always rests
    below the ask, so a future executor can enforce post-only semantics.
    """
    if state['signaled']:
        return _reject(state, 'one_signal_per_event')
    model = observation.get('model') or {}
    if model.get('completed'):
        return _reject(state, 'event_completed')
    if model.get('blockers') or not observation.get('admission_ok', False):
        state['candidate'] = None
        return _reject(state, 'source_or_mapping_gate')
    signature = model.get('signature')
    if not signature:
        state['candidate'] = None
        return _reject(state, 'missing_play_signature')

    choices = []
    for side, probability in (model.get('probabilities') or {}).items():
        market = (observation.get('markets') or {}).get(side) or {}
        quote = market.get('quote') or {}
        try:
            bid, ask = float(quote['bid']), float(quote['ask'])
            coefficient = float(market['maker_fee_coefficient'])
        except (KeyError, TypeError, ValueError):
            continue
        if (not market.get('valid') or not 0 <= bid < ask <= 1
                or ask - bid > config['max_spread'] or not 0 <= probability <= 1):
            continue
        # Join the bid by default.  Improve one tick only when it remains passive.
        limit_price = min(bid + config['improve_by'], ask - config['tick_size'])
        if limit_price < bid or limit_price >= ask:
            continue
        count = config['contracts']
        entry_fee = _maker_fee_cents(limit_price, count, coefficient) / (100 * count)
        exit_fee = _maker_fee_cents(probability, count, coefficient) / (100 * count)
        stressed_cost = entry_fee + exit_fee + config['adverse_selection_stress']
        edge = probability - limit_price - stressed_cost
        if edge >= config['minimum_stressed_edge']:
            choices.append((edge, side, probability, limit_price, stressed_cost, market.get('ticker')))
    if not choices:
        state['candidate'] = None
        return _reject(state, 'no_cost_stressed_passive_edge')

    edge, side, probability, price, stressed_cost, ticker = max(choices)
    current = {'side': side, 'signature': str(signature), 'at': now.isoformat(),
               'probability': probability, 'limit_price': price, 'edge': edge,
               'stressed_cost': stressed_cost, 'ticker': ticker}
    previous = state.get('candidate')
    if not previous or previous['side'] != side:
        state['candidate'] = current
        return _reject(state, 'awaiting_second_distinct_play')
    if previous['signature'] == current['signature']:
        return _reject(state, 'awaiting_second_distinct_play')
    elapsed = (now - datetime.fromisoformat(previous['at'])).total_seconds()
    if elapsed < config['minimum_confirmation_seconds']:
        return _reject(state, 'confirmation_too_soon')
    state['signaled'] = True
    state['signal'] = {**current, 'contracts': config['contracts'], 'post_only': True,
                       'first_signature': previous['signature'],
                       'confirmation_seconds': elapsed}
    return {'action': 'shadow_post_only_signal', 'signal': state['signal']}


def position_action(position, observation, contradictory_fresh_states, config):
    """Describe position handling; a feed outage alone can never force a sale."""
    market = (observation.get('markets') or {}).get(position['side']) or {}
    if market.get('settlement_ok') and market.get('result') in ('yes', 'no'):
        return {'action': 'settle'}
    model = observation.get('model') or {}
    if model.get('blockers') or not model.get('probabilities'):
        return {'action': 'hold', 'reason': 'feed_or_mapping_failure_freeze'}
    probability = model['probabilities'].get(position['side'])
    if probability is not None and contradictory_fresh_states >= config['contradictory_states_for_exit']:
        return {'action': 'stage_post_only_exit', 'reason': 'persistent_thesis_reversal'}
    return {'action': 'hold', 'reason': 'hold_for_settlement_or_persistent_reversal'}
