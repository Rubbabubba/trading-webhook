"""Measured-delay IOC paper execution, conservative top-level partial fills. No order APIs."""
import math
from .college_football_paper import timestamp


def initial():
    return {'cash_cents': 100000, 'realized_cents': 0, 'entries': 0, 'exits': 0,
            'position': None, 'pending': None, 'fees_cents': 0, 'slippage_cents': 0,
            'peak_cents': 100000, 'drawdown_cents': 0, 'equity_cents': 100000,
            'halted': False, 'last_exit_at': None, 'bad_since': None}


def cost(price, count, coefficient, buy):
    if not 0 <= price <= 1 or not math.isfinite(coefficient) or not 0 <= coefficient <= .14:
        raise ValueError('Invalid price/fee coefficient')
    fee = math.ceil(coefficient * count * price * (1 - price) * 100 - 1e-9)
    slip = count  # One cent per contract on each simulated transaction.
    gross = math.ceil(price * count * 100 - 1e-9) if buy else math.floor(price * count * 100 + 1e-9)
    total = gross + fee + slip if buy else max(0, gross - fee - slip)
    return {'total': total, 'fee': fee, 'slippage': slip, 'gross': gross}


def available(market, count=1):
    q = market.get('quote') or {}
    return bool(market.get('valid') and all(isinstance(q.get(k), (float, int)) and math.isfinite(q[k])
        for k in ('ask', 'bid', 'ask_size', 'bid_size')) and 0 <= q['bid'] <= q['ask'] <= 1
        and min(q['bid_size'], q['ask_size']) >= count)


def entry(market, probability, cash, margin, staged=None, max_count=10):
    if not available(market) or not market.get('entry_service_ok',True) or probability is None or not math.isfinite(probability) or not 0 <= probability <= 1:
        return None, 'unavailable_quote_or_probability'
    q = market['quote']
    if q['ask'] - q['bid'] > market.get('max_spread',.04) + 1e-9:
        return None, 'spread_over_four_cents'
    if staged is not None and q['ask'] > staged + .02 + 1e-9:
        return None, 'confirmation_price_moved'
    price = max(q['ask'], staged) if staged is not None else q['ask']
    rejected = set()
    for count in range(min(10, max_count), 0, -1):
        if not available(market, count):
            rejected.add('insufficient_displayed_size')
            continue
        buy = cost(price, count, market['fee_coefficient'], True)
        sell = cost(q['bid'], count, market['fee_coefficient'], False)
        reserve = cost(probability, count, market['fee_coefficient'], False)
        edge = probability - (buy['total'] + reserve['fee'] + reserve['slippage']) / (100 * count)
        if buy['total'] > min(cash, 1000):
            rejected.add('cash_or_ten_dollar_entry_limit')
            continue
        if buy['total'] <= 0 or (buy['total'] - sell['total']) / buy['total'] > market.get('max_entry_loss_fraction',.15):
            rejected.add('immediate_liquidation_loss_over_15_percent')
            continue
        if edge >= margin:
            return {'price': price, 'count': count, 'cost': buy, 'edge': edge}, None
        rejected.add('insufficient_cost_adjusted_model_edge')
    return None, '; '.join(sorted(rejected))


def mark(state, markets):
    position = state['position']
    equity = state['cash_cents']
    if position:
        market = markets[position['side']]
        if not available(market, position['count']):
            state['equity_cents'] = None
            return
        equity += cost(market['quote']['bid'], position['count'], market['fee_coefficient'], False)['total']
    state['equity_cents'] = equity
    state['peak_cents'] = max(state['peak_cents'], equity)
    state['drawdown_cents'] = max(state['drawdown_cents'], state['peak_cents'] - equity)
    state['halted'] |= 100000 - equity >= 2000


def decide(state, observation, now, hold_seconds, margin, paused=False):
    markets, model = observation['markets'], observation['model']
    actions = []
    position = state['position']
    if position:
        market = markets[position['side']]
        if market.get('settlement_ok') and market.get('result') in ('yes', 'no'):
            payout = 100 * position['count'] if market['result'] == 'yes' else 0
            state['cash_cents'] += payout
            profit = payout - position['cost']['total']
            state['realized_cents'] += profit
            state['position'] = state['pending'] = None
            state['exits'] += 1
            state['last_exit_at'] = now.isoformat()
            mark(state, markets)
            return [{'action': 'settle', 'position': position, 'payout_cents': payout, 'profit_cents': profit}]
    mark(state, markets)
    if paused:
        state['pending'] = None
        return [{'action': 'wait', 'reason': 'operator_pause'}]
    valid_signal = not model['blockers'] and bool(model['probabilities'])
    if valid_signal:
        state['bad_since'] = None
    else:
        state['bad_since'] = state['bad_since'] or now.isoformat()
    pending = state['pending']
    if pending:
        if (now-timestamp(pending['at'])).total_seconds()>30:
            state['pending']=None
            return [{'action':'cancel','reason':'confirmation_expired'}]
        # A second call on the same quote cannot simulate confirmation.
        if markets[pending['side']].get('book_id') == pending['book_id']:
            return [{'action': 'wait', 'reason': 'waiting_new_quote'}]
        elapsed = (now - timestamp(pending['at'])).total_seconds()
        latency = pending['latency_seconds']
        if elapsed < latency:
            return [{'action': 'wait', 'reason': 'measured_arrival_delay'}]
        received = timestamp(markets[pending['side']]['book_received_at'])
        if (received - timestamp(pending['at'])).total_seconds() < latency:
            return [{'action': 'wait', 'reason': 'waiting_post_arrival_book'}]
        if (now-received).total_seconds() > 5:
            return [{'action': 'wait', 'reason': 'post_arrival_book_stale'}]
        state['pending'] = None
        m = markets[pending['side']]
        if elapsed > 30:
            return [{'action': 'cancel', 'reason': 'confirmation_expired'}]
        if pending['action'] == 'buy' and valid_signal and not position and not state['halted']:
            chosen, reason = entry(m, model['probabilities'].get(pending['side']), state['cash_cents'], margin, pending['price'], pending['count'])
            if chosen:
                position = {**chosen, 'side': pending['side'], 'ticker': m['ticker'],
                            'opened_at': now.isoformat(), 'probability': model['probabilities'][pending['side']]}
                state['position'] = position
                state['cash_cents'] -= chosen['cost']['total']
                state['fees_cents'] += chosen['cost']['fee']
                state['slippage_cents'] += chosen['cost']['slippage']
                state['entries'] += 1
                mark(state, markets)
                return [{'action': 'buy', 'position': position, 'requested_count': pending['count'],
                         'unfilled_canceled': pending['count']-position['count'],
                         'latency_seconds': latency, 'observed_fill_delay_seconds': elapsed}]
            return [{'action': 'cancel', 'reason': reason}]
        if pending['action'] == 'sell' and position and available(m, 1):
            price = min(pending['price'], m['quote']['bid'])
            filled = min(position['count'], int(m['quote']['bid_size']))
            proceeds = cost(price, filled, m['fee_coefficient'], False)
            allocated = {k: (v if filled == position['count'] else v*filled//position['count'])
                         for k,v in position['cost'].items()}
            closed_part = {**position, 'count': filled, 'cost': allocated}
            profit = proceeds['total'] - allocated['total']
            state['cash_cents'] += proceeds['total']
            state['realized_cents'] += profit
            state['fees_cents'] += proceeds['fee']
            state['slippage_cents'] += proceeds['slippage']
            remaining = position['count'] - filled
            if remaining:
                state['position'] = {**position, 'count': remaining,
                    'cost': {k:v-allocated[k] for k,v in position['cost'].items()}}
            else:
                state['position'] = None
                state['exits'] += 1
                state['last_exit_at'] = now.isoformat()
            mark(state, markets)
            return [{'action': 'partial_sell' if remaining else 'sell', 'position': closed_part,
                     'price': price, 'proceeds': proceeds, 'profit_cents': profit,
                     'remaining_count': remaining, 'reason': pending['reason'],
                     'latency_seconds': latency, 'observed_fill_delay_seconds': elapsed}]
        return [{'action': 'cancel', 'reason': 'confirmation_gate'}]
    if position:
        m = markets[position['side']]
        if not available(m, 1):
            return [{'action': 'hold', 'reason': 'exit_liquidity_unavailable'}]
        proceeds = cost(m['quote']['bid'], position['count'], m['fee_coefficient'], False)['total']
        probability = model['probabilities'].get(position['side'])
        failed = state['bad_since'] and (now - timestamp(state['bad_since'])).total_seconds() >= 180
        reason = 'loss_limit' if state['halted'] or proceeds <= .75 * position['cost']['total'] else (
            'holding_limit' if (now - timestamp(position['opened_at'])).total_seconds() >= hold_seconds else (
            'game_finished' if model.get('completed') else ('feed_failure' if failed else (
            'fair_value' if valid_signal and probability is not None and proceeds / (100 * position['count']) >= probability - .02 else None))))
        if reason:
            state['pending'] = {'action': 'sell', 'side': position['side'], 'price': m['quote']['bid'],
                                'at': now.isoformat(), 'snapshot': observation['snapshot'], 'book_id': m['book_id'], 'reason': reason, 'latency_seconds': observation['latency_seconds']}
            return [{'action': 'stage_sell', 'reason': reason}]
        return [{'action': 'hold', 'reason': 'remaining_value'}]
    if (not valid_signal or state['halted'] or state['entries'] >= 5
            or not observation.get('admission_ok') or observation.get('latency_seconds') is None):
        return [{'action': 'wait', 'reason': 'entry_gate', 'blockers': model['blockers']}]
    if state['last_exit_at'] and (now - timestamp(state['last_exit_at'])).total_seconds() < 300:
        return [{'action': 'wait', 'reason': 'cooldown'}]
    choices, rejected = [], {}
    for side, market in markets.items():
        candidate, reason = entry(market, model['probabilities'].get(side), state['cash_cents'], margin)
        if candidate:
            choices.append((candidate['edge'], side, candidate))
        else:
            rejected[side] = reason
    if choices:
        _, side, candidate = max(choices)
        state['pending'] = {'action': 'buy', 'side': side, 'price': candidate['price'],
                            'at': now.isoformat(), 'snapshot': observation['snapshot'], 'book_id': markets[side]['book_id'],
                            'count': candidate['count'], 'latency_seconds': observation['latency_seconds']}
        return [{'action': 'stage_buy', 'side': side, 'candidate': candidate}]
    return [{'action': 'wait', 'reason': 'no_cost_adjusted_edge', 'outcomes': rejected}]
