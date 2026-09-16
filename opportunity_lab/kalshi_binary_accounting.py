"""Explicit YES/NO intent accounting used by the atomic binary journal."""
from copy import deepcopy
from fractions import Fraction

from .kalshi_execution_risk import amount, replay
from .kalshi_order_direction import terms, outcome_for_book


def reservation(outcome, action, count, price_cents, fee_cents):
    terms(outcome, action, price_cents)
    if type(count) is not int or not 1 <= count <= 1000000 or type(fee_cents) is not int or fee_cents < 0:
        raise ValueError('invalid_reservation')
    return count * price_cents + fee_cents if action == 'buy' else fee_cents


def replay_binary(records, evidence, *, as_of, settlements=()):
    """Require immutable economic intent alongside V2 payloads; never infer it.

    Existing YES-only FIFO replay is used in each outcome's own price space.
    Raw evidence is checked before normalization; it is never modified in place.
    Opposing inventories/pending directions in one ticker are conservatively
    disallowed for this protocol, including sequential switches in one ledger.
    Binary settlements are validated in the matching outcome's price space.
    """
    groups = {}; outcomes = {}; seen = set(); client_ids = set()
    reserved_exits = {}
    for record in records:
        intent = record['intent']; p = record['payload']; cid = p['client_order_id']
        if not isinstance(cid, str) or not cid or cid in client_ids:
            raise ValueError('duplicate_intent')
        client_ids.add(cid)
        outcome, action = intent['outcome'], intent['action']
        expected = terms(outcome, action, intent['price_cents'])
        count = intent['count']
        reserve = reservation(outcome, action, count, intent['price_cents'], intent['fee_cents'])
        if (type(record['reserve']) is not int or record['reserve'] != reserve
                or type(record['filled']) is not int or not 0 <= record['filled'] <= count
                or record['state'] not in ('reserved','uncertain','working','terminal')
                or p.get('side') != expected['side'] or amount(p['price']) != amount(expected['price'])
                or amount(p['count']) != count or p.get('reduce_only',False) is not expected['reduce_only']
                or type(p.get('subaccount')) is not int or p['subaccount'] != 0):
            raise ValueError('intent_payload_mismatch')
        ticker = p['ticker']
        if not isinstance(ticker, str) or not ticker:
            raise ValueError('invalid_ticker')
        if ticker in outcomes and outcomes[ticker] != outcome:
            raise ValueError('opposing_outcomes_not_supported')
        outcomes[ticker] = outcome
        if action == 'sell' and record['state'] != 'terminal':
            reserved_exits[ticker] = reserved_exits.get(ticker,0) + count-record['filled']
        normalized = deepcopy(record)
        normalized['payload']['side'] = 'bid' if action == 'buy' else 'ask'
        group_records, group_evidence = groups.setdefault(ticker, ([], {}))
        group_records.append(normalized)
        detail = evidence.get(cid)
        if detail is None:
            continue
        converted = deepcopy(detail); gross = Fraction(0); original_gross = Fraction(0); fees = Fraction(0)
        for original, fill in zip(detail['fills'],converted['fills']):
            fid = original['fill_id']
            if not isinstance(fid,str) or not fid or fid in seen:
                raise ValueError('duplicate_fill')
            seen.add(fid)
            if original.get('book_side') != p['side'] or original.get('outcome_side') != outcome_for_book(p['side']):
                raise ValueError('fill_direction_mismatch')
            q, yes = amount(original['count_fp']), amount(original['yes_price_dollars'])
            limit = amount(p['price'])
            if yes > 1 or (yes > limit if p['side']=='bid' else yes < limit):
                raise ValueError('fill_exceeds_limit')
            economic = yes if outcome == 'yes' else 1-yes
            original_gross += q*yes; gross += q*economic; fees += amount(original['fee_cost'])
            # Decimal division below preserves exact finite binary-market prices.
            from decimal import Decimal
            fill['yes_price_dollars'] = str(Decimal(economic.numerator)/Decimal(economic.denominator))
            fill['book_side'] = normalized['payload']['side']
            fill['outcome_side'] = outcome_for_book(fill['book_side'])
        if original_gross != amount(detail['gross_dollars']) or fees != amount(detail['fees_dollars']):
            raise ValueError('raw_totals_mismatch')
        if (gross+fees if action=='buy' else fees)*100 > reserve:
            raise ValueError('actual_cost_exceeds_reservation')
        from .kalshi_execution_risk import display
        converted['gross_dollars'] = display(gross)
        group_evidence[cid] = converted
    normalized_settlements = {}
    for row in settlements:
        ticker = row.get('ticker')
        if ticker not in groups or ticker in normalized_settlements:
            raise ValueError('external_or_duplicate_settlement')
        converted = deepcopy(row)
        if outcomes[ticker] == 'no':
            if row.get('market_result') not in ('yes','no'):
                raise ValueError('unsupported_settlement_result')
            converted['market_result'] = 'yes' if row['market_result']=='no' else 'no'
            for suffix in ('count_fp','total_cost_dollars'):
                converted['yes_'+suffix],converted['no_'+suffix] = row['no_'+suffix],row['yes_'+suffix]
            if row.get('value') is not None:
                if type(row['value']) is not int or row['value'] not in (0,100):
                    raise ValueError('unsupported_settlement_value')
                converted['value'] = 100-row['value']
        normalized_settlements[ticker] = converted
    states = {ticker: replay(rs, es, as_of=as_of,
                            settlements=[normalized_settlements[ticker]] if ticker in normalized_settlements else ())
              for ticker,(rs,es) in groups.items()}
    positions = {t: s['positions'].get(t,0)*(1 if outcomes[t]=='yes' else -1) for t,s in states.items()}
    for ticker, quantity in reserved_exits.items():
        if quantity > abs(positions[ticker]):
            raise ValueError('reserved_exits_exceed_inventory')
    result = {key: sum((s[key] for s in states.values()), Fraction(0))
              for key in ('open_basis','pending_reserves','capital_at_risk','fees','cashflow','realized','daily_realized')}
    from .kalshi_execution_risk import CENTRAL
    # Sum per-ticker lows is a conservative lower bound on portfolio daily P/L.
    result.update(day=as_of.astimezone(CENTRAL).date().isoformat(),
                  daily_low=sum((s['daily_low'] for s in states.values()),Fraction(0)),
                  outcomes=outcomes, positions={t:q for t,q in positions.items() if q},
                  available_to_exit={t:abs(q)-reserved_exits.get(t,0) for t,q in positions.items()})
    return result


def check_budget(state, *, outcome, action, ticker, count, price_cents, fee_cents,
                 cash_cents, capital_limit_cents, order_limit_cents):
    if any(type(v) is not int or v < 0 for v in (cash_cents,capital_limit_cents,order_limit_cents)):
        raise ValueError('invalid_budget')
    reserve = reservation(outcome,action,count,price_cents,fee_cents)
    position = state['positions'].get(ticker,0)
    if ticker in state['outcomes'] and state['outcomes'][ticker] != outcome:
        raise ValueError('opposing_outcomes_not_supported')
    if position and (position>0) != (outcome=='yes'):
        raise ValueError('opposing_outcomes_not_supported')
    if action=='sell' and count > state['available_to_exit'].get(ticker,0):
        raise ValueError('exit_exceeds_inventory')
    if reserve > order_limit_cents or state['pending_reserves']*100+reserve > cash_cents:
        raise ValueError('cash_or_order_limit')
    if action=='buy' and state['capital_at_risk']*100+reserve > capital_limit_cents:
        raise ValueError('capital_limit')
    return reserve


def reconcile_positions(state, rows):
    from decimal import Decimal, InvalidOperation
    observed = {}
    for row in rows:
        ticker = row['ticker']; value = row['position_fp']
        if not isinstance(ticker,str) or not ticker or ticker in observed or not isinstance(value,str) or len(value)>40:
            raise ValueError('invalid_position')
        try: quantity = Decimal(value)
        except InvalidOperation: raise ValueError('invalid_position') from None
        if not quantity.is_finite() or quantity != quantity.to_integral_value() or abs(quantity)>1000000:
            raise ValueError('invalid_position')
        observed[ticker] = int(quantity)
    if {t:q for t,q in observed.items() if q} != state['positions']:
        raise ValueError('external_or_unreconciled_position')
    return True
