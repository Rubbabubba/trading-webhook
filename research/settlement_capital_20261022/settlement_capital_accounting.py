"""Read-only binary-contract capital accounting; no order or forecast interface.

Receipt times govern availability. Settlement labels never unlock cash: only an
explicit cash credit does. Input provenance remains a prerequisite, not a claim
made by this calculator. All money is Decimal dollars, all times UTC seconds.
"""
from decimal import Decimal, InvalidOperation
from statistics import median


def number(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('invalid_number') from None
    if not result.is_finite():
        raise ValueError('nonfinite_number')
    return result


def review(entries, credits, *, cutoff, initial_cash):
    """Account a fixed cohort as known at cutoff; reject ambiguous cash identity.

    Entries: id,event,filled_at,observed_at,debit (price plus recorded fees).
    Credits: id,entry_id,resolved_at,credited_at,observed_at,credit (net fees).
    One complete cash release per entry; partial/offset/collateral netting inputs
    are unsupported and must stay excluded, never silently aggregated.
    """
    cutoff, cash = number(cutoff), number(initial_cash)
    if cash < 0:
        raise ValueError('negative_initial_cash')
    by_id, credit_by_entry, identities = {}, {}, set()
    for row in entries:
        key = row['id']
        if key in by_id or not key or not row['event']:
            raise ValueError('duplicate_or_missing_entry')
        filled, observed, debit = map(number, (row['filled_at'], row['observed_at'], row['debit']))
        if filled > observed or debit <= 0:
            raise ValueError('invalid_entry')
        by_id[key] = dict(row, filled_at=filled, observed_at=observed, debit=debit)
    for row in credits:
        key, entry_id = row['id'], row['entry_id']
        if not key or key in identities or entry_id in credit_by_entry:
            raise ValueError('duplicate_credit')
        identities.add(key)
        if entry_id not in by_id:
            raise ValueError('unowned_credit')
        entry = by_id[entry_id]
        resolved, credited, observed, credit = map(number, (row['resolved_at'], row['credited_at'], row['observed_at'], row['credit']))
        if not entry['filled_at'] <= resolved <= credited <= observed or credit < 0:
            raise ValueError('invalid_credit_chronology_or_amount')
        credit_by_entry[entry_id] = dict(row, resolved_at=resolved, credited_at=credited, observed_at=observed, credit=credit)
    cash_events, paths, known_delays = [], [], []
    for key, entry in by_id.items():
        if entry['observed_at'] > cutoff:
            continue
        debit = entry['debit']
        # Conservative receipt availability, not backdated economic timestamps.
        start = entry['observed_at']
        cash_events.append((start, 0, key, -debit))
        credit = credit_by_entry.get(key)
        complete = credit is not None and credit['observed_at'] <= cutoff
        end = credit['observed_at'] if complete else cutoff
        net = credit['credit'] - debit if complete else None
        paths.append({'entry_id': key, 'event': entry['event'], 'complete': complete,
                      'locked_dollar_seconds': str(debit * (end-start)),
                      'net_dollars': str(net) if complete else None,
                      'locked_at_cutoff': '0' if complete else str(debit)})
        if complete:
            cash_events.append((end, 1, credit['id'], credit['credit']))
            known_delays.append(credit['credited_at']-credit['resolved_at'])
    minimum_cash = cash
    for _, _, _, amount in sorted(cash_events):
        cash += amount
        minimum_cash = min(minimum_cash, cash)
    if minimum_cash < 0:
        raise ValueError('cash_capacity_exceeded')
    return {'schema': 'settlement_capital_accounting_v1', 'cutoff': str(cutoff),
            'cash': str(cash), 'minimum_cash': str(minimum_cash), 'paths': paths,
            'locked_dollar_seconds': str(sum((number(p['locked_dollar_seconds']) for p in paths), Decimal(0))),
            'open_locked_dollars': str(sum((number(p['locked_at_cutoff']) for p in paths), Decimal(0))),
            'known_delay_samples': len(known_delays),
            'median_known_credit_delay_seconds': str(median(known_delays)) if known_delays else None,
            'profitability_verified': False, 'fills_verified_by_calculator': False,
            'execution_enabled': False}
