"""Offline normalized receipt validation. Never verifies source authenticity or submits orders."""
import hashlib
import json
from decimal import Decimal, InvalidOperation


def number(value):
    if isinstance(value, bool):
        raise ValueError('invalid_number')
    try:
        n = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError('invalid_number') from None
    if not n.is_finite() or n < 0:
        raise ValueError('nonfinite_or_negative')
    return n


def canonical(record):
    return json.dumps(record, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def validate(bundles):
    """Return capital-calculator rows only after exact receipt joins.

    A bundle contains entry, resolution and explicit cash-credit normalized
    receipts; each has payload and sha256 of its canonical bytes. This does
    not authenticate the broker, certify an observed balance change, infer
    settlement payment, or support partial credits/collateral netting.
    Historical data must not be promoted merely because this function passes.
    """
    entries, credits, seen, accounts = [], [], set(), set()
    for b in bundles:
        decoded = {}
        for kind in ('entry', 'resolution', 'cash_credit'):
            envelope = b[kind]
            row = envelope['payload']
            if hashlib.sha256(canonical(row)).hexdigest() != envelope['sha256']:
                raise ValueError('receipt_hash_mismatch')
            if row.get('kind') != kind or row.get('environment') != 'demo':
                raise ValueError('receipt_kind_or_environment')
            for key in ('receipt_id', 'account', 'entry_id', 'event', 'ticker'):
                if not isinstance(row.get(key), str) or not row[key]:
                    raise ValueError('missing_identity')
            identity = (row['account'], row['receipt_id'])
            if identity in seen:
                raise ValueError('duplicate_receipt')
            seen.add(identity)
            decoded[kind] = row
        e, s, c = (decoded[k] for k in ('entry', 'resolution', 'cash_credit'))
        for key in ('account', 'entry_id', 'event', 'ticker'):
            if len({r[key] for r in (e, s, c)}) != 1:
                raise ValueError('unmatched_identity')
        accounts.add(e['account'])
        if len(accounts) != 1:
            raise ValueError('mixed_account_cash')
        if e.get('side') not in ('yes', 'no') or s.get('result') not in ('yes', 'no'):
            raise ValueError('unsupported_side_or_settlement')
        qty, price, fee = map(number, (e['quantity'], e['price_dollars'], e['fee_dollars']))
        if qty <= 0 or qty != qty.to_integral_value() or not 0 < price < 1:
            raise ValueError('unsupported_quantity_or_price')
        filled, entry_seen = map(number, (e['filled_at'], e['observed_at']))
        resolved, resolution_seen = map(number, (s['resolved_at'], s['observed_at']))
        credited, credit_seen = map(number, (c['credited_at'], c['observed_at']))
        if not (filled <= entry_seen <= credit_seen and filled <= resolved <= resolution_seen <= credit_seen and resolved <= credited <= credit_seen):
            raise ValueError('receipt_chronology')
        gross, credit_fee, net = map(number, (c['gross_dollars'], c['fee_dollars'], c['net_dollars']))
        expected = qty if e['side'] == s['result'] else Decimal(0)
        if gross != expected or net != gross - credit_fee or c.get('complete') is not True:
            raise ValueError('unsupported_or_unreconciled_credit')
        entries.append(dict(id=e['entry_id'], event=e['event'], filled_at=str(filled), observed_at=str(entry_seen), debit=str(qty * price + fee)))
        credits.append(dict(id=c['receipt_id'], entry_id=e['entry_id'], resolved_at=str(resolved), credited_at=str(credited), observed_at=str(credit_seen), credit=str(net)))
    if len({r['id'] for r in entries}) != len(entries):
        raise ValueError('duplicate_entry')
    return dict(entries=entries, credits=credits, execution_enabled=False,
                authenticity_verified=False, cash_balance_reconciliation_verified=False,
                profitability_verified=False, scope='normalized_full_binary_settlement_only')
