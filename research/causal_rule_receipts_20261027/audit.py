"""Pure receipt joins for the existing rule-vintage archive. No network/orders."""
import hashlib
import json
import math
from datetime import datetime


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def audit(version, receipt, *, ticker, decision_at, maximum_age_seconds=30):
    """Audit integrity/causality, never source authenticity or execution readiness.

    No future receipt or latest-version substitution may repair an old decision.
    Inputs are the original SQLite rows decoded to mappings, not new GETs.
    """
    out = dict(schema='causal_rule_receipt_audit_v1_20261027',
               source_authenticity_verified=False, fees_verified=False,
               queue_verified=False, execution_enabled=False, promotion_ready=False)
    def blocked(reason):
        return dict(out, state='blocked', reason=reason)
    if not ticker or version.get('ticker') != ticker or receipt.get('ticker') != ticker:
        return blocked('identity_mismatch')
    fields = json.loads(version['fields_json'])
    if fields.get('ticker') != ticker:
        return blocked('fields_ticker_mismatch')
    vh = digest(fields)
    if vh != version.get('version_hash') or receipt.get('version_hash') != vh:
        return blocked('version_hash_mismatch')
    clocks = [receipt.get(k) for k in ('metadata_started_at', 'metadata_observed_at',
                                      'book_started_at', 'book_observed_at')]
    nums = clocks + [decision_at, version.get('first_observed_at'), maximum_age_seconds]
    if any(type(x) not in (int, float) or not math.isfinite(x) for x in nums):
        return blocked('invalid_clock')
    if maximum_age_seconds != 30:
        return blocked('fixed_freshness_policy_changed')
    if clocks != sorted(clocks) or version['first_observed_at'] > clocks[1]:
        return blocked('receipt_clock_order')
    if digest(dict(ticker=ticker, version=vh, clocks=clocks)) != receipt.get('receipt_hash'):
        return blocked('receipt_hash_mismatch')
    if clocks[-1] > decision_at:
        return blocked('future_book_or_metadata_receipt')
    if decision_at - clocks[1] > maximum_age_seconds:
        return blocked('stale_metadata_receipt')
    if 'status' not in fields:
        return blocked('active_status_not_retained_in_v1_archive')
    if fields.get('status') != 'active':
        return blocked('inactive_contract')
    if not any(fields.get(k) for k in ('rules_primary', 'rules_secondary')):
        return blocked('rules_missing')
    def stamp(value):
        at = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if at.tzinfo is None:
            raise ValueError('timezone_missing')
        return at.timestamp()
    try:
        opened = stamp(fields['open_time'])
        closed = min(stamp(fields[k]) for k in ('close_time', 'expected_expiration_time')
                     if fields.get(k))
    except (ValueError, TypeError, KeyError):
        return blocked('lifecycle_missing_or_ambiguous')
    if not opened <= decision_at < closed:
        return blocked('outside_observed_lifecycle')
    return dict(out, state='receipt_integrity_and_causality_only',
                version_hash=vh, receipt_hash=receipt['receipt_hash'],
                metadata_age_seconds=decision_at-clocks[1],
                reason='archive_hash_does_not_authenticate_venue_rules_fees_or_queue')
