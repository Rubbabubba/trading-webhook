"""Read-only v5/v4 final review. No collection, requests, database or order writes."""
import hashlib
import json
import math
from datetime import datetime, timezone
from fractions import Fraction


def count(value):
    if type(value) is not int or value < 0:
        raise ValueError('invalid_count')
    return value


def close(proof, audit_bytes, expected_audit_sha256):
    """Bind a previously audited, unchanged outcome prefix to a final cap receipt.

    Audit bytes are retained evidence, not an independent new outcome analysis.
    This adapter cannot issue a promotion pass or infer owned executions.
    """
    actual = hashlib.sha256(audit_bytes).hexdigest()
    if actual != expected_audit_sha256:
        raise ValueError('retained_audit_hash_mismatch')
    audit = json.loads(audit_bytes)
    if audit['schema'] != 'conditional_bid_exit_audit_v1_20261024':
        raise ValueError('wrong_audit_protocol')
    source = proof['source']
    comparison = source['formula_comparison']
    if (source['protocol'] != 'demo_continuous_stream_availability_v5_20261023'
            or comparison['protocol'] != 'stream_native_formula_comparison_v4_20261023'):
        raise ValueError('wrong_source_protocol')
    for packet in (source, comparison):
        if packet.get('execution_enabled') is not False or packet.get('fill_assumed') is not False:
            raise ValueError('execution_authority_mismatch')
    if source['state'] != 'capacity_stop' or source.get('storage_stop_reason') != 'archive_capacity':
        raise ValueError('source_not_terminal_archive_capacity')
    if count(proof['source_page_count']) < 11264:
        raise ValueError('capacity_not_independently_supported')
    if count(proof['source_db_records']) != count(source['records']):
        raise ValueError('source_database_count_mismatch')
    if source.get('active') is not False:
        raise ValueError('source_still_active')
    outcomes = comparison['outcome_coverage']
    if count(outcomes['pending']):
        raise ValueError('outcome_drain_pending')
    counts = proof['comparison_counts']
    if (count(counts['inputs']) != count(comparison['accepted_inputs'])
            or count(counts['signals']) != count(comparison['signals'])
            or count(counts['marks']) != count(outcomes['complete'])):
        raise ValueError('status_database_count_mismatch')
    complete = count(comparison['complete_signals'])
    events = count(comparison['complete_events'])
    if (complete != count(audit['complete_signals'])
            or events != count(audit['complete_events'])
            or count(outcomes['complete']) != count(audit['marks_arithmetic_and_time_verified'])
            or count(outcomes['requests']) != count(audit['receipts_hash_verified'])):
        raise ValueError('audit_prefix_changed_reaudit_required')
    # Require full archived signal identity coverage, not just equal aggregate counts.
    audited_ids = {count(r['signal_id']) for r in audit['rows']}
    if audited_ids != set(range(1, counts['signals'] + 1)):
        raise ValueError('audit_signal_identity_coverage')
    if count(outcomes['actual_fills']) or count(comparison['native_quote_evidence']['owned_fills']):
        raise ValueError('owned_execution_requires_separate_dossier')
    latest = source['latest_received_at']
    if type(latest) not in (float, int) or not math.isfinite(latest) or latest < 0:
        raise ValueError('invalid_terminal_time')
    totals = {}
    for horizon in ('5', '30', '300'):
        row = audit['totals_complete_only'][horizon]['challenger']
        if count(row['conditional_paths']) != complete:
            raise ValueError('economic_sample_mismatch')
        totals[horizon] = str(Fraction(row['bid_exit_stressed_cents'])) if complete else None
    return {
        'schema': 'bounded_stream_closure_v1_20261025',
        'state': 'insufficient_evidence',
        'collection_state': 'terminal_original_archive_admission_cap',
        'last_source_receipt_utc': datetime.fromtimestamp(latest, timezone.utc).isoformat(),
        'records': count(source['records']), 'accepted_inputs': counts['inputs'],
        'signals': counts['signals'], 'complete_signals': complete,
        'complete_event_paths': events, 'missing_horizons': count(outcomes['missed']),
        'owned_fills': 0, 'realized_trading_pnl': None,
        'conditional_displayed_bid_exit_stressed_cents': totals,
        'retained_prior_audit_sha256': actual,
        'retained_files': proof['files'],
        'gates': {'minimum_100_complete_signals': complete >= 100,
                  'minimum_40_complete_events': events >= 40,
                  'positive_conditional_bid_exit_all_horizons': all(v is not None and Fraction(v) > 0 for v in totals.values()),
                  'independently_current_metadata': comparison.get('metadata_independently_current') is True,
                  'owned_entry_queue_fee_exit_proof': False,
                  'independent_untouched_validation': False},
        'binding_stage': 'authentic_entry_fillability_and_positive_executable_exit_economics',
        'next_work': 'Prepare source/metadata and causal queue/fillability adapters separately; no rollover or new price collection authorized by this closure.',
        'old_usable_books_are_current': False,
        'comparison_collecting_label_is_not_active_source_collection': True,
        'reused_outcomes_are_new_evidence': False,
        'execution_enabled': False, 'promotion_ready': False, 'profitability_proven': False,
    }


if __name__ == '__main__':
    import argparse
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument('proof'); parser.add_argument('prior_audit'); parser.add_argument('audit_sha256')
    args = parser.parse_args()
    print(json.dumps(close(json.loads(Path(args.proof).read_text()), Path(args.prior_audit).read_bytes(), args.audit_sha256), indent=2))
