import copy
import hashlib
import json
import unittest
from close import close


def fixture():
    outcomes = dict(pending=0, complete=3, missed=0, requests=3, actual_fills=0)
    comparison = dict(protocol='stream_native_formula_comparison_v4_20261023',
        execution_enabled=False, fill_assumed=False, accepted_inputs=10, signals=1,
        complete_signals=1, complete_events=1, outcome_coverage=outcomes,
        native_quote_evidence={'owned_fills': 0}, metadata_independently_current=False)
    proof = dict(source_page_count=11264, source_db_records=5, comparison_counts=dict(inputs=10, signals=1, marks=3),
        files={}, source=dict(protocol='demo_continuous_stream_availability_v5_20261023',
            state='capacity_stop', storage_stop_reason='archive_capacity', active=False,
            latest_received_at=1000, records=5, execution_enabled=False,
            fill_assumed=False, formula_comparison=comparison))
    audit = dict(schema='conditional_bid_exit_audit_v1_20261024', complete_signals=1,
        complete_events=1, marks_arithmetic_and_time_verified=3, receipts_hash_verified=3,
        rows=[dict(signal_id=1)], totals_complete_only={h:dict(challenger=dict(conditional_paths=1,
            bid_exit_stressed_cents='-4')) for h in ('5','30','300')})
    return proof, audit


def evaluate(proof, audit):
    raw=json.dumps(audit).encode()
    return close(proof, raw, hashlib.sha256(raw).hexdigest())


class ClosureTests(unittest.TestCase):
    def test_capped_negative_paths_are_insufficient_not_realized_losses(self):
        proof,audit=fixture(); before=copy.deepcopy(proof)
        result=evaluate(proof,audit)
        self.assertEqual(result['state'],'insufficient_evidence')
        self.assertIsNone(result['realized_trading_pnl'])
        self.assertFalse(result['promotion_ready'])
        self.assertFalse(result['old_usable_books_are_current'])
        self.assertEqual(proof,before)

    def test_empty_experiment_is_not_zero_return_trades(self):
        proof,audit=fixture(); c=proof['source']['formula_comparison']
        for k in ('signals','complete_signals','complete_events'):c[k]=0
        for k in ('complete','requests'):c['outcome_coverage'][k]=0
        proof['comparison_counts'].update(signals=0, marks=0)
        for k in ('complete_signals','complete_events','marks_arithmetic_and_time_verified','receipts_hash_verified'):audit[k]=0
        audit['rows']=[]
        for row in audit['totals_complete_only'].values():row['challenger'].update(conditional_paths=0,bid_exit_stressed_cents=None)
        result=evaluate(proof,audit)
        self.assertEqual(list(result['conditional_displayed_bid_exit_stressed_cents'].values()),[None]*3)

    def test_positive_quotes_and_large_sample_never_create_owned_execution_pass(self):
        proof,audit=fixture(); c=proof['source']['formula_comparison']
        c.update(signals=100,complete_signals=100,complete_events=40)
        c['outcome_coverage'].update(complete=300,requests=300)
        proof['comparison_counts'].update(signals=100,marks=300)
        audit.update(complete_signals=100,complete_events=40,marks_arithmetic_and_time_verified=300,receipts_hash_verified=300)
        audit['rows']=[dict(signal_id=i) for i in range(1,101)]
        for row in audit['totals_complete_only'].values():row['challenger'].update(conditional_paths=100,bid_exit_stressed_cents='100')
        result=evaluate(proof,audit)
        self.assertTrue(result['gates']['minimum_100_complete_signals'])
        self.assertFalse(result['promotion_ready'])
        self.assertFalse(result['profitability_proven'])

    def test_tampered_retained_audit_rejected(self):
        proof,audit=fixture()
        with self.assertRaisesRegex(ValueError,'hash_mismatch'):close(proof,json.dumps(audit).encode(),'0'*64)

    def test_pending_deadline_must_drain_before_closure(self):
        proof,audit=fixture();proof['source']['formula_comparison']['outcome_coverage']['pending']=1
        with self.assertRaisesRegex(ValueError,'drain_pending'):evaluate(proof,audit)

    def test_low_disk_is_not_terminal_lifetime_capacity(self):
        proof,audit=fixture();proof['source']['storage_stop_reason']='archive_low_disk'
        with self.assertRaisesRegex(ValueError,'terminal_archive'):evaluate(proof,audit)

    def test_changed_prefix_cannot_reuse_old_audit(self):
        proof,audit=fixture();proof['source']['formula_comparison']['complete_signals']=2
        with self.assertRaisesRegex(ValueError,'reaudit_required'):evaluate(proof,audit)

    def test_status_counts_must_match_independent_database_receipt(self):
        proof,audit=fixture();proof['comparison_counts']['inputs']=11
        with self.assertRaisesRegex(ValueError,'database_count'):evaluate(proof,audit)

    def test_unsupported_capacity_claim_rejected(self):
        proof,audit=fixture();proof['source_page_count']=11263
        with self.assertRaisesRegex(ValueError,'independently_supported'):evaluate(proof,audit)

    def test_owned_fill_needs_separate_execution_dossier(self):
        proof,audit=fixture();proof['source']['formula_comparison']['outcome_coverage']['actual_fills']=1
        with self.assertRaisesRegex(ValueError,'separate_dossier'):evaluate(proof,audit)

    def test_wrong_protocol_cannot_replace_frozen_study(self):
        proof,audit=fixture();proof['source']['protocol']='replacement'
        with self.assertRaisesRegex(ValueError,'wrong_source'):evaluate(proof,audit)

    def test_same_count_different_signal_id_is_rejected(self):
        proof,audit=fixture();audit['rows'][0]['signal_id']=2
        with self.assertRaisesRegex(ValueError,'signal_identity'):evaluate(proof,audit)


if __name__=='__main__':unittest.main()
