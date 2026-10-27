import unittest
from audit import audit, canonical, digest


def fixture():
    fields = dict(ticker='X', status='active', rules_primary='Exact settlement source',
                  open_time='1970-01-01T00:00:00Z', close_time='1970-01-01T00:03:00Z')
    clocks = [90, 91, 92, 93]
    h = digest(fields)
    v = dict(ticker='X', version_hash=h, first_observed_at=91, fields_json=canonical(fields))
    r = dict(ticker='X', version_hash=h, receipt_hash=digest(dict(ticker='X', version=h, clocks=clocks)),
             metadata_started_at=90, metadata_observed_at=91, book_started_at=92, book_observed_at=93)
    return v, r


class Audit(unittest.TestCase):
    def test_valid_component_cannot_grant_execution_or_authentication(self):
        r = audit(*fixture(), ticker='X', decision_at=100)
        self.assertEqual(r['state'], 'receipt_integrity_and_causality_only')
        for k in ('execution_enabled', 'promotion_ready', 'fees_verified', 'queue_verified', 'source_authenticity_verified'):
            self.assertIs(r[k], False)
    def test_ticker_binding(self):
        self.assertEqual(audit(*fixture(), ticker='Y', decision_at=100)['reason'], 'identity_mismatch')
    def test_altered_rule_bytes(self):
        v, r = fixture(); v['fields_json'] = v['fields_json'].replace('Exact', 'Other')
        self.assertEqual(audit(v, r, ticker='X', decision_at=100)['reason'], 'version_hash_mismatch')
    def test_clock_hash_binding(self):
        v, r = fixture(); r['book_observed_at'] = 94
        self.assertEqual(audit(v, r, ticker='X', decision_at=100)['reason'], 'receipt_hash_mismatch')
    def test_future_receipt_not_backdated(self):
        self.assertEqual(audit(*fixture(), ticker='X', decision_at=92)['reason'], 'future_book_or_metadata_receipt')
    def test_stale_not_substituted(self):
        self.assertEqual(audit(*fixture(), ticker='X', decision_at=122)['reason'], 'stale_metadata_receipt')
    def test_nonfinite_clock(self):
        self.assertEqual(audit(*fixture(), ticker='X', decision_at=float('nan'))['reason'], 'invalid_clock')
    def test_bool_clock(self):
        self.assertEqual(audit(*fixture(), ticker='X', decision_at=True)['reason'], 'invalid_clock')
    def test_fixed_policy_not_widened(self):
        self.assertEqual(audit(*fixture(), ticker='X', decision_at=100, maximum_age_seconds=60)['reason'], 'fixed_freshness_policy_changed')
    def test_malformed_order(self):
        v, r = fixture(); r['metadata_observed_at'] = 95
        self.assertEqual(audit(v, r, ticker='X', decision_at=100)['reason'], 'receipt_clock_order')
    def test_first_seen_not_after_receipt(self):
        v, r = fixture(); v['first_observed_at'] = 92
        self.assertEqual(audit(v, r, ticker='X', decision_at=100)['reason'], 'receipt_clock_order')
    def test_hashed_invalid_lifecycle_still_blocks(self):
        v, r = fixture(); f=__import__('json').loads(v['fields_json']);f['open_time']='1970-01-01T00:00:00'
        v['fields_json']=canonical(f);v['version_hash']=r['version_hash']=digest(f)
        r['receipt_hash']=digest(dict(ticker='X',version=digest(f),clocks=[90,91,92,93]))
        self.assertEqual(audit(v,r,ticker='X',decision_at=100)['reason'],'lifecycle_missing_or_ambiguous')
    def test_v1_archive_cannot_certify_unretained_active_status(self):
        v,r=fixture();f=__import__('json').loads(v['fields_json']);del f['status']
        v['fields_json']=canonical(f);v['version_hash']=r['version_hash']=digest(f)
        r['receipt_hash']=digest(dict(ticker='X',version=digest(f),clocks=[90,91,92,93]))
        self.assertEqual(audit(v,r,ticker='X',decision_at=100)['reason'],'active_status_not_retained_in_v1_archive')


if __name__ == '__main__': unittest.main()
