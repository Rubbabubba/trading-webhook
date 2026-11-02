import base64
from pathlib import Path
from types import SimpleNamespace as NS
import tempfile
import unittest
from process_lock import ExclusiveArchive
from receipt import BASE
from response_observer import observe_existing


class ResponseAdmission(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.a = ExclusiveArchive(Path(self.tmp.name)/'fresh.db', started_at=90)
        self.r = NS(request=NS(method='GET', url=BASE+'TEST-X'),
                    url=BASE+'TEST-X', history=[], headers={'Age':'0'},
                    content=b'{ "market": {"ticker":"TEST-X","status":"active"} }',
                    status_code=200)

    def tearDown(self):
        self.a.close(); self.tmp.cleanup()

    def observe(self, **kw):
        return observe_existing(self.r, self.a, ticker='TEST-X',
                                requested_at=kw.get('requested_at',100),
                                received_at=kw.get('received_at',101))

    def test_exact_bytes_retained_without_authority(self):
        self.assertTrue(self.observe()['accepted'])
        import json
        raw = self.a.inner.inner.db.execute('SELECT raw FROM receipts').fetchone()[0]
        self.assertEqual(base64.b64decode(json.loads(raw)['raw_body_base64']),self.r.content)
        self.assertFalse(self.a.status()['source_authenticity_verified'])

    def test_post_never_reaches_archive(self):
        self.r.request.method='POST'
        with self.assertRaisesRegex(ValueError,'existing_get'):self.observe()
        self.assertEqual(self.a.status()['receipts'],0)

    def test_production_or_query_endpoints_rejected(self):
        for url in ('https://api.elections.kalshi.com/trade-api/v2/markets/TEST-X',BASE+'TEST-X?x=1'):
            self.r.url=url
            with self.assertRaisesRegex(ValueError,'exact_demo'):self.observe()

    def test_prepared_url_must_match_response(self):
        self.r.request.url=BASE+'OTHER'
        with self.assertRaisesRegex(ValueError,'exact_demo'):self.observe()

    def test_redirect_cannot_be_relabelled(self):
        self.r.history=[NS(status_code=302)]
        with self.assertRaisesRegex(ValueError,'redirected'):self.observe()

    def test_missing_and_nonfinite_age_stay_unknown_or_rejected(self):
        for age in (None,'NaN','Infinity','-1','3','bogus','9'*33):
            self.r.headers={'Age':age}
            with self.assertRaises(ValueError):self.observe()
        self.assertEqual(self.a.status()['receipts'],0)

    def test_identity_and_duplicate_json_cannot_pass(self):
        for body in (b'{"market":{"ticker":"OTHER"}}',b'{"market":{},"market":{}}'):
            self.r.content=body
            with self.assertRaises(ValueError):self.observe()

    def test_failed_status_and_transport_delay_rejected(self):
        self.r.status_code=500
        with self.assertRaises(ValueError):self.observe()
        self.r.status_code=200
        with self.assertRaises(ValueError):self.observe(received_at=103)

    def test_preregistration_and_clock_reversal_enforced(self):
        with self.assertRaisesRegex(ValueError,'causal_registration'):self.observe(requested_at=89,received_at=90)
        self.assertTrue(self.observe()['accepted'])
        self.r.headers={'Age':'1'}
        with self.assertRaisesRegex(ValueError,'clock_reversal'):self.observe(requested_at=99,received_at=100)

    def test_restart_deduplicates_and_duration_does_not_roll_over(self):
        self.assertTrue(self.observe()['accepted'])
        path=self.a.path; self.a.close()
        self.a=ExclusiveArchive(path,started_at=90)
        self.assertEqual(self.observe()['reason'],'duplicate')
        self.assertEqual(self.observe(requested_at=21690,received_at=21691)['reason'],'duration_cap')
        self.a.close(); self.a=ExclusiveArchive(path,started_at=90)
        self.assertEqual(self.a.status()['stop_reason'],'duration_cap')


if __name__=='__main__': unittest.main()
