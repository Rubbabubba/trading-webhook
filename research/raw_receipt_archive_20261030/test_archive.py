import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from archive import Archive, PROTOCOL
from receipt import build, BASE


def fixture(ticker='TEST-X', t=100):
    return build(json.dumps({'market': {'ticker': ticker, 'status': 'active'}}).encode(),
                 ticker=ticker, url=BASE+ticker, requested_at=t, received_at=t+1,
                 http_status=200, http_age_seconds=0)


class DurableArchive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name)/'raw.db')
        self.a = Archive(self.path, started_at=90)

    def tearDown(self):
        self.a.close()
        self.tmp.cleanup()

    def reopen(self, **kw):
        self.a.close()
        self.a = Archive(self.path, started_at=90, **kw)

    def test_original_envelope_survives_reopen(self):
        r = fixture(); self.a.observe(r, at=102); self.reopen()
        self.assertEqual(json.loads(self.a.db.execute('SELECT raw FROM receipts').fetchone()[0]), r)

    def test_duplicates_do_not_consume_capacity(self):
        r=fixture();self.a.observe(r, at=102)
        self.assertEqual(self.a.observe(r, at=103)['reason'], 'duplicate')
        self.assertEqual(self.a.status()['receipts'], 1)

    def test_registration_cannot_roll_forward(self):
        with self.assertRaisesRegex(ValueError, 'registration_changed'):
            Archive(self.path, started_at=91)

    def test_protocol_change_rejected(self):
        with self.assertRaisesRegex(ValueError, 'registration_changed'):
            Archive(self.path, started_at=90, protocol={**PROTOCOL, 'maximum_receipts': 999})

    def test_no_cap_increase(self):
        with self.assertRaises(ValueError):
            Archive(self.path, started_at=90, protocol={**PROTOCOL, 'maximum_receipts': 1001})

    def test_exact_duration_stops_durably(self):
        self.assertEqual(self.a.observe(fixture(), at=21690)['reason'], 'duration_cap')
        self.reopen()
        self.assertEqual(self.a.observe(fixture(), at=102)['reason'], 'duration_cap')

    def test_record_cap_and_no_resume(self):
        self.a.close();self.path=str(Path(self.tmp.name)/'small.db')
        p={**PROTOCOL,'maximum_receipts':1};self.a=Archive(self.path,started_at=90,protocol=p)
        self.a.observe(fixture(),at=102)
        self.assertEqual(self.a.observe(fixture('TEST-Y'),at=103)['reason'],'receipt_cap')
        self.reopen(protocol=p)
        self.assertEqual(self.a.status()['stop_reason'],'receipt_cap')

    def test_byte_cap_no_partial_receipt(self):
        self.a.close();self.path=str(Path(self.tmp.name)/'bytes.db')
        self.a=Archive(self.path,started_at=90,protocol={**PROTOCOL,'maximum_payload_bytes':1})
        self.assertEqual(self.a.observe(fixture(),at=102)['reason'],'payload_cap')
        self.assertEqual(self.a.status()['receipts'],0)

    def test_future_and_pre_registration_rejected(self):
        for r,at in ((fixture(t=89),102),(fixture(),100),(fixture(),89)):
            with self.assertRaises(ValueError):self.a.observe(r,at=at)

    def test_clock_reversal_preserves_prefix(self):
        self.a.observe(fixture(),at=105)
        with self.assertRaises(ValueError):self.a.observe(fixture('TEST-Y'),at=104)
        self.assertEqual(self.a.status()['receipts'],1)

    def test_tampered_bytes_or_envelope_rejected(self):
        for field in ('raw_body_base64','raw_sha256','receipt_sha256'):
            r=fixture();r[field]='e30=' if field=='raw_body_base64' else '0'*64
            with self.assertRaises(ValueError):self.a.observe(r,at=102)

    def test_immutable_data_and_registration(self):
        self.a.observe(fixture(),at=102)
        for sql in ('DELETE FROM receipts','UPDATE receipts SET raw=\'{}\'','DELETE FROM registration'):
            with self.assertRaises(sqlite3.IntegrityError):self.a.db.execute(sql)

    def test_integrity_is_not_source_fee_queue_or_profit_proof(self):
        self.a.observe(fixture(),at=102)
        s=self.a.status()
        for k in ('execution_enabled','source_authenticity_verified','fees_verified','queue_verified','profitability_proven'):
            self.assertIs(s[k],False)


if __name__ == '__main__': unittest.main()
