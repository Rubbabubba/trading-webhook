import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from resource_guard import GuardedArchive, LIMITS
from receipt import build, BASE


def fixture():
    return build(b'{"market":{"ticker":"TEST-X","status":"active"}}',
                 ticker='TEST-X', url=BASE+'TEST-X', requested_at=100,
                 received_at=101, http_status=200, http_age_seconds=0)


class Resources(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'new.db'
        self.free=1024**3
        self.a=None

    def tearDown(self):
        if self.a: self.a.close()
        self.tmp.cleanup()

    def open(self):
        self.a=GuardedArchive(self.path, started_at=90,
                             disk_usage=lambda _: SimpleNamespace(free=self.free))
        return self.a

    def test_exact_receipt_and_registration_survive_reopen(self):
        a=self.open();self.assertTrue(a.observe(fixture(),at=102)['accepted'])
        a.close();a=self.open()
        self.assertEqual(json.loads(a.inner.db.execute('SELECT raw FROM receipts').fetchone()[0]),fixture())
        self.assertEqual(a.status()['receipts'],1)

    def test_floor_stop_is_durable_after_disk_recovers(self):
        a=self.open();self.free=1
        self.assertEqual(a.observe(fixture(),at=102)['reason'],'physical_free_space_floor')
        a.close();self.free=1024**3;a=self.open()
        self.assertIsNone(a.inner)
        self.assertEqual(a.observe(fixture(),at=103)['reason'],'physical_free_space_floor')

    def test_reserves_sqlite_overhead_not_only_payload(self):
        self.free=LIMITS['free_floor_bytes']+LIMITS['database_bytes']
        a=self.open()
        self.assertIsNone(a.inner)
        self.assertFalse(self.path.exists())
        self.free=1024**3
        self.assertEqual(self.open().status()['reason'],'physical_free_space_floor')
        self.assertFalse(self.path.exists())

    def test_cannot_adopt_old_database(self):
        self.path.write_bytes(b'old evidence')
        with self.assertRaisesRegex(ValueError,'no_legacy_adoption'):self.open()
        self.assertEqual(self.path.read_bytes(),b'old evidence')

    def test_missing_database_is_not_recreated(self):
        a=self.open();a.close();self.path.unlink()
        with self.assertRaisesRegex(ValueError,'no_recreation'):self.open()
        self.assertFalse(self.path.exists())

    def test_registration_cannot_move_forward(self):
        self.open().close()
        with self.assertRaisesRegex(ValueError,'registration_changed'):
            GuardedArchive(self.path, started_at=91)

    def test_actual_sqlite_full_stops_externally_without_partial_row(self):
        a=self.open();db=a.inner.db
        used=db.execute('PRAGMA page_count').fetchone()[0]
        db.execute(f'PRAGMA max_page_count={used}')
        original=db.execute('SELECT count(*) FROM receipts').fetchone()[0]
        with self.assertRaises(ValueError):
            a.observe({**fixture(),'oversize': 'x'*88000},at=102)
        # Invalid envelopes are rejected before storage and do not latch disk stops.
        self.assertFalse(a.stopped.exists())
        body=json.dumps({'market':{'ticker':'TEST-X','status':'active','rules_primary':'x'*40000}}).encode()
        r=build(body,ticker='TEST-X',url=BASE+'TEST-X',requested_at=100,received_at=101,http_status=200,http_age_seconds=0)
        with self.assertRaises(sqlite3.DatabaseError):a.observe(r,at=102)
        self.assertEqual(a.status()['reason'],'sqlite_storage_fault')
        self.assertEqual(db.execute('SELECT count(*) FROM receipts').fetchone()[0],original)

    def test_unexpected_wal_is_not_ignored(self):
        a=self.open();Path(str(self.path)+'-wal').write_bytes(b'x')
        self.assertEqual(a.observe(fixture(),at=102)['reason'],'physical_file_cap')


if __name__=='__main__':unittest.main()
