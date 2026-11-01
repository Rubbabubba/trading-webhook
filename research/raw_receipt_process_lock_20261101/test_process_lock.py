import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

from process_lock import ProcessLease, ExclusiveArchive
from receipt import BASE, build


def receipt():
    return build(b'{"market":{"ticker":"TEST-X","status":"active"}}',
        ticker='TEST-X',url=BASE+'TEST-X',requested_at=100,received_at=101,
        http_status=200,http_age_seconds=0)


class ProcessLocks(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'new.db'
        self.opened=[]

    def tearDown(self):
        for archive in self.opened:
            archive.close()
        self.tmp.cleanup()

    def archive(self, **kwargs):
        result=ExclusiveArchive(self.path,started_at=90,**kwargs)
        self.opened.append(result)
        return result

    def child(self, code):
        return subprocess.run([sys.executable,'-c',code,str(self.path)],
            env=os.environ.copy(),capture_output=True,text=True,timeout=10)

    def test_live_owner_blocks_another_process_without_database_change(self):
        a=self.archive();self.assertTrue(a.observe(receipt(),at=102)['accepted'])
        before=self.path.read_bytes()
        result=self.child('from process_lock import ExclusiveArchive;import sys;ExclusiveArchive(sys.argv[1],started_at=90)')
        self.assertNotEqual(result.returncode,0)
        self.assertIn('archive_writer_already_owned',result.stderr)
        self.assertEqual(self.path.read_bytes(),before)
        self.assertEqual(a.status()['receipts'],1)

    def test_same_process_distinct_open_cannot_reenter(self):
        self.archive()
        with self.assertRaisesRegex(RuntimeError,'archive_writer_already_owned'):
            self.archive()

    def test_kernel_releases_lock_after_process_death(self):
        code='from process_lock import ProcessLease;import os,sys;p=ProcessLease(sys.argv[1]+".writer-lock");os._exit(0)'
        self.assertEqual(self.child(code).returncode,0)
        self.assertTrue(self.archive().observe(receipt(),at=102)['accepted'])

    def test_lock_acquired_before_any_registration_writes(self):
        lock=ProcessLease(str(self.path)+'.writer-lock')
        try:
            with self.assertRaises(RuntimeError):self.archive()
            self.assertFalse(self.path.exists())
            self.assertFalse(Path(str(self.path)+'.resources.json').exists())
        finally:lock.close()

    def test_constructor_error_releases_lock_preserving_old_bytes(self):
        self.path.write_bytes(b'old evidence')
        with self.assertRaisesRegex(ValueError,'no_legacy_adoption'):self.archive()
        lock=ProcessLease(str(self.path)+'.writer-lock');lock.close()
        self.assertEqual(self.path.read_bytes(),b'old evidence')

    def test_durable_stop_is_not_reset_after_process_reopen(self):
        a=self.archive(disk_usage=lambda _:SimpleNamespace(free=1))
        self.assertEqual(a.status()['reason'],'physical_free_space_floor')
        a.close();a=self.archive()
        self.assertEqual(a.status()['reason'],'physical_free_space_floor')
        self.assertFalse(self.path.exists())

    def test_close_is_idempotent_and_never_unlinks_lock_inode(self):
        a=self.archive();lock=Path(str(self.path)+'.writer-lock')
        before=lock.stat().st_ino;a.close();a.close()
        self.assertEqual(lock.stat().st_ino,before)
        self.assertEqual(lock.stat().st_size,1)
        with self.assertRaisesRegex(RuntimeError,'archive_writer_closed'):a.observe(receipt(),at=102)
        self.assertEqual(self.archive().status()['receipts'],0)

    def test_unexpected_lock_contents_fail_closed(self):
        lock=Path(str(self.path)+'.writer-lock');lock.write_bytes(b'bad-large-lock')
        with self.assertRaisesRegex(ValueError,'unexpected_lock_file_size'):self.archive()
        self.assertFalse(self.path.exists())


if __name__=='__main__':unittest.main()
