"""Offline physical-resource guard; never opens a network connection or an order.

Caller must use a new path. This does not reopen a stopped source experiment.
The prior component's receipt, time and payload limits remain unchanged.
"""
import json
import os
from pathlib import Path
import shutil
import sqlite3

from archive import Archive

LIMITS = dict(schema='raw_receipt_physical_guard_v1_20261031',
              database_bytes=8*1024*1024, journal_bytes=8*1024*1024,
              free_floor_bytes=128*1024*1024, sidecar_allowance_bytes=4096)


def exclusive_json(path, value):
    with path.open('x', encoding='utf-8') as handle:
        handle.write(json.dumps(value, sort_keys=True, allow_nan=False))
        handle.flush()
        os.fsync(handle.fileno())


class GuardedArchive:
    def __init__(self, path, *, started_at, disk_usage=shutil.disk_usage):
        self.path = Path(path)
        self.registration = self.path.with_suffix(self.path.suffix+'.resources.json')
        self.stopped = self.path.with_suffix(self.path.suffix+'.resource-stop.json')
        self.disk_usage = disk_usage
        self.inner = None
        spec = dict(limits=LIMITS, started_at=started_at)
        if self.registration.exists():
            if json.loads(self.registration.read_text()) != spec:
                raise ValueError('resource_registration_changed')
            if not self.path.exists() and not self.stopped.exists():
                raise ValueError('registered_database_missing_no_recreation')
        else:
            if self.path.exists() or self.stopped.exists():
                raise ValueError('fresh_path_required_no_legacy_adoption')
            exclusive_json(self.registration, spec)
        # Never open the database after a durable physical stop. In particular,
        # a disk-full restart must not execute schema or journal writes.
        if self.stopped.exists():
            return
        if not self._preflight():
            return
        self.inner = Archive(str(self.path), started_at=started_at)
        try:
            if self.inner.db.execute('PRAGMA journal_mode=DELETE').fetchone()[0] != 'delete':
                raise ValueError('bounded_delete_journal_required')
            page = self.inner.db.execute('PRAGMA page_size').fetchone()[0]
            maximum = LIMITS['database_bytes']//page
            if self.inner.db.execute('PRAGMA page_count').fetchone()[0] > maximum:
                raise ValueError('existing_database_exceeds_physical_cap')
            actual = self.inner.db.execute(f'PRAGMA max_page_count={maximum}').fetchone()[0]
            if actual != maximum:
                raise ValueError('physical_page_cap_not_applied')
        except Exception:
            self.close()
            raise

    def _stop(self, reason):
        if not self.stopped.exists():
            exclusive_json(self.stopped, {'reason': reason, 'limits': LIMITS})
        return {'accepted': False, 'reason': json.loads(self.stopped.read_text())['reason']}

    def _preflight(self):
        # Reserve a FULL database growth and rollback-journal budget, rather
        # than treating serialized receipt bytes as actual filesystem usage.
        reserve = sum(LIMITS[k] for k in
                      ('database_bytes', 'journal_bytes', 'sidecar_allowance_bytes'))
        if self.disk_usage(self.path.parent).free < LIMITS['free_floor_bytes']+reserve:
            self._stop('physical_free_space_floor')
            return False
        for suffix, cap in [('', LIMITS['database_bytes']),
                            ('-journal', LIMITS['journal_bytes']), ('-wal', 0)]:
            file = Path(str(self.path)+suffix)
            if file.exists() and file.stat().st_size > cap:
                self._stop('physical_file_cap')
                return False
        return True

    def observe(self, receipt, *, at):
        if self.stopped.exists():
            return self._stop('already_stopped')
        if not self._preflight():
            return self._stop('physical_preflight_failed')
        if self.inner is None:
            raise RuntimeError('archive_unavailable')
        try:
            result = self.inner.observe(receipt, at=at)
        except sqlite3.DatabaseError:
            # SQLite FULL may roll back automatically. Do not issue a second
            # rollback or attempt to store a stop in the already full database.
            self._stop('sqlite_storage_fault')
            raise
        if not self._preflight():
            return {**self._stop('physical_postflight_failed'),
                    'receipt_may_be_retained': result.get('accepted', False)}
        return result

    def close(self):
        if self.inner is not None:
            self.inner.close()
            self.inner = None

    def status(self):
        if self.stopped.exists():
            return dict(state='stopped', **json.loads(self.stopped.read_text()),
                        execution_enabled=False, profitability_proven=False)
        return dict(self.inner.status(), physical_limits=LIMITS)
