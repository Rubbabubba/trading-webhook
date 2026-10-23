"""Finite prospective successor and read-only retention of exhausted evidence."""
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time

from .kalshi_demo_stream_archive import Archive
from .kalshi_continuing_capture import Recorder, LIMITS, PROTOCOL, encoded


def lifetime_start(path, clock=time.time, *, protocol='demo_continuous_stream_availability_v4_20261023'):
    path = Path(path)
    try:
        with path.open('x') as out:
            json.dump({'protocol': protocol,
                       'started_at': clock()}, out)
    except FileExistsError:
        pass
    value = json.loads(path.read_text())
    at = value.get('started_at')
    if (value.get('protocol') != protocol
            or type(at) not in (int, float) or not math.isfinite(at) or not 0 <= at <= clock()):
        raise ValueError('capture_start_receipt_invalid')
    return at


class RecoveryArchive(Archive):
    def _capacity(self, extra):
        super()._capacity(extra)
        # Four MiB remain for sealing/SQLite bookkeeping inside the same48MiB cap.
        if hasattr(self, 'db') and self.db.execute('PRAGMA page_count').fetchone()[0] >= 11264:
            raise ValueError('archive_capacity')


def retained_evidence(root):
    """No old ledger is opened writable or imported as a new observation."""
    result = {}
    for name in ('stream-source-v3.sqlite3', 'stream-shadow-comparison-v2.sqlite3',
                 'continuing-capture-v1.sqlite3', 'stream-source-v4.sqlite3', 'stream-shadow-comparison-v3.sqlite3'):
        path = Path(root) / name
        if path.exists():
            result[name] = {'bytes': path.stat().st_size, 'state': 'retained_read_only',
                            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            if name == 'stream-shadow-comparison-v2.sqlite3':
                try:
                    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
                        from .kalshi_comparison_deadlines import outcome_status
                        result[name]['outcome_coverage'] = outcome_status(db,time.time())
                except sqlite3.Error:
                    result[name]['coverage_unavailable'] = True
    return result


class FrozenCapture:
    def __init__(self, value):
        self.value = dict(value, state='capacity_insufficient_coverage',
                          new_inputs_active=False, retained_evidence_only=True)
    def status(self, now=None):
        return dict(self.value)
    def quote(self, *args, **kwargs):
        return None
    def poll(self, *args, **kwargs):
        return False
    def fault(self, *args, **kwargs):
        return None
    def close(self):
        pass


def open_continuing_capture(path):
    path = Path(path)
    if path.exists():
        with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True) as db:
            row = db.execute('SELECT records,accounted,compressed,last_error FROM meta WHERE id=1').fetchone()
            if row and (row[0] >= LIMITS['maximum_records']
                        or row[1] >= LIMITS['maximum_accounted_bytes']
                        or row[2] >= LIMITS['maximum_compressed_bytes']
                        or row[3] in {'capture_record_capacity', 'capture_accounted_capacity',
                                      'capture_compressed_capacity'}):
                if db.execute('SELECT detail FROM protocol WHERE id=1').fetchone()[0] != encoded(PROTOCOL):
                    raise ValueError('continuing_capture_protocol_changed')
                view = object.__new__(Recorder)
                view.db = db
                return FrozenCapture(view.status())
    return Recorder(path)
