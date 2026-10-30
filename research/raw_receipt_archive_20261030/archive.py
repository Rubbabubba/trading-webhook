"""Offline bounded persistence for caller-provided receipts. No transport or orders.

This is a separate engineering component, not an extension of a stopped study.
An eventual existing-GET observer must authenticate transport independently.
"""
import hashlib
import json
import math
import sqlite3

PROTOCOL = dict(schema='kalshi_raw_metadata_archive_component_v1_20261030',
                maximum_receipts=1000, maximum_payload_bytes=4194304,
                duration_seconds=21600, maximum_receipt_bytes=90000)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


class Archive:
    def __init__(self, path, *, started_at, protocol=None):
        if not finite(started_at):
            raise ValueError('finite_registration_time_required')
        self.protocol = dict(PROTOCOL if protocol is None else protocol)
        if set(self.protocol) != set(PROTOCOL) or self.protocol['schema'] != PROTOCOL['schema']:
            raise ValueError('protocol_shape')
        for key in ('maximum_receipts', 'maximum_payload_bytes', 'duration_seconds', 'maximum_receipt_bytes'):
            value = self.protocol[key]
            if type(value) is not int or not 0 < value <= PROTOCOL[key]:
                raise ValueError('bounded_protocol_required')
        self.db = sqlite3.connect(path)
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS registration(id INTEGER PRIMARY KEY CHECK(id=1), raw TEXT NOT NULL, started REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS receipts(hash TEXT PRIMARY KEY, observed REAL NOT NULL, raw TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS stop(id INTEGER PRIMARY KEY CHECK(id=1), reason TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS immutable_receipt_update BEFORE UPDATE ON receipts BEGIN SELECT RAISE(ABORT,'immutable_receipt'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_receipt_delete BEFORE DELETE ON receipts BEGIN SELECT RAISE(ABORT,'immutable_receipt'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_registration_update BEFORE UPDATE ON registration BEGIN SELECT RAISE(ABORT,'immutable_registration'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_registration_delete BEFORE DELETE ON registration BEGIN SELECT RAISE(ABORT,'immutable_registration'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_stop_update BEFORE UPDATE ON stop BEGIN SELECT RAISE(ABORT,'immutable_stop'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_stop_delete BEFORE DELETE ON stop BEGIN SELECT RAISE(ABORT,'immutable_stop'); END;
        ''')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO registration VALUES(1,?,?)', (encode(self.protocol), started_at))
        registered = self.db.execute('SELECT raw,started FROM registration').fetchone()
        if registered != (encode(self.protocol), started_at):
            self.db.close()
            raise ValueError('registration_changed_no_rollover')
        self.started_at = registered[1]

    def close(self):
        self.db.close()

    def stop(self, reason):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO stop VALUES(1,?)', (reason,))
        return {'accepted': False, 'reason': reason}

    def observe(self, receipt, *, at):
        # Accept only an intact, original v2 envelope. No decoded projection may
        # be relabelled as an original HTTP body; source authenticity stays false.
        from receipt import build
        import base64
        if not finite(at) or at < self.started_at:
            raise ValueError('pre_registration_or_invalid_time')
        prior = self.db.execute('SELECT reason FROM stop').fetchone()
        if prior:
            return {'accepted': False, 'reason': prior[0]}
        if at >= self.started_at + self.protocol['duration_seconds']:
            return self.stop('duration_cap')
        raw = encode(receipt)
        if len(raw.encode()) > self.protocol['maximum_receipt_bytes']:
            raise ValueError('receipt_size')
        body = base64.b64decode(receipt['raw_body_base64'], validate=True)
        rebuilt = build(body, **{k: receipt[k] for k in
            ('ticker', 'url', 'requested_at', 'received_at', 'http_status', 'http_age_seconds')})
        if rebuilt != receipt:
            raise ValueError('receipt_integrity')
        if not self.started_at <= receipt['requested_at'] <= receipt['received_at'] <= at:
            raise ValueError('causal_registration_time')
        digest = hashlib.sha256(raw.encode()).hexdigest()
        existing = self.db.execute('SELECT raw FROM receipts WHERE hash=?', (digest,)).fetchone()
        if existing:
            if existing[0] != raw:
                raise ValueError('hash_conflict')
            return {'accepted': False, 'reason': 'duplicate', 'hash': digest}
        count, size, latest = self.db.execute('SELECT count(*),coalesce(sum(length(cast(raw AS BLOB))),0),max(observed) FROM receipts').fetchone()
        if latest is not None and at < latest:
            raise ValueError('observation_clock_reversal')
        if count >= self.protocol['maximum_receipts']:
            return self.stop('receipt_cap')
        if size + len(raw.encode()) > self.protocol['maximum_payload_bytes']:
            return self.stop('payload_cap')
        with self.db:
            self.db.execute('INSERT INTO receipts VALUES(?,?,?)', (digest, at, raw))
        return {'accepted': True, 'hash': digest}

    def status(self):
        count, size = self.db.execute('SELECT count(*),coalesce(sum(length(cast(raw AS BLOB))),0) FROM receipts').fetchone()
        stopped = self.db.execute('SELECT reason FROM stop').fetchone()
        return dict(state='stopped' if stopped else 'offline_component',
                    stop_reason=stopped[0] if stopped else None, receipts=count,
                    payload_bytes=size, execution_enabled=False,
                    source_authenticity_verified=False, fees_verified=False,
                    queue_verified=False, profitability_proven=False)
