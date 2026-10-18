"""Durable bounded Demo source archive. No network, broker or order interface."""
import gzip
import hashlib
import json
import math
from pathlib import Path
import shutil
import sqlite3
import uuid
from .kalshi_demo_stream_source import Books, SourceFault

PROTOCOL = 'demo_stream_archive_v1_20261016'
MAX_RECORDS = 100000
MAX_COMPRESSED = 32 * 1024 * 1024
MAX_MESSAGE = 262144
MIN_FREE = 128 * 1024 * 1024
ALLOWED = {
    'subscribed': {'channel', 'sid'},
    'orderbook_snapshot': {'market_id', 'market_ticker', 'yes_dollars_fp', 'no_dollars_fp'},
    'orderbook_delta': {'market_id', 'market_ticker', 'side', 'price_dollars', 'delta_fp', 'ts', 'ts_ms'},
    'trade': {'market_id', 'market_ticker', 'trade_id', 'yes_price_dollars', 'no_price_dollars', 'count_fp', 'taker_side', 'taker_outcome_side', 'ts', 'ts_ms'},
}

def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

def digest(value):
    return hashlib.sha256(value).hexdigest()

def clock(at):
    if type(at) not in (int, float) or not math.isfinite(at) or at < 0:
        raise ValueError('archive_invalid_clock')

class Archive:
    def __init__(self, path, *, free_bytes=None, protocol=PROTOCOL, decoder_factory=Books):
        self.path = Path(path)
        self.free_bytes = free_bytes or (lambda: shutil.disk_usage(self.path.parent).free)
        self.connection = None
        self.decoder = None
        self.protocol, self.decoder_factory = protocol, decoder_factory
        self._capacity(0)
        self.db = sqlite3.connect(str(self.path))
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('PRAGMA max_page_count=12288')  # 48MiB at required4096 page size
        if self.db.execute('PRAGMA page_size').fetchone()[0] != 4096:
            self.db.close()
            raise ValueError('archive_page_size')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS protocol(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS connections(id TEXT PRIMARY KEY, targets TEXT NOT NULL, started REAL NOT NULL, ended REAL, reason TEXT);
        CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, connection TEXT NOT NULL REFERENCES connections(id), received REAL NOT NULL, raw_sha TEXT NOT NULL, compressed_sha TEXT NOT NULL, previous_sha TEXT NOT NULL, chain_sha TEXT NOT NULL, data BLOB NOT NULL);
        CREATE TRIGGER IF NOT EXISTS messages_immutable_update BEFORE UPDATE ON messages BEGIN SELECT RAISE(ABORT,'archive_immutable'); END;
        CREATE TRIGGER IF NOT EXISTS messages_immutable_delete BEFORE DELETE ON messages BEGIN SELECT RAISE(ABORT,'archive_immutable'); END;
        ''')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO protocol VALUES(1,?)', (self.protocol,))
            if self.db.execute('SELECT name FROM protocol WHERE id=1').fetchone()[0] != self.protocol:
                raise ValueError('archive_protocol_changed')
            # No new connection inherits a crashed connection's book or sequence.
            self.db.execute("UPDATE connections SET reason='process_interrupted' WHERE ended IS NULL AND reason IS NULL")
        self.verify()
        self._records, self._compressed, self._latest = self.db.execute(
            'SELECT count(*),coalesce(sum(length(data)),0),max(received) FROM messages').fetchone()

    def _capacity(self, extra):
        if self.free_bytes() < MIN_FREE:
            raise ValueError('archive_low_disk')
        if hasattr(self, 'db'):
            if hasattr(self, '_records'):
                count, total = self._records, self._compressed
            else:
                count, total = self.db.execute('SELECT count(*),coalesce(sum(length(data)),0) FROM messages').fetchone()
            if count >= MAX_RECORDS or total + extra > MAX_COMPRESSED:
                raise ValueError('archive_capacity')

    def begin(self, targets, at):
        clock(at)
        if self.connection is not None:
            raise ValueError('archive_connection_active')
        decoder = self.decoder_factory(targets)
        self._capacity(0)
        # Bound empty or repeatedly failed sessions too.
        if self.db.execute('SELECT count(*) FROM connections').fetchone()[0] >= 1024:
            raise ValueError('archive_connection_capacity')
        identity = str(uuid.uuid4())
        with self.db:
            self.db.execute('INSERT INTO connections VALUES(?,?,?,NULL,NULL)', (identity, encode(list(targets)).decode(), at))
        self.connection, self.decoder = identity, decoder
        return identity

    def append(self, payload, at):
        if self.connection is None:
            raise ValueError('archive_no_active_connection')
        try:
            clock(at)
            at = float(at)  # SQLite REAL roundtrips must retain identical chain bytes.
            started = self.db.execute('SELECT started FROM connections WHERE id=?', (self.connection,)).fetchone()[0]
            if at < started:
                raise ValueError('archive_receipt_before_connection')
            if not isinstance(payload, dict) or set(payload) - {'id','type','msg','sid','seq','sending_ts_ms'}:
                raise ValueError('archive_unexpected_envelope_fields')
            kind = payload.get('type')
            if kind not in ALLOWED or not isinstance(payload.get('msg'), dict) or set(payload['msg']) - ALLOWED[kind]:
                raise ValueError('archive_unexpected_public_fields')
            raw = encode(payload)
            if len(raw) > MAX_MESSAGE:
                raise ValueError('archive_message_capacity')
            packed = gzip.compress(raw, mtime=0)
            self._capacity(len(packed))
            outcome = self.decoder.apply(payload, at)
            previous = self.db.execute('SELECT chain_sha FROM messages ORDER BY id DESC LIMIT 1').fetchone()
            previous = previous[0] if previous else '0' * 64
            sha = digest(raw)
            chain = digest(encode([previous,self.connection,at,sha]))
            with self.db:
                self.db.execute('INSERT INTO messages(connection,received,raw_sha,compressed_sha,previous_sha,chain_sha,data) VALUES(?,?,?,?,?,?,?)', (self.connection,at,sha,digest(packed),previous,chain,packed))
            self._records += 1
            self._compressed += len(packed)
            self._latest = at
            return outcome
        except Exception:
            # Never persist arbitrary exception text, credentials or invalid messages.
            self.end(at, 'source_or_storage_fault')
            raise

    def end(self, at, reason='normal_stop'):
        if reason not in {'normal_stop','source_or_storage_fault','capacity_stop','shutdown'}:
            raise ValueError('archive_invalid_end_reason')
        connection = self.connection
        self.connection = self.decoder = None
        if connection is not None:
            started = self.db.execute('SELECT started FROM connections WHERE id=?', (connection,)).fetchone()[0]
            valid = type(at) in (int,float) and math.isfinite(at) and at >= started
            with self.db:
                self.db.execute('UPDATE connections SET ended=?,reason=? WHERE id=?', (at if valid else None,reason,connection))

    def verify(self):
        previous = '0' * 64
        for connection, at, sha, compressed_sha, prev, chain, data in self.db.execute('SELECT connection,received,raw_sha,compressed_sha,previous_sha,chain_sha,data FROM messages ORDER BY id'):
            if digest(data) != compressed_sha:
                raise ValueError('archive_compressed_hash_mismatch')
            raw = gzip.decompress(data)
            if len(raw) > MAX_MESSAGE or digest(raw) != sha or prev != previous or digest(encode([prev,connection,at,sha])) != chain:
                raise ValueError('archive_chain_mismatch')
            previous = chain
        return previous

    def status(self):
        records, compressed, latest = self._records, self._compressed, self._latest
        interrupted = self.db.execute("SELECT count(*) FROM connections WHERE reason='process_interrupted'").fetchone()[0]
        return dict(protocol=self.protocol, records=records, compressed_bytes=compressed, latest_received_at=latest, interrupted_connections=interrupted, active=self.connection is not None, execution_enabled=False, fill_assumed=False, profitability_proven=False)

    def close(self):
        if self.connection is not None:
            self.end(None, 'shutdown')
        self.db.close()
