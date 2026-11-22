"""Record fresh, cap-blocked wallet observations without any order interface."""

from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import shutil
import sqlite3
import time

from .kalshi_process_lock import acquire
from .kalshi_wallet_copy_worker import SIGNAL_SECONDS, error_code, executable
from .kalshi_wallet_matched_demo import FREE_FLOOR, read_only
from .kalshi_wallet_monitor import DEMO, public_get, read_fee, read_quote
from .kalshi_wallet_sports import discover


DIRECTORY = 'wallet-occupancy-shadow-v1'
MAX_BYTES = 16 * 1024 * 1024
CADENCE = 30
MAX_ROWS = 20_000


def read_status(path, *, now=None):
    try:
        with Path(path).open('rb') as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            return None
        report = json.loads(raw)
        age = (time.time() if now is None else now) - report['at']
        if (report['schema'] == 'wallet_occupancy_shadow_v1'
                and report['environment'] == 'demo' and report['actual_fills'] is False
                and 0 <= age <= 120):
            return report
    except (OSError, ValueError, TypeError, KeyError):
        return None
    return None


def source_candidates(root, *, since, now):
    signals = Path(root) / 'ranked-wallet-copy-v1' / 'signals.sqlite3'
    with closing(read_only(signals)) as broker_signals:
        return broker_signals.execute(
            """SELECT id,trade FROM signals
               WHERE side='BUY' AND state='demo_entry_limit' AND at BETWEEN ? AND ?
               ORDER BY at DESC, rowid DESC LIMIT 64""",
            (max(since, now - SIGNAL_SECONDS), now),
        ).fetchall()


def healthy(root, now):
    status = json.loads((Path(root) / 'ranked-wallet-copy-v1' / 'status.json').read_bytes())
    lifecycle = status.get('lifecycle') or {}
    at = datetime.fromisoformat(status['at']).timestamp()
    return (status.get('environment') == 'demo' and 0 <= now - at <= 120
            and lifecycle.get('state') == 'reconciled' and lifecycle.get('unresolved_orders') == 0
            and status.get('execution_error') is None and status.get('source_errors') == [])


def inspect(trade, *, read, clock):
    now = clock()
    if not 0 <= now - trade['timestamp'] <= SIGNAL_SECONDS:
        return {'state': 'expired_before_quote'}
    mapping = discover(trade, DEMO, read, research_nhl=True)
    frame = read_quote(mapping, DEMO, read)
    state = 'executable_quote'
    try:
        price = executable(trade, mapping, frame)
        fee_basis = read_fee(frame, DEMO, read)
    except (ValueError, OSError, KeyError) as error:
        price, fee_basis = None, None
        state = error_code(error)
    ready = clock()
    if not (trade['timestamp'] <= trade['observed_at'] <= frame['observed_at'] <= ready
            and ready - frame['observed_at'] <= 5
            and ready - trade['timestamp'] <= SIGNAL_SECONDS):
        state = 'expired_or_noncausal_quote'
    return dict(state=state, mapping=mapping, frame=frame, fee_basis=fee_basis,
                price_cents=price, completed_at=ready)


def open_archive(path, *, now):
    db = sqlite3.connect(path, isolation_level=None, timeout=3)
    db.execute('PRAGMA max_page_count=4096')
    db.executescript("""
        CREATE TABLE IF NOT EXISTS start(id INTEGER PRIMARY KEY CHECK(id=1), at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS captures(id TEXT PRIMARY KEY, at REAL NOT NULL, detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS stops(id INTEGER PRIMARY KEY CHECK(id=1), reason TEXT NOT NULL);
    """)
    db.execute('INSERT OR IGNORE INTO start VALUES(1,?)', (now,))
    return db


def tick(root, db, *, archive, read=public_get, clock=time.time):
    now = clock()
    existing_stop = db.execute('SELECT reason FROM stops WHERE id=1').fetchone()
    count = db.execute('SELECT count(*) FROM captures').fetchone()[0]
    capacity = (count >= MAX_ROWS or shutil.disk_usage(archive).free < FREE_FLOOR
                or (archive / 'research.sqlite3').stat().st_size >= MAX_BYTES - 3 * 1024 * 1024)
    if existing_stop or capacity:
        db.execute("INSERT OR IGNORE INTO stops VALUES(1,'capacity_stop')")
        state = 'capacity_stop'
    elif not healthy(root, now):
        state = 'reconciliation_hold'
    else:
        state = 'no_fresh_candidate'
        started, = db.execute('SELECT at FROM start WHERE id=1').fetchone()
        for ident, raw in source_candidates(root, since=started, now=now):
            if db.execute('SELECT 1 FROM captures WHERE id=?', (ident,)).fetchone():
                continue
            trade = json.loads(raw)
            try:
                evidence = inspect(trade, read=read, clock=clock)
            except Exception as error:
                evidence = {'state': error_code(error)}
            db.execute('INSERT INTO captures VALUES(?,?,?)', (ident, now, json.dumps(
                dict(evidence, trade=trade, shadow=True, actual_fill=False))))
            state = 'observed'
            break
    report = dict(schema='wallet_occupancy_shadow_v1', environment='demo', actual_fills=False,
                  at=clock(), state=state, counts=dict(db.execute(
                      "SELECT json_extract(detail,'$.state'),count(*) FROM captures GROUP BY 1")))
    temp = archive / 'status.tmp'
    temp.write_text(json.dumps(report, sort_keys=True), encoding='utf-8')
    temp.replace(archive / 'status.json')
    return report


def run(root):
    archive = Path(root) / DIRECTORY
    archive.mkdir(parents=True, exist_ok=True)
    with (acquire(archive / 'worker.lock'),
          closing(open_archive(archive / 'research.sqlite3', now=time.time())) as db):
        while True:
            start = time.monotonic()
            tick(root, db, archive=archive)
            time.sleep(max(5, CADENCE - (time.monotonic() - start)))
