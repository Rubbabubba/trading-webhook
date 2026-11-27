"""Prospective same-series comparator collection. Only the Demo research thread reads books."""
from contextlib import closing
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from queue import Empty, Full, Queue
import shutil
import time

from . import kalshi_wallet_matched_demo as trial
from .kalshi_wallet_monitor import DEMO, public_get, read_fee, read_quote
from .kalshi_process_lock import acquire

DIRECTORY = 'wallet-control-capture-v2-20261127'
REGISTER = 'wallet_control_capture_v2_20261127'
HASH = '83808bab8f0d0d99cf703b4982749b28a19c2a51451c63d7b1aaae8f9584f81e'
pending = Queue(maxsize=1)


def offer(client, frame):
    # No network, disk, or waiting on the copier's execution path.
    try:
        pending.put_nowait((client, frame))
    except Full:
        pass


def shortlist(wallet, used, read, clock):
    series, _, bucket, horizon = trial.stratum(wallet)
    cursor = None
    ranked = []
    for _ in range(3):
        if clock() - wallet['received_at'] > trial.MAX_WINDOW:
            break
        params = dict(series_ticker=series, status='open', limit=200)
        if cursor:
            params['cursor'] = cursor
        page = read(DEMO + '/markets', params)[0]
        for m in page['markets']:
            event = m.get('event_ticker', '')
            if m.get('market_type') != 'binary' or not event.startswith(series + '-'):
                continue
            parent = trial.normalized_parent(event, series)
            if parent == wallet['parent'] or parent in used:
                continue
            try:
                expiry = datetime.fromisoformat(m['expected_expiration_time'])
            except (KeyError, TypeError, ValueError):
                continue
            if expiry.utcoffset() is None or int((expiry.timestamp()-wallet['received_at']) // 3600) != horizon:
                continue
            for side in ('yes','no'):
                value = m.get(side + '_ask_dollars')
                if value is None:
                    continue
                try:
                    cents = Decimal(str(value))*100
                except InvalidOperation:
                    continue
                if cents.is_finite() and 0 < cents < 100 and cents // 5 == bucket:
                    ranked.append((abs(cents-Decimal(wallet['ask_cents'])),m['ticker'],side,m))
        cursor = page.get('cursor')
        if not cursor:
            break
    return sorted(ranked,key=lambda row:row[:3])


def sample(frame, used, read, clock):
    market = frame['market']
    series = market['event_ticker'].split('-',1)[0]
    wallet = dict(series=series, received_at=frame['observed_at'],
                  ask_cents=str(100*Decimal(frame['ask'])),
                  parent=trial.normalized_parent(market['event_ticker'],series),
                  expected_settlement=datetime.fromisoformat(market['expected_expiration_time']).timestamp())
    candidates = shortlist(wallet, used, read, clock)
    for _, ticker, side, m in candidates[:2]:
        if clock()-wallet['received_at'] > trial.MAX_WINDOW:
            return None, 'stale_opportunity'
        mapping = dict(ticker=ticker,side=side,event_ticker=m['event_ticker'],
                       exchange_index=m.get('exchange_index',0),
                       kalshi_rules={k:m[k] for k in ('rules_primary','rules_secondary')})
        try:
            book = read_quote(mapping,DEMO,read)
            fees = read_fee(book,DEMO,read)
            quote = trial.checked_quote(book,fees,ready=clock())
        except (KeyError, ValueError, ArithmeticError):
            continue
        if trial.stratum(quote)==trial.stratum(wallet):
            return dict(quote,frame=book,fee_basis=fees), 'book_verified'
    return None, 'no_verified_book' if candidates else 'no_reference_in_roster'


def available(path):
    return (shutil.disk_usage(path.parent).free >= trial.FREE_FLOOR and
            path.stat().st_size < trial.MAX_BYTES - 1024*1024)


def run(root):
    registration = Path(__file__).resolve().parents[1]/'configs'/REGISTER/'registration.json'
    if hashlib.sha256(registration.read_bytes().replace(b'\r\n',b'\n')).hexdigest()!=HASH:
        raise ValueError('registration_changed')
    root = Path(root)
    archive = root/DIRECTORY
    path = archive/'quotes.sqlite3'
    if not path.exists() and shutil.disk_usage(root).free < trial.FREE_FLOOR + trial.MAX_BYTES:
        return
    archive.mkdir(exist_ok=True)
    with acquire(archive/'worker.lock'), closing(trial.open_archive(archive)) as db:
        db.execute('CREATE TABLE IF NOT EXISTS attempts(client TEXT PRIMARY KEY, reason TEXT, detail TEXT)')
        if not db.execute("SELECT 1 FROM cursors WHERE name='submit_quotes'").fetchone():
            with closing(trial.read_only(root/'ranked-wallet-copy-v1'/'journal.sqlite3')) as journal:
                high = journal.execute('SELECT coalesce(max(rowid),0) FROM submission_quotes').fetchone()[0]
            db.execute("INSERT INTO cursors VALUES('submit_quotes',?)",(high,))
        if db.execute("SELECT 1 FROM cursors WHERE name='capacity_stop'").fetchone():
            return
        next_tick = 0
        while True:
            if time.monotonic() >= next_tick or not available(path):
                status = trial.run_tick(root,db,targeted=True)
                if status['state']=='capacity_stop':
                    return
                next_tick = time.monotonic()+30
            try:
                client, frame = pending.get(timeout=max(0,next_tick-time.monotonic()))
            except Empty:
                continue
            if not available(path) or db.execute('SELECT 1 FROM attempts WHERE client=?',(client,)).fetchone():
                continue
            used = {p for p, in db.execute("SELECT json_extract(detail,'$.control.parent') FROM pairs")}
            try:
                quote, reason = sample(frame,used,public_get,time.time)
            except Exception as error:
                quote, reason = None, type(error).__name__
            db.execute('INSERT OR IGNORE INTO attempts VALUES(?,?,?)',
                       (client,reason,json.dumps(quote,sort_keys=True) if quote else None))


def supervise(root):
    while True:
        try:
            run(root)
        except Exception as error:
            print(json.dumps(dict(event='targeted_control_error',error=type(error).__name__)),flush=True)
        time.sleep(60)
