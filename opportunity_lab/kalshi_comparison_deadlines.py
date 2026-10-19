"""Bounded prospective quote outcomes; no broker, order or model interface."""
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import sqlite3
import time
from urllib.parse import quote
from urllib.request import Request, build_opener
import zlib

from .kalshi_account_monitor import NoRedirect
from .kalshi_maker_v10 import stressed_markout
from .kalshi_shadow import price_book

HORIZONS=(5,30,300)
MAX_REQUESTS=900


def init(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS outcome_attempts(id INTEGER PRIMARY KEY,signal_id INTEGER,horizon INTEGER,started_at REAL);
    CREATE TABLE IF NOT EXISTS outcome_receipts(attempt_id INTEGER PRIMARY KEY,received_at REAL,raw_sha TEXT,raw_z BLOB,result TEXT);
    CREATE TABLE IF NOT EXISTS outcome_terminal(signal_id INTEGER,horizon INTEGER,state TEXT,at REAL,PRIMARY KEY(signal_id,horizon));
    CREATE TRIGGER IF NOT EXISTS outcome_receipt_update BEFORE UPDATE ON outcome_receipts BEGIN SELECT RAISE(ABORT,'immutable_outcome_receipt'); END;
    CREATE TRIGGER IF NOT EXISTS outcome_receipt_delete BEFORE DELETE ON outcome_receipts BEGIN SELECT RAISE(ABORT,'immutable_outcome_receipt'); END;
    ''')


def outcome_status(db,now):
    # Derive missing outcomes even if the process was down across a deadline.
    try:
        attempts=db.execute('SELECT count(*) FROM outcome_attempts').fetchone()[0]
    except sqlite3.OperationalError:
        return {'state':'initializing','complete':0,'pending':0,'missed':0,'requests':0}
    counts={'complete':0,'pending':0,'missed':0}
    for sid,at in db.execute('SELECT id,at FROM high_edge_v15_signals'):
        marked={r[0] for r in db.execute('SELECT horizon FROM high_edge_v15_marks WHERE signal_id=?',(sid,))}
        for h in HORIZONS:
            counts['complete' if h in marked else 'missed' if now>at+h+5 else 'pending']+=1
    complete_signals=db.execute('SELECT count(*) FROM (SELECT signal_id FROM high_edge_v15_marks GROUP BY signal_id HAVING count(*)=3)').fetchone()[0]
    return dict(counts,complete_signals=complete_signals,requests=attempts,request_cap=MAX_REQUESTS,
                state='missing_outcomes' if counts['missed'] else 'awaiting_outcomes' if counts['pending'] else 'observed' if counts['complete'] else 'awaiting_signal',
                actual_fills=0,settlement_outcomes=False,quote_outcomes_only=True)


def comparison_status(db,now):
    # A quiet stream must not leave the last book callback's zero-signal status
    # on screen after this independent runner has collected real outcomes.
    from .kalshi_high_edge_shadow import status as native_status
    native=native_status(db)
    pages=db.execute('PRAGMA page_count').fetchone()[0]
    inputs=db.execute('SELECT count(*) FROM comparison_inputs').fetchone()[0]
    return dict(protocol='stream_native_formula_comparison_v2_20261019',state='capacity_stop' if pages>=4096 or inputs>=10000 else native['state'],
        accepted_inputs=inputs,signals=native['signals'],complete_signals=native['complete_signals'],complete_events=native['independent_events'],
        outcome_coverage=outcome_status(db,now),native_quote_evidence=native,sqlite_pages=pages,sqlite_page_cap=4096,
        stop_reason='sqlite_page_cap' if pages>=4096 else 'accepted_frame_cap' if inputs>=10000 else None,
        metadata_independently_current=False,original_rest_evidence_reused=False,
        execution_enabled=False,fill_assumed=False,profitability_proven=False,promotion_ready=False)


def fetch(ticker):
    if not re.fullmatch(r'[A-Z0-9_.-]{1,160}',ticker):raise ValueError('invalid_ticker')
    # No credentials, redirects, production host or inferred cached timestamps.
    url='https://demo-api.kalshi.co/trade-api/v2/markets/'+quote(ticker,safe='')+'/orderbook?depth=20'
    start=time.time()
    with build_opener(NoRedirect()).open(Request(url),timeout=2) as response:
        raw=response.read(262145)
        age=float(response.headers.get('Age','0'))
    end=time.time()
    if len(raw)>262144:raise ValueError('response_size')
    # Return original bytes even when the timestamp/shape cannot support a mark.
    return raw,start,end,age


class Deadlines:
    def __init__(self,path,*,clock=time.time,reader=fetch,free_bytes=None):
        self.path=Path(path);self.clock=clock;self.reader=reader
        self.free_bytes=free_bytes or (lambda:shutil.disk_usage(self.path.parent).free)
        self.db=sqlite3.connect(path,isolation_level=None,timeout=2)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA max_page_count=4096')
        init(self.db)
        prior=self.db.execute('SELECT max(started_at) FROM outcome_attempts').fetchone()[0]
        self.next_at=prior+2 if prior is not None else 0

    def tick(self):
        now=self.clock()
        prior=self.db.execute('SELECT max(started_at) FROM outcome_attempts').fetchone()[0]
        if prior is not None and now<prior:raise ValueError('outcome_clock_reversal')
        for sid,at in self.db.execute('SELECT id,at FROM high_edge_v15_signals').fetchall():
            for h in HORIZONS:
                marked=self.db.execute('SELECT 1 FROM high_edge_v15_marks WHERE signal_id=? AND horizon=?',(sid,h)).fetchone()
                if marked or now>at+h+5:
                    self.db.execute('INSERT OR IGNORE INTO outcome_terminal VALUES(?,?,?,?)',(sid,h,'complete' if marked else 'missed_window',now))
        if now<self.next_at or self.free_bytes()<128*1024*1024:return
        if self.db.execute('SELECT count(*) FROM outcome_attempts').fetchone()[0]>=MAX_REQUESTS:return
        rows=self.db.execute('''SELECT s.id,s.ticker,s.at,h.h FROM high_edge_v15_signals s
            CROSS JOIN (SELECT 5 h UNION ALL SELECT 30 UNION ALL SELECT 300) h
            LEFT JOIN high_edge_v15_marks m ON m.signal_id=s.id AND m.horizon=h.h
            LEFT JOIN outcome_terminal t ON t.signal_id=s.id AND t.horizon=h.h
            WHERE m.signal_id IS NULL AND t.signal_id IS NULL AND s.at+h.h<=? AND s.at+h.h+5>=?
            AND (SELECT count(*) FROM outcome_attempts a WHERE a.signal_id=s.id AND a.horizon=h.h)<3
            ORDER BY s.at+h.h LIMIT 1''',(now,now)).fetchall()
        if not rows:return
        sid,ticker,signal_at,h=rows[0]
        attempt=self.db.execute('INSERT INTO outcome_attempts(signal_id,horizon,started_at) VALUES(?,?,?)',(sid,h,now)).lastrowid
        self.next_at=now+2
        raw=b'';received=now;start=None;age=None;result='unverified';marks=None
        try:
            raw,start,received,age=self.reader(ticker)
            if not all(math.isfinite(v) for v in (start,received,age)):raise ValueError('nonfinite_clock')
            if not now<=start<=received<=self.clock() or not 0<=received-start<=2 or not 0<=age<=2:raise ValueError('stale_response')
            if not signal_at+h<=received<=signal_at+h+5:raise ValueError('late_response')
            payload=json.loads(raw)
            # REST values carry no verified exchange timestamp. Record that limit;
            # never label HTTP request freshness as exchange-to-client latency.
            frame={'ticker':ticker,'received_at':received,'orderbook_fp':payload['orderbook_fp']}
            bid,ask,_,_=price_book(frame,'yes');mid=(bid+ask)/2
            detail=json.loads(self.db.execute('SELECT detail FROM high_edge_v15_signals WHERE id=?',(sid,)).fetchone()[0])
            marks={name:stressed_markout(signal,mid) if signal else {'gross_cents':'0','stressed_cents':'0','no_trade':True} for name,signal in detail.items()}
            result='valid_quote;exchange_timestamp_unverified'
        except Exception as exc:
            reason=str(exc) if isinstance(exc,ValueError) and re.fullmatch(r'[a-z_]{1,60}',str(exc)) else type(exc).__name__
            result='rejected_'+reason
        receipt=json.dumps({'result':result,'request_started_at':start,'http_age_seconds':age if age is not None and math.isfinite(age) else None,
                            'exchange_timestamp_verified':False},sort_keys=True,allow_nan=False)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            self.db.execute('INSERT INTO outcome_receipts VALUES(?,?,?,?,?)',(attempt,received,hashlib.sha256(raw).hexdigest(),sqlite3.Binary(zlib.compress(raw)),receipt))
            if marks is not None:
                self.db.execute('INSERT OR IGNORE INTO high_edge_v15_marks VALUES(?,?,?,?)',(sid,h,received,json.dumps(marks,sort_keys=True)))
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def close(self):self.db.close()


def run(path,stop,publish):
    runner=None
    try:
        runner=Deadlines(path)
        while not stop.is_set():
            runner.tick()
            now=time.time()
            publish({'comparison_outcomes':outcome_status(runner.db,now),'formula_comparison':comparison_status(runner.db,now)})
            stop.wait(.25)
    except Exception as exc:
        publish({'comparison_outcomes':{'state':'storage_or_runner_fault','error_class':type(exc).__name__}})
    finally:
        if runner:runner.close()
