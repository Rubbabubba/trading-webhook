"""Finite capture of entity bytes from existing Demo GETs. No transport/orders.

Runs its single SQLite writer off the trading thread. An unknown HTTP Age is
retained as unknown, never a freshness pass. This is transport observation,
not venue publication, fee, fillability or profitability certification.
"""
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import shutil
import sqlite3
import threading
import time

from .kalshi_metadata_process_lock import ProcessLease

SCHEMA = 'kalshi_existing_get_metadata_capture_v1_20261102'
BASE = 'https://demo-api.kalshi.co/trade-api/v2/markets/'
SPEC = dict(schema=SCHEMA, duration_seconds=21600, maximum_receipts=1000,
            maximum_payload_bytes=4194304, maximum_entity_bytes=65536,
            database_bytes=8388608, journal_bytes=8388608,
            free_floor_bytes=134217728, queue_slots=8,
            execution_enabled=False, fill_assumed=False, rollover=False)


def encode(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def finite(x):
    return type(x) in (int,float) and math.isfinite(x)


def exclusive_json(path,value):
    with path.open('x',encoding='utf-8') as f:
        f.write(encode(value));f.flush();os.fsync(f.fileno())


def envelope(raw,*,url,method,status,requested_at,received_at,age):
    if method!='GET' or not isinstance(url,str) or not url.startswith(BASE):
        raise ValueError('exact_demo_get_required')
    ticker=url[len(BASE):]
    if not 1<=len(ticker)<=160 or any(c not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-' for c in ticker):
        raise ValueError('exact_metadata_endpoint_required')
    if type(status) is not int or status!=200:
        raise ValueError('successful_metadata_response_required')
    if not isinstance(raw,bytes) or not 0<len(raw)<=SPEC['maximum_entity_bytes']:
        raise ValueError('entity_size')
    if not finite(requested_at) or not finite(received_at) or not 0<=received_at-requested_at<=8:
        raise ValueError('transport_clock')
    # Parse identity without ever projecting a new status or trading decision.
    def unique(pairs):
        d={}
        for k,v in pairs:
            if k in d:raise ValueError('duplicate_json_key')
            d[k]=v
        return d
    def bad_constant(x):raise ValueError('nonfinite_json')
    def parsed_float(x):
        value=float(x)
        if not math.isfinite(value):raise ValueError('nonfinite_json')
        return value
    value=json.loads(raw.decode('utf-8'),object_pairs_hook=unique,parse_constant=bad_constant,parse_float=parsed_float)
    if not isinstance(value,dict) or not isinstance(value.get('market'),dict) or value['market'].get('ticker')!=ticker:
        raise ValueError('metadata_identity')
    if age is None:
        age_value=None; freshness='http_age_unknown'
    else:
        if not isinstance(age,str) or len(age)>32:raise ValueError('invalid_http_age')
        age_value=float(age)
        if not finite(age_value) or age_value<0:raise ValueError('invalid_http_age')
        freshness='transport_freshness_only' if age_value<=2 and received_at-requested_at<=2 else 'transport_not_fresh'
    return dict(schema=SCHEMA,ticker=ticker,url=url,method=method,http_status=status,
                requested_at=requested_at,received_at=received_at,http_age_seconds=age_value,
                freshness=freshness,entity_sha256=sha(raw),entity_base64=base64.b64encode(raw).decode(),
                entity_scope='decoded_http_entity_not_original_wire_compression',
                source_authenticity_verified=False,fees_verified=False,fill_assumed=False)


class MetadataCapture:
    def __init__(self,root,*,clock=time.time,disk_usage=shutil.disk_usage):
        self.root=Path(root);self.path=self.root/'existing-get-metadata-v1.sqlite3'
        self.registration=Path(str(self.path)+'.registration.json')
        self.stop_path=Path(str(self.path)+'.stop.json')
        self.clock=clock;self.disk_usage=disk_usage
        self.q=queue.Queue(maxsize=SPEC['queue_slots']);self.closed=threading.Event()
        self.thread=None;self.lease=None;self.db=None
        self.view=dict(schema=SCHEMA,state='starting',receipts=0,payload_bytes=0,
                       rejected=0,queue_dropped=0,age_unknown=0,execution_enabled=False,
                       fill_assumed=False,profitability_proven=False,source_authenticity_verified=False,
                       full_reference=self.path.name,protocol_sha256=sha(encode(SPEC).encode()))

    def status(self):
        return dict(self.view,pending=self.q.qsize(),process_only_counters=True)

    def _stop(self,reason):
        if not self.stop_path.exists():exclusive_json(self.stop_path,dict(reason=reason))
        self.view.update(state='stopped',stop_reason=json.loads(self.stop_path.read_text())['reason'])

    def _physical(self):
        reserve=SPEC['database_bytes']+SPEC['journal_bytes']+4096
        if self.disk_usage(self.root).free<SPEC['free_floor_bytes']+reserve:
            self._stop('physical_free_space_floor');return False
        for suffix,cap in (('',SPEC['database_bytes']),('-journal',SPEC['journal_bytes']),('-wal',0)):
            p=Path(str(self.path)+suffix)
            if p.exists() and p.stat().st_size>cap:
                self._stop('physical_file_cap');return False
        return True

    def _open(self):
        self.lease=ProcessLease(str(self.path)+'.writer-lock')
        if any(p.is_symlink() for p in (self.path,self.registration,self.stop_path)):
            raise ValueError('capture_symlink_forbidden')
        if self.registration.exists():
            reg=json.loads(self.registration.read_text())
            if reg.get('spec')!=SPEC or not finite(reg.get('started_at')):
                raise ValueError('registration_changed')
            if not self.path.exists() and not self.stop_path.exists():raise ValueError('registered_database_missing')
        else:
            if self.path.exists() or self.stop_path.exists():raise ValueError('fresh_path_required')
            reg=dict(spec=SPEC,started_at=self.clock());exclusive_json(self.registration,reg)
        self.started_at=reg['started_at'];self.view['started_at']=self.started_at
        if self.stop_path.exists():self._stop('retained_stop');return
        if self.clock()>=self.started_at+SPEC['duration_seconds']:self._stop('duration_cap');return
        if not self._physical():return
        self.db=sqlite3.connect(self.path,timeout=0)
        if self.db.execute('PRAGMA journal_mode=DELETE').fetchone()[0]!='delete':raise ValueError('delete_journal_required')
        page=self.db.execute('PRAGMA page_size').fetchone()[0]
        if self.db.execute('PRAGMA max_page_count='+str(SPEC['database_bytes']//page)).fetchone()[0]*page!=SPEC['database_bytes']:
            raise ValueError('physical_page_cap')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS receipts(hash TEXT PRIMARY KEY,observed REAL NOT NULL,raw TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS no_receipt_update BEFORE UPDATE ON receipts BEGIN SELECT RAISE(ABORT,'immutable');END;
        CREATE TRIGGER IF NOT EXISTS no_receipt_delete BEFORE DELETE ON receipts BEGIN SELECT RAISE(ABORT,'immutable');END;''')
        self._counts();self.view['state']='collecting'

    def _counts(self):
        count,size,latest=self.db.execute('SELECT count(*),coalesce(sum(length(cast(raw AS BLOB))),0),max(observed) FROM receipts').fetchone()
        self.view.update(receipts=count,payload_bytes=size,latest_received_at=latest)
        self.view['age_unknown']=sum(1 for (s,) in self.db.execute('SELECT raw FROM receipts') if json.loads(s)['http_age_seconds'] is None)

    def _store(self,r):
        if self.view['state']!='collecting':return
        now=self.clock()
        if now>=self.started_at+SPEC['duration_seconds']:self._stop('duration_cap');return
        if not self.started_at<=r['requested_at']<=r['received_at']<=now:
            raise ValueError('pre_registration_or_future_receipt')
        if not self._physical():return
        raw=encode(r);digest=sha(raw.encode())
        if self.db.execute('SELECT 1 FROM receipts WHERE hash=?',(digest,)).fetchone():return
        latest=self.view.get('latest_received_at')
        if latest is not None and r['received_at']<latest:raise ValueError('receipt_clock_reversal')
        if self.view['receipts']>=SPEC['maximum_receipts']:self._stop('receipt_cap');return
        if self.view['payload_bytes']+len(raw.encode())>SPEC['maximum_payload_bytes']:self._stop('payload_cap');return
        try:
            with self.db:self.db.execute('INSERT INTO receipts VALUES(?,?,?)',(digest,r['received_at'],raw))
        except sqlite3.DatabaseError:
            self._stop('sqlite_storage_fault');raise
        self.view.update(receipts=self.view['receipts']+1,payload_bytes=self.view['payload_bytes']+len(raw.encode()),
                         latest_received_at=r['received_at'],latest_receipt_sha256=digest,latest_entity_sha256=r['entity_sha256'],
                         latest_freshness=r['freshness'],age_unknown=self.view['age_unknown']+(r['http_age_seconds'] is None))
        self._physical()

    def submit(self,raw,**facts):
        # Called by the existing reader: no disk, JSON parsing or waiting here.
        if self.view['state']!='collecting':return
        if not isinstance(raw,bytes) or not 0<len(raw)<=SPEC['maximum_entity_bytes']:
            self.view['rejected']+=1;return
        try:self.q.put_nowait((raw,facts))
        except queue.Full:self.view['queue_dropped']+=1

    def _run(self):
        try:
            self._open()
            while not self.closed.is_set() and self.view['state']=='collecting':
                if self.clock()>=self.started_at+SPEC['duration_seconds']:self._stop('duration_cap');break
                try:raw,facts=self.q.get(timeout=0.25)
                except queue.Empty:continue
                try:self._store(envelope(raw,**facts))
                except (ValueError,TypeError,KeyError,UnicodeError):self.view['rejected']+=1
                finally:self.q.task_done()
        except Exception as exc:
            self.view.update(state='collector_fault',error_class=type(exc).__name__)
        finally:
            if self.db is not None:self.db.close();self.db=None
            if self.lease is not None:self.lease.close()

    def start(self):
        if self.thread is not None:raise RuntimeError('no_collector_restart_or_rollover')
        self.thread=threading.Thread(target=self._run,daemon=True,name='bounded-metadata-receipts')
        self.thread.start();return self

    def close(self):
        self.closed.set()
        if self.thread is not None:self.thread.join(timeout=1)
