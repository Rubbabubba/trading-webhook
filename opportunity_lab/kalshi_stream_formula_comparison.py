"""Separate prospective stream ledger for unchanged native quote formulas.

This module has no network, account, broker, order or activation interface.
Old REST evidence is never opened. Cached event context is explicitly not a
verified metadata vintage or execution qualification.
"""
from collections import deque
from datetime import datetime
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import time

from .kalshi_high_edge_shadow import init as native_init, observe as native_observe, status as native_status
from .kalshi_shadow import price_book

PROTOCOL='stream_native_formula_comparison_v1_20261018'
REGISTRATION=Path(__file__).resolve().parents[1]/'configs/kalshi_demo_continuous_stream_20261018/native-comparison-v1.json'
MAX_FRAMES=10000

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def sha(value):return hashlib.sha256(value.encode()).hexdigest()


def metadata_for(path,tickers):
    try:
        with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=1) as db:
            row=db.execute("SELECT detail FROM settings WHERE name='sampling_window'").fetchone()
    except sqlite3.Error:return {}
    window=json.loads(row[0]) if row else {}
    markets=window.get('markets')
    if not isinstance(markets,list):return {}
    return {m['ticker']:m for m in markets if isinstance(m,dict) and m.get('ticker') in tickers}


class Comparison:
    def __init__(self,path,*,clock=time.time,free_bytes=None):
        self.path=Path(path);self.clock=clock
        self.free_bytes=free_bytes or (lambda:shutil.disk_usage(self.path.parent).free)
        if self.free_bytes()<128*1024*1024:raise ValueError('comparison_low_disk')
        self.db=sqlite3.connect(self.path,isolation_level=None)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA max_page_count=4096')
        if self.db.execute('PRAGMA page_size').fetchone()[0]!=4096:raise ValueError('comparison_page_size')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS comparison_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS comparison_inputs(id INTEGER PRIMARY KEY,epoch TEXT,at REAL,ticker TEXT,event TEXT,source_sha TEXT,input_sha TEXT,input_json TEXT);
        CREATE TABLE IF NOT EXISTS comparison_exclusions(reason TEXT PRIMARY KEY,n INTEGER NOT NULL);
        CREATE TRIGGER IF NOT EXISTS comparison_input_update BEFORE UPDATE ON comparison_inputs BEGIN SELECT RAISE(ABORT,'comparison_inputs_immutable'); END;
        CREATE TRIGGER IF NOT EXISTS comparison_input_delete BEFORE DELETE ON comparison_inputs BEGIN SELECT RAISE(ABORT,'comparison_inputs_immutable'); END;
        ''')
        spec=json.loads(REGISTRATION.read_text())
        if spec['protocol']!=PROTOCOL or spec['execution_enabled'] is not False:raise ValueError('comparison_registration')
        module_root=Path(__file__).parent
        hashes={name:hashlib.sha256((module_root/name).read_bytes()).hexdigest() for name in (
            'kalshi_high_edge_shadow.py','kalshi_maker_v10.py','kalshi_maker_v12.py')}
        detail=encoded({'registration':spec,'native_implementation_sha256':hashes})
        self.db.execute('INSERT OR IGNORE INTO comparison_protocol VALUES(1,?)',(detail,))
        if self.db.execute('SELECT detail FROM comparison_protocol WHERE id=1').fetchone()[0]!=detail:raise ValueError('comparison_protocol_changed')
        native_init(self.db,self.clock())
        self.history={};self.last={};self.epoch=None;self.blocked=False
        self.accepted=self.db.execute('SELECT count(*) FROM comparison_inputs').fetchone()[0]

    def begin_epoch(self,identity):
        self.epoch=identity;self.history.clear();self.last.clear()

    def invalidate(self,ticker):
        self.history.pop(ticker,None);self.last.pop(ticker,None)

    def exclude(self,reason):
        self.db.execute('INSERT INTO comparison_exclusions VALUES(?,1) ON CONFLICT(reason) DO UPDATE SET n=n+1',(reason,))
        return False

    def observe(self,ticker,book,metadata,source_sha):
        if self.blocked:return False
        if self.accepted>=MAX_FRAMES or self.free_bytes()<128*1024*1024:
            self.blocked=True;return self.exclude('comparison_capacity_or_low_disk')
        at=book['received_at']
        if self.epoch is None:return self.exclude('epoch_missing')
        if not 0<=self.clock()-at<=2 or not 0<=at-book['sending_ts_ms']/1000<=2:
            return self.exclude('stale_or_unsynchronized_source')
        if ticker in self.last and at-self.last[ticker]<1:return False
        if (not isinstance(metadata,dict) or metadata.get('ticker')!=ticker
                or metadata.get('market_type')!='binary' or metadata.get('status')!='active'
                or not isinstance(metadata.get('event_ticker'),str) or not metadata['event_ticker']):
            return self.exclude('missing_exact_event_metadata')
        try:
            closing=datetime.fromisoformat(metadata['close_time'].replace('Z','+00:00'))
            if closing.tzinfo is None:return self.exclude('invalid_original_close')
            close=closing.timestamp()
        except (KeyError,TypeError,ValueError):return self.exclude('invalid_original_close')
        if close<=at+1800:return self.exclude('outside_original_lifecycle_guard')
        identity=metadata.get('id') or metadata.get('market_id')
        if identity and identity!=book['market_id']:return self.exclude('metadata_market_identity_mismatch')
        frame={'ticker':ticker,'received_at':at,'fee_coefficient':'.07',
               'orderbook_fp':{side+'_dollars':[[str(p),str(n)] for p,n in sorted(book[side].items(),reverse=True)[:20]] for side in ('yes','no')}}
        try:bid,ask,_,_=price_book(frame,'yes')
        except (ValueError,TypeError,ArithmeticError):return self.exclude('native_book_rejected')
        history=self.history.setdefault(ticker,deque())
        while history and history[0][0]<at-300:history.popleft()
        source={'frame':frame,'metadata':metadata,'metadata_independently_current':False,
                'epoch':self.epoch,'source_message_sha256':source_sha,'sid':book['sid'],'seq':book['seq']}
        raw=encoded(source)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            native_observe(self.db,ticker,metadata['event_ticker'],list(history),frame)
            self.db.execute('INSERT INTO comparison_inputs(epoch,at,ticker,event,source_sha,input_sha,input_json) VALUES(?,?,?,?,?,?,?)',
                (self.epoch,at,ticker,metadata['event_ticker'],source_sha,sha(raw),raw))
            self.db.execute('COMMIT')
        except (ValueError,TypeError,ArithmeticError):
            self.db.execute('ROLLBACK');return self.exclude('native_formula_input_rejected')
        except sqlite3.Error:
            self.db.execute('ROLLBACK');self.blocked=True;raise
        self.accepted+=1;history.append((at,(bid+ask)/2));self.last[ticker]=at
        return True

    def status(self):
        value=native_status(self.db)
        return dict(protocol=PROTOCOL,accepted_inputs=self.accepted,signals=value['signals'],
            complete_signals=value['complete_signals'],complete_events=value['independent_events'],
            state='capacity_stop' if self.blocked else value['state'],
            native_quote_evidence=value,metadata_independently_current=False,
            original_rest_evidence_reused=False,execution_enabled=False,fill_assumed=False,
            profitability_proven=False,promotion_ready=False)

    def close(self):self.db.close()
