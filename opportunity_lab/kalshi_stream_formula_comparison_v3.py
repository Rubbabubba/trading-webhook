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
import zlib

from .kalshi_high_edge_shadow import init as native_init, observe as native_observe, status as native_status
from .kalshi_shadow import price_book

PROTOCOL='stream_native_formula_comparison_v3_20261023'
REGISTRATION=Path(__file__).resolve().parents[1]/'configs/kalshi_collection_recovery_20261023/native-comparison-v3.json'
MAX_FRAMES=10000
MAX_PAGES=4096

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
        self.db.execute(f'PRAGMA max_page_count={MAX_PAGES}')
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
        from .kalshi_comparison_deadlines import init as outcome_init
        outcome_init(self.db)
        self.history={};self.last={};self.epoch=None;self.blocked=False
        self.accepted=self.db.execute('SELECT count(*) FROM comparison_inputs').fetchone()[0]
        self.stop_reason=None
        self.stop_path=self.path.with_name(self.path.name+'.capacity-stop.json')
        if self.stop_path.exists():
            saved=json.loads(self.stop_path.read_text())
            if saved.get('protocol')!=PROTOCOL or saved.get('sqlite_page_cap')!=MAX_PAGES or saved.get('reason')!='sqlite_page_cap':
                raise ValueError('comparison_capacity_receipt_mismatch')
            self._page_cap_stop()
        # SQLite page allocation is durable even after SQLITE_FULL auto-rolls
        # back its transaction. Preserve the frozen cap across process boots.
        if self.db.execute('PRAGMA page_count').fetchone()[0]>=MAX_PAGES:
            self._page_cap_stop()
        elif self.accepted>=MAX_FRAMES:
            self.blocked=True;self.stop_reason='accepted_frame_cap'

    def _page_cap_stop(self):
        self.blocked=True;self.stop_reason='sqlite_page_cap'
        # Tiny operational receipt, outside the capped immutable evidence DB.
        # No new market observation or increase to its 16MiB allowance.
        if not self.stop_path.exists():
            raw=encoded({'protocol':PROTOCOL,'reason':'sqlite_page_cap',
                         'sqlite_page_cap':MAX_PAGES,'accepted_inputs':self.accepted})
            temporary=self.stop_path.with_name(self.stop_path.name+'.tmp')
            temporary.write_text(raw);temporary.replace(self.stop_path)

    def begin_epoch(self,identity):
        self.epoch=identity;self.history.clear();self.last.clear()

    def invalidate(self,ticker):
        self.history.pop(ticker,None);self.last.pop(ticker,None)

    def exclude(self,reason):
        try:
            self.db.execute('INSERT INTO comparison_exclusions VALUES(?,1) ON CONFLICT(reason) DO UPDATE SET n=n+1',(reason,))
        except sqlite3.Error as exc:
            if getattr(exc,'sqlite_errorcode',None)!=sqlite3.SQLITE_FULL:raise
            self._page_cap_stop()
        return False

    def observe(self,ticker,book,metadata,source_sha):
        if self.blocked:return False
        if self.accepted>=MAX_FRAMES or self.free_bytes()<128*1024*1024:
            self.blocked=True
            self.stop_reason='accepted_frame_cap' if self.accepted>=MAX_FRAMES else 'shared_free_disk_floor'
            return False
        if self.db.execute('PRAGMA page_count').fetchone()[0]>=MAX_PAGES:
            self._page_cap_stop();return False
        # Reserve four MiB for deadline receipts/marks, never grow the old cap.
        start=self.db.execute('SELECT started_at FROM high_edge_v15_protocol').fetchone()[0]
        total=self.db.execute('SELECT count(*) FROM high_edge_v15_signals').fetchone()[0]
        if self.clock()-start>72*3600 or total>=100 or self.db.execute('PRAGMA page_count').fetchone()[0]>=3072:
            self.blocked=True
            self.stop_reason='admission_time_cap' if self.clock()-start>72*3600 else 'signal_cap' if total>=100 else 'outcome_storage_reserve'
            return False
        # Admit one signal at a time so eight simultaneous deadlines cannot
        # exhaust the fixed two-second GET allowance. Marks never create signals.
        if self.db.execute('SELECT 1 FROM high_edge_v15_signals WHERE at+305>=? LIMIT 1',(self.clock(),)).fetchone():
            return False
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
                (self.epoch,at,ticker,metadata['event_ticker'],source_sha,sha(raw),sqlite3.Binary(zlib.compress(raw.encode()))))
            self.db.execute('COMMIT')
        except (ValueError,TypeError,ArithmeticError):
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            return self.exclude('native_formula_input_rejected')
        except sqlite3.Error as exc:
            # SQLITE_FULL can already have rolled back the entire transaction.
            # A second ROLLBACK masks the actual capacity condition.
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            self.blocked=True
            if getattr(exc,'sqlite_errorcode',None)==sqlite3.SQLITE_FULL:
                self._page_cap_stop();return False
            self.stop_reason='sqlite_fault';raise
        self.accepted+=1;history.append((at,(bid+ask)/2));self.last[ticker]=at
        return True

    def status(self):
        value=native_status(self.db)
        from .kalshi_comparison_deadlines import outcome_status
        outcomes=outcome_status(self.db,self.clock())
        return dict(outcome_coverage=outcomes,protocol=PROTOCOL,accepted_inputs=self.accepted,signals=value['signals'],
            complete_signals=value['complete_signals'],complete_events=value['independent_events'],
            state=('storage_fault' if self.stop_reason=='sqlite_fault' else 'capacity_stop') if self.blocked else value['state'],
            stop_reason=self.stop_reason,sqlite_page_cap=MAX_PAGES,
            sqlite_pages=self.db.execute('PRAGMA page_count').fetchone()[0],
            native_quote_evidence=value,metadata_independently_current=False,
            original_rest_evidence_reused=False,execution_enabled=False,fill_assumed=False,
            profitability_proven=False,promotion_ready=False)

    def close(self):self.db.close()
