"""Bounded compressed prospective source evidence; no order interface."""
import gzip,hashlib,json,math,shutil,sqlite3,time
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from .kalshi_depth_replenishment import _levels
from .kalshi_rule_vintage_archive import PROTOCOL as RULE_PROTOCOL
PROTOCOL=json.loads((Path(__file__).parents[1]/'configs/kalshi_continuing_capture_v1_20261016/registration.json').read_text())
LIMITS=PROTOCOL['limits']

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def finite(value):return type(value) in (int,float) and math.isfinite(value)

def base_summary():
    return dict(schema=PROTOCOL['schema'],protocol_sha256=hashlib.sha256(encoded(PROTOCOL).encode()).hexdigest(),execution_enabled=False,fill_assumed=False,profitability_evidence=False,actual_orders=0,actual_fills=0,owned_fills=0,promotion_ready=False)

def unavailable_status(error):
    return dict(**base_summary(),state='initialization_blocked',quotes=0,events=0,latest_quote_at=None,polls=0,public_trades=0,expired_unpolled_quotes=0,blocked_polls=0,incomplete_windows=0,accounted_bytes=0,compressed_bytes=0,records=0,last_error=str(error)[:120],error_count=1)

class Recorder:
    def __init__(self,path,*,now=None,free_bytes=None):
        self.path=Path(path);self.free_bytes=free_bytes or (lambda:shutil.disk_usage(self.path.parent).free)
        if self.free_bytes()<LIMITS['minimum_free_bytes']:raise ValueError('capture_low_disk')
        self.db=sqlite3.connect(str(self.path))
        self.db.execute('PRAGMA foreign_keys=ON')
        page_size=self.db.execute('PRAGMA page_size').fetchone()[0]
        self.db.execute('PRAGMA max_page_count='+str(LIMITS['maximum_database_bytes']//page_size))
        self.db.executescript('''CREATE TABLE IF NOT EXISTS protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY,records INTEGER NOT NULL,accounted INTEGER NOT NULL,compressed INTEGER NOT NULL,last_success REAL,last_error_at REAL,last_error TEXT,error_count INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS blobs(id TEXT PRIMARY KEY,kind TEXT NOT NULL,raw_bytes INTEGER NOT NULL,compressed_sha TEXT NOT NULL,data BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS quotes(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,event_id TEXT NOT NULL,observed_at REAL NOT NULL,usable INTEGER NOT NULL,metadata_blob TEXT NOT NULL REFERENCES blobs(id),book_blob TEXT NOT NULL REFERENCES blobs(id),receipt TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS quote_window ON quotes(usable,observed_at);
        CREATE TABLE IF NOT EXISTS polls(id INTEGER PRIMARY KEY,quote_id TEXT NOT NULL UNIQUE REFERENCES quotes(id),attempted_at REAL NOT NULL,state TEXT NOT NULL,detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS page_receipts(id TEXT PRIMARY KEY,poll_id INTEGER NOT NULL REFERENCES polls(id),observed_at REAL NOT NULL,blob_id TEXT NOT NULL REFERENCES blobs(id),detail TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS trades(id TEXT PRIMARY KEY,ticker TEXT NOT NULL,event_id TEXT NOT NULL,executed_at REAL NOT NULL,economic_json TEXT NOT NULL,page_id TEXT NOT NULL REFERENCES page_receipts(id));
        CREATE INDEX IF NOT EXISTS trade_event ON trades(event_id);''')
        for table in ('blobs','quotes','page_receipts','trades'):
            self.db.executescript(f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT,'capture_append_only'); END; CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'capture_append_only'); END;")
        raw=encoded(PROTOCOL);at=time.time() if now is None else now
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO protocol VALUES(1,?,?)',(raw,at))
            if self.db.execute('SELECT detail FROM protocol WHERE id=1').fetchone()[0]!=raw:raise ValueError('continuing_capture_protocol_changed')
            if self.db.execute('SELECT 1 FROM meta WHERE id=1').fetchone() is None:
                if any(self.db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ('blobs','quotes','polls','page_receipts','trades')):raise ValueError('capture_missing_accounting')
                self.db.execute('INSERT INTO meta VALUES(1,0,0,0,NULL,NULL,NULL,0)')
            self.db.execute("UPDATE polls SET state='interrupted_no_retry',detail=? WHERE state='started'",(encoded({'state':'interrupted_no_retry','execution_enabled':False}),))
            expected_records=sum(self.db.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in ('blobs','quotes','polls','page_receipts','trades'))
            compressed=self.db.execute('SELECT coalesce(sum(length(data)),0) FROM blobs').fetchone()[0]
            accounted=self.db.execute('SELECT coalesce(sum(length(data)+256),0) FROM blobs').fetchone()[0]
            for table,column,overhead in (('quotes','receipt',512),('page_receipts','detail',256),('trades','economic_json',384)):
                accounted+=self.db.execute('SELECT coalesce(sum(length(CAST('+column+' AS BLOB))+'+str(overhead)+'),0) FROM '+table).fetchone()[0]
            accounted+=1024*self.db.execute('SELECT count(*) FROM polls').fetchone()[0]
            if self.db.execute('SELECT records,accounted,compressed FROM meta WHERE id=1').fetchone()!=(expected_records,accounted,compressed):raise ValueError('capture_accounting_mismatch')
        self.started_at=self.db.execute('SELECT started_at FROM protocol WHERE id=1').fetchone()[0]

    def close(self):self.db.close()

    def fault(self,code):
        with self.db:self.db.execute('UPDATE meta SET last_error_at=?,last_error=?,error_count=error_count+1 WHERE id=1',(time.time(),str(code)[:120]))

    def reserve(self,amount,records=1,compressed=0):
        if amount<0 or records<0 or compressed<0:raise ValueError('capture_negative_accounting')
        if self.free_bytes()<LIMITS['minimum_free_bytes']:raise ValueError('capture_low_disk')
        n,b,c=self.db.execute('SELECT records,accounted,compressed FROM meta WHERE id=1').fetchone()
        if n+records>LIMITS['maximum_records']:raise ValueError('capture_record_capacity')
        if b+amount>LIMITS['maximum_accounted_bytes']:raise ValueError('capture_accounted_capacity')
        if c+compressed>LIMITS['maximum_compressed_bytes']:raise ValueError('capture_compressed_capacity')
        self.db.execute('UPDATE meta SET records=records+?,accounted=accounted+?,compressed=compressed+? WHERE id=1',(records,amount,compressed))

    def blob(self,kind,value):
        raw=encoded(value).encode();limit=LIMITS['maximum_page_bytes'] if kind=='trade_page' else LIMITS['maximum_quote_bytes']
        if len(raw)>limit:raise ValueError('capture_payload_too_large')
        identity=hashlib.sha256(kind.encode()+b'\0'+raw).hexdigest()
        if self.db.execute('SELECT 1 FROM blobs WHERE id=?',(identity,)).fetchone():return identity
        data=gzip.compress(raw,compresslevel=6,mtime=0)
        self.reserve(len(data)+256,compressed=len(data))
        self.db.execute('INSERT INTO blobs VALUES(?,?,?,?,?)',(identity,kind,len(raw),hashlib.sha256(data).hexdigest(),data))
        return identity

    def read_blob(self,identity):
        row=self.db.execute('SELECT kind,raw_bytes,compressed_sha,data FROM blobs WHERE id=?',(identity,)).fetchone()
        if row is None:raise ValueError('capture_blob_missing')
        kind,size,digest,data=row
        if hashlib.sha256(data).hexdigest()!=digest:raise ValueError('capture_compressed_hash_mismatch')
        raw=gzip.decompress(data)
        if len(raw)!=size or hashlib.sha256(kind.encode()+b'\0'+raw).hexdigest()!=identity:raise ValueError('capture_content_hash_mismatch')
        return json.loads(raw)

    def quote(self,q):
        try:
            m=q['market'];ticker=q['ticker'];event=m.get('event_ticker')
            clocks=[q.get(k) for k in ('market_started_at','market_observed_at','started_at','observed_at')]
            if (q.get('environment')!='demo' or not isinstance(ticker,str) or not 1<=len(ticker)<=160 or m.get('ticker')!=ticker or m.get('status')!='active' or m.get('market_type')!='binary'
                or not isinstance(event,str) or not 1<=len(event)<=160 or not all(finite(t) for t in clocks) or clocks!=sorted(clocks) or clocks[0]<self.started_at):raise ValueError('capture_quote_identity_or_clock')
            sides={side:_levels(q['orderbook_fp'],side+'_dollars') for side in ('yes','no')}
            best={s:max((Decimal(p) for p,n in rows if Decimal(n)>0),default=None) for s,rows in sides.items()}
            if all(v is not None for v in best.values()) and sum(best.values())>=1:raise ValueError('capture_crossed_book')
            usable=all(any(Decimal(n)>=1 for p,n in rows) for rows in sides.values())
            metadata={k:m.get(k) for k in RULE_PROTOCOL['metadata_fields']}
            receipt=dict(ticker=ticker,event_id=event,clocks=clocks,execution_enabled=False,owned_fill=False)
            if sum(len(encoded(v).encode()) for v in (metadata,sides,receipt))>LIMITS['maximum_quote_bytes']:raise ValueError('capture_quote_too_large')
            identity=hashlib.sha256(encoded(dict(receipt=receipt,metadata=metadata,sides=sides)).encode()).hexdigest()
            if self.db.execute('SELECT 1 FROM quotes WHERE id=?',(identity,)).fetchone():return identity
            with self.db:
                mb=self.blob('metadata',metadata);bb=self.blob('book',sides);raw=encoded(receipt)
                self.reserve(len(raw.encode())+512)
                self.db.execute('INSERT INTO quotes VALUES(?,?,?,?,?,?,?,?)',(identity,ticker,event,clocks[-1],int(usable),mb,bb,raw))
                self.db.execute('UPDATE meta SET last_success=? WHERE id=1',(time.time(),))
            return identity
        except (ValueError,KeyError,TypeError,ArithmeticError,sqlite3.Error) as e:
            try:self.fault(str(e) if isinstance(e,ValueError) else 'capture_quote_'+type(e).__name__)
            except sqlite3.Error:pass
            return None

    def ingest_page(self,poll_id,q,payload,params,started,received):
        if not finite(started) or not finite(received) or not started<=received or started<q['at']+30 or received>q['at']+120:raise ValueError('capture_trade_page_clock')
        page_id=hashlib.sha256(encoded(dict(poll=poll_id,params=params,started=started,received=received,payload=payload)).encode()).hexdigest()
        if not self.db.execute('SELECT 1 FROM page_receipts WHERE id=?',(page_id,)).fetchone():
            with self.db:
                blob=self.blob('trade_page',payload)
                detail=encoded(dict(params=params,started_at=started,received_at=received,execution_enabled=False,owned_fill=False))
                self.reserve(len(detail.encode())+256)
                self.db.execute('INSERT INTO page_receipts VALUES(?,?,?,?,?)',(page_id,poll_id,received,blob,detail))
        rows=payload.get('trades') if isinstance(payload,dict) else None;cursor=payload.get('cursor') if isinstance(payload,dict) else None
        if not isinstance(rows,list) or len(rows)>100 or not isinstance(cursor,str) or len(cursor)>2000:raise ValueError('capture_invalid_trade_page')
        prepared=[];seen=set()
        for row in rows:
            if not isinstance(row,dict):raise ValueError('capture_invalid_trade')
            tid=row.get('trade_id')
            if not isinstance(tid,str) or not 1<=len(tid)<=160 or tid in seen or row.get('ticker')!=q['ticker']:raise ValueError('capture_trade_identity')
            seen.add(tid);at=datetime.fromisoformat(row['created_time'].replace('Z','+00:00'))
            if at.tzinfo is None:raise ValueError('capture_trade_timezone')
            t=at.timestamp();size,yes,no=[Decimal(str(row[k])) for k in ('count_fp','yes_price_dollars','no_price_dollars')]
            if not self.started_at<=t<=started:raise ValueError('capture_trade_time')
            if any(not v.is_finite() for v in (size,yes,no)) or not 0<size<=1000000000 or not 0<=yes<=1 or not 0<=no<=1 or yes+no!=1:raise ValueError('capture_trade_values')
            if not q['at']<t<=q['at']+30:continue
            economic=encoded({k:row.get(k) for k in ('trade_id','ticker','created_time','count_fp','yes_price_dollars','no_price_dollars','taker_side','is_block_trade')})
            prior=self.db.execute('SELECT economic_json FROM trades WHERE id=?',(tid,)).fetchone()
            if prior and prior[0]!=economic:raise ValueError('capture_conflicting_trade')
            if not prior:prepared.append((tid,t,q['ticker'],q['event'],economic))
        with self.db:
            for tid,t,ticker,event,economic in prepared:
                self.reserve(len(economic.encode())+384)
                self.db.execute('INSERT INTO trades VALUES(?,?,?,?,?,?)',(tid,ticker,event,t,economic,page_id))
            self.db.execute('UPDATE meta SET last_success=? WHERE id=1',(time.time(),))
        return len(prepared),cursor

    def poll(self,markets,now,*,legacy_capped,priority_blocked):
        if not legacy_capped or priority_blocked:return False
        last=self.db.execute('SELECT max(attempted_at) FROM polls').fetchone()[0]
        if last is not None and now-last<60:return False
        row=self.db.execute('SELECT q.id,q.ticker,q.event_id,q.observed_at FROM quotes q LEFT JOIN polls p ON p.quote_id=q.id WHERE p.id IS NULL AND q.usable=1 AND q.observed_at>=? AND q.observed_at<=? ORDER BY q.observed_at,q.ticker LIMIT 1',(max(self.started_at,now-90),now-30)).fetchone()
        if row is None:return False
        q=dict(identity=row[0],ticker=row[1],event=row[2],at=row[3]);pages=0;trades=0;cursor=None
        try:
            with self.db:
                self.reserve(1024)
                pid=self.db.execute("INSERT INTO polls(quote_id,attempted_at,state,detail) VALUES(?,?,'started',?)",(q['identity'],now,encoded({'execution_enabled':False}))).lastrowid
        except (ValueError,sqlite3.Error) as e:self.fault(str(e));return False
        try:
            for unused in range(2):
                params=dict(ticker=q['ticker'],limit=100,min_ts=int(q['at']),max_ts=int(q['at']+30))
                if cursor:params['cursor']=cursor
                payload,started,received=markets.get_targeted_trades(params=params)
                added,cursor=self.ingest_page(pid,q,payload,params,started,received)
                pages+=1;trades+=added
                if not cursor:break
            state='observed';detail=dict(pages=pages,new_trades=trades,page_complete=not bool(cursor),execution_enabled=False,owned_fill=False)
        except (ValueError,KeyError,TypeError,ArithmeticError,OSError,sqlite3.Error) as e:
            state='blocked';detail=dict(pages=pages,new_trades=trades,page_complete=False,error=type(e).__name__,execution_enabled=False)
            self.fault(str(e) if isinstance(e,ValueError) else 'capture_page_'+type(e).__name__)
        with self.db:self.db.execute('UPDATE polls SET state=?,detail=? WHERE id=?',(state,encoded(detail),pid))
        return True

    def status(self,now=None):
        now=time.time() if now is None else now
        n,b,c,success,err_at,error,errors=self.db.execute('SELECT records,accounted,compressed,last_success,last_error_at,last_error,error_count FROM meta WHERE id=1').fetchone()
        quotes,events,latest=self.db.execute('SELECT count(*),count(DISTINCT event_id),max(observed_at) FROM quotes').fetchone()
        polls=self.db.execute('SELECT count(*) FROM polls').fetchone()[0];trades=self.db.execute('SELECT count(*) FROM trades').fetchone()[0]
        expired=self.db.execute('SELECT count(*) FROM quotes q LEFT JOIN polls p ON p.quote_id=q.id WHERE p.id IS NULL AND q.usable=1 AND q.observed_at<?',(now-120,)).fetchone()[0]
        blocked=self.db.execute("SELECT count(*) FROM polls WHERE state IN ('blocked','interrupted_no_retry')").fetchone()[0]
        incomplete=self.db.execute("SELECT count(*) FROM polls WHERE state!='observed' OR json_extract(detail,'$.page_complete') IS NOT 1").fetchone()[0]
        active_error=err_at is not None and (success is None or err_at>=success)
        return dict(**base_summary(),state='capacity_insufficient_coverage' if active_error and ('capacity' in (error or '') or error=='capture_low_disk') else 'blocked' if active_error else 'collecting' if quotes else 'awaiting_observation',quotes=quotes,events=events,latest_quote_at=latest,polls=polls,public_trades=trades,expired_unpolled_quotes=expired,blocked_polls=blocked,incomplete_windows=incomplete,accounted_bytes=b,compressed_bytes=c,records=n,last_error=error,error_count=errors)

def priority_pending(db,now):
    """Read-only deadline guard; unknown tables fail closed in worker caller."""
    if db.execute('SELECT 1 FROM maker_fills f WHERE (SELECT count(*) FROM markouts m WHERE m.client_id=f.client_id)<3 LIMIT 1').fetchone():return True
    for horizon in (5,30,300):
        if db.execute('SELECT 1 FROM high_edge_v15_signals s LEFT JOIN high_edge_v15_marks m ON m.signal_id=s.id AND m.horizon=? WHERE m.signal_id IS NULL AND s.at+?+5>=? AND s.at+?<=? LIMIT 1',(horizon,horizon,now,horizon,now+30)).fetchone():return True
        if db.execute('SELECT 1 FROM timely_shadow_signals s LEFT JOIN timely_shadow_marks m ON m.strategy=s.strategy AND m.source_id=s.source_id AND m.horizon=? WHERE m.source_id IS NULL AND s.at+?+5>=? AND s.at+?<=? LIMIT 1',(horizon,horizon,now,horizon,now+30)).fetchone():return True
    return False
