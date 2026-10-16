"""Forward-only rule observations from existing Demo quote reads, not releases."""
import hashlib,json,math,sqlite3,time
from pathlib import Path

PROTOCOL=json.loads((Path(__file__).parents[1]/'configs/kalshi_rule_vintage_archive_v1_20261016/registration.json').read_text())


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


def init(db,now=None):
    now=time.time() if now is None else now
    db.executescript('''CREATE TABLE IF NOT EXISTS rule_archive_protocol(id INTEGER PRIMARY KEY,detail TEXT NOT NULL,started_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS rule_archive_versions(ticker TEXT NOT NULL,version_hash TEXT NOT NULL,first_observed_at REAL NOT NULL,fields_json TEXT NOT NULL,PRIMARY KEY(ticker,version_hash));
    CREATE TABLE IF NOT EXISTS rule_archive_receipts(receipt_hash TEXT PRIMARY KEY,ticker TEXT NOT NULL,version_hash TEXT NOT NULL,metadata_started_at REAL NOT NULL,metadata_observed_at REAL NOT NULL,book_started_at REAL NOT NULL,book_observed_at REAL NOT NULL,server_updated_time TEXT);
    CREATE TABLE IF NOT EXISTS rule_archive_fault(id INTEGER PRIMARY KEY,observed_at REAL NOT NULL,error TEXT NOT NULL,total INTEGER NOT NULL);
    CREATE TRIGGER IF NOT EXISTS rule_versions_no_update BEFORE UPDATE ON rule_archive_versions BEGIN SELECT RAISE(ABORT,'rule_archive_append_only'); END;
    CREATE TRIGGER IF NOT EXISTS rule_versions_no_delete BEFORE DELETE ON rule_archive_versions BEGIN SELECT RAISE(ABORT,'rule_archive_append_only'); END;
    CREATE TRIGGER IF NOT EXISTS rule_receipts_no_update BEFORE UPDATE ON rule_archive_receipts BEGIN SELECT RAISE(ABORT,'rule_archive_append_only'); END;
    CREATE TRIGGER IF NOT EXISTS rule_receipts_no_delete BEFORE DELETE ON rule_archive_receipts BEGIN SELECT RAISE(ABORT,'rule_archive_append_only'); END;''')
    raw=canonical(PROTOCOL)
    db.execute('INSERT OR IGNORE INTO rule_archive_protocol VALUES(1,?,?)',(raw,now))
    if db.execute('SELECT detail FROM rule_archive_protocol WHERE id=1').fetchone()[0]!=raw:raise ValueError('rule_archive_protocol_changed')


def capture(db,quote):
    protocol=db.execute('SELECT detail,started_at FROM rule_archive_protocol WHERE id=1').fetchone()
    if protocol is None or protocol[0]!=canonical(PROTOCOL):raise ValueError('rule_archive_protocol_changed')
    if quote.get('environment')!='demo':raise ValueError('rule_archive_demo_only')
    market=quote.get('market',{});ticker=quote.get('ticker')
    if not isinstance(ticker,str) or not 1<=len(ticker)<=160 or market.get('ticker')!=ticker:raise ValueError('rule_archive_identity')
    clocks=[quote.get(k) for k in ('market_started_at','market_observed_at','started_at','observed_at')]
    if any(type(t) not in (float,int) or not math.isfinite(t) for t in clocks) or clocks!=sorted(clocks):raise ValueError('rule_archive_clock_order')
    start=protocol[1]
    if clocks[0]<start:raise ValueError('rule_archive_pre_registration')
    fields={k:market.get(k) for k in PROTOCOL['metadata_fields']}
    raw=canonical(fields)
    if len(raw.encode())>PROTOCOL['limits']['maximum_version_bytes']:raise ValueError('rule_archive_version_too_large')
    digest=hashlib.sha256(raw.encode()).hexdigest()
    receipt=hashlib.sha256(canonical({'ticker':ticker,'version':digest,'clocks':clocks}).encode()).hexdigest()
    existing=db.execute('SELECT fields_json FROM rule_archive_versions WHERE ticker=? AND version_hash=?',(ticker,digest)).fetchone()
    if existing and existing[0]!=raw:raise ValueError('rule_archive_version_integrity')
    if db.execute('SELECT 1 FROM rule_archive_receipts WHERE receipt_hash=?',(receipt,)).fetchone():return digest
    if not existing and db.execute('SELECT count(*) FROM rule_archive_versions').fetchone()[0]>=PROTOCOL['limits']['maximum_versions']:raise ValueError('rule_archive_version_capacity')
    if db.execute('SELECT count(*) FROM rule_archive_receipts').fetchone()[0]>=PROTOCOL['limits']['maximum_receipts']:raise ValueError('rule_archive_receipt_capacity')
    db.execute('SAVEPOINT rule_archive_capture')
    try:
        db.execute('INSERT OR IGNORE INTO rule_archive_versions VALUES(?,?,?,?)',(ticker,digest,clocks[1],raw))
        db.execute('INSERT INTO rule_archive_receipts VALUES(?,?,?,?,?,?,?,?)',(receipt,ticker,digest,*clocks,market.get('updated_time')))
        db.execute('RELEASE rule_archive_capture')
    except Exception:
        db.execute('ROLLBACK TO rule_archive_capture');db.execute('RELEASE rule_archive_capture')
        raise
    return digest


def record_fault(db,error,at=None):
    db.execute('INSERT INTO rule_archive_fault VALUES(1,?,?,1) ON CONFLICT(id) DO UPDATE SET observed_at=excluded.observed_at,error=excluded.error,total=total+1',(time.time() if at is None else at,str(error)[:160]))


def status(db):
    try:
        row=db.execute('SELECT count(*),max(metadata_observed_at),max(book_observed_at) FROM rule_archive_receipts').fetchone()
        versions=db.execute('SELECT count(*) FROM rule_archive_versions').fetchone()[0]
        fault=db.execute('SELECT observed_at,error,total FROM rule_archive_fault WHERE id=1').fetchone()
        protocol=db.execute('SELECT detail,started_at FROM rule_archive_protocol WHERE id=1').fetchone()
        bad=protocol is None or protocol[0]!=canonical(PROTOCOL)
        active_fault=bad or (fault is not None and (row[1] is None or fault[0]>=row[1]))
        return {'schema':PROTOCOL['schema'],'state':'fault' if active_fault else 'collecting' if row[0] else 'awaiting_observation','versions':versions,'receipts':row[0],
                'latest_metadata_receipt_at':row[1],'latest_book_receipt_at':row[2],'last_error':fault[1] if fault else None,'error_count':fault[2] if fault else 0,
                'protocol_verified':not bad,'historical_publication_verified':False,'execution_enabled':False}
    except sqlite3.OperationalError as e:
        if 'no such table' not in str(e):raise
        return {'schema':PROTOCOL['schema'],'state':'not_initialized','versions':0,'receipts':0,'execution_enabled':False}
