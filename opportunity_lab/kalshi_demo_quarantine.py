"""Audited exception for the specific expired demo IOC, never production."""
from datetime import datetime, timezone
import hashlib
import json
import math

ORIGINAL_ID='codex-demo-20260914-entry-001'
ORIGINAL_TICKER='KXWTIH-26SEP1417-T99.39'


def fingerprint(record):
    return hashlib.sha256(json.dumps(record,sort_keys=True).encode()).hexdigest()


def eligible(record,market,now):
    p=record['payload']
    if (record['state']!='uncertain' or record['filled']!=0 or record['broker_id'] is not None
            or record['reserve']!=60 or p.get('client_order_id')!=ORIGINAL_ID
            or p.get('ticker')!=ORIGINAL_TICKER or p.get('side')!='bid'
            or p.get('count')!='1.00' or p.get('price')!='0.5000'
            or p.get('time_in_force')!='immediate_or_cancel' or p.get('subaccount')!=0):
        raise ValueError('not_the_approved_demo_incident')
    if market.get('ticker')!=ORIGINAL_TICKER or market.get('status') not in ('closed','determined','finalized'):
        raise ValueError('original_market_not_closed')
    close=datetime.fromisoformat(market['close_time'].replace('Z','+00:00'))
    if close.tzinfo is None or close.timestamp()>now:raise ValueError('original_market_not_expired')


def enroll(journal,evidence,*,key_id,now):
    if journal.db.execute('SELECT environment FROM journal_scope WHERE id=1').fetchone()!=('demo',):
        raise ValueError('demo_scope_required')
    start,end=evidence.get('started_at'),evidence.get('observed_at')
    if (evidence.get('environment')!='demo'
            or any(type(v) not in (int,float) or not math.isfinite(v) for v in (start,end,now))
            or not 0<=end-start<=30 or not 0<=now-end<=60):
        raise ValueError('fresh_quarantine_evidence_required')
    for field in ('positions','resting_orders','orders','historical_orders','fills','historical_fills','settlements'):
        if evidence.get(field)!=[]:raise ValueError('account_or_incident_not_clear')
    cash=evidence['balance']['balance']
    if type(cash) is not int or cash<60:raise ValueError('invalid_quarantine_cash')
    if not isinstance(key_id,str) or not key_id:raise ValueError('demo_key_identity_required')
    journal.db.execute('CREATE TABLE IF NOT EXISTS demo_quarantine(id TEXT PRIMARY KEY,detail TEXT NOT NULL)')
    journal.db.execute('CREATE TABLE IF NOT EXISTS demo_quarantine_history('
                       'id TEXT NOT NULL,renewed_at REAL NOT NULL,detail TEXT NOT NULL,'
                       'PRIMARY KEY(id,renewed_at))')
    journal.db.execute('BEGIN IMMEDIATE')
    try:
        record=journal.get(ORIGINAL_ID);eligible(record,evidence['market'],now)
        entry=dict(environment='demo',record_sha256=fingerprint(record),reserve_cents=60,
                   key_id_sha256=hashlib.sha256(key_id.encode()).hexdigest(),
                   enrolled_at=now,expires_at=now+86400,evidence=evidence,
                   reason='user_approved_expired_demo_ioc_quarantine_not_resolution')
        encoded=json.dumps(entry,sort_keys=True)
        prior=journal.db.execute('SELECT detail FROM demo_quarantine WHERE id=?',(ORIGINAL_ID,)).fetchone()
        if prior is None:
            journal.db.execute('INSERT INTO demo_quarantine VALUES(?,?)',(ORIGINAL_ID,encoded))
        else:
            old=json.loads(prior[0])
            # Renewal never resolves or edits the uncertain order. It is allowed
            # only for the same journal record/key after the prior grant expires,
            # with a new full account/market audit already validated above.
            if (old.get('environment')!='demo' or old.get('record_sha256')!=entry['record_sha256']
                    or old.get('reserve_cents')!=60 or old.get('key_id_sha256')!=entry['key_id_sha256']
                    or type(old.get('expires_at')) not in (int,float) or now<old['expires_at']):
                raise ValueError('quarantine_renewal_not_allowed')
            journal.db.execute('INSERT INTO demo_quarantine_history VALUES(?,?,?)',
                               (ORIGINAL_ID,now,prior[0]))
            journal.db.execute('UPDATE demo_quarantine SET detail=? WHERE id=?',(encoded,ORIGINAL_ID))
        journal.db.execute('COMMIT');return entry
    except Exception:
        journal.db.execute('ROLLBACK');raise


def validate(db,record,*,key_id,market,now):
    if db.execute('SELECT environment FROM journal_scope WHERE id=1').fetchone()!=('demo',):
        raise ValueError('demo_scope_required')
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='demo_quarantine'").fetchone():
        raise ValueError('earlier_demo_intent_unresolved')
    row=db.execute('SELECT detail FROM demo_quarantine WHERE id=?',(record['payload']['client_order_id'],)).fetchone()
    if row is None:raise ValueError('earlier_demo_intent_unresolved')
    entry=json.loads(row[0]);eligible(record,market,now)
    if (entry.get('environment')!='demo' or entry.get('record_sha256')!=fingerprint(record)
            or entry.get('reserve_cents')!=60 or not entry['enrolled_at']<=now<entry['expires_at']
            or entry.get('key_id_sha256')!=hashlib.sha256(key_id.encode()).hexdigest()):
        raise ValueError('quarantine_expired_or_changed')
    return 60
