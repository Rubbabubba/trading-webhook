"""Fixed read-only recovery of retained Demo receipts. No broker or write API."""
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib
import traceback

SCHEMA = 'kalshi_historical_diagnostics_v1_20261101'
TERMINAL = {'terminal'}

def fault_receipt(error,at):
    known={'archive_capacity','archive_lifetime','archive_disk_floor','stream_subscription_timeout',
           'stream_targets_unavailable','comparison_protocol_changed'}
    return {'observed_at':at,'scope':'New worker exception observation; not a reconstructed historical fault time.',
        'error_class':type(error).__name__,'code':str(error) if str(error) in known else 'detail_not_allowlisted',
        'frames':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name}
            for f in traceback.extract_tb(error.__traceback__)[-4:]]}

def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False,ensure_ascii=False)

def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()

@contextmanager
def database(root,name):
    # Names are fixed by this collector, never supplied by a model or report.
    path=Path(root)/name
    db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=1)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    db.set_progress_handler(lambda:1,20_000_000)
    try:
        db.execute('BEGIN')
        yield db
    finally:db.close()

def decoded(value):
    if len(value.encode())>131072:raise ValueError('row_capacity')
    return json.loads(value)

def result_code(value):
    try:
        parsed=decoded(value)
        return str(parsed.get('result','unclassified_retained_result'))[:80] if isinstance(parsed,dict) else 'unclassified_retained_result'
    except (ValueError,TypeError):return 'unclassified_retained_result'

def source(root):
    with database(root,'stream-source-v5.sqlite3') as db:
        connections=[dict(r) for r in db.execute('SELECT started,ended,reason FROM connections ORDER BY started DESC LIMIT 3')]
        n,last=db.execute('SELECT count(*),max(received) FROM messages').fetchone()
        tail=db.execute('SELECT chain_sha FROM messages ORDER BY id DESC LIMIT 1').fetchone()
        path=Path(root)/'stream-source-v5-status.json'
        status=decoded(path.read_text()) if path.exists() and path.stat().st_size<=131072 else {}
        return {'archive':'stream-source-v5.sqlite3','protocol':db.execute('SELECT name FROM protocol WHERE id=1').fetchone()[0],
            'records':n,'latest_received_at':last,'last_chain_sha256':tail[0] if tail else None,
            'connection_count':db.execute('SELECT count(*) FROM connections').fetchone()[0],
            'last_connections':connections,'sqlite_pages':db.execute('PRAGMA page_count').fetchone()[0],
            'timestamp_scope':'Stored connection boundaries and last message only; not an exact exception timestamp.',
            'exception_message_and_trace':'not_retained_by_original_collector',
            'new_exception_observation':status.get('fault_receipt')}

def outcomes(root):
    with database(root,'stream-shadow-comparison-v4.sqlite3') as db:
        attempts=[dict(r) for r in db.execute('SELECT a.id,a.signal_id,a.horizon,a.started_at,r.received_at,r.raw_sha,r.raw_z,r.result FROM outcome_attempts a LEFT JOIN outcome_receipts r ON r.attempt_id=a.id ORDER BY a.id LIMIT 901')]
        if len(attempts)>900:raise ValueError('original_request_cap_exceeded')
        hashes=[];codes=Counter();missing=0;total=0
        for r in attempts:
            if r['raw_z'] is None:
                missing+=1;continue
            decoder=zlib.decompressobj();raw=decoder.decompress(r['raw_z'],262145)
            if len(raw)>262144 or not decoder.eof or decoder.unconsumed_tail:raise ValueError('outcome_decompression_capacity')
            total+=len(raw)
            if total>8*1024*1024:raise ValueError('outcome_total_capacity')
            if hashlib.sha256(raw).hexdigest()!=r['raw_sha']:raise ValueError('outcome_hash_mismatch')
            hashes.append(r['raw_sha']);codes[result_code(r['result'])]+=1
        signals=[tuple(r) for r in db.execute('SELECT id,at FROM high_edge_v15_signals ORDER BY id LIMIT 101')]
        if len(signals)>100:raise ValueError('original_signal_cap_exceeded')
        marks={(r[0],r[1]):r[2] for r in db.execute('SELECT signal_id,horizon,at FROM high_edge_v15_marks LIMIT 301')}
        if len(marks)>300:raise ValueError('original_mark_cap_exceeded')
        absent=[];late=0
        for sid,at in signals:
            for h in (5,30,300):
                if (sid,h) not in marks:
                    absent.append({'signal_id':sid,'horizon':h,'attempts':sum(r['signal_id']==sid and r['horizon']==h for r in attempts),
                        'results':dict(Counter(result_code(r['result']) for r in attempts if r['signal_id']==sid and r['horizon']==h))})
                elif not 0<=marks[sid,h]-(at+h)<=5:late+=1
        return {'archive':'stream-shadow-comparison-v4.sqlite3','signals':len(signals),'complete_marks':len(marks),
            'missing_marks':absent,'late_marks':late,'requests':len(attempts),'hashed_responses':len(hashes),
            'missing_response_receipts':missing,'response_results':dict(codes),'response_hash_set_sha256':digest(hashes),
            'quote_outcomes_only':True,'actual_owned_fills_verified':False}

def trial(root):
    with database(root,'journal.sqlite3') as db,database(root,'worker.sqlite3') as worker:
        rows=db.execute("SELECT i.id,i.payload,i.state,i.filled,e.detail AS economic,b.detail AS broker FROM intents i JOIN economic_intents e ON e.id=i.id LEFT JOIN broker_evidence b ON b.client_id=i.id WHERE i.id LIKE 'v12-fill-%' ORDER BY i.id LIMIT 201").fetchall()
        if len(rows)>200:raise ValueError('original_order_cap_exceeded')
        metadata={r[0]:list(r[1:]) for r in worker.execute("SELECT client_id,event_id,ticker,created_at FROM intent_meta WHERE client_id LIKE 'v12-fill-%' LIMIT 201")}
        queue_ids={r[0] for r in worker.execute("SELECT DISTINCT client_id FROM queue_records WHERE client_id LIKE 'v12-fill-%' LIMIT 201")}
        if len(metadata)>200 or len(queue_ids)>200:raise ValueError('trial_link_capacity')
        states=Counter();statuses=Counter();ids=[];facts=[];filled=0;missing_broker=0;fill_events=0;linked=0;queues=0;events=set();families=set();times=[]
        for r in rows:
            payload=decoded(r['payload']);economic=decoded(r['economic'])
            if economic.get('order_mode')!='post_only_gtc':continue
            if payload.get('client_order_id')!=r['id']:raise ValueError('order_identity_mismatch')
            broker=decoded(r['broker']) if r['broker'] else None
            states[r['state']]+=1;filled+=bool(r['filled']);ids.append(r['id'])
            if broker is None:missing_broker+=1
            else:
                statuses[str((broker.get('order') or {}).get('status'))]+=1
                fill_events+=len(broker.get('fills') or [])
            meta=metadata.get(r['id'])
            if meta:
                linked+=1;events.add(meta[0]);families.add(meta[1].split('-')[0]);times.append(meta[2])
            queues+=r['id'] in queue_ids
            facts.append({'id':r['id'],'payload':payload,'economic':economic,'state':r['state'],'filled':r['filled'],'broker':broker,'meta':list(meta) if meta else None})
        # Commit to all inspected rows without exporting client/broker identities.
        return {'archive':'journal.sqlite3 + worker.sqlite3','attempts':len(ids),'states':dict(states),
            'terminal_orders':states['terminal'],'filled_orders':filled,'broker_fill_events':fill_events,
            'broker_statuses':dict(statuses),'missing_broker_records':missing_broker,'metadata_linked':linked,
            'orders_with_queue_records':queues,'independent_event_labels':len(events),'families':len(families),
            'first_created_at':min(times) if times else None,'last_created_at':max(times) if times else None,
            'all_order_facts_sha256':digest(facts),'scope':'Retained frozen v12-fill post-only trial only; queue records do not prove queue execution or profitability.'}

def collect(root,*,now=None):
    out={'schema':SCHEMA,'observed_at':(now or datetime.now(timezone.utc)).isoformat(),'scope':'Read-only retained evidence; no new prices, broker reads, orders, backfill or cap reset.'}
    for name,reader in (('source',source),('outcomes',outcomes),('trial',trial)):
        try:out[name]={'state':'observed','facts':reader(root)}
        except (OSError,sqlite3.Error,ValueError,TypeError,KeyError,IndexError,AttributeError,zlib.error) as e:
            code=str(e) if type(e) is ValueError and str(e).replace('_','').isalnum() else type(e).__name__
            out[name]={'state':'unavailable','error_code':code[:80]}
    out['receipt_sha256']=digest(out)
    if len(encoded(out).encode())>8000:raise ValueError('history_receipt_capacity')
    return out
