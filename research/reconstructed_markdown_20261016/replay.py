import gzip,json,math,hashlib
from collections import Counter,deque
from pathlib import Path
from .feed import Feed
from research.ask_markdown_20261016.replay import signal,enter
ROOT=Path(__file__).parent

def run():
    spec=json.loads((ROOT/'registration.json').read_text());files=json.loads((ROOT/'acquisition.json').read_text())['files']
    feed=Feed();counts=Counter();latest={};history={};attempted=set();pending={};signals=[];grid=None;window=None;fault=None
    def decide(at):
        for ticker,r in list(latest.items()):
            if ticker in attempted or not feed.valid(r['key']) or not 0<=at-r['receipt_at']<=1 or not 0<=at-r['available_at']<=1:continue
            old=next(((t,q) for t,q in reversed(history[ticker]) if t<=at-10),None)
            if old is None or at-10-old[0]>2:counts['missing_history']+=1;continue
            side=signal(r['quote'],old[1])
            if side is None:counts['signal_conditions_failed']+=1;continue
            attempted.add(ticker)
            s=dict(ticker=ticker,side=side,decision_at=at,subscription=list(r['key']),decision_quote=r['quote'],prior_quote=old[1],decision_receipt_at=r['receipt_at'],decision_available_at=r['available_at'],decision_ask=r['quote']['ya' if side=='yes' else 'na'],state='awaiting_entry',fill_assumed=False)
            pending[ticker]=s;signals.append(s)
    try:
        for i,receipt in enumerate(files):
            p=Path(receipt['path'])
            if hashlib.sha256(p.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('source_hash_changed')
            for line in gzip.open(p,'rt',encoding='utf-8'):
                counts['records']+=1
                if counts['records']>spec['limits']['maximum_records']:raise ValueError('record_cap')
                raw=json.loads(line);available=max(feed.available_ns or raw['received_ts_ns'],raw['received_ts_ns'])/1e9
                # Decisions are made before ingesting the first row available
                # after the grid time. Later serialized rows cannot leak back.
                if i>=spec['warmup_files']:
                    if window is None:window=available;grid=math.ceil(available/5)*5
                    while grid<available:decide(grid);grid+=5
                r=feed.ingest(raw)
                if raw['record_type']=='universe':continue
                ticker=raw['ticker']
                if ticker.startswith('KXMVE'):continue
                if r is None:latest.pop(ticker,None);history.pop(ticker,None);counts['unusable_noncombo_rows']+=1;continue
                counts['usable_noncombo_rows']+=1
                if ticker in pending:
                    if 0<=r['available_at']-r['receipt_at']<=1:
                        if enter(pending[ticker],r['key'],r['available_at'],r['quote']):pending.pop(ticker)
                    else:counts['stale_delayed_entry_quote']+=1
                if ticker in latest and latest[ticker]['key']!=r['key']:history.pop(ticker,None)
                latest[ticker]=r;h=history.setdefault(ticker,deque());h.append((r['available_at'],r['quote']))
                while h and h[0][0]<r['available_at']-13:h.popleft()
            print(json.dumps(dict(file_complete=i+1,records=counts['records'],preliminary_attempts=len(signals))),flush=True)
    except (ValueError,KeyError,TypeError,ArithmeticError,OSError) as e:
        fault=dict(reason=type(e).__name__+': '+str(e),record=counts['records'])
    for s in pending.values():s['state']='entry_observation_unresolved_source_fault' if fault else 'entry_unfilled_end_of_capture'
    report=dict(schema='reconstructed_markdown_preliminary_v2',registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),source_fault=fault,complete_registered_window=fault is None,decision_window=[window,feed.available_ns/1e9 if feed.available_ns else None],counts=dict(counts),feed_counts=dict(feed.counts),audit_counts=dict(feed.audit.counts),fault_samples=feed.audit.samples,states=dict(Counter(s['state'] for s in signals)),preliminary_signals=signals,actual_orders=0,actual_fills=0,execution_enabled=False,profitability_proven=False)
    with (ROOT/'preliminary.json').open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k not in ('preliminary_signals','fault_samples')}))
if __name__=='__main__':run()
