"""Fixed offline markdown signal. Every simulated price remains conditional."""
import gzip,json,math,hashlib
from pathlib import Path
from collections import Counter,deque
from research.receipt_depth_20261015.replay import quote
ROOT=Path(__file__).parent

def signal(q,old):
    for side,bid,ask in [('yes','yb','ya'),('no','nb','na')]:
        if old[ask]-q[ask]>=3 and q[bid]>=old[bid] and q[ask]-q[bid]<=3 and 10<=q[ask]<=90:
            return side
    return None

def enter(s,key,t,q):
    if s['subscription']!=list(key):s['state']='subscription_changed_unfilled';return True
    if t<s['decision_at']+1:return False
    if t>s['decision_at']+2:s['state']='entry_unfilled_late';return True
    ask=math.ceil(q['ya' if s['side']=='yes' else 'na'])
    if not 10<=ask<=90 or ask>s['decision_ask']:s['state']='entry_price_guard_rejected';return True
    s.update(state='conditional_entry_awaiting_settlement',entry_at=t,entry_price=ask)
    return True

def run():
    spec=json.loads((ROOT/'registration.json').read_text());receipts=json.loads((ROOT/'acquisition.json').read_text())['files']
    counts=Counter();sequence={};broken=set();initialized=set();latest={};history={};attempted=set();pending={};signals=[];grid=None;last_t=None;gaps=[];window=None;source_fault=None
    def decide(at):
        for ticker,(t,q,key) in list(latest.items()):
            if ticker in attempted or key in broken or at-t>1:continue
            old=next(((ot,oq) for ot,oq in reversed(history[ticker]) if ot<=at-10),None)
            if old is None or at-10-old[0]>2:counts['missing_history']+=1;continue
            side=signal(q,old[1])
            if side is None:counts['conditions_failed']+=1;continue
            attempted.add(ticker)
            s=dict(ticker=ticker,side=side,decision_at=at,subscription=list(key),decision_quote=q,prior_quote=old[1],decision_ask=q['ya' if side=='yes' else 'na'],state='awaiting_entry',fill_assumed=False)
            signals.append(s);pending[ticker]=s
    for index,receipt in enumerate(receipts):
        p=Path(receipt['path'])
        if hashlib.sha256(p.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('source_changed')
        for line in gzip.open(p,'rt',encoding='utf-8'):
            counts['records']+=1
            r=json.loads(line);t=r['received_ts_ns']/1e9
            if r['record_type']=='universe':continue
            if last_t is not None and t<last_t:
                source_fault=dict(reason='time_reorder',file=index+1,previous_seconds=last_t,current_seconds=t)
                break
            last_t=t
            if index>=spec['warmup_files']:
                if window is None:window=t;grid=math.ceil(t/5)*5
                while grid<t:decide(grid);grid+=5
            key=(r.get('shard_id'),r.get('sid'));seq=r.get('seq');ticker=r['ticker']
            if key in sequence and seq!=sequence[key]+1:
                broken.add(key);gaps.append({'at':t,'key':key,'previous':sequence[key],'current':seq})
            sequence[key]=seq;identity=(*key,ticker)
            if r['record_type']=='snapshot':initialized.add(identity)
            if identity not in initialized:counts['uninitialized']+=1;continue
            if key in broken:counts['broken_sequence_rows']+=1;continue
            if ticker.startswith('KXMVE'):counts['combo_excluded']+=1;continue
            try:q=quote(r)
            except ValueError:q=None
            if q is None:
                latest.pop(ticker,None);history.pop(ticker,None);counts['invalid_book']+=1;continue
            counts['valid_books']+=1
            if ticker in pending and enter(pending[ticker],key,t,q):pending.pop(ticker)
            if ticker in latest and latest[ticker][2]!=key:history.pop(ticker,None)
            latest[ticker]=(t,q,key);h=history.setdefault(ticker,deque());h.append((t,q))
            while h and h[0][0]<t-13:h.popleft()
        if source_fault:break
    for s in pending.values():s['state']='entry_observation_unresolved_source_fault' if source_fault else 'entry_unfilled_end_of_capture'
    report=dict(schema='external_ask_markdown_preliminary_v1',registration_sha256=hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),counts=dict(counts),sequence_defects=gaps,source_fault=source_fault,complete_registered_window=source_fault is None,decision_window=[window,last_t],states=dict(Counter(s['state'] for s in signals)),preliminary_signals=signals,execution_enabled=False,actual_orders=0,actual_fills=0,metadata_verified=False,profitability_proven=False)
    with (ROOT/'preliminary.json').open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in report.items() if k!='preliminary_signals'}))
if __name__=='__main__':run()
