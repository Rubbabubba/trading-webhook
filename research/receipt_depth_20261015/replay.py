"""Streaming preliminary depth replay; source prices never count as broker fills."""
import gzip,json,math,hashlib
from collections import Counter,deque
from pathlib import Path

ROOT=Path(__file__).parent


def quote(row):
    yes=row.get('book',{}).get('yes',{})
    no=row.get('book',{}).get('no',{})
    def best(levels):
        good=[]
        for p,q in levels:
            if not(math.isfinite(p) and math.isfinite(q)) or not 0<p<100 or q<0:raise ValueError('bad_level')
            if q>=1:good.append((p,q))
        return max(good,key=lambda x:x[0]) if good else None
    y=best(yes.get('bid_levels',[]));n=best(no.get('bid_levels',[]))
    if not y or not n:return None
    if y[0]+n[0]>=100:return None
    return {'yb':y[0],'ya':100-n[0],'nb':n[0],'na':100-y[0], 'mid':(y[0]+100-n[0])/2,'imbalance':y[1]/(y[1]+n[1])}


def signal(current,old):
    if current['ya']-current['yb']>3:return None
    change=current['mid']-old['mid']
    side='yes' if change>=2 and current['imbalance']>=.8 else 'no' if change<=-2 and 1-current['imbalance']>=.8 else None
    if side and 10<=current['ya' if side=='yes' else 'na']<=90:return side
    return None


def advance(s,key,t,q):
    if s['subscription']!=list(key):s['state']='subscription_changed_unresolved';return True
    if s['state']=='awaiting_entry' and t>=s['decision_at']+1:
        if t>s['decision_at']+2:s['state']='entry_unfilled_late';return True
        s['entry_at']=t;s['entry_price']=math.ceil(q['ya' if s['side']=='yes' else 'na']);s['state']='awaiting_exit'
        if not 10<=s['entry_price']<=90:s['state']='entry_price_guard_rejected';return True
    elif s['state']=='awaiting_exit' and t>=s['entry_at']+30:
        if t>s['entry_at']+35:s['state']='exit_unresolved_late';return True
        s['exit_at']=t;s['exit_price']=math.floor(q['yb' if s['side']=='yes' else 'nb']);s['state']='conditional_round_trip_complete'
        s['net_cents_by_extra_stress']={str(stress):s['exit_price']-s['entry_price']-2*(5+1+stress) for stress in (0,2,5)}
        return True
    return False


def run():
    receipts=json.loads((ROOT/'three-file-acquisition.json').read_text())['files']
    counts=Counter();sequence={};broken=set();initialized=set();histories={};latest={};attempted=set();pending={};signals=[]
    grid=None;last_t=None;gaps=[]
    def decision(at):
        for ticker,(t,q,key) in list(latest.items()):
            if ticker in attempted or key in broken or at-t>1:continue
            history=histories[ticker]
            old=next(((ht,hq) for ht,hq in reversed(history) if ht<=at-10),None)
            if old is None or at-10-old[0]>2:counts['missing_ten_second_history']+=1;continue
            side=signal(q,old[1])
            if not side:counts['signal_conditions_failed']+=1;continue
            # Preliminary records are later filtered by official market/event identity.
            if len(signals)>=1000:counts['preliminary_cap_reached']+=1;continue
            attempted.add(ticker)
            s={'ticker':ticker,'side':side,'decision_at':at,'subscription':list(key),'decision_quote':q,'prior_mid':old[1]['mid'],'state':'awaiting_entry','fill_assumed':False}
            signals.append(s);pending[ticker]=s
    for receipt in receipts:
        p=Path(receipt['path'])
        if hashlib.sha256(p.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('source_hash_changed')
        for line in gzip.open(p,'rt',encoding='utf-8'):
            counts['records']+=1
            if counts['records']>4500000:raise ValueError('record_cap')
            r=json.loads(line);t=r['received_ts_ns']/1e9
            if r['record_type']=='universe':counts['universe_records']+=1;continue
            if last_t is not None and t<last_t:raise ValueError('receipt_time_reorder')
            last_t=t
            if grid is None:grid=math.ceil(t/5)*5
            while grid<t:
                decision(grid);grid+=5
            key=(r.get('shard_id'),r.get('sid'));seq=r.get('seq');ticker=r['ticker']
            if key in sequence and seq!=sequence[key]+1:
                broken.add(key);gaps.append({'at':t,'subscription':key,'prior':sequence[key],'current':seq})
            sequence[key]=seq
            state=(*key,ticker)
            if r['record_type']=='snapshot':initialized.add(state)
            if state not in initialized:counts['uninitialized_rows']+=1;continue
            if key in broken:counts['sequence_invalid_rows']+=1;continue
            if ticker.startswith('KXMVE'):counts['combo_rows_excluded']+=1;continue
            try:q=quote(r)
            except ValueError:counts['invalid_levels']+=1;q=None
            if q is None:
                counts['empty_or_crossed_rows']+=1
                latest.pop(ticker,None);histories.pop(ticker,None)
                continue
            counts['valid_noncombo_quote_rows']+=1
            s=pending.get(ticker)
            if s:
                if advance(s,key,t,q):pending.pop(ticker)
            latest[ticker]=(t,q,key)
            history=histories.setdefault(ticker,deque());history.append((t,q))
            while history and history[0][0]<t-13:history.popleft()
    states=Counter(s['state'] for s in signals)
    report={'schema':'external_receipt_depth_preliminary_v1','counts':dict(counts),'sequence_defects':gaps,'states':dict(states),'source_files':receipts,'registration_sha256':hashlib.sha256((ROOT/'registration.json').read_bytes()).hexdigest(),'preliminary_signals':signals,'execution_enabled':False,'profitability_proven':False,'metadata_verified':False,'note':'Preliminary ticker records only; no eligible-entry total or performance result until official event identity and open/close validation. Unresolved positions never treated as zero.'}
    with (ROOT/'preliminary-v2-invalid-book-corrected.json').open('x') as f:json.dump(report,f,indent=2)
    print(json.dumps({'counts':dict(counts),'sequence_defects':len(gaps),'preliminary_signals':len(signals),'states':dict(states)}))


if __name__=='__main__':run()
