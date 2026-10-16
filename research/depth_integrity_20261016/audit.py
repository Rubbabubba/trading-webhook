"""Independent full-ladder verification, without any trading decisions."""
import gzip,json,hashlib
from decimal import Decimal
from collections import Counter
from pathlib import Path
ROOT=Path(__file__).parent
TOL=Decimal('0.000001')

def number(x):
    d=Decimal(str(x))
    if not d.is_finite():raise ValueError('nonfinite_number')
    return d

def ladder(rows):
    result={}
    for p,q in rows:
        p,q=number(p),number(q)
        if not 0<=p<=100 or q<0 or p in result:raise ValueError('bad_ladder')
        if q:result[p]=q
    return result

def equal_levels(actual,expected):
    if len(actual)!=len(expected):return False
    return all(a[0]==b[0] and abs(a[1]-b[1])<=TOL for a,b in zip(actual,expected))

def verify(book,row):
    for side,other in [('yes','no'),('no','yes')]:
        bids=sorted(book[side].items(),reverse=True)[:3]
        asks=sorted(((100-p,q) for p,q in book[other].items()))[:3]
        source=row['book'][side]
        actual_bids=sorted(ladder(source['bid_levels']).items(),reverse=True)
        actual_asks=sorted(ladder(source['ask_levels']).items())
        if not equal_levels(actual_bids,bids) or not equal_levels(actual_asks,asks):raise ValueError('derived_top3_disagreement')

class Audit:
    def __init__(self):
        self.channels={};self.books={};self.counts=Counter();self.samples=[];self.latest=None;self.channel_times={};self.levels=0
    def fault(self,reason,row):
        self.counts[reason]+=1
        if len(self.samples)<20:self.samples.append(dict(reason=reason,ticker=row.get('ticker'),shard=row.get('shard_id'),sid=row.get('sid'),seq=row.get('seq'),received_ts_ns=row.get('received_ts_ns')))
    def ingest(self,row):
        self.counts['records']+=1
        if row['record_type']=='universe':self.counts['universe']+=1;return
        key=(row['shard_id'],row['sid']);seq=row['seq'];t=row['received_ts_ns'];kind=row['record_type'];ticker=row['ticker']
        if self.latest is not None and t<self.latest:self.fault('global_clock_regression',row)
        self.latest=t
        if key in self.channel_times and t<self.channel_times[key]:self.fault('within_channel_clock_regression',row)
        self.channel_times[key]=t
        state=self.channels.get(key)
        if state is None:
            state=dict(epoch=0,seq=seq,valid=kind=='snapshot');self.channels[key]=state
            self.counts['first_observed_channels']+=1
        elif seq==1 and kind=='snapshot' and state['seq']>1:
            state.update(epoch=state['epoch']+1,seq=seq,valid=True);self.counts['inferred_epoch_restarts']+=1
        else:
            if seq!=state['seq']+1:
                state['valid']=False;self.fault('sequence_discontinuity',row)
            state['seq']=seq
        if not state['valid']:self.counts['unverified_channel_rows']+=1;return
        ident=(key,ticker);owned=self.books.get(ident)
        try:
            if kind=='snapshot':
                book=dict(yes=ladder(row['snapshot_yes_bids']),no=ladder(row['snapshot_no_bids']))
                self.levels-=sum(len(v) for v in owned['book'].values()) if owned else 0
                self.books[ident]=dict(epoch=state['epoch'],book=book)
                self.levels+=sum(len(v) for v in book.values())
                self.counts['raw_snapshots']+=1
            elif kind=='delta':
                if owned is None or owned['epoch']!=state['epoch']:
                    self.counts['uninitialized_ticker_rows']+=1;return
                book=owned['book'];side=row['delta_side']
                if side not in ('yes','no'):raise ValueError('invalid_delta_side')
                price=number(row['delta_price_cents']);delta=number(row['delta_qty'])
                before=book[side].get(price,Decimal(0));reported_before=number(row['level_qty_before']);after=number(row['level_qty_after'])
                if not 0<=price<=100 or after<0 or abs(before-reported_before)>TOL or abs(before+delta-after)>TOL:raise ValueError('raw_delta_disagreement')
                existed=price in book[side]
                if after:book[side][price]=after
                else:book[side].pop(price,None)
                self.levels+=int(after>0)-int(existed)
                self.counts['raw_deltas']+=1
            else:raise ValueError('unknown_record_type')
            if self.levels>3000000:raise ValueError('ladder_capacity')
            verify(book,row);self.counts['fully_reconstructed_rows']+=1
        except (ValueError,KeyError,TypeError) as e:
            self.fault(str(e),row)
            # An integrity failure invalidates this ticker; only another full
            # snapshot can restore it, never a later apparently good delta.
            broken=self.books.pop(ident,None)
            if broken:self.levels-=sum(len(v) for v in broken['book'].values())
            if str(e)=='ladder_capacity':raise

def run():
    receipt=json.loads((ROOT.parent/'ask_markdown_20261016/acquisition.json').read_text())['files'][5]
    path=Path(receipt['path'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('source_hash_changed')
    a=Audit()
    for line in gzip.open(path,'rt',encoding='utf-8'):
        if a.counts['records']>=1500000:raise ValueError('record_cap')
        a.ingest(json.loads(line))
    report=dict(schema='external_depth_integrity_audit_v1',source=receipt,protocol_sha256=hashlib.sha256((ROOT/'protocol.json').read_bytes()).hexdigest(),counts=dict(a.counts),fault_samples=a.samples,final_ladder_levels=a.levels,execution_enabled=False,actual_orders=0,profitability_proven=False,connection_uuid_verified=False,original_clock_order_preserved=True)
    with (ROOT/'audit-result.json').open('x') as f:json.dump(report,f,indent=2)
    print(json.dumps(report))
if __name__=='__main__':run()
