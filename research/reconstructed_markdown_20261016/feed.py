"""New preregistered source transform. Never sorts retrospective receipt times."""
from collections import Counter
from research.depth_integrity_20261016.audit import Audit,verify
from research.depth_integrity_20261016.diagnose import zero_truncation

class Feed:
    def __init__(self):
        self.counts=Counter();self.audit=Audit(verifier=self.verify);self.available_ns=None
    def verify(self,book,row):
        try:verify(book,row)
        except ValueError:
            if not zero_truncation(book,row):raise
            self.counts['zero_truncations_rebuilt_from_raw']+=1
    def valid(self,key):
        current=self.audit.channels.get(tuple(key[:2]))
        return current is not None and current['valid'] and current['epoch']==key[2]
    def ingest(self,row):
        raw_ns=row['received_ts_ns']
        self.available_ns=max(self.available_ns or raw_ns,raw_ns)
        self.counts['maximum_availability_fence_ns']=max(self.counts['maximum_availability_fence_ns'],self.available_ns-raw_ns)
        base=(row.get('shard_id'),row.get('sid'))
        previous=self.audit.channel_times.get(base)
        passed=self.audit.counts['fully_reconstructed_rows']
        self.audit.ingest(row)
        if row['record_type']=='universe':return None
        state=self.audit.channels[base]
        if previous is not None and raw_ns<previous:
            state['valid']=False;self.counts['channel_clock_invalidations']+=1;return None
        if passed==self.audit.counts['fully_reconstructed_rows'] or not state['valid']:return None
        ticker=row['ticker'];owned=self.audit.books.get((base,ticker))
        if not owned or owned['epoch']!=state['epoch']:return None
        book=owned['book'];best={}
        for side in ('yes','no'):
            eligible=[(p,q) for p,q in book[side].items() if 0<p<100 and q>=1]
            if not eligible:return None
            best[side]=max(eligible)
        y,yq=best['yes'];n,nq=best['no']
        if y+n>=100:return None
        q=dict(yb=float(y),ya=float(100-n),nb=float(n),na=float(100-y),mid=float((y+100-n)/2),imbalance=float(yq/(yq+nq)))
        return dict(ticker=ticker,key=(*base,state['epoch']),quote=q,receipt_at=raw_ns/1e9,available_at=self.available_ns/1e9)
