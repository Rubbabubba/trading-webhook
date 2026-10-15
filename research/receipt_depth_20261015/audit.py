"""Source quality inventory; does not change or qualify any trading protocol."""
import gzip,hashlib,json
from collections import Counter
from pathlib import Path

root=Path(__file__).parent
source=Path(json.loads((root/'acquisition.json').read_text())['path'])
types=Counter();series=Counter();missing=Counter();tickers=set();snapshots=set();subscriptions={};gaps=[];valid=0;uninitialized=0;start=None;end=None;n=0;unknownexchange=0
for line in gzip.open(source,'rt',encoding='utf-8'):
    n+=1
    if n>1500000:raise ValueError('record_cap')
    r=json.loads(line);types[r.get('record_type')]+=1
    t=r.get('received_ts_ns');start=t if start is None else min(start,t);end=t if end is None else max(end,t)
    if r['record_type']=='universe':continue
    if r.get('exchange_ts_ms') is None:unknownexchange+=1
    ticker=r['ticker'];tickers.add(ticker);series[r.get('series_ticker')]+=1
    key=(r.get('shard_id'),r.get('sid'));seq=r.get('seq')
    if key in subscriptions and seq!=subscriptions[key]+1:gaps.append({'record':n,'subscription':key,'prior':subscriptions[key],'current':seq})
    subscriptions[key]=seq
    state=(*key,ticker)
    if r['record_type']=='snapshot':snapshots.add(state)
    elif state not in snapshots:uninitialized+=1
    b=r.get('book',{})
    if not b.get('yes',{}).get('bid_levels') or not b.get('no',{}).get('bid_levels'):missing['one_or_both_sides_empty']+=1
    else:valid+=1
out={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'records':n,'record_types':dict(types),'unique_tickers':len(tickers),'series_counts':dict(series),'first_received_ns':start,'last_received_ns':end,'duration_seconds':(end-start)/1e9,'subscription_count':len(subscriptions),'sequence_gap_or_reorder_count':len(gaps),'first_20_sequence_defects':gaps[:20],'uninitialized_delta_records':uninitialized,'rows_with_both_bid_sides':valid,'empty_book_counts':dict(missing),'missing_exchange_timestamps':unknownexchange,'trading_authority':False}
with (root/'audit-first-file.json').open('x') as f:json.dump(out,f,indent=2)
print(json.dumps({k:v for k,v in out.items() if k not in ('series_counts','first_20_sequence_defects')}))
