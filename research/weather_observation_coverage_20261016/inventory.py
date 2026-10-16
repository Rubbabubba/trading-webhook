"""Identity-only weather coverage audit of already inspected cached files.

No strategy price selection or PnL. Do not call inspected inputs a holdout.
"""
import gzip,hashlib,json
from pathlib import Path
from collections import Counter
from datetime import datetime,timezone

ROOT=Path(__file__).parent


def run():
    catalog=json.loads((ROOT.parent/'receipt_depth_20261015/source-catalog.json').read_text())['files'][:34]
    cache=ROOT.parents[2]/'research/receipt-depth-cache-20261015'
    registry=json.loads((ROOT.parents[1]/'configs/kalshi_weather_ensemble_v14_20260930/registration.json').read_text())['universe']['registered_series']
    tickers={};universe_tickers=set();counts=Counter();receipts=[]
    for i,row in enumerate(catalog,1):
        p=cache/Path(row['path']).name
        digest=hashlib.sha256(p.read_bytes()).hexdigest()
        if digest!=row['lfs']['oid']:raise ValueError('cached_source_hash_changed')
        for line in gzip.open(p,'rb'):
            counts['source_lines']+=1
            if b'KXHIGH' not in line and b'KXLOW' not in line:continue
            raw=json.loads(line)
            if raw['record_type']=='universe':
                for m in raw['markets']:
                    if m['ticker'].split('-',1)[0] in registry:universe_tickers.add(m['ticker'])
                continue
            ticker=raw['ticker']
            if ticker.split('-',1)[0] not in registry:continue
            at=raw['received_ts_ns']/1e9
            value=tickers.setdefault(ticker,dict(rows=0,first_at=at,last_at=at))
            value['rows']+=1;value['first_at']=min(value['first_at'],at);value['last_at']=max(value['last_at'],at)
            counts['weather_book_rows']+=1
        receipts.append(dict(file=i,path=str(p),bytes=p.stat().st_size,sha256=digest))
        print(json.dumps(dict(file_complete=i,weather_tickers=len(tickers),weather_rows=counts['weather_book_rows'])),flush=True)
    result=dict(schema='cached_weather_identity_coverage_v1',captured_at=datetime.now(timezone.utc).isoformat(),
        scope='Already inspected first34files; identities/timestamps only, no new strategy holdout',
        counts=dict(counts),weather_tickers=tickers,weather_universe_tickers=sorted(universe_tickers),
        registry_series=registry,files=receipts,actual_orders=0,actual_fills=0,profitability_proven=False)
    with (ROOT/'inventory.json').open('x') as f:json.dump(result,f,indent=2,allow_nan=False)
    print(json.dumps({k:v for k,v in result.items() if k not in ('files','registry_series','weather_tickers','weather_universe_tickers')}))


if __name__=='__main__':run()
