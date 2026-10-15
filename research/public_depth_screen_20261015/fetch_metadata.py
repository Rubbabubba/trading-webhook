"""Bounded public GET-only metadata recovery; immutable cache, no credentials."""
import json
import urllib.error
import urllib.request
import sys
from datetime import datetime, timezone
from pathlib import Path

source=Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).parents[1]/'public-kalshi-benchmark-20261015'/'episodes'
dest=Path(__file__).with_name('official-metadata')
dest.mkdir(exist_ok=True)
tickers=sorted({t for p in source.glob('*/metadata.json') for t in json.loads(p.read_text())['tickers']})
assert len(tickers)<=33
counts={}
for ticker in tickers:
    out=dest/(ticker+'.json')
    if out.exists():
        counts['cached']=counts.get('cached',0)+1
        continue
    url='https://external-api.kalshi.com/trade-api/v2/historical/markets/'+ticker
    receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'method':'GET','authenticated':False}
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'bounded-offline-research/1'}),timeout=12) as response:
            receipt.update(status=response.status,payload=json.load(response))
    except urllib.error.HTTPError as e:
        receipt.update(status=e.code,error=e.read(4096).decode('utf-8',errors='replace'))
    except Exception as e:
        receipt.update(status=None,error=type(e).__name__+': '+str(e))
    with out.open('x') as f: json.dump(receipt,f,indent=2)
    status=str(receipt['status'])
    counts[status]=counts.get(status,0)+1
    if receipt['status'] in (401,403,429) or receipt['status'] is None:
        counts['stopped_on_access_rate_or_transport_error']=1
        break
print(json.dumps(counts))
