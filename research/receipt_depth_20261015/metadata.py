"""Join preliminary signals to official public market identities, no prices selected."""
import json,urllib.request
from urllib.parse import urlencode
from datetime import datetime,timezone
from pathlib import Path
root=Path(__file__).parent
pre=json.loads((root/'preliminary-v2-invalid-book-corrected.json').read_text())
tickers=sorted({r['ticker'] for r in pre['preliminary_signals']})
assert len(tickers)<=1000
folder=root/'official-metadata';folder.mkdir(exist_ok=True)
for i in range(0,len(tickers),25):
    subset=tickers[i:i+25]
    path=folder/(str(i//25)+'.json')
    url='https://external-api.kalshi.com/trade-api/v2/historical/markets?'+urlencode({'tickers':','.join(subset),'limit':1000})
    if not path.exists():
        with urllib.request.urlopen(url,timeout=15) as response:b=response.read(3000000);payload=json.loads(b)
        with path.open('x') as f:json.dump({'url':url,'captured_at':datetime.now(timezone.utc).isoformat(),'status':200,'requested_tickers':subset,'payload':payload},f,indent=2)
    r=json.loads(path.read_text())
    assert r['url']==url and not r['payload'].get('cursor')
    print(json.dumps({'batch':i//25,'requested':len(subset),'returned':len(r['payload']['markets'])}))
