import hashlib,json,urllib.request
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).parent
spec=json.loads((ROOT/'registration.json').read_text())
cache=ROOT.parents[2]/'research/receipt-depth-cache-20261015'
receipts=[]
for row in spec['files']:
    path=cache/Path(row['path']).name
    url='https://huggingface.co/datasets/lerchen3/kalshi-orderbook-alpha/resolve/main/'+row['path']
    if not path.exists():
        temp=path.with_suffix('.partial')
        with urllib.request.urlopen(url,timeout=30) as response,temp.open('xb') as out:
            count=0
            while b:=response.read(1024*1024):
                count+=len(b)
                if count>row['size']:raise ValueError('source_size_cap')
                out.write(b)
        if hashlib.sha256(temp.read_bytes()).hexdigest()!=row['lfs']['oid']:raise ValueError('source_hash_mismatch')
        temp.rename(path)
    actual=hashlib.sha256(path.read_bytes()).hexdigest()
    if actual!=row['lfs']['oid'] or path.stat().st_size!=row['size']:raise ValueError('source_integrity')
    receipts.append({'url':url,'path':str(path),'sha256':actual,'bytes':path.stat().st_size})
    print(json.dumps({'file':len(receipts),'bytes':path.stat().st_size,'hash_verified':True}),flush=True)
with (ROOT/'acquisition.json').open('x') as f:json.dump({'captured_at':datetime.now(timezone.utc).isoformat(),'files':receipts},f,indent=2)
