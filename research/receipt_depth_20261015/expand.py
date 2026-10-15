import hashlib,json,urllib.request
from datetime import datetime,timezone
from pathlib import Path
root=Path(__file__).parent
catalog=json.loads((root/'source-catalog.json').read_text())['files']
files=sorted((r for r in catalog if r['type']=='file'),key=lambda r:r['path'])[:3]
assert sum(r['size'] for r in files)<=180000000
cache=Path(json.loads((root/'acquisition.json').read_text())['path']).parent
receipts=[]
for r in files:
    p=cache/Path(r['path']).name
    url='https://huggingface.co/datasets/lerchen3/kalshi-orderbook-alpha/resolve/main/'+r['path']
    if not p.exists():
        with urllib.request.urlopen(url,timeout=30) as response,p.open('xb') as out:
            count=0
            while b:=response.read(1024*1024):
                count+=len(b)
                if count>r['size']:raise ValueError('size_mismatch')
                out.write(b)
    actual=hashlib.sha256(p.read_bytes()).hexdigest()
    if actual!=r['lfs']['oid'] or p.stat().st_size!=r['size']:raise ValueError('source_hash_size_mismatch')
    receipts.append({'url':url,'path':str(p),'sha256':actual,'bytes':r['size'],'hash_verified':True})
    print(json.dumps({'file':p.name,'bytes':r['size'],'hash_verified':True}),flush=True)
with (root/'three-file-acquisition.json').open('x') as f:json.dump({'captured_at':datetime.now(timezone.utc).isoformat(),'files':receipts},f,indent=2)
