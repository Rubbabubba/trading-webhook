"""Fetch one public source file with a fixed byte cap and known hash."""
import hashlib,json,urllib.request
from datetime import datetime,timezone
from pathlib import Path

root=Path(__file__).parent
spec=json.loads((root/'registration.json').read_text())
cache=Path('C:/Users/matth/OneDrive/Documents/ChatGPT/AI Agent Ecosystem/research/receipt-depth-cache-20261015')
cache.mkdir(exist_ok=True)
path=cache/Path(spec['first_file']).name
url='https://huggingface.co/datasets/lerchen3/kalshi-orderbook-alpha/resolve/main/'+spec['first_file']
if not path.exists():
    with urllib.request.urlopen(url,timeout=30) as response,path.open('xb') as out:
        count=0
        while b:=response.read(1024*1024):
            count+=len(b)
            if count>40000000:raise ValueError('source_size_cap')
            out.write(b)
actual=hashlib.sha256(path.read_bytes()).hexdigest()
if actual!=spec['expected_first_file_sha256']:raise ValueError('source_hash_mismatch')
receipt={'url':url,'captured_at':datetime.now(timezone.utc).isoformat(),'path':str(path),'bytes':path.stat().st_size,'sha256':actual,'expected_sha256_verified':True,'authenticated':False}
with (root/'acquisition.json').open('x') as out:json.dump(receipt,out,indent=2)
print(json.dumps(receipt))
