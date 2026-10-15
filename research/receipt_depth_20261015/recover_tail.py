import gzip,hashlib,json,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from replay import advance,quote
ROOT=Path(__file__).parent


def recover():
    receipt=json.loads((ROOT/'three-file-acquisition.json').read_text())
    catalog=json.loads((ROOT/'source-catalog.json').read_text())['files']
    fourth=sorted((r for r in catalog if r['type']=='file'),key=lambda r:r['path'])[3]
    assert fourth['size']<=75000000
    cache=Path(receipt['files'][0]['path']).parent
    p=cache/Path(fourth['path']).name
    url='https://huggingface.co/datasets/lerchen3/kalshi-orderbook-alpha/resolve/main/'+fourth['path']
    if not p.exists():
        with urllib.request.urlopen(url,timeout=30) as response,p.open('xb') as out:
            count=0
            while b:=response.read(1024*1024):
                count+=len(b)
                if count>fourth['size']:raise ValueError('source_size')
                out.write(b)
    if hashlib.sha256(p.read_bytes()).hexdigest()!=fourth['lfs']['oid']:raise ValueError('source_hash')
    pre=json.loads((ROOT/'preliminary-v2-invalid-book-corrected.json').read_text())
    pending={s['ticker']:s for s in pre['preliminary_signals'] if s['state']=='awaiting_exit'}
    seq={}
    for f in receipt['files']:
        if hashlib.sha256(Path(f['path']).read_bytes()).hexdigest()!=f['sha256']:raise ValueError('original_source_changed')
        for line in gzip.open(f['path'],'rt'):
            r=json.loads(line)
            if r['record_type']!='universe':seq[(r['shard_id'],r['sid'])]=r['seq']
    original_pending=len(pending);defects=[];processed=0;broken=set()
    for line in gzip.open(p,'rt'):
        r=json.loads(line);processed+=1
        if r['record_type']=='universe':continue
        key=(r['shard_id'],r['sid'])
        if key not in seq or r['seq']!=seq[key]+1:
            broken.add(key);defects.append({'key':key,'previous':seq.get(key),'current':r['seq']})
        seq[key]=r['seq']
        s=pending.get(r['ticker'])
        if not s:continue
        if key in broken:s['state']='tail_sequence_unresolved';pending.pop(r['ticker']);continue
        try:q=quote(r)
        except ValueError:q=None
        if q and advance(s,key,r['received_ts_ns']/1e9,q):pending.pop(r['ticker'])
        if not pending:break
    pre['tail_recovery']={'source':url,'sha256':fourth['lfs']['oid'],'bytes':fourth['size'],'captured_at':datetime.now(timezone.utc).isoformat(),'original_pending':original_pending,'remaining_pending':len(pending),'sequence_defects':defects,'processed_records':processed,'no_new_entries':True}
    with (ROOT/'preliminary-v3-tail-recovery.json').open('x') as f:json.dump(pre,f,indent=2)
    print(json.dumps(pre['tail_recovery']))


if __name__=='__main__':recover()
