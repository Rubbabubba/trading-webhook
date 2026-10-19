"""Read-only verification of prospective v2 source and complete quote outcomes."""
import gzip,hashlib,json,sqlite3,time,zlib
from fractions import Fraction
from pathlib import Path
root=Path('/var/data/kalshi-demo-v9')
def ro(name):return sqlite3.connect('file:'+str(root/name)+'?mode=ro',uri=True)
db=ro('stream-shadow-comparison-v2.sqlite3');source=ro('stream-source-v3.sqlite3')
inputs=0
for epoch,at,ticker,digest,blob in db.execute('SELECT epoch,at,ticker,input_sha,input_json FROM comparison_inputs'):
    raw=zlib.decompress(blob);assert hashlib.sha256(raw).hexdigest()==digest
    item=json.loads(raw);assert item['epoch']==epoch and item['frame']['received_at']==at
    found=source.execute('SELECT connection,received,data FROM messages WHERE raw_sha=?',(item['source_message_sha256'],)).fetchall()
    assert any(c==epoch and received==at and hashlib.sha256(gzip.decompress(data)).hexdigest()==item['source_message_sha256'] for c,received,data in found)
    inputs+=1
attempts=db.execute('SELECT id,signal_id,horizon,started_at FROM outcome_attempts ORDER BY id').fetchall()
assert len(attempts)<=900
assert all(b[3]-a[3]>=2 for a,b in zip(attempts,attempts[1:]))
assert not db.execute('SELECT signal_id,horizon,count(*) FROM outcome_attempts GROUP BY signal_id,horizon HAVING count(*)>3').fetchall()
verified=[]
for sid,ticker,event,signal_at,detail_raw in db.execute('SELECT id,ticker,event,at,detail FROM high_edge_v15_signals'):
    detail=json.loads(detail_raw);horizons=[]
    for h,at,result_raw in db.execute('SELECT horizon,at,detail FROM high_edge_v15_marks WHERE signal_id=? ORDER BY horizon',(sid,)):
        matches=db.execute('SELECT r.raw_sha,r.raw_z,r.result,a.started_at FROM outcome_receipts r JOIN outcome_attempts a ON a.id=r.attempt_id WHERE a.signal_id=? AND a.horizon=? AND r.received_at=?',(sid,h,at)).fetchall()
        assert len(matches)==1
        digest,blob,receipt_raw,attempt_at=matches[0];raw=zlib.decompress(blob)
        assert hashlib.sha256(raw).hexdigest()==digest
        receipt=json.loads(receipt_raw);assert receipt['result'].startswith('valid_quote')
        assert signal_at+h<=at<=signal_at+h+5
        assert attempt_at<=receipt['request_started_at']<=at and at-receipt['request_started_at']<=2
        assert 0<=receipt['http_age_seconds']<=2
        book=json.loads(raw)['orderbook_fp'];ys=[(Fraction(p),Fraction(q)) for p,q in book['yes_dollars']];ns=[(Fraction(p),Fraction(q)) for p,q in book['no_dollars']]
        assert ys and ns and all(0<p<1 and q>0 for p,q in ys+ns)
        bid=max(p for p,q in ys);ask=1-max(p for p,q in ns);assert bid<ask;mid=(bid+ask)/2
        result=json.loads(result_raw)
        for name,signal in detail.items():
            if signal:
                outcome=mid if signal['outcome']=='yes' else 1-mid
                gross=outcome*100-Fraction(str(signal['price_cents']))
                assert abs(Fraction(result[name]['gross_cents'])-gross)<=Fraction(1,1000000)
                assert abs(Fraction(result[name]['stressed_cents'])-(gross-2))<=Fraction(1,1000000)
            else:assert result[name]['no_trade'] is True
        horizons.append({'horizon':h,'lateness_seconds':at-signal_at-h,'response_sha256':digest,'results':result})
    verified.append({'id':sid,'ticker':ticker,'event':event,'at':signal_at,'horizons':horizons,'complete':len(horizons)==3})
old=root/'stream-shadow-comparison-v1.sqlite3'
old_sha=hashlib.sha256(old.read_bytes()).hexdigest()
assert old_sha=='1c0d1f1654930d6f1094d7cf708a36796dbcad9cc7ff4c1b9df9fd130c935bd3'
report={'at':time.time(),'independent_fraction_audit_passed':True,'source_linked_inputs':inputs,'attempts':len(attempts),'verified_signals':verified,'complete_signals':sum(v['complete'] for v in verified),'old_ledger_sha256':old_sha,'old_ledger_bytes':old.stat().st_size,'new_ledger_bytes':(root/'stream-shadow-comparison-v2.sqlite3').stat().st_size,'actual_fills':0,'profitability_proven':False}
print('V2_AUDIT='+json.dumps(report,sort_keys=True))
