from research.ask_markdown_20261016.replay import signal,enter
from research.ask_markdown_20261016.review import settlement

def test_bid_holds_while_ask_drops_and_both_sides_are_symmetric():
    old=dict(yb=50,ya=57,nb=43,na=50)
    q=dict(yb=50,ya=53,nb=47,na=50)
    assert signal(q,old)=='yes'
    assert signal(dict(yb=47,ya=50,nb=50,na=53),dict(yb=43,ya=50,nb=50,na=57))=='no'
    assert signal(dict(yb=49,ya=52,nb=48,na=51),old) is None
    assert signal(dict(yb=50,ya=54,nb=46,na=50),old) is None

def attempt():return dict(subscription=[1,2],decision_at=10,decision_ask=53,side='yes',state='awaiting_entry')

def test_delayed_entry_does_not_chase_or_repair_late_quotes():
    q=dict(ya=53,na=50)
    s=attempt();assert not enter(s,(1,2),10.9,q)
    assert enter(s,(1,2),11,q) and s['entry_price']==53
    s=attempt();enter(s,(1,2),12.01,q);assert s['state']=='entry_unfilled_late'
    s=attempt();enter(s,(1,2),11,dict(ya=54));assert s['state']=='entry_price_guard_rejected'
    s=attempt();enter(s,(1,3),11,q);assert s['state']=='subscription_changed_unfilled'

def test_settlement_requires_numeric_agreement_and_final_state():
    s=dict(side='no');m=dict(status='settled',result='no',settlement_value_dollars='0.0000')
    assert settlement(s,m)==100
    assert settlement(dict(side='yes'),m)==0
    assert settlement(s,dict(m,settlement_value_dollars='1.0000')) is None
    assert settlement(s,dict(m,settlement_value_dollars=None)) is None
    assert settlement(s,dict(m,status='closed')) is None

def test_source_clock_fault_preserves_prefix_and_stops_without_sorting(tmp_path,monkeypatch):
    import gzip,json,hashlib
    from research.ask_markdown_20261016 import replay
    monkeypatch.setattr(replay,'ROOT',tmp_path)
    (tmp_path/'registration.json').write_text(json.dumps({'warmup_files':4}))
    def row(t,seq,nb=47,kind='delta'):
        return dict(received_ts_ns=int(t*1e9),record_type=kind,ticker='TEST-A',shard_id=1,sid=1,seq=seq,book=dict(yes=dict(bid_levels=[[50,5]]),no=dict(bid_levels=[[nb,5]])))
    records=[[row(1000,1,43,'snapshot')],[],[],[],[row(1010,2),row(1010.1,3),row(1011,4),row(1010.9,5)],[]]
    receipts=[]
    for i,rows in enumerate(records):
        p=tmp_path/(str(i)+'.gz')
        with gzip.open(p,'wt') as f:
            for r in rows:f.write(json.dumps(r)+'\n')
        receipts.append(dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
    (tmp_path/'acquisition.json').write_text(json.dumps(dict(files=receipts)))
    replay.run();r=json.loads((tmp_path/'preliminary.json').read_text())
    assert r['complete_registered_window'] is False
    assert r['source_fault']['reason']=='time_reorder'
    assert len(r['preliminary_signals'])==1
    assert r['preliminary_signals'][0]['entry_at']==1011
    assert r['actual_orders']==r['actual_fills']==0
