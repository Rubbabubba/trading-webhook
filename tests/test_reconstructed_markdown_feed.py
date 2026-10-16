import pytest
from research.reconstructed_markdown_20261016.feed import Feed
from tests.test_external_depth_integrity import row

def test_zero_display_truncation_uses_raw_ladder_without_phantom_depth():
    f=Feed();r=row()
    r['snapshot_yes_bids']=[[90,.1],[89,.1],[88,1000],[87,15]];r['snapshot_no_bids']=[[9,2750]]
    r['book']=dict(yes=dict(bid_levels=[[90,.1],[89,.1],[88,1000]],ask_levels=[[91,2750]]),no=dict(bid_levels=[[9,2750]],ask_levels=[[10,.1],[11,.1],[12,1000]]))
    f.ingest(r)
    r.update(record_type='delta',seq=2,received_ts_ns=101,delta_price_cents=88,delta_qty=-1000,level_qty_before=1000,level_qty_after=0)
    r['book']['yes']['bid_levels']=[[90,.1],[89,.1],[88,0]];r['book']['no']['ask_levels']=[[10,.1],[11,.1],[12,0]]
    q=f.ingest(r)
    assert q['quote']['yb']==87 and q['quote']['ya']==91
    assert f.counts['zero_truncations_rebuilt_from_raw']==1

def test_positive_disagreement_invalidates_ticker_until_new_snapshot():
    f=Feed();f.ingest(row());r=row(2,'delta',t=101);r['book']['yes']['bid_levels']=[[51,2]]
    assert f.ingest(r) is None
    assert f.ingest(row(3,'delta',t=102)) is None
    assert f.ingest(row(4,t=103)) is not None

def test_cross_channel_fence_never_moves_availability_backwards():
    f=Feed();f.ingest(row(t=100));r=f.ingest(row(shard=2,t=99))
    assert r['available_at']==pytest.approx(100/1e9)
    assert r['receipt_at']==pytest.approx(99/1e9)
    assert f.ingest(row(2,'delta',shard=2,t=98)) is None
    assert not f.valid((2,1,0))
    assert f.ingest(row(1,shard=2,t=102)) is not None

def test_epoch_key_changes_and_prior_ticker_is_not_reused():
    f=Feed();q=f.ingest(row());f.ingest(row(2,ticker='B',t=101));r=f.ingest(row(1,ticker='B',t=102))
    assert not f.valid(q['key']) and r['key']==(1,1,1)
    assert f.ingest(row(2,'delta',ticker='A',t=103)) is None

def test_replay_decision_precedes_later_serialized_quote(tmp_path,monkeypatch):
    import gzip,json,hashlib
    from copy import deepcopy
    from research.reconstructed_markdown_20261016 import replay
    monkeypatch.setattr(replay,'ROOT',tmp_path)
    (tmp_path/'registration.json').write_text(json.dumps(dict(warmup_files=6,limits=dict(maximum_records=100))))
    initial=row(t=int(1000*1e9));initial['snapshot_no_bids']=[[43,3]]
    initial['book']['yes']['ask_levels']=[[57,3]];initial['book']['no']['bid_levels']=[[43,3]]
    a=deepcopy(initial);a.update(record_type='delta',seq=2,received_ts_ns=int(1010*1e9),delta_side='no',delta_price_cents=47,delta_qty=3,level_qty_before=0,level_qty_after=3)
    a['book']['no']['bid_levels']=[[47,3],[43,3]];a['book']['yes']['ask_levels']=[[53,3],[57,3]]
    b=deepcopy(a);b.update(seq=3,received_ts_ns=int(1010.1*1e9),delta_price_cents=48)
    b['book']['no']['bid_levels']=[[48,3],[47,3],[43,3]];b['book']['yes']['ask_levels']=[[52,3],[53,3],[57,3]]
    c=deepcopy(b);c.update(seq=4,received_ts_ns=int(1011*1e9),delta_side='yes',delta_price_cents=50,delta_qty=0,level_qty_before=2,level_qty_after=2)
    later=deepcopy(c);later.update(seq=5,received_ts_ns=int(1020*1e9))
    records=[[initial],[],[],[],[],[],[a,b,c],[later]];receipts=[]
    for i,rows in enumerate(records):
        p=tmp_path/(str(i)+'.gz')
        with gzip.open(p,'wt') as f:
            for r in rows:f.write(json.dumps(r)+'\n')
        receipts.append(dict(path=str(p),sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
    (tmp_path/'acquisition.json').write_text(json.dumps(dict(files=receipts)))
    replay.run();r=json.loads((tmp_path/'preliminary.json').read_text());s=r['preliminary_signals'][0]
    assert r['complete_registered_window'] and len(r['preliminary_signals'])==1
    assert s['decision_at']==1010 and s['decision_quote']['ya']==53
    assert s['entry_at']==1011 and s['entry_price']==52
    assert r['actual_orders']==r['actual_fills']==0
