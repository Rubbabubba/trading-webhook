from research.depth_integrity_20261016.audit import Audit

def row(seq=1,kind='snapshot',ticker='A',t=100,shard=1):
    return dict(record_type=kind,ticker=ticker,shard_id=shard,sid=1,seq=seq,received_ts_ns=t,snapshot_yes_bids=[[50,2]],snapshot_no_bids=[[40,3]],book=dict(yes=dict(bid_levels=[[50,2]],ask_levels=[[60,3]]),no=dict(bid_levels=[[40,3]],ask_levels=[[50,2]])),delta_side='yes',delta_price_cents=50,delta_qty=0,level_qty_before=2,level_qty_after=2)

def test_full_snapshot_delta_and_complement_are_independent():
    a=Audit();a.ingest(row());a.ingest(row(2,'delta',t=101))
    assert a.counts['fully_reconstructed_rows']==2
    bad=row(3,'delta',t=102);bad['book']['yes']['ask_levels']=[[61,3]];a.ingest(bad)
    assert a.counts['derived_top3_disagreement']==1
    a.ingest(row(4,'delta',t=103));assert a.counts['uninitialized_ticker_rows']==1

def test_epoch_restart_never_reuses_other_ticker_snapshot():
    a=Audit();a.ingest(row());a.ingest(row(2,ticker='B',t=101));a.ingest(row(1,ticker='B',t=102))
    assert a.counts['inferred_epoch_restarts']==1
    a.ingest(row(2,'delta',ticker='A',t=103));assert a.counts['uninitialized_ticker_rows']==1
    a.ingest(row(3,ticker='A',t=104));a.ingest(row(4,'delta',ticker='A',t=105))
    assert a.counts['fully_reconstructed_rows']==5

def test_gap_and_duplicate_do_not_self_repair():
    a=Audit();a.ingest(row());a.ingest(row(3,'delta',t=101));a.ingest(row(4,t=102))
    assert a.counts['sequence_discontinuity']==1
    assert a.counts['unverified_channel_rows']==2
    a.ingest(row(1,t=103));assert a.counts['inferred_epoch_restarts']==1
    a.ingest(row(1,t=104));assert a.counts['sequence_discontinuity']==2

def test_bad_raw_arithmetic_and_clock_regressions_remain_visible():
    a=Audit();a.ingest(row(t=100));bad=row(2,'delta',t=99);bad['delta_qty']=1;a.ingest(bad)
    assert a.counts['raw_delta_disagreement']==1
    assert a.counts['global_clock_regression']==1
    assert a.counts['within_channel_clock_regression']==1

def test_zero_level_diagnostic_does_not_accept_wrong_positive_prices():
    from decimal import Decimal
    from research.depth_integrity_20261016.diagnose import zero_truncation
    book=dict(yes={Decimal(50):Decimal(2),Decimal(49):Decimal(3)},no={Decimal(40):Decimal(3)})
    r=row();r['book']['yes']['bid_levels']=[[50,2],[49.5,0]]
    r['book']['no']['ask_levels']=[[50,2],[50.5,0]]
    assert zero_truncation(book,r)
    r['book']['yes']['bid_levels']=[[51,2],[49.5,0]]
    assert not zero_truncation(book,r)
