import pytest
from opportunity_lab.kalshi_demo_stream_source import Books, SourceFault
from opportunity_lab.kalshi_demo_stream_archive import Archive
from research.stream_integration_20261018.audit import audit

ACK=dict(type='subscribed',sending_ts_ms=1000000,msg=dict(channel='orderbook_delta',sid=1))

def snapshot(ticker,seq):
    return dict(type='orderbook_snapshot',sid=1,seq=seq,sending_ts_ms=1000000,msg=dict(market_ticker=ticker,
        market_id='01a1190b-2c97-79db-8080-d1c508c39855',yes_dollars_fp=[['.4','2']],no_dollars_fp=[['.5','3']]))

def unavailable(ticker,seq):
    return dict(type='orderbook_snapshot',sid=1,seq=seq,sending_ts_ms=1000000,msg=dict(market_ticker=ticker,market_id=''))

def test_strict_original_parser_still_rejects_unavailable_identity():
    b=Books(['A']);b.apply(ACK,1000)
    with pytest.raises(SourceFault,match='invalid_market_identity'):b.apply(unavailable('A',1),1000)

def test_unavailable_invalidates_only_that_symbol_and_never_invents_a_book():
    b=Books(['A','B'],allow_unavailable=True);b.apply(ACK,1000)
    b.apply(snapshot('A',1),1000);b.apply(snapshot('B',2),1000)
    result=b.apply(unavailable('A',3),1000)
    assert result['kind']=='market_unavailable' and result['usable'] is False
    assert result['actual_fill_proven'] is False and 'A' not in b.books and 'B' in b.books
    b.apply(snapshot('B',4),1000)
    assert not b.poisoned

@pytest.mark.parametrize('change',[{'market_id':'bad-id'}, {'yes_dollars_fp':[]}, {'account_id':'private'}])
def test_other_malformed_snapshots_still_poison_connection(change):
    b=Books(['A'],allow_unavailable=True);b.apply(ACK,1000)
    p=unavailable('A',1);p['msg'].update(change)
    with pytest.raises(SourceFault):b.apply(p,1000)
    assert b.poisoned

def test_gap_in_unavailable_control_still_rejected():
    b=Books(['A'],allow_unavailable=True);b.apply(ACK,1000);b.apply(snapshot('A',1),1000)
    with pytest.raises(SourceFault,match='gap'):b.apply(unavailable('A',3),1000)

def test_v2_archive_and_independent_audit_preserve_raw_unavailable_record(tmp_path):
    path=tmp_path/'v2.sqlite3'
    a=Archive(path,free_bytes=lambda:10**9,protocol='demo_stream_archive_availability_v2_20261018',
              decoder_factory=lambda ts:Books(ts,allow_unavailable=True))
    a.begin(['A','B'],1000);a.append(ACK,1000);a.append(snapshot('A',1),1000)
    a.append(unavailable('B',2),1000);a.append(snapshot('A',3),1000)
    result=audit(path)
    assert result['unavailable_source_records']==1 and result['reconstructed_states']==2
    assert result['records']==4 and result['owned_fills']==0
    a.close()


def test_v3_missing_ladders_with_valid_id_are_unavailable_without_empty_price_substitution():
    b=Books(['A','B'],allow_unavailable=True,allow_missing_book=True);b.apply(ACK,1000)
    b.apply(snapshot('A',1),1000);b.apply(snapshot('B',2),1000)
    p=snapshot('A',3);del p['msg']['yes_dollars_fp'];del p['msg']['no_dollars_fp']
    assert b.apply(p,1000)['kind']=='market_unavailable'
    assert 'A' not in b.books and 'B' in b.books and not b.poisoned


def test_v2_semantics_still_reject_valid_id_without_ladders():
    b=Books(['A'],allow_unavailable=True);b.apply(ACK,1000)
    p=snapshot('A',1);del p['msg']['yes_dollars_fp'];del p['msg']['no_dollars_fp']
    with pytest.raises(SourceFault):b.apply(p,1000)
