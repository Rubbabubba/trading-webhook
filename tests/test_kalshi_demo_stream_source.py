import copy
from decimal import Decimal
import pytest
from opportunity_lab.kalshi_demo_stream_source import Books,SourceFault
MID="01a1190b-2c97-79db-8080-d1c508c39855"
def ack(channel="orderbook_delta",sid=1):return dict(type="subscribed",sending_ts_ms=1000000,msg=dict(channel=channel,sid=sid))
def snap(seq=1,ticker="A"):
 return dict(type="orderbook_snapshot",sid=1,seq=seq,sending_ts_ms=1000000,msg=dict(market_ticker=ticker,market_id=MID,yes_dollars_fp=[[".40","3.50"]],no_dollars_fp=[[".50","4.25"]]))
def delta(seq=2,size="-1.25"):
 return dict(type="orderbook_delta",sid=1,seq=seq,sending_ts_ms=1000000,msg=dict(market_ticker="A",market_id=MID,side="yes",price_dollars=".40",delta_fp=size))
def seed():
 b=Books(["A","B"]);b.apply(ack(),1000);b.apply(snap(),1000);return b

def test_fractional_depth_and_shared_sid_across_tickers():
 b=seed();b.apply(snap(2,"B"),1000);b.apply(delta(3),1000);assert b.books["A"]["yes"][Decimal(".4")]==Decimal("2.25")
 assert b.books["B"]["yes"][Decimal(".4")]==Decimal("3.50")

def test_sequence_gap_poisoning_cannot_be_silently_recovered():
 b=seed()
 with pytest.raises(SourceFault,match="sequence_gap"):b.apply(delta(3),1000)
 assert not b.books
 with pytest.raises(SourceFault,match="connection_invalid"):b.apply(snap(4),1000)

@pytest.mark.parametrize("change",["negative","identity","missing","outside","clock"])
def test_bad_source_paths_fail_closed(change):
 b=seed();d=delta()
 if change=="negative":d["msg"]["delta_fp"]="-10"
 if change=="identity":d["msg"]["market_id"]="11a1190b-2c97-79db-8080-d1c508c39855"
 if change=="missing":d["msg"]["market_ticker"]="B"
 if change=="outside":d["msg"]["market_ticker"]="C"
 with pytest.raises(SourceFault):b.apply(d,999 if change=="clock" else 1000)
 assert b.poisoned and not b.books

def test_duplicate_level_and_non_text_decimal_rejected():
 for value in [[['.4','1'],['.4','2']],[[.4,'1']]]:
  b=Books(['A']);b.apply(ack(),1000);p=snap();p['msg']['yes_dollars_fp']=value
  with pytest.raises(SourceFault):b.apply(p,1000)

def test_trade_receipt_is_never_our_fill_and_sequences_are_separate():
 b=seed();b.apply(ack('trade',2),1000)
 p=dict(type='trade',sid=2,seq=1,sending_ts_ms=1000100,msg=dict(market_ticker='A',yes_price_dollars='.4',no_price_dollars='.6',count_fp='2.25',taker_side='yes',ts_ms=1000050))
 r=b.apply(p,1000.2);assert r['book_available'] and r['actual_fill_proven'] is False
 assert b.sequence=={1:1,2:1}
 p['seq']=2;p['msg']['taker_outcome_side']='no'
 with pytest.raises(SourceFault,match='conflicting_trade_side'):b.apply(p,1000.3)

def test_book_crossing_is_explicitly_unusable_not_a_trading_signal():
 b=Books(['A']);b.apply(ack(),1000);p=snap();p['msg']['no_dollars_fp']=[['.70','4']]
 assert b.apply(p,1000)['usable'] is False
