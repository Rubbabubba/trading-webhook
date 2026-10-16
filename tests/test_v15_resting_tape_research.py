import pytest
from research.v15_resting_tape_20261016.model import classify,exit_observation,summarize
from research.v15_resting_tape_20261016.tape import Tape
from datetime import datetime,timezone


A=dict(ticker='T',activation_at=1001,cancel_request_at=1061,cancel_ack_at=1062)
S=dict(outcome='yes',price_cents=42)
V=dict(yes_bid=40,yes_ask=50,yes_bid_size=10,no_bid_size=4)


def trade(at=1010,price='0.40',**kw):
    return dict(trade_id='synthetic',ticker='T',at=at,count='1.00',
        yes_price=price,no_price=str(1-float(price)),taker_outcome_side='no')|kw


def test_strict_opposing_trade_through_is_only_conditional():
    p=classify(A,S,V,[trade()],complete=True)
    assert p['state']=='conditional_trade_through' and p['entry_at']==1010
    assert p['modeled_entry_price']==42 and p['modeled_entry_cost']==45
    assert p['actual_fills']==0 and p['owned_fill'] is False
    assert p['displayed_ahead_size']==0 and p['queue_independently_verified'] is False


@pytest.mark.parametrize('at',[1000.5,1001,1061,1062.5])
def test_activation_and_cancellation_clock_bands_remain_uncertain(at):
    assert classify(A,S,V,[trade(at)],complete=True)['state']=='uncertain_possible_fill'


def test_price_touch_partial_and_prior_ambiguity_never_skipped():
    for row in [trade(price='0.42'),trade(count='.5')]:
        p=classify(A,S,V,[row,trade(1020,trade_id='later')],complete=True)
        assert p['state']=='uncertain_possible_fill' and p['public_trade_ids']==['synthetic']


def test_same_timestamp_ambiguous_group_is_not_ordered_into_a_favorable_fill():
    p=classify(A,S,V,[trade(),trade(price='0.42',trade_id='touch')],complete=True)
    assert p['state']=='uncertain_possible_fill'


def test_passivity_ticker_tape_completeness_and_direction_guards():
    assert classify(A,S,None,[],complete=True)['state']=='activation_unobserved'
    assert classify(A,S,{**V,'yes_bid':43},[],complete=True)['state']=='activation_quote_rejected'
    assert classify(A,S,V,[],complete=False)['state']=='tape_incomplete_uncertain_possible_fill'
    assert classify(A,S,V,[trade(taker_outcome_side='yes')],complete=True)['state']=='no_qualifying_public_trade'
    with pytest.raises(ValueError,match='ticker'):
        classify(A,S,V,[trade(ticker='OTHER')],complete=True)


def test_no_outcome_complements_are_applied_twice():
    s=dict(outcome='no',price_cents=52)
    p=classify(A,s,V,[trade(no_price='.50',yes_price='.50',taker_outcome_side='yes')],complete=True)
    assert p['state']=='conditional_trade_through' and p['modeled_entry_cost']==55


def test_benchmark_best_bid_records_visible_ahead_queue_without_claiming_it():
    p=classify(A,{**S,'price_cents':40},V,[trade(price='.39')],complete=True)
    assert p['state']=='conditional_trade_through' and p['displayed_ahead_size']==10


def test_independent_horizon_costs_and_ambiguous_full_principal_stress():
    p=classify(A,S,V,[trade()],complete=True)
    p['exits']={str(h):exit_observation(p,h,{**V,'yes_bid':50},1010+h) for h in (5,30,300)}
    u=classify(A,S,V,[trade(1062)],complete=True)
    r=summarize([p,u])
    assert r['horizons']['5']['scenarios']['0']['complete_net_cents']==2
    assert r['horizons']['5']['scenarios']['2']['complete_net_cents']==-2
    assert r['horizons']['5']['scenarios']['0']['combined_cents']==-43
    assert r['complete_all_horizons']==1 and r['uncertain_possible_fills']==1
    assert summarize([classify(A,S,V,[],complete=True)])['horizons']['5']['scenarios']['0']['combined_cents'] is None


def test_missing_or_late_exit_cannot_be_replaced_by_favorable_quote():
    p=classify(A,S,V,[trade()],complete=True)
    assert exit_observation(p,5,None,1015)['state']=='unresolved_exit'
    assert exit_observation(p,5,V,1020.001)['state']=='unresolved_exit'


def raw_trade(identity='x',price='.40'):
    from decimal import Decimal
    return dict(trade_id=identity,ticker='T',count_fp='1.00',yes_price_dollars=price,
        no_price_dollars=str(1-Decimal(price)),taker_outcome_side='no',
        taker_book_side='ask',taker_side='no',is_block_trade=False,
        created_time=datetime.fromtimestamp(1010,timezone.utc).isoformat())


def test_tape_pagination_dedup_and_exact_cache_reuse(tmp_path):
    calls=[]
    def reader(params):
        calls.append(params)
        return dict(trades=[raw_trade()]+([raw_trade('y')] if params.get('cursor') else []),
                    cursor='' if params.get('cursor') else 'next')
    source=Tape(tmp_path,reader);result=source.fetch(A)
    assert result['complete'] and len(result['trades'])==2 and len(calls)==2
    assert calls[0]['min_ts']==1000 and calls[0]['max_ts']==1063
    assert calls[1]['ticker']==calls[0]['ticker'] and calls[1]['is_block_trade']=='false'
    def forbidden(params):raise AssertionError('cached request repeated')
    again=Tape(tmp_path,forbidden)
    assert again.fetch(A)['complete'] and again.requests==0


def test_conflicting_trade_identity_blocks_partial_tape(tmp_path):
    def reader(params):
        return dict(trades=[raw_trade(price='.30' if params.get('cursor') else '.40')],
                    cursor='' if params.get('cursor') else 'next')
    r=Tape(tmp_path,reader).fetch(A)
    assert not r['complete'] and 'conflicting_trade_identity' in r['error']
    assert classify(A,S,V,r['trades'],complete=r['complete'])['state']=='tape_incomplete_uncertain_possible_fill'


def test_page_cap_and_cursor_loop_do_not_mean_no_trades(tmp_path):
    calls=[]
    def reader(params):
        calls.append(params);return dict(trades=[],cursor='c'+str(len(calls)))
    r=Tape(tmp_path/'cap',reader).fetch(A)
    assert len(calls)==3 and not r['complete'] and r['error']=='page_capacity_incomplete'
    r=Tape(tmp_path/'cycle',lambda p:dict(trades=[],cursor='same')).fetch(A)
    assert not r['complete'] and 'pagination_cursor_repeated' in r['error']


def test_cache_tampering_blocks_without_refetch(tmp_path):
    source=Tape(tmp_path,lambda p:dict(trades=[],cursor=''))
    assert source.fetch(A)['complete']
    next(tmp_path.glob('*.body')).write_text('tampered')
    def forbidden(params):raise AssertionError('no blind refetch')
    r=Tape(tmp_path,forbidden).fetch(A)
    assert not r['complete'] and 'hash_changed' in r['error']
