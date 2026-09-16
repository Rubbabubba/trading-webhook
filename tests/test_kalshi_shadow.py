import pytest
from opportunity_lab.kalshi_shadow import Shadow

CONFIG=dict(strategy_id='test',initial_cash_cents=1000,take_profit_cents=5,
            stop_loss_cents=5,max_hold_seconds=60,latency_seconds=1,slippage_cents=1)


def frame(at,side='yes',bid='.39',ask='.40',size='10'):
    from decimal import Decimal
    own=[[bid,size]];other=[[str(1-Decimal(ask)),size]]
    yes,no=(own,other) if side=='yes' else (other,own)
    return dict(ticker='TEST',book_id=str(at),received_at=at,fee_coefficient='0.07',
                orderbook_fp=dict(yes_dollars=yes,no_dollars=no))


@pytest.mark.parametrize('side',['yes','no'])
def test_both_sides_take_profit_and_restart(tmp_path,side):
    path=tmp_path/'shadow.sqlite3';s=Shadow(path,CONFIG)
    assert s.step(frame(100,side),now=100,signal=dict(side=side,strategy_id='test'))['action']=='stage_entry'
    assert s.step(frame(101,side),now=101)['action']=='buy'
    assert s.state()['cash_cents']==957
    s.close();s=Shadow(path,CONFIG)
    assert s.step(frame(102,side,bid='.55',ask='.56'),now=102)['reason']=='take_profit'
    result=s.step(frame(103,side,bid='.54',ask='.55'),now=103)
    assert result['action']=='sell' and result['net_cents']==8
    assert s.state()['cash_cents']==1008 and s.state()['position'] is None
    s.close()


@pytest.mark.parametrize('reason,at,bid,invalid',[('stop_loss',102,'.25',False),
    ('time_limit',162,'.44',False),('signal_invalidated',102,'.39',True)])
def test_exit_reasons(tmp_path,reason,at,bid,invalid):
    s=Shadow(tmp_path/'s.sqlite3',CONFIG)
    s.step(frame(100),now=100,signal=dict(side='yes',strategy_id='test'));s.step(frame(101),now=101)
    f=frame(at,bid=bid,ask=str(float(bid)+.01));f['signal_valid']=not invalid
    assert s.step(f,now=at)['reason']==reason
    s.close()


def test_missing_liquidity_retains_position(tmp_path):
    s=Shadow(tmp_path/'s.sqlite3',CONFIG)
    s.step(frame(100),now=100,signal=dict(side='no',strategy_id='test'));s.step(frame(101,'no'),now=101)
    assert s.step(frame(102,'no',bid='.2',ask='.21',size='.5'),now=102)['action']=='wait'
    assert s.state()['position'] is not None
    s.close()


def test_stale_wrong_market_and_config_do_not_mutate(tmp_path):
    path=tmp_path/'s.sqlite3';s=Shadow(path,CONFIG)
    s.step(frame(100),now=100,signal=dict(side='yes',strategy_id='test'))
    before=s.state()
    with pytest.raises(ValueError):s.step(frame(101),now=110)
    f=frame(101);f['ticker']='OTHER'
    with pytest.raises(ValueError):s.step(f,now=101)
    assert s.state()==before
    with pytest.raises(ValueError,match='frozen_config_changed'):Shadow(path,{**CONFIG,'take_profit_cents':9})
    s.close()


def test_duplicate_book_cannot_fill_and_adverse_entry_rejected(tmp_path):
    s=Shadow(tmp_path/'s.sqlite3',CONFIG)
    s.step(frame(100),now=100,signal=dict(side='yes',strategy_id='test'))
    f=frame(101);f['book_id']='100'
    assert s.step(f,now=101)['action']=='wait'
    assert s.step(frame(102,bid='.44',ask='.45'),now=102)['action']=='entry_rejected'
    assert s.state()['cash_cents']==1000
    s.close()
