from datetime import datetime, timezone
from screen import book, fee, future_fill, replay


def row(t,y=40,n=50):
    return dict(ts=datetime.fromtimestamp(t,timezone.utc),sequence_id=t,ticker='X',yes_bids=[{'price_cents':y,'size':2}],no_bids=[{'price_cents':n,'size':2}])


def test_fee_and_crossed_book():
    assert fee(50)==2
    assert fee(90)==1
    assert book(row(0,60,50)) is None
    assert book(row(0))['yes_ask']==50


def test_delay_and_missing_execution():
    rows=[row(0),row(2)]
    assert future_fill(rows,[0,2],0,'yes','buy')['at']==2
    assert future_fill(rows,[0,2],2,'yes','buy') is None
    assert future_fill([row(7)],[7],0,'yes','buy') is None


def test_favorite_loss_kept_and_stress():
    rows=[row(0,90,9),row(1,90,9),row(300,90,9)]
    trades,_=replay(rows,{'result':'no','settled_ts':'2026-01-01T00:00:00+00:00'},'favorite_taker')
    assert len(trades)==1
    assert trades[0]['net_cents']==-92
    assert trades[0]['stressed_net_cents']['2']==-94


def test_missing_settlement_not_zero_profit():
    trades,_=replay([row(0,90,9),row(1,90,9),row(300,90,9)],{},'favorite_taker')
    assert trades[0]['complete'] is False
    assert 'net_cents' not in trades[0]


def test_provider_uppercase_results():
    trades,_=replay([row(0,90,9),row(1,90,9),row(300,90,9)],{'result':'NO','settled_ts':'2026-01-01T00:00:00+00:00'},'favorite_taker')
    assert trades[0]['complete'] is True
    assert trades[0]['net_cents']==-92


def test_structural_fee_and_leg_stress():
    from structural import surplus
    assert surplus(40,53)==1
    assert surplus(40,54)==0


def test_earliest_official_cutoff_and_missing_window():
    import pytest
    from screen import trading_window
    opened,cutoff=trading_window({'open_time':'2026-01-01T00:00:00Z','close_time':'2026-01-01T03:00:00Z','expected_expiration_time':'2026-01-01T02:00:00Z'})
    assert cutoff.hour==2 and opened.hour==0
    with pytest.raises(ValueError): trading_window({'open_time':'2026-01-01T00:00:00Z'})
