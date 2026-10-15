from replay import quote,signal,advance


def row(y,n,yq=10,nq=10):return {'book':{'yes':{'bid_levels':[[y,yq]]},'no':{'bid_levels':[[n,nq]]}}}


def test_complement_asks_and_depth():
    q=quote(row(40,58))
    assert q['ya']==42 and q['na']==60
    assert quote(row(40,58,.5,10)) is None


def test_crossed_book_rejected():
    assert quote(row(60,50)) is None


def test_momentum_and_direction_must_agree():
    old=quote(row(38,60))
    current=quote(row(40,58,90,10))
    assert signal(current,old)=='yes'
    assert signal(quote(row(40,58,10,90)),old) is None


def test_delay_and_full_round_trip_costs():
    s={'subscription':[1,1],'side':'yes','decision_at':100,'state':'awaiting_entry'}
    q=quote(row(40,58))
    assert advance(s,(1,1),100.5,q) is False and 'entry_at' not in s
    assert advance(s,(1,1),101,q) is False and s['entry_price']==42
    assert advance(s,(1,1),131,q) is True
    assert s['net_cents_by_extra_stress']=={'0':-14,'2':-18,'5':-24}


def test_missing_exit_never_zero_or_backfilled():
    s={'subscription':[1,1],'side':'yes','decision_at':100,'state':'awaiting_entry'}
    q=quote(row(40,58))
    advance(s,(1,1),101,q)
    assert advance(s,(1,1),137,q) is True
    assert s['state']=='exit_unresolved_late' and 'net_cents_by_extra_stress' not in s


def test_late_entry_and_changed_subscription_not_fills():
    q=quote(row(40,58))
    s={'subscription':[1,1],'side':'yes','decision_at':100,'state':'awaiting_entry'}
    assert advance(s,(1,1),103,q) is True and 'entry_at' not in s
    s={'subscription':[1,1],'side':'yes','decision_at':100,'state':'awaiting_entry'}
    assert advance(s,(1,2),101,q) is True and 'entry_at' not in s


def test_official_event_and_closed_execution_rejected():
    from review import eligible
    s={'ticker':'X','decision_at':100,'entry_at':101,'exit_at':131}
    m={'ticker':'X','event_ticker':'E','market_type':'binary','rules_primary':'rule','open_time':'1970-01-01T00:00:00Z','close_time':'1970-01-01T00:02:00Z'}
    assert eligible(s,m)=='outside_official_trading_window'
    assert eligible(s,None)=='missing_official_metadata'
    m['close_time']='1970-01-01T00:03:00Z'
    assert eligible(s,m) is None
