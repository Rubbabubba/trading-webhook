import json,sqlite3
import pytest
from opportunity_lab.kalshi_postquote_capture import init as init_capture
from opportunity_lab.kalshi_postquote_diagnostic import init,status,compare


def quote():
    return {'ticker':'A','best_bids':{'yes':{'price_dollars':'.4','size_fp':'2'},'no':{'price_dollars':'.5','size_fp':'3'}}}


def trade():
    return {'ticker':'A','on_book':True,'count_fp':'2','yes_price_dollars':'.4','no_price_dollars':'.6'}


def test_comparison_never_infers_side_or_fill():
    result=compare(quote(),trade())
    assert result['yes']=={'at_or_below_prior_bid':True,'print_size_at_least_prior_depth':True}
    assert not result['no']['at_or_below_prior_bid']
    assert compare(quote(),dict(trade(),on_book=False)) is None
    assert compare(quote(),dict(trade(),no_price_dollars='.7')) is None
    assert compare(quote(),dict(trade(),count_fp='NaN')) is None


def test_prospective_boundaries_overlap_and_incomplete_pages():
    db=sqlite3.connect(':memory:');init_capture(db,10);init(db,100)
    for source,at,complete in [(1,99,True),(2,100,True),(3,105,True),(4,140,False)]:
        db.execute('INSERT INTO postquote_v6_windows VALUES(?,?,?,?,?,?)',(source,at,'A','E',json.dumps(quote()),json.dumps({'state':'observed','page_complete':complete})))
    for identity,at in [('old',99),('boundary',100),('valid',110),('end',130),('late',135),('partial',145)]:
        db.execute('INSERT INTO postquote_v6_trades VALUES(?,?,?,?,?)',(identity,'A','E',at,json.dumps(trade())))
    report=status(db)
    assert report['prospective_windows']==3 and report['incomplete_windows']==1
    assert report['distinct_compared_prints']==3 and report['independent_events']==1
    assert report['by_side']['yes']['comparable_prints']==3
    assert report['owned_fills']==0 and not report['promotion_ready'] and not report['fee_edge_assessed']
    assert not report['aggressor_side_known'] and not report['queue_position_known']


def test_registration_cannot_be_replaced_or_restart_baseline_reset():
    db=sqlite3.connect(':memory:');init(db,100);init(db,500)
    assert db.execute('SELECT started_at FROM postquote_v7_protocol').fetchone()[0]==100
    db.execute("UPDATE postquote_v7_protocol SET detail='{}'")
    with pytest.raises(ValueError):init(db,600)
