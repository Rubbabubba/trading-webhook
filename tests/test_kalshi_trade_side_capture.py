import sqlite3
from opportunity_lab.kalshi_trade_side_capture import init,capture,status

def test_future_only_deduplicated_missing_sides_never_imputed():
    db=sqlite3.connect(':memory:');init(db,100)
    def row(identity,at,**extra):
        from datetime import datetime,timezone
        return {'trade_id':identity,'created_time':datetime.fromtimestamp(at,timezone.utc).isoformat(),'is_block_trade':False,**extra}
    rows=[row('old',99),row('known',110,taker_outcome_side='yes',taker_book_side='ask'),row('unknown',111,taker_side='no'),row('conflict',112,taker_side='no',taker_outcome_side='yes',taker_book_side='bid')]
    selected=[(r['trade_id'],'T','E',0,'{}') for r in rows]
    capture(db,{'trades':rows},selected,120);capture(db,{'trades':rows},selected,121)
    result=status(db)
    assert result['records']==3 and result['known_side_records']==1 and result['unknown_side_records']==2
    assert result['owned_fills']==0 and not result['promotion_ready']

def test_partial_page_does_not_become_complete_or_reset_start():
    db=sqlite3.connect(':memory:');init(db,100);init(db,500)
    row={'trade_id':'a','created_time':'1970-01-01T00:01:50+00:00','is_block_trade':False,'taker_outcome_side':'yes','taker_book_side':'ask'}
    capture(db,{'trades':[row],'cursor':'more'},[('a','T','E',110,'{}')],120)
    assert status(db)['complete_onbook_records']==0
    assert db.execute('SELECT started_at FROM trade_side_v8_protocol').fetchone()[0]==100
