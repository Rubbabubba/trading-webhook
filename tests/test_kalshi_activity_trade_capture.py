import json
import sqlite3
from datetime import datetime, timezone
from opportunity_lab.kalshi_activity_trade_capture import init,poll,status

class Markets:
    def __init__(self): self.calls=[]; self.now=400; self.fail=False
    def row(self,ticker):
        return dict(trade_id=ticker+str(self.now),ticker=ticker,created_time=datetime.fromtimestamp(self.now-1,timezone.utc).isoformat(),count_fp='1',yes_price_dollars='.4',no_price_dollars='.6',is_block_trade=False)
    def get_trades(self,**kw):
        self.calls.append('global')
        if self.fail: raise ValueError('transport')
        return {'trades':[self.row('A'),self.row('B')],'cursor':'more'},self.now,self.now+1
    def quote(self,value):
        self.calls.extend(['market','book'])
        return {'ticker':value['ticker'],'observed_at':self.now+2,'market':{'event_ticker':'E'+value['ticker']},'orderbook_fp':{'yes_dollars':[['.4','5']],'no_dollars':[['.5','5']]}}
    def get_targeted_trades(self,**kw):
        self.calls.append('targeted')
        return {'trades':[self.row(kw['params']['ticker'])],'cursor':''},self.now+2,self.now+3

def test_activity_rotation_is_durable_bounded_and_never_retroactive_quote_or_fill():
    db=sqlite3.connect(':memory:',isolation_level=None);init(db,100);m=Markets()
    poll(db,m,400)
    s=status(db)
    assert s['public_trades']==1 and s['matched_prior_quote_trades']==0
    assert s['no_prior_quote_trades']==1 and s['owned_fills']==0 and s['promotion_ready'] is False
    assert m.calls==['global','market','book','targeted']
    init(db,450);poll(db,m,500);assert len(m.calls)==4
    m.now=600;poll(db,m,600)
    assert [r[0] for r in db.execute('SELECT ticker FROM activity_trade_v5_polls ORDER BY id')]==['A','B']
    assert status(db)['trade_events']==2
    try: db.execute('DELETE FROM activity_trade_v5_trades')
    except sqlite3.IntegrityError: pass
    else: assert False
    assert db.execute("SELECT count(*) FROM sqlite_master WHERE name='scan_trade_v4_trades'").fetchone()[0]==0

def test_failed_activity_attempt_cannot_bypass_cadence():
    db=sqlite3.connect(':memory:',isolation_level=None);init(db,100);m=Markets();m.fail=True
    poll(db,m,400);poll(db,m,450)
    assert m.calls==['global'] and status(db)['polls']==1
    assert status(db)['latest_poll']['state']=='blocked'

def test_activity_cap_and_protocol_survive_restart():
    db=sqlite3.connect(':memory:',isolation_level=None);init(db,100);m=Markets()
    db.execute("INSERT INTO activity_trade_v5_polls SELECT n,100,'','','{}','{}' FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<4096) SELECT n FROM c)")
    init(db,500);poll(db,m,600)
    assert not m.calls and status(db)['capacity_reached'] is True
    db.execute("UPDATE activity_trade_v5_protocol SET detail='{}'")
    try: init(db,700)
    except ValueError: pass
    else: assert False

def test_invalid_global_trade_blocks_before_quote_or_order_calls():
    db=sqlite3.connect(':memory:',isolation_level=None);init(db,100);m=Markets()
    m.get_trades=lambda **kw:({'trades':[{'ticker':'A'}],'cursor':''},400,401)
    poll(db,m,400)
    assert not m.calls and status(db)['latest_poll']['state']=='blocked'
    assert status(db)['public_trades']==0
