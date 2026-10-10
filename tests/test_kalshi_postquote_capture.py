import json
import sqlite3
from datetime import datetime,timezone
import pytest
from opportunity_lab.kalshi_activity_trade_capture import init as init_source
from opportunity_lab.kalshi_postquote_capture import init,poll,status

def setup(at=400):
    db=sqlite3.connect(':memory:',isolation_level=None)
    init_source(db,100);init(db,300)
    db.execute("INSERT INTO activity_trade_v5_polls VALUES(1,400,'A','E',?, '{}')",(json.dumps({'observed_at':at,'ticker':'A'}),))
    return db
class Markets:
    def __init__(self): self.calls=[];self.bad=False
    def get_targeted_trades(self,**kw):
        self.calls.append(kw)
        if self.bad: raise ValueError('transport')
        rows=[dict(trade_id=str(at),ticker='A',created_time=datetime.fromtimestamp(at,timezone.utc).isoformat(),count_fp='1',yes_price_dollars='.4',no_price_dollars='.6',is_block_trade=False) for at in (399,400,410,430,431)]
        return {'trades':rows,'cursor':''},435,436

def test_prospective_window_bounds_and_no_duplicate_transport():
    db=setup();m=Markets();poll(db,m,429);assert not m.calls
    poll(db,m,435);poll(db,m,440)
    assert len(m.calls)==1
    assert [r[0] for r in db.execute('SELECT executed_at FROM postquote_v6_trades ORDER BY executed_at')]==[410,430]
    s=status(db);assert s['public_trades']==2 and s['owned_fills']==0 and not s['promotion_ready']
    with pytest.raises(sqlite3.IntegrityError): db.execute('DELETE FROM postquote_v6_trades')

def test_quote_receipt_controls_window_not_poll_start():
    db=setup(405);m=Markets();poll(db,m,431);assert not m.calls
    poll(db,m,435);assert m.calls[0]['params']['min_ts']==405

def test_stale_and_failed_windows_are_not_retried():
    db=setup();m=Markets();poll(db,m,491);assert not m.calls and status(db)['states']['expired']==1
    db=setup();m.bad=True;poll(db,m,435);poll(db,m,440)
    assert len(m.calls)==1 and status(db)['states']['blocked']==1

def test_registration_and_existing_windows_survive_restart():
    db=setup();m=Markets();poll(db,m,435);init(db,600);poll(db,m,601);assert len(m.calls)==1
    db.execute("UPDATE postquote_v6_protocol SET detail='{}'")
    with pytest.raises(ValueError): init(db,700)
