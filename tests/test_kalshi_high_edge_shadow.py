import json
import sqlite3
import pytest
from opportunity_lab.kalshi_high_edge_shadow import init,observe,status,pending_ticker

def frame(at):
    return {'received_at':at,'orderbook_fp':{'yes_dollars':[['.40','12']], 'no_dollars':[['.52','3']]}}

def test_future_only_restart_and_strict_markout_deadlines():
    db=sqlite3.connect(':memory:');init(db,100)
    observe(db,'T','E',[],frame(99));assert status(db)['signals']==0
    observe(db,'T','E',[],frame(100))
    detail=json.loads(db.execute('SELECT detail FROM high_edge_v15_signals').fetchone()[0])
    assert detail['challenger']['price_cents']==42 and detail['frozen_v12']['price_cents']==40
    init(db,200)
    for at in (105,130,400):observe(db,'T','E',[],frame(at))
    s=status(db);assert s['complete_signals']==1 and s['independent_events']==1
    assert not s['execution_enabled'] and not s['promotion_ready'] and s['owned_fills']==0
    assert s['same_observation_benchmarks']['5']['frozen_v12']-s['stressed_quote_net_cents']['5']==2

def test_late_marks_missing_not_backfilled_and_registration_immutable():
    db=sqlite3.connect(':memory:');init(db,100);observe(db,'T','E',[],frame(100))
    observe(db,'T','E',[],frame(111));assert db.execute('SELECT count(*) FROM high_edge_v15_marks').fetchone()[0]==0
    db.execute("UPDATE high_edge_v15_protocol SET detail='{}'")
    with pytest.raises(ValueError):init(db,300)

def test_low_edge_does_not_chase_or_repeat_event():
    db=sqlite3.connect(':memory:');init(db,100)
    f=frame(100);f['orderbook_fp']['no_dollars']=[['.56','3']]
    observe(db,'T','E',[],f);assert status(db)['signals']==0
    observe(db,'T','E',[],frame(101));observe(db,'T2','E',[],frame(102))
    assert status(db)['signals']==1


def test_due_quote_slot_has_deadline_and_never_backfills_expired_marks():
    db=sqlite3.connect(':memory:');init(db,100);observe(db,'T','E',[],frame(100))
    assert pending_ticker(db,104) is None
    assert pending_ticker(db,105)=='T'
    observe(db,'T','E',[],frame(105))
    assert pending_ticker(db,106) is None
    assert pending_ticker(db,130)=='T'
    assert pending_ticker(db,136) is None
    assert pending_ticker(db,400)=='T'
    assert pending_ticker(db,406) is None
    assert status(db)['complete_signals']==0
