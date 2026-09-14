import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from opportunity_lab.sports_live_load import LatestBooks, RateBudget, fresh_stream
from opportunity_lab.sports_live_books_v33 import StreamBooks
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from review_sports_live import assess_live, review_snapshot


def test_review_snapshot_excludes_raw_archive(tmp_path):
    path = tmp_path/'paper.sqlite3'
    db = sqlite3.connect(path)
    for table in ('games','accounts','probes','samples','actions'):
        columns = 'id,slug,observation' if table == 'samples' else 'sample_id,detail' if table == 'actions' else 'value'
        db.execute('create table '+table+'('+columns+')')
    db.execute('insert into games values(?)', ('preserved',))
    db.execute('create table responses(raw)')
    db.commit();db.close()
    snapshot = review_snapshot(path)
    assert snapshot.execute('select * from games').fetchall() == [('preserved',)]
    assert not snapshot.execute("select name from sqlite_master where name='responses'").fetchall()
    snapshot.close()


def test_slow_consumer_100000_deltas_bounded_and_exact():
    books = StreamBooks()
    box = LatestBooks()
    books.apply({'type':'orderbook_snapshot','sid':1,'seq':1,'msg':{'market_ticker':'T','yes_dollars_fp':[['.4','1']], 'no_dollars_fp':[['.5','1']]}}, 0)
    for i in range(100000):
        books.apply({'type':'orderbook_delta','sid':1,'seq':i+2,'msg':{'market_ticker':'T','side':'yes','price_dollars':'.4','delta_fp':'1'}}, i)
        box.put({'kind':'stream_book','ticker':'T','quote':books.books['T'].quote()})
    assert box.stats()['high_water'] == 1
    assert box.stats()['coalesced_complete_views'] == 99999
    assert box.drain()[0]['quote']['bid_size'] == 100001


def test_session_reset_discards_stale_pending_books():
    box = LatestBooks()
    box.put({'kind':'stream_book','ticker':'T'})
    box.put({'kind':'stream_state','state':'disconnected'})
    assert box.drain() == [{'kind':'stream_state','state':'disconnected'}]


def test_budget_backoff_and_freshness():
    b = RateBudget()
    b.limited(10)
    assert not b.ready(39) and b.ready(40)
    b.limited(40)
    assert not b.ready(99) and b.ready(100)
    b.success()
    assert b.consecutive == 0 and b.incidents == 2
    assert fresh_stream({'at':10,'valid':True,'transport':'websocket'},15)
    assert not fresh_stream({'at':10,'valid':True,'transport':'websocket'},16)


def test_reviews_loss_threshold_and_variants_not_double_counted():
    db = sqlite3.connect(':memory:')
    db.executescript('create table games(slug,config,anchor,memory,state); create table accounts(slug,horizon,state); create table samples(id,slug,observation); create table actions(sample_id,horizon,detail); create table probes(state);')
    for g in range(5):
        slug = str(g)
        db.execute('insert into games values(?,?,null,null,?)',(slug,json.dumps({'league':'nfl','game':slug,'kickoff':'2026-09-12T00:00:00+00:00'}),'completed'))
        for h in [300,900]:
            db.execute('insert into accounts values(?,?,?)',(slug,h,json.dumps({'entries':4,'realized_cents':-10})))
        for episode in range(4):
            sid=g*4+episode
            db.execute('insert into samples values(?,?,?)',(sid,slug,json.dumps({'model':{'state':'in','blockers':[]},'markets':{},'admission_ok':False})))
            for h in [300,900]:
                db.execute('insert into actions values(?,?,?)',(sid,h,json.dumps([{'action':'sell','position':{'ticker':'T','side':'yes','opened_at':str(episode)}}])))
    r=assess_live(db,datetime(2026,9,12,23,tzinfo=timezone.utc))
    assert r['sports']['nfl']['closed_unique_episodes']==20
    assert sum(a['kind']=='loss_strategy_review' for a in r['alerts'])==1
    assert sum(a['kind']=='zero_trade_review' for a in r['alerts'])==5
    db.execute('delete from actions where sample_id=0')
    r=assess_live(db,datetime(2026,9,12,23,tzinfo=timezone.utc))
    assert not any(a['kind']=='loss_strategy_review' for a in r['alerts'])
