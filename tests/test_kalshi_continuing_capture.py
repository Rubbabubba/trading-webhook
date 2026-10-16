import json,sqlite3
from datetime import datetime,timezone
import pytest
from opportunity_lab import kalshi_continuing_capture as capture
from opportunity_lab.kalshi_demo_market_data import DemoMarkets

def quote(at=1001):
    return dict(environment='demo',ticker='T',market=dict(ticker='T',event_ticker='E',market_type='binary',status='active',title='Test',rules_primary='Synthetic rule'),market_started_at=at,market_observed_at=at+.1,started_at=at+1,observed_at=at+1.2,orderbook_fp=dict(yes_dollars=[['0.40','10']],no_dollars=[['0.50','12']]))

def recorder(tmp_path):return capture.Recorder(tmp_path/'capture.sqlite3',now=1000,free_bytes=lambda:10**9)

def trade(q,tid='p',**changes):
    r=dict(trade_id=tid,ticker='T',created_time=datetime.fromtimestamp(q['observed_at']+1,timezone.utc).isoformat(),count_fp='1.00',yes_price_dollars='0.4000',no_price_dollars='0.6000',taker_side='yes',is_block_trade=False)
    return r|changes

class Markets:
    def __init__(self,q,rows=None,cursors=None):self.q=q;self.calls=[];self.rows=rows if rows is not None else [trade(q)];self.cursors=cursors or ['']
    def get_targeted_trades(self,*,params):
        self.calls.append(params);i=len(self.calls)-1
        return dict(trades=self.rows,cursor=self.cursors[min(i,len(self.cursors)-1)]),self.q['observed_at']+31,self.q['observed_at']+31.1

def test_compressed_dedup_receipts_hashes_and_restart(tmp_path):
    r=recorder(tmp_path);a=r.quote(quote());r.quote(quote(1010))
    assert r.db.execute('SELECT count(*) FROM blobs').fetchone()[0]==2
    assert r.quote(quote())==a and r.status(1020)['quotes']==2
    mb,bb=r.db.execute('SELECT metadata_blob,book_blob FROM quotes WHERE id=?',(a,)).fetchone()
    assert r.read_blob(bb)['yes']==[['0.40','10']]
    assert r.read_blob(mb)['ticker']=='T'
    before=r.status(1020);r.close();r=recorder(tmp_path)
    assert r.status(1020)['records']==before['records'] and r.status(1020)['quotes']==2
    for sql in ('DELETE FROM quotes','DELETE FROM blobs',"UPDATE quotes SET ticker='X'"):
        with pytest.raises(sqlite3.IntegrityError,match='append_only'):r.db.execute(sql)

@pytest.mark.parametrize('change',[dict(environment='production'),dict(ticker='wrong'),dict(market_started_at=999),dict(observed_at=float('nan'))])
def test_invalid_or_preregistration_quote_never_adds_evidence(tmp_path,change):
    r=recorder(tmp_path);assert r.quote(quote()|change) is None
    assert r.status(1100)['quotes']==0 and r.status(1100)['error_count']==1

def test_capacity_and_disk_stop_atomically_without_erasing(tmp_path,monkeypatch):
    r=recorder(tmp_path);r.quote(quote());s=r.status(1100)
    monkeypatch.setattr(capture,'LIMITS',dict(capture.LIMITS,maximum_accounted_bytes=s['accounted_bytes']+1))
    q=quote(1010);q['market']['title']='changed'
    assert r.quote(q) is None
    assert r.status(1100)['quotes']==1 and r.status(1100)['records']==s['records']
    assert r.status(1100)['state']=='capacity_insufficient_coverage'
    r.free_bytes=lambda:0
    assert r.quote(quote(1020)) is None and r.status(1100)['quotes']==1

def test_postquote_public_trade_is_not_an_owned_fill_and_poll_never_repeats(tmp_path):
    r=recorder(tmp_path);q=quote();r.quote(q);m=Markets(q)
    assert not r.poll(m,q['observed_at']+29,legacy_capped=True,priority_blocked=False)
    assert not r.poll(m,q['observed_at']+31,legacy_capped=False,priority_blocked=False)
    assert not r.poll(m,q['observed_at']+31,legacy_capped=True,priority_blocked=True)
    assert r.poll(m,q['observed_at']+31,legacy_capped=True,priority_blocked=False)
    s=r.status(1100);assert s['public_trades']==1 and s['owned_fills']==0 and s['incomplete_windows']==0
    assert not r.poll(m,q['observed_at']+91,legacy_capped=True,priority_blocked=False)
    assert len(m.calls)==1 and m.calls[0]['min_ts']==int(q['observed_at'])
    assert r.db.execute('SELECT count(*) FROM page_receipts').fetchone()[0]==1

def test_invalid_semantic_page_is_retained_but_trades_are_rejected(tmp_path):
    r=recorder(tmp_path);q=quote();r.quote(q);m=Markets(q,[trade(q,no_price_dollars='0.5')])
    r.poll(m,q['observed_at']+31,legacy_capped=True,priority_blocked=False)
    assert r.status(1100)['public_trades']==0 and r.status(1100)['blocked_polls']==1
    blob=r.db.execute('SELECT blob_id FROM page_receipts').fetchone()[0]
    assert r.read_blob(blob)['trades'][0]['no_price_dollars']=='0.5'

def test_pagination_is_bounded_and_incomplete_page_stays_visible(tmp_path):
    r=recorder(tmp_path);q=quote();r.quote(q);m=Markets(q,cursors=['a','b'])
    r.poll(m,q['observed_at']+31,legacy_capped=True,priority_blocked=False)
    assert len(m.calls)==2 and r.status(1100)['public_trades']==1
    assert r.status(1100)['incomplete_windows']==1

def test_interrupted_request_restarts_without_retry_or_counter_reset(tmp_path):
    r=recorder(tmp_path);q=quote();r.quote(q)
    class Interrupted:
        def get_targeted_trades(self,*,params):raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):r.poll(Interrupted(),q['observed_at']+31,legacy_capped=True,priority_blocked=False)
    records=r.status(1100)['records'];r.close();r=recorder(tmp_path)
    assert r.status(1100)['blocked_polls']==1 and r.status(1100)['records']==records
    assert not r.poll(Markets(q),q['observed_at']+91,legacy_capped=True,priority_blocked=False)

def test_protocol_and_accounting_drift_cannot_initialize_new_baseline(tmp_path):
    r=recorder(tmp_path);r.quote(quote());r.db.execute('UPDATE meta SET records=0');r.db.commit();r.close()
    with pytest.raises(ValueError,match='accounting_mismatch'):recorder(tmp_path)

def test_market_hook_reuses_exact_two_existing_reads(tmp_path,monkeypatch):
    r=recorder(tmp_path);m=DemoMarkets();m.continuing_capture=r;calls=[];q=quote()
    def get(ticker,book=False,params=None):
        calls.append(book)
        return (dict(orderbook_fp=q['orderbook_fp']),q['started_at'],q['observed_at']) if book else (dict(market=q['market']),q['market_started_at'],q['market_observed_at'])
    monkeypatch.setattr(m,'get',get)
    result=m.quote(dict(ticker='T'))
    assert calls==[False,True] and result['ticker']=='T' and r.status(1100)['quotes']==1

def test_imminent_frozen_horizons_block_research_reads():
    db=sqlite3.connect(':memory:')
    db.executescript('CREATE TABLE maker_fills(client_id TEXT);CREATE TABLE markouts(client_id TEXT);CREATE TABLE high_edge_v15_signals(id INTEGER,at REAL);CREATE TABLE high_edge_v15_marks(signal_id INTEGER,horizon INTEGER);CREATE TABLE timely_shadow_signals(strategy TEXT,source_id INTEGER,at REAL);CREATE TABLE timely_shadow_marks(strategy TEXT,source_id INTEGER,horizon INTEGER);')
    assert not capture.priority_pending(db,100)
    db.execute('INSERT INTO high_edge_v15_signals VALUES(1,100)')
    assert capture.priority_pending(db,100)
    db.executemany('INSERT INTO high_edge_v15_marks VALUES(1,?)',[(5,),(30,)])
    assert not capture.priority_pending(db,200)
    assert capture.priority_pending(db,380)
    db.execute("INSERT INTO maker_fills VALUES('x')")
    assert capture.priority_pending(db,1000)
