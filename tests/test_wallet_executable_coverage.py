"""Real shard/fee shapes and cadence regressions; all fills here are fixtures."""
from contextlib import nullcontext
from datetime import datetime,timezone
import copy,json
import pytest
from opportunity_lab.kalshi_wallet_monitor import read_fee
from opportunity_lab.kalshi_factory_fee_probe import fee_basis
from opportunity_lab import kalshi_wallet_profitability as research
from test_kalshi_wallet_production import setup,signal,production_quote


@pytest.mark.parametrize('kind',['quadratic','quadratic_with_maker_fees','quadratic_with_combo_maker_fees'])
def test_wallet_taker_joins_real_shard_metadata_without_changing_factory_scope(kind):
    now=datetime.now(timezone.utc).timestamp()
    quote={'side':'yes','observed_at':now,'market':{
        'ticker':'GAME','event_ticker':'EVENT','exchange_index':3},
        'orderbook_fp':{'yes_dollars':[['.44','2']],'no_dollars':[['.50','2']]}}
    event={'event_ticker':'EVENT','series_ticker':'SERIES','exchange_index':3}
    series={'ticker':'SERIES','exchange_index':3,'fee_type':kind,'fee_multiplier':1}
    def read(url):return ({'event':event} if '/events/' in url else {'series':series},0,now+1,None,None)
    proof=read_fee(quote,'https://example.test',read)
    assert proof['fee_type']==kind and proof['exchange_index']==3
    assert proof['liquidity']=='taker_ioc' and proof['model_fee_upper_bound_cents']==2
    row=dict(market_type='binary',exchange_index=3,event_id='EVENT',price_cents=50,
             observed_at=datetime.fromtimestamp(now,timezone.utc).isoformat())
    with pytest.raises(ValueError,match='identity'):fee_basis(row,event,series,fetched_at=now+1)
    for field,value in [('exchange_index',2),('exchange_index',True),('ticker','OTHER')]:
        before=copy.deepcopy(series);series[field]=value
        with pytest.raises(ValueError):read_fee(quote,'https://example.test',read)
        series.clear();series.update(before)
    series['fee_type']='flat'
    with pytest.raises(ValueError,match='unsupported_fee_type'):read_fee(quote,'https://example.test',read)


@pytest.mark.parametrize('kind',['quadratic_with_maker_fees','quadratic_with_combo_maker_fees'])
def test_production_ioc_retains_shard_fee_and_owner_guards(kind,tmp_path):
    now,db,j,x,b,w,approved=setup(tmp_path)
    key,cid=signal(db,now,'BUY')
    trade,mapping=db.execute('SELECT trade,mapping FROM signals WHERE id=?',(key,)).fetchone()
    mapping=json.loads(mapping);mapping['exchange_index']=3
    db.execute('UPDATE signals SET mapping=? WHERE id=?',(json.dumps(mapping),key))
    j.reserve(cid,'GAME',1,50,2,outcome='yes',action='buy',account_snapshot=b.snapshot())
    q=production_quote(j,cid,key);q['market']['exchange_index']=3
    q['fee_basis'].update(fee_type=kind,exchange_index=3,liquidity='taker_ioc')
    bad=copy.deepcopy(q);bad['fee_basis']['exchange_index']=2
    with pytest.raises(ValueError,match='fee_identity'):b.submit(cid,quote_snapshot=bad)
    assert x.posts==0
    assert b.submit(cid,quote_snapshot=q)['filled']==1
    j.close();db.close()


@pytest.mark.parametrize('work_seconds,expected_pause',[(0,30),(25,5),(45,5)])
def test_loop_cadence_includes_work_and_never_catches_up_in_a_burst(work_seconds,expected_pause,tmp_path,monkeypatch):
    elapsed=[0];pauses=[];closed=[]
    class DB:
        def close(self):closed.append(True)
    class Worker:
        def __init__(self,*a):self.db=DB()
        def tick(self):elapsed[0]+=work_seconds
    def sleep(seconds):pauses.append(seconds);raise KeyboardInterrupt
    monkeypatch.setattr(research,'Research',Worker)
    monkeypatch.setattr(research.time,'monotonic',lambda:elapsed[0])
    monkeypatch.setattr(research.time,'sleep',sleep)
    monkeypatch.setattr('opportunity_lab.kalshi_process_lock.acquire',lambda *a:nullcontext())
    with pytest.raises(KeyboardInterrupt):research.run(tmp_path)
    assert pauses==[expected_pause] and closed==[True]


def test_nhl_shadow_match_checks_venue_alias_winner_and_game_rules():
    from opportunity_lab.kalshi_wallet_sports import discover
    source = {
        'conditionId': 'fixture', 'gameStartTime': '2026-11-21 20:30:00+00',
        'sportsMarketType': 'moneyline', 'outcomes': '["Blackhawks","Flames"]',
        'clobTokenIds': '["1","2"]', 'description': 'Final score including any overtime periods and shootouts.',
        'question': 'Blackhawks vs. Flames',
    }
    event = {'slug': 'nhl-chi-cal-2026-11-21', 'title': source['question'], 'markets': [source]}
    market = {
        'ticker': 'KXNHLGAME-26NOV21CHICGY-CGY', 'event_ticker': 'KXNHLGAME-26NOV21CHICGY',
        'market_type': 'binary', 'strike_type': 'structured', 'yes_sub_title': 'Calgary',
        'rules_primary': ('If Calgary wins the Chicago vs Calgary NHL game originally scheduled '
                          'for Nov 21, 2026, then the market resolves to Yes.'),
        'rules_secondary': '',
    }

    def read(url, params):
        if 'polymarket' in url:
            return ([event],)
        assert params['event_ticker'] == market['event_ticker']
        return ({'markets': [market]},)

    trade = dict(event_slug=event['slug'], condition_id='fixture', token_id='2', outcome='Flames')
    with pytest.raises(ValueError, match='unsupported_sports_event'):
        discover(trade, 'https://test.invalid', read)
    assert discover(trade, 'https://test.invalid', read, research_nhl=True)['ticker'] == market['ticker']
    market['rules_primary'] = market['rules_primary'].replace('Calgary wins', 'Chicago wins')
    with pytest.raises(ValueError, match='sports_moneyline_unverified'):
        discover(trade, 'https://test.invalid', read, research_nhl=True)


def test_exploratory_nhl_fill_does_not_use_a_training_slot(tmp_path, monkeypatch):
    worker = research.Research(tmp_path, clock=lambda: 2000000000)
    wallet = '0x' + '1' * 40
    worker.db.execute('INSERT INTO cohort VALUES(1,?,?)', (worker.clock(), json.dumps([wallet])))
    worker.db.execute('INSERT INTO trades(id,wallet,event,day,phase,detail) VALUES(?,?,?,?,?,?)',
                      ('nhl', wallet, 'NHL', '2026-11-20', 'coverage',
                       json.dumps({'net_cents': 42, 'exit_kind': 'settlement'})))
    report = worker.report()
    assert report['scores'][0]['training']['resolved'] == 0
    assert report['exploratory_nhl'] == {'entries': 1, 'resolved': 1, 'counts_toward_qualification': False}
    assert report['selected_wallets'] == []
    worker.db.close()


def test_unsupported_buys_are_audited_without_waiting_in_the_pricing_queue(tmp_path):
    wallet = '0x' + '1' * 40
    now = [2000000000]
    worker = research.Research(tmp_path, clock=lambda: now[0])
    trade = dict(proxy_wallet=wallet, transaction_hash='0x'+'a'*64, type='TRADE',
                 condition_id='0x'+'b'*64, token_id='1', timestamp=now[0], side='BUY',
                 price='.50', size='1', event_slug='unsupported-event')
    assert worker.observe(trade) == 'unsupported_sports_event'
    assert worker.observe(trade) is None
    assert worker.observe({**trade, 'side': 'SELL', 'transaction_hash': '0x'+'c'*64}) == 'observed'
    states = [row[0] for row in worker.db.execute('SELECT state FROM observations ORDER BY rowid')]
    assert states == ['unsupported_sports_event', 'observed']
    worker.db.close()


@pytest.mark.parametrize('demo_verified', [True, False])
def test_fresh_nhl_entry_uses_scoring_when_demo_is_executable(tmp_path, monkeypatch, demo_verified):
    wallet = '0x' + '1' * 40
    now = 2000000000
    worker = research.Research(tmp_path, clock=lambda: now)
    worker.db.execute('INSERT INTO cohort VALUES(1,?,?)', (now, json.dumps([wallet])))
    trade = dict(proxy_wallet=wallet, condition_id='fixture', token_id='1',
                 timestamp=now, side='BUY', price='.50', event_slug='nhl-edm-car-2026-11-21')
    mapping = dict(ticker='GAME', event_ticker='EVENT', side='yes')
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_sports.discover', lambda *args, **kwargs: mapping)
    monkeypatch.setattr(worker, 'quote', lambda _: (
        {'observed_at': now, 'orderbook_fp': {'yes_dollars': [['.44','2']], 'no_dollars': [['.50','2']]}},
        {'fee_multiplier': '1'}))

    def demo(*args):
        if not demo_verified:
            raise ValueError('wallet_book_unavailable')
        return {'ticker': 'GAME', 'quote_at': now}

    monkeypatch.setattr(worker, 'nhl_demo_evidence', demo)
    assert worker.process('nhl-entry', trade) == ('modeled_entry' if demo_verified else 'modeled_coverage_entry')
    phase, detail = worker.db.execute('SELECT phase,detail FROM trades').fetchone()
    assert phase == ('training' if demo_verified else 'coverage')
    assert bool(json.loads(detail)['nhl_demo_evidence']) == demo_verified
    worker.db.close()
