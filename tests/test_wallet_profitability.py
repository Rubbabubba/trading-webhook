from datetime import datetime,timezone
import copy,json,sqlite3
import pytest
from opportunity_lab import kalshi_wallet_profitability as r
from opportunity_lab import kalshi_wallet_copy_worker as c
from opportunity_lab.wallet_qualification import validate_report,score


def test_forward_cycle_costs_holdout_restart_and_selection(tmp_path,monkeypatch):
    wallet='0x'+'1'*40;loser='0x'+'2'*40
    now=[datetime(2026,11,1,tzinfo=timezone.utc).timestamp()]
    worker=r.Research(tmp_path,clock=lambda:now[0]);worker.db.execute('INSERT INTO cohort VALUES(1,?,?)',(now[0],json.dumps([wallet,loser])))
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_sports.discover',lambda t,*_:dict(ticker='GAME-'+t['token_id'],event_ticker='EVENT-'+t['token_id'],side='yes'))
    side=['BUY'];price=['.65']
    def quote(mapping):
        bid,opposite=('.44','.50') if side[0]=='BUY' else (price[0],'.20')
        return ({'orderbook_fp':{'yes_dollars':[[bid,'2']],'no_dollars':[[opposite,'2']]},'observed_at':now[0]}, {'fee_multiplier':'1'})
    worker.quote=quote
    def t(w,i,s,p):return dict(proxy_wallet=w,transaction_hash='0x'+f'{i*2+(s=="SELL"):064x}',type='TRADE',condition_id='0x'+f'{i:064x}',token_id=str(i),timestamp=int(now[0]),side=s,price=p,size='1')
    # Live dates progress; membership freezes on entry, before any outcome.
    for i in range(40):
        now[0]=worker.started+(i//7)*86400+i*120
        for w in (wallet,loser):
            side[0]='BUY';trade=t(w,i,'BUY','.50');worker.observe(trade)
            key=r.json.loads(worker.db.execute('SELECT detail FROM observations ORDER BY rowid DESC LIMIT 1').fetchone()[0])
            from opportunity_lab.kalshi_wallet_monitor import trade_key
            assert worker.process(trade_key(key),key)=='modeled_entry'
            now[0]+=2;side[0]='SELL';price[0]='.65' if w==wallet or i<20 else '.35'
            assert worker.process('sell',t(w,i,'SELL',price[0]))=='modeled_exit'
    now[0]+=86400  # Today's open observations cannot withdraw a prior-session qualification.
    report=worker.report()
    assert report['selected_wallets']==[wallet]
    assert report['modeled_entries']==80 and report['resolved']==80
    assert report['actual_fills'] is False
    # 50c ask ->65c bid, 2c conservative fee each way =11c.
    assert report['scores'][0]['holdout']['net_cents']==220
    assert report['scores'][1]['qualified'] is False
    assert validate_report(report,now=datetime.fromtimestamp(now[0],timezone.utc))==report
    side[0]='BUY';worker.process('fresh-open',t(wallet,99,'BUY','.50'))
    assert worker.report()['selected_wallets']==[wallet]
    worker.db.close();again=r.Research(tmp_path,clock=lambda:now[0])
    assert again.report()['selected_wallets']==[wallet]
    assert again.db.execute('SELECT count(*) FROM trades').fetchone()[0]==81
    assert again.process('duplicate',t(wallet,0,'BUY','.50'))=='duplicate_event'
    again.db.close()


def test_unresolved_losses_cannot_be_censored_and_stress_can_fail():
    rows=[dict(phase=p,event=str(i),day=str(i//7),net_cents=10,exit_kind='wallet_sell') for p in ('training','holdout') for i in range(20)]
    assert score(rows)['qualified']
    rows.append(dict(phase='holdout',event='pending',day='3',net_cents=None,exit_kind=None))
    assert not score(rows)['qualified']
    rows.pop()
    for v in rows:v['net_cents']=1
    assert 'holdout:stress_positive' in score(rows)['reasons']


def test_demo_attempts_are_bounded_separately_without_forgiving_unknowns(tmp_path):
    from opportunity_lab.kalshi_binary_journal import BinaryJournal
    from opportunity_lab.kalshi_binary_broker import BinaryDemoBroker
    from test_kalshi_binary_journal import Exchange
    now=2000000000
    journal=BinaryJournal(tmp_path/'j',clock=lambda:datetime.fromtimestamp(now,timezone.utc))
    db=sqlite3.connect(':memory:',isolation_level=None);worker=c.WalletCopy(tmp_path,db,BinaryDemoBroker(journal,Exchange(journal)),clock=lambda:now)
    since=now-1000
    for i in range(10):db.execute("INSERT INTO signals VALUES(?,?,'BUY','{}','ioc_no_fill',NULL)",(str(i),now-100))
    assert worker.buy_attempts(since)==10 and worker.entry_admission(since) is None
    for i in range(10,50):db.execute("INSERT INTO signals VALUES(?,?,'BUY','{}','quarantined_zero_fill',NULL)",(str(i),now-100))
    assert worker.entry_admission(since)=='daily_attempt_limit'
    assert db.execute('SELECT count(*) FROM signals').fetchone()[0]==50
    journal.close();db.close()


def test_no_hindsight_quote_and_no_broker_interface(tmp_path):
    worker=r.Research(tmp_path,clock=lambda:2000000000)
    assert not hasattr(worker,'broker')
    with pytest.raises(ValueError,match='expired'):worker.process('old',{'timestamp':1999999800})
    worker.db.close()


def test_scheduler_tick_collects_quotes_resolves_and_persists_decision(tmp_path,monkeypatch):
    from opportunity_lab.kalshi_wallet_monitor import trade_key
    now=[2000000000];wallet='0x'+'1'*40;worker=r.Research(tmp_path,clock=lambda:now[0])
    trade=dict(proxy_wallet=wallet,transaction_hash='0x'+'a'*64,type='TRADE',condition_id='0x'+'b'*64,token_id='123',timestamp=now[0]+1,side='BUY',price='.50',size='1')
    monkeypatch.setattr(worker,'candidates',lambda:[wallet])
    worker.db.execute('INSERT INTO cohort VALUES(1,?,?)',(now[0],json.dumps([wallet])))
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_monitor.read_trades',lambda *a:[trade])
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_sports.discover',lambda *a:dict(ticker='GAME',event_ticker='EVENT',side='yes'))
    monkeypatch.setattr(worker,'quote',lambda m:({'observed_at':now[0],'orderbook_fp':{'yes_dollars':[['.44','2']],'no_dollars':[['.50','2']]}},{'fee_multiplier':'1'}))
    worker.read=lambda *a:({'market':{'ticker':'GAME','event_ticker':'EVENT','status':'finalized','result':'yes'}},)
    now[0]+=2;result=worker.tick()
    assert result['modeled_entries']==1 and result['resolved']==1 and result['selected_wallets']==[]
    assert result['scores'][0]['training']['net_cents']==0  # Same-session evidence cannot qualify.
    now[0]+=86400;assert worker.report()['scores'][0]['training']['net_cents']==48
    assert worker.db.execute('SELECT count(*) FROM decisions').fetchone()[0]==2
    now[0]+=30;assert worker.tick()['modeled_entries']==1
    assert worker.db.execute('SELECT count(*) FROM decisions').fetchone()[0]==3
    worker.db.close()


def test_newest_first_activity_is_processed_in_source_order(tmp_path,monkeypatch):
    now=[2000000000];wallet='0x'+'1'*40;worker=r.Research(tmp_path,clock=lambda:now[0])
    worker.db.execute('INSERT INTO cohort VALUES(1,?,?)',(now[0],json.dumps([wallet])))
    monkeypatch.setattr(worker,'candidates',lambda:[wallet])
    entry=dict(proxy_wallet=wallet,transaction_hash='0x'+'a'*64,type='TRADE',condition_id='0x'+'b'*64,token_id='123',timestamp=now[0]+1,side='BUY',price='.50',size='1')
    exit=dict(entry,transaction_hash='0x'+'c'*64,timestamp=now[0]+2,side='SELL',price='.65')
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_monitor.read_trades',lambda *a:[exit,entry])
    monkeypatch.setattr('opportunity_lab.kalshi_wallet_sports.discover',lambda *a:dict(ticker='GAME',event_ticker='EVENT',side='yes'))
    quotes=iter([('.44','.50'),('.65','.20')])
    def q(m):
        bid,opposite=next(quotes)
        return {'observed_at':now[0],'orderbook_fp':{'yes_dollars':[[bid,'2']],'no_dollars':[[opposite,'2']]}},{'fee_multiplier':'1'}
    monkeypatch.setattr(worker,'quote',q)
    worker.read=lambda *a:({'market':{'ticker':'GAME','event_ticker':'EVENT','status':'active'}},)
    now[0]+=3;result=worker.tick()
    assert result['modeled_entries']==1 and result['resolved']==1
    detail=json.loads(worker.db.execute('SELECT detail FROM trades').fetchone()[0])
    assert detail['exit_kind']=='wallet_sell' and detail['net_cents']==11
    worker.db.close()
