from datetime import datetime,timezone
from fractions import Fraction
import sqlite3
from types import SimpleNamespace

from opportunity_lab.kalshi_wallet_copy_worker import WalletCopy
from opportunity_lab.kalshi_wallet_lifecycle import normalize


def test_budget_rollover_and_after_cost_lifecycle_are_read_only(tmp_path):
    now=datetime(2026,11,18,23,59,tzinfo=timezone.utc).timestamp()
    ledger=sqlite3.connect(':memory:');ledger.execute('CREATE TABLE settlements(ticker TEXT)')
    ledger.execute("INSERT INTO settlements VALUES('SETTLED')")
    records=[{'state':'terminal','filled':1,'intent':{'action':'buy'}},
             {'state':'terminal','filled':1,'intent':{'action':'sell'}}]
    accounting={'positions':{},'day':'2026-11-18','realized':Fraction(-7,100),
                'daily_realized':Fraction(-7,100),'fees':Fraction(2,100),
                'open_basis':Fraction(0),'pending_reserves':Fraction(0)}
    journal=SimpleNamespace(db=ledger,records=lambda:records,accounting=lambda:accounting)
    with sqlite3.connect(':memory:',isolation_level=None) as db:
        worker=WalletCopy(tmp_path,db,SimpleNamespace(journal=journal),clock=lambda:now)
        for i in range(50):db.execute('INSERT INTO signals VALUES(?,?,?,?,?,NULL)',
            (str(i),int(now),'BUY','{}','quarantined_zero_fill' if i else 'filled'))
        before=list(db.iterdump())
        result=worker.lifecycle()
        assert result['admission']=='daily_attempt_limit'
        assert result['buy_attempts_today']==50 and result['remaining_buy_attempts']==0
        assert result['entries_filled']==1 and result['exits_filled']==1 and result['settlements']==1
        assert result['realized_net_usd']=='-0.07' and result['fees_usd']=='0.02'
        exported=normalize(result)
        assert exported['sell_attempts']==1 and exported['sell_signals_observed']==0
        assert exported['sell_verification']=='broker_fill_verified'
        assert exported['sell_admission']=='eligible_owned_sells_only'
        assert list(db.iterdump())==before
        now+=120
        result=worker.lifecycle()
        assert result['budget_session_utc']=='2026-11-19'
        assert result['buy_attempts_today']==0 and result['remaining_buy_attempts']==50
        assert db.execute('SELECT count(*) FROM signals').fetchone()[0]==50
    ledger.close()


def test_lifecycle_export_strips_private_fields_and_keeps_unknowns_unknown():
    value=normalize({'state':'unavailable','admission':'unavailable','api_key':'private',
                     'realized_net_usd':'nan','buy_attempts_today':True})
    assert value['realized_net_usd'] is None and value['buy_attempts_today'] is None
    assert 'api_key' not in value


def test_attempt_day_uses_durable_submission_clock_across_midnight(tmp_path):
    from opportunity_lab.kalshi_wallet_copy_worker import client_order_id
    import json
    midnight=datetime(2026,11,19,tzinfo=timezone.utc).timestamp()
    ledger=sqlite3.connect(':memory:')
    ledger.execute('CREATE TABLE submission_quotes(client_id TEXT,detail TEXT)')
    key='a'*64
    ledger.execute('INSERT INTO submission_quotes VALUES(?,?)',
                   (client_order_id(key),json.dumps({'observed_at':midnight+2})))
    journal=SimpleNamespace(db=ledger)
    with sqlite3.connect(':memory:',isolation_level=None) as db:
        # A source trade before midnight was actually submitted after midnight.
        db.execute('CREATE TABLE signals(id TEXT PRIMARY KEY,at INTEGER,side TEXT,trade TEXT,state TEXT,mapping TEXT)')
        db.execute('INSERT INTO signals VALUES(?,?,?,?,?,NULL)',
                   (key,int(midnight-20),'BUY','{}','filled'))
        worker=WalletCopy(tmp_path,db,SimpleNamespace(journal=journal),clock=lambda:midnight+30)
        before=list(db.iterdump())
        assert worker.buy_attempts(midnight)==1
        assert list(db.iterdump())==before
        # Unknown and zero-fill attempts still consume the same ten-attempt cap.
        db.execute("UPDATE signals SET state='quarantined_zero_fill'")
        assert worker.buy_attempts(midnight)==1
        assert worker.buy_attempts(midnight+86400)==0
        # A legacy row without a verifiable submit timestamp keeps its old day.
        db.execute('INSERT INTO signals VALUES(?,?,?,?,?,NULL)',
                   ('legacy',int(midnight+1),'BUY','{}','submission_unresolved'))
        assert worker.buy_attempts(midnight)==2
    ledger.close()
