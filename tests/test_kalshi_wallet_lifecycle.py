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
        for i in range(10):db.execute('INSERT INTO signals VALUES(?,?,?,?,?,NULL)',
            (str(i),int(now),'BUY','{}','quarantined_zero_fill' if i else 'filled'))
        before=list(db.iterdump())
        result=worker.lifecycle()
        assert result['admission']=='daily_attempt_limit'
        assert result['buy_attempts_today']==10 and result['remaining_buy_attempts']==0
        assert result['entries_filled']==1 and result['exits_filled']==1 and result['settlements']==1
        assert result['realized_net_usd']=='-0.07' and result['fees_usd']=='0.02'
        assert list(db.iterdump())==before
        now+=120
        result=worker.lifecycle()
        assert result['budget_session_utc']=='2026-11-19'
        assert result['buy_attempts_today']==0 and result['remaining_buy_attempts']==10
        assert db.execute('SELECT count(*) FROM signals').fetchone()[0]==10
    ledger.close()


def test_lifecycle_export_strips_private_fields_and_keeps_unknowns_unknown():
    value=normalize({'state':'unavailable','admission':'unavailable','api_key':'private',
                     'realized_net_usd':'nan','buy_attempts_today':True})
    assert value['realized_net_usd'] is None and value['buy_attempts_today'] is None
    assert 'api_key' not in value
