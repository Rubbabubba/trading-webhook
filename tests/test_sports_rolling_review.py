import json
import sqlite3

from tools.review_sports_rolling import assess


def ledger():
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE games(slug,config,anchor,memory,state);
        CREATE TABLE accounts(slug,horizon,state);
        CREATE TABLE samples(id,slug,observation);
        CREATE TABLE actions(sample_id,horizon,detail);''')
    for n in range(3):
        slug = 'epl_' + str(n)
        db.execute('INSERT INTO games VALUES(?,?,NULL,NULL,?)', (slug, json.dumps({'league':'epl'}), 'completed'))
        account = {'position':None, 'entries':1, 'exits':1, 'realized_cents':10,
                   'fees_cents':2, 'slippage_cents':2, 'drawdown_cents':3}
        for h in (300,900):
            db.execute('INSERT INTO accounts VALUES(?,?,?)', (slug,h,json.dumps(account)))
            action = [{'action':'sell','position':{'ticker':'T','opened_at':'same-entry'}}]
            db.execute('INSERT INTO actions VALUES(?,?,?)', (n,h,json.dumps(action)))
        db.execute('INSERT INTO samples VALUES(?,?,?)', (n,slug,json.dumps({'model':{'state':'in','probabilities':{'home':.5},'blockers':[]}})))
    return db


def test_variants_not_counted_twice_and_returns_separate():
    result = assess(ledger(), {})
    sport = result['sports']['epl']
    assert sport['closed_entry_episodes'] == 3
    assert sport['accounts']['300']['realized_cents'] == 30
    assert sport['accounts']['900']['realized_cents'] == 30
    assert not sport['candidate_evaluation_screen_met']
    assert len(result['pending_checkpoints']) == 1


def test_review_journal_prevents_repeat_and_pregame_excluded():
    db = ledger()
    db.execute('INSERT INTO samples VALUES(9,?,?)', ('epl_0',json.dumps({'model':{'state':'pre','blockers':['not_active_play']}})))
    first = assess(db,{})
    assert first['sports']['epl']['in_play_samples'] == 3
    second = assess(db,{'reviewed_checkpoints':[first['pending_checkpoints'][0]['id']]})
    assert second['pending_checkpoints'] == []
    db.execute('INSERT INTO games VALUES(?,?,NULL,NULL,?)', ('epl_-1',json.dumps({'league':'epl'}),'completed'))
    # One later completion sorting before the old cohort must not trigger a
    # new three-game strategy checkpoint.
    third = assess(db,{'reviewed_checkpoints':[first['pending_checkpoints'][0]['id']]})
    assert not any(c['kind']=='strategy_review' for c in third['pending_checkpoints'])


def test_one_zero_trade_game_triggers_diagnostic_without_strategy_tuning():
    db = ledger()
    db.execute("DELETE FROM games WHERE slug!='epl_0'")
    account={'position':None,'entries':0,'exits':0,'realized_cents':0,'fees_cents':0,'slippage_cents':0,'drawdown_cents':0}
    db.execute('UPDATE accounts SET state=? WHERE slug=?',(json.dumps(account),'epl_0'))
    db.execute('DELETE FROM actions')
    report=assess(db,{})
    assert [c['kind'] for c in report['pending_checkpoints']] == ['no_trade_diagnostic']
