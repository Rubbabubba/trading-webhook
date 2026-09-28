from datetime import datetime, timedelta, timezone
import json
import sqlite3
from tools.review_sports_rolling_v2 import assess, snapshot_ledger


def test_snapshot_is_consistent_and_does_not_block_live_writer(tmp_path):
    path = tmp_path / 'live.sqlite3'
    writer = sqlite3.connect(path, timeout=.1)
    writer.execute('CREATE TABLE sample(value)')
    writer.execute('INSERT INTO sample VALUES(1)')
    writer.commit()
    copy = snapshot_ledger(path)
    copy.execute('BEGIN')
    assert copy.execute('SELECT value FROM sample').fetchall() == [(1,)]
    writer.execute('INSERT INTO sample VALUES(2)')
    writer.commit()
    assert copy.execute('SELECT value FROM sample').fetchall() == [(1,)]
    assert writer.execute('SELECT count(*) FROM sample').fetchone()[0] == 2
    copy.close()
    writer.close()


def test_report_labels_old_lifetime_evidence_as_stale():
    db = sqlite3.connect(':memory:')
    db.executescript('''
      CREATE TABLE games(slug TEXT,config TEXT,anchor TEXT,memory TEXT,state TEXT);
      CREATE TABLE accounts(slug TEXT,horizon INTEGER,state TEXT);
      CREATE TABLE samples(id INTEGER PRIMARY KEY,slug TEXT,at TEXT,snapshot TEXT,observation TEXT);
      CREATE TABLE actions(sample_id INTEGER,horizon INTEGER,detail TEXT,account TEXT);
    ''')
    old = datetime(2026, 9, 18, tzinfo=timezone.utc)
    account = {'position': None, 'entries': 0, 'exits': 0, 'realized_cents': 0,
               'fees_cents': 0, 'slippage_cents': 0, 'drawdown_cents': 0}
    observation = {'model': {'state': 'in', 'completed': False, 'blockers': [],
                             'probabilities': {'home': .5}}}
    db.execute('INSERT INTO games VALUES(?,?,?,?,?)',
               ('nfl_1', json.dumps({'league': 'nfl'}), None, '{}', 'completed'))
    db.execute('INSERT INTO accounts VALUES(?,?,?)', ('nfl_1', 300, json.dumps(account)))
    db.execute('INSERT INTO samples VALUES(?,?,?,?,?)',
               (1, 'nfl_1', old.isoformat(), 's', json.dumps(observation)))
    report = assess(db, {'reviewed_checkpoints': []},
                    now=old + timedelta(days=10), freshness_hours=24)
    assert report['status'] == 'historical_stale'
    assert report['evidence_window']['latest_sample_at'] == old.isoformat()
    assert 'lifetime results' in report['note']
    db.close()
