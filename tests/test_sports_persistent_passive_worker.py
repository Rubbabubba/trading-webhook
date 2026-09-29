from datetime import datetime, timezone
import json
import sqlite3

import opportunity_lab.sports_persistent_passive_worker as worker
from opportunity_lab.sports_persistent_passive_worker import (
    active_games,
    observe_one,
    open_db,
    status,
    sync_manifest,
)


def test_manifest_sync_persists_new_events_without_erasing_state(tmp_path):
    db = open_db(tmp_path/'shadow.sqlite3')
    game = {'league': 'nfl', 'event_id': '1', 'kickoff': '2026-09-28T20:00:00+00:00'}
    manifest = {'generated_at': '2026-09-28T10:00:00+00:00', 'games': [game]}
    assert sync_manifest(db, manifest) == 1
    state = json.loads(db.execute('SELECT state FROM games').fetchone()[0])
    state['signaled'] = True
    db.execute('UPDATE games SET state=?', (json.dumps(state),))
    sync_manifest(db, {**manifest, 'generated_at': '2026-09-28T11:00:00+00:00'})
    assert json.loads(db.execute('SELECT state FROM games').fetchone()[0])['signaled'] is True
    db.close()


def test_open_db_migrates_existing_games_table_as_active(tmp_path):
    path = tmp_path/'shadow.sqlite3'
    legacy = sqlite3.connect(path)
    legacy.execute('''CREATE TABLE games(
        slug TEXT PRIMARY KEY, config TEXT, anchor TEXT, state TEXT, updated_at TEXT
    )''')
    legacy.execute('INSERT INTO games VALUES(?,?,?,?,?)',
                   ('nfl_legacy', '{}', None, '{}', '2026-09-28T10:00:00+00:00'))
    legacy.commit()
    legacy.close()

    db = open_db(path)
    assert db.execute('SELECT active FROM games WHERE slug=?', ('nfl_legacy',)).fetchone()[0] == 1
    db.close()


def test_manifest_sync_deactivates_missing_events_and_updates_current_config(tmp_path):
    db = open_db(tmp_path/'shadow.sqlite3')
    old = {'league': 'nfl', 'event_id': 'old', 'kickoff': '2026-09-28T20:00:00+00:00'}
    kept = {'league': 'nfl', 'event_id': 'kept', 'kickoff': '2026-09-28T21:00:00+00:00'}
    sync_manifest(db, {'generated_at': '2026-09-28T10:00:00+00:00', 'games': [old, kept]})
    refreshed = {**kept, 'kickoff': '2026-09-28T22:00:00+00:00'}
    sync_manifest(db, {'generated_at': '2026-09-28T11:00:00+00:00', 'games': [refreshed]})

    assert [slug for slug, _ in active_games(db)] == ['nfl_kept']
    assert json.loads(active_games(db)[0][1])['kickoff'] == refreshed['kickoff']
    assert db.execute('SELECT active FROM games WHERE slug=?', ('nfl_old',)).fetchone()[0] == 0
    db.close()


def test_inactive_market_is_retired_without_cycle_failure(tmp_path, monkeypatch):
    db = open_db(tmp_path/'shadow.sqlite3')
    game = {
        'league': 'nfl', 'event_id': 'closed',
        'kickoff': '2026-09-28T20:00:00+00:00',
        'markets': {'home': 'CLOSED-HOME', 'away': 'CLOSED-AWAY'},
    }
    sync_manifest(db, {'generated_at': '2026-09-28T10:00:00+00:00', 'games': [game]})
    monkeypatch.setattr(worker, 'scoreboard', lambda *args, **kwargs: ({}, {}))

    class ClosedMarketClient:
        def quote(self, _market):
            raise ValueError('demo_market_not_active_binary')

    result = observe_one(db, ClosedMarketClient(), 'nfl_closed',
                         datetime(2026, 9, 28, 20, tzinfo=timezone.utc))
    assert result == {'action': 'market_inactive', 'reason': 'demo_market_not_active_binary'}
    assert active_games(db) == []
    assert db.execute('SELECT count(*) FROM observations').fetchone()[0] == 0
    db.close()


def test_status_explicitly_reports_shadow_only(tmp_path):
    db = open_db(tmp_path/'shadow.sqlite3')
    report = status(db, 120)
    assert report['execution_enabled'] is False
    assert report['registry_events'] == 120
    assert report['active_games'] == 0
    assert report['signals'] == 0
    db.close()
