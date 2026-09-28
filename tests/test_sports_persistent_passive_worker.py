import json

from opportunity_lab.sports_persistent_passive_worker import open_db, status, sync_manifest


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


def test_status_explicitly_reports_shadow_only(tmp_path):
    db = open_db(tmp_path/'shadow.sqlite3')
    report = status(db, 120)
    assert report['execution_enabled'] is False
    assert report['registry_events'] == 120
    assert report['signals'] == 0
    db.close()
