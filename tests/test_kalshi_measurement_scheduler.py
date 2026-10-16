import json
from types import SimpleNamespace

from opportunity_lab import kalshi_measurement_scheduler as scheduler
from opportunity_lab import kalshi_demo_v5_maker_worker as worker
from opportunity_lab.kalshi_timely_shadow_measurement import observe


def seed(tmp_path, at=1000):
    state = worker.MakerState(tmp_path / 'worker.sqlite3')
    state.db.execute('INSERT INTO timely_shadow_signals VALUES(?,?,?,?,?,?,?)',
                     ('v12', 1, 'TIMELY', 'E', at, 'yes', 40))
    return state


def test_joint_earliest_deadline_and_expired_windows(tmp_path):
    state = seed(tmp_path)
    state.db.execute('INSERT INTO high_edge_v15_signals VALUES(?,?,?,?,?)',
                     (1, 'V15', 'OTHER', 1002, '{}'))
    assert scheduler.due_ticker(state.db, 1007) == 'TIMELY'
    assert scheduler.due_ticker(state.db, 1010.01) == 'V15'
    assert scheduler.due_ticker(state.db, 1012.01) is None
    assert scheduler.protected(state.db, 1012.01)  # thirty-second horizons
    assert not scheduler.protected(state.db, 1330)
    assert state.db.execute('SELECT count(*) FROM timely_shadow_marks').fetchone()[0] == 0
    state.close()


def test_marked_horizons_stop_reserving_and_no_backfill(tmp_path):
    state = seed(tmp_path)
    for horizon in (5, 30):
        state.db.execute('INSERT INTO timely_shadow_marks VALUES(?,?,?,?,?)',
                         ('v12', 1, horizon, 1000 + horizon, '{}'))
    assert not scheduler.protected(state.db, 1100)
    assert scheduler.protected(state.db, 1235)
    assert scheduler.due_ticker(state.db, 1235) is None
    assert scheduler.due_ticker(state.db, 1300) == 'TIMELY'
    assert scheduler.due_ticker(state.db, 1305.001) is None
    state.close()


def test_each_optional_stage_rechecks_clock_and_persists_deferral(tmp_path):
    state = seed(tmp_path)
    # Complete earlier horizons, leaving the 300-second measurement.
    for h in (5, 30):
        state.db.execute('INSERT INTO timely_shadow_marks VALUES(?,?,?,?,?)',
                         ('v12', 1, h, 1000 + h, '{}'))
    assert scheduler.allow_optional(state, 'activity', clock=lambda: 1220)
    assert not scheduler.allow_optional(state, 'targeted_trades', clock=lambda: 1240)
    assert state.load('measurement_scheduler')['deferred'] == {'targeted_trades': 1}
    state.close()
    state = worker.MakerState(tmp_path / 'worker.sqlite3')
    assert state.load('measurement_scheduler')['last_deferred_at'] == 1240
    state.close()


def run_flat_cycle(tmp_path, monkeypatch, now):
    state = seed(tmp_path)
    state.close()
    calls = []
    monkeypatch.setenv('KALSHI_DEMO_API_KEY_ID', 'synthetic')
    monkeypatch.setenv('KALSHI_DEMO_PRIVATE_KEY_PATH', 'synthetic')
    monkeypatch.setattr(worker, 'DemoClient', lambda *a: object())
    monkeypatch.setattr(worker, 'release_legacy_uncertainty_stop', lambda *a: None)
    monkeypatch.setattr(worker, 'check_exchange', lambda *a: None)
    monkeypatch.setattr(worker, 'recover_or_report', lambda *a: True)
    monkeypatch.setattr(worker, 'log_event', lambda *a, **k: None)
    monkeypatch.setattr(worker.time, 'time', lambda: now)
    monkeypatch.setattr(worker, 'ContinuingCapture', lambda *a: SimpleNamespace(close=lambda: None))

    def snapshot():
        calls.append('reconcile_snapshot')
        return {'balance': {'balance': 50000}}

    monkeypatch.setattr(worker, 'BinaryDemoBroker', lambda *a: SimpleNamespace(snapshot=snapshot))

    def forbidden(*a, **k):
        raise AssertionError('optional read or new entry reached protected window')

    monkeypatch.setattr(worker, 'refresh_cohort', forbidden)
    monkeypatch.setattr(worker, 'scan_market_candidate', forbidden)
    for name in ('poll_activity_trade_capture', 'poll_postquote_capture',
                 'poll_scan_trade_capture', 'poll_trade_probe',
                 'poll_targeted_trade_probe', 'poll_depth_trade_probe', 'submit'):
        monkeypatch.setattr(worker, name, forbidden)

    def quote(payload):
        calls.append(('quote', payload['ticker']))
        return {'received_at': now + 4, 'observed_at': now + 4}

    monkeypatch.setattr(worker, 'DemoMarkets', lambda: SimpleNamespace(quote=quote))
    monkeypatch.setattr(worker, 'one_contract_frame', lambda q, **k: q)
    # Preserve the real frozen measurement writer, rather than synthesizing a mark.
    monkeypatch.setattr(worker, 'observe_frame', lambda s, t, f, e:
                        observe(s.db, t, now=f['received_at'], yes_mid=50))
    monkeypatch.setattr(worker, 'record_due_markouts', lambda *a: None)
    monkeypatch.setattr(worker, 'write_status', lambda *a, **k: calls.append('status'))
    worker.run(tmp_path, cycles=1)
    return calls


def test_real_runner_reserves_future_window_but_retains_broker_snapshot(tmp_path, monkeypatch):
    calls = run_flat_cycle(tmp_path, monkeypatch, 1002)
    assert not any(isinstance(c, tuple) for c in calls)
    assert 'reconcile_snapshot' in calls and 'status' in calls


def test_real_runner_due_measurement_precedes_discovery_and_collectors(tmp_path, monkeypatch):
    calls = run_flat_cycle(tmp_path, monkeypatch, 1005)
    assert ('quote', 'TIMELY') in calls
    state = worker.MakerState(tmp_path / 'worker.sqlite3')
    assert state.db.execute('SELECT horizon,at FROM timely_shadow_marks').fetchall() == [(5, 1009)]
    assert state.db.execute('SELECT count(*) FROM timely_shadow_signals').fetchone()[0] == 1
    state.close()
