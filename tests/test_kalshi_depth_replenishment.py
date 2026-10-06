import json
import sqlite3
import pytest

from opportunity_lab.kalshi_depth_replenishment import init, capture, status, PROTOCOL


def frame(at, size=10, price='.40'):
    return {'ticker': 'T', 'received_at': at,
            'orderbook_fp': {'yes_dollars': [[price, str(size)]], 'no_dollars': [['.50', '20']]}}


def db():
    result = sqlite3.connect(':memory:', isolation_level=None)
    init(result, now=100)
    return result


def test_depletion_refill_persists_across_restart_without_claiming_fills(tmp_path):
    path = tmp_path / 'depth.db'
    first = sqlite3.connect(path, isolation_level=None); init(first, now=100)
    capture(first, 'T', 'E', frame(101))
    capture(first, 'T', 'E', frame(103, 2)); first.close()
    second = sqlite3.connect(path, isolation_level=None); init(second, now=200)
    capture(second, 'T', 'E', frame(110, 8))
    result = status(second)
    assert result['snapshots'] == 3 and result['independent_events'] == 1
    assert result['depletion_episodes'] == result['displayed_refills'] == 1
    assert result['actual_fills'] == 0 and result['realized_net_cents'] is None
    assert result['promotion_ready'] is False and result['execution_enabled'] is False
    second.close()


def test_gap_or_price_change_does_not_count_as_refill():
    for changed in [frame(300, 10), frame(105, 10, '.41')]:
        connection = db()
        capture(connection, 'T', 'E', frame(101)); capture(connection, 'T', 'E', frame(103, 2))
        capture(connection, 'T', 'E', changed)
        assert status(connection)['displayed_refills'] == 0
        assert status(connection)['incomplete_episodes'] == 1


def test_retrospective_duplicate_and_identity_changed_snapshots_excluded():
    connection = db()
    with pytest.raises(ValueError): capture(connection, 'T', 'E', frame(99))
    capture(connection, 'T', 'E', frame(101))
    assert capture(connection, 'T', 'E', frame(101))['captured'] is False
    with pytest.raises(ValueError): capture(connection, 'T', 'another-event', frame(102))
    assert status(connection)['snapshots'] == 1


def test_captured_snapshots_cannot_be_rewritten_or_deleted():
    connection = db(); capture(connection, 'T', 'E', frame(101))
    for sql in ["UPDATE depth_snapshots SET event_id='fake'", 'DELETE FROM depth_snapshots']:
        with pytest.raises(sqlite3.IntegrityError, match='append_only'): connection.execute(sql)
    assert status(connection)['snapshots'] == 1


@pytest.mark.parametrize('book', [
    {'yes_dollars': [['.40', 'NaN']], 'no_dollars': []},
    {'yes_dollars': [['.60', '1']], 'no_dollars': [['.50', '1']]},
    {'yes_dollars': [['.40', '1'], ['.40', '2']], 'no_dollars': []},
    {'yes_dollars': [['.40', '-1']], 'no_dollars': []},
])
def test_invalid_depth_is_not_evidence(book):
    connection = db()
    with pytest.raises(ValueError):
        capture(connection, 'T', 'E', {**frame(101), 'orderbook_fp': book})
    assert status(connection)['snapshots'] == 0


def test_protocol_mutation_rejected_and_capacity_preserves_all_prior_snapshots(monkeypatch):
    connection = db()
    monkeypatch.setitem(PROTOCOL, 'maximum_snapshots', 1)
    with pytest.raises(ValueError, match='depth_protocol_changed'): init(connection, now=200)
    capture(connection, 'T', 'E', frame(101))
    assert capture(connection, 'T', 'E', frame(102))['reason'] == 'snapshot_cap_reached'
    assert status(connection)['snapshots'] == 1
