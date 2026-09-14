import gzip
import hashlib
import json

import pytest

from opportunity_lab.sports_capture_v3 import open_store, persist, capture, run


def test_raw_payload_round_trip_and_config_lock(tmp_path):
    p = tmp_path / 'capture.db'
    db = open_store(p, {'version': 'test'})
    raw = b'{"bad": not-json}\n'
    with db:
        persist(db, 'cycle', 'game', 'game', {'raw': raw, 'url': 'https://example.com', 'error': None})
    digest, compressed = db.execute('SELECT sha256,body_gzip FROM responses').fetchone()
    assert gzip.decompress(compressed) == raw
    assert digest == hashlib.sha256(raw).hexdigest()
    db.close()
    with pytest.raises(ValueError, match='Frozen'):
        open_store(p, {'version': 'changed'})


def test_capture_preserves_errors_before_parser(tmp_path, monkeypatch):
    from opportunity_lab import sports_capture_v3 as module
    monkeypatch.setattr(module, 'request', lambda url: {'url': url, 'raw': b'not-json', 'error': None})
    monkeypatch.setattr(module, 'diagnose', lambda *args: (_ for _ in ()).throw(ValueError('parser defect')))
    db = open_store(tmp_path / 'capture.db', {})
    result = capture(db, {'league': 'mlb', 'event_id': '1', 'markets': {'home': 'H', 'away': 'A'}})
    assert db.execute('SELECT count(*) FROM responses').fetchone()[0] == 6
    assert len(result['errors']) == 6
    assert result['diagnostic_error'] == 'parser defect'
    assert result['validation'] == 'unvalidated'
    db.close()


def test_execution_enabled_manifest_rejected(tmp_path):
    p = tmp_path / 'manifest.json'
    p.write_text(json.dumps({'version': 'capture_v3.0', 'execution_enabled': True}))
    with pytest.raises(ValueError, match='Capture-only'):
        run(p, tmp_path / 'out', once=True)
