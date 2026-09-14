import sqlite3
from tools.review_sports_rolling_v2 import snapshot_ledger


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
