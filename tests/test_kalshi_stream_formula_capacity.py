import json
import sqlite3
import pytest
from tests.test_kalshi_stream_formula_comparison import setup,book,context
from opportunity_lab.kalshi_stream_formula_comparison import Comparison


def test_real_sqlite_full_auto_rollback_is_terminal_not_a_repeated_error(tmp_path):
    study,now=setup(tmp_path)
    study.observe('A',book(),context(),'a'*64)
    before=list(study.db.execute('SELECT * FROM comparison_inputs'))
    signals=list(study.db.execute('SELECT * FROM high_edge_v15_signals'))
    pages=study.db.execute('PRAGMA page_count').fetchone()[0]
    study.db.execute(f'PRAGMA max_page_count={pages}')
    # A genuine oversized input causes SQLite's automatic transaction rollback.
    metadata=context();metadata['source_padding']='x'*100000
    now[0]=1005
    assert study.observe('A',book(1005),metadata,'b'*64) is False
    assert not study.db.in_transaction
    assert study.status()['stop_reason']=='sqlite_page_cap'
    assert study.status()['state']=='capacity_stop'
    assert list(study.db.execute('SELECT * FROM comparison_inputs'))==before
    assert list(study.db.execute('SELECT * FROM high_edge_v15_signals'))==signals
    assert study.db.execute('SELECT count(*) FROM high_edge_v15_marks').fetchone()[0]==0
    for at in [1006,1030,1300]:
        now[0]=at
        assert study.observe('A',book(at),context(),'c'*64) is False
    assert study.status()['complete_signals']==0
    path=study.path
    study.close()
    # Auto-rollback may leave page_count below the production cap. The
    # durable stop receipt must still suppress writes after restart.
    restored=Comparison(path,clock=lambda:1301,free_bytes=lambda:10**9)
    assert restored.status()['stop_reason']=='sqlite_page_cap'
    assert not restored.observe('A',book(1301),context(),'e'*64)
    assert restored.accepted==len(before)
    restored.close()


def test_frozen_page_cap_restores_after_restart_without_new_inputs(tmp_path,monkeypatch):
    from opportunity_lab import kalshi_stream_formula_comparison as module
    study,now=setup(tmp_path);study.observe('A',book(),context(),'a'*64)
    pages=study.db.execute('PRAGMA page_count').fetchone()[0]
    path=study.path;before=study.accepted;study.close()
    monkeypatch.setattr(module,'MAX_PAGES',pages)
    restored=Comparison(path,clock=lambda:1005,free_bytes=lambda:10**9)
    restored.begin_epoch('new-epoch')
    assert restored.status()['state']=='capacity_stop'
    assert restored.status()['stop_reason']=='sqlite_page_cap'
    assert not restored.observe('A',book(1005),context(),'d'*64)
    assert restored.accepted==before
    with pytest.raises(sqlite3.IntegrityError):restored.db.execute('DELETE FROM comparison_inputs')
    restored.close()


def test_noncapacity_sqlite_fault_is_not_called_capacity(tmp_path):
    study,now=setup(tmp_path)
    study.db.execute("CREATE TRIGGER reject_input BEFORE INSERT ON comparison_inputs BEGIN SELECT RAISE(ABORT,'fixture fault'); END")
    with pytest.raises(sqlite3.IntegrityError):study.observe('A',book(),context(),'a'*64)
    assert not study.db.in_transaction
    assert study.status()['state']=='storage_fault'
    assert study.status()['stop_reason']=='sqlite_fault'
    assert study.accepted==0
    study.close()
