import hashlib
import json
import sqlite3

import pytest

from opportunity_lab import kalshi_collection_recovery as recovery
from opportunity_lab.kalshi_continuing_capture import Recorder
from opportunity_lab import kalshi_demo_stream_worker_v4 as worker
from opportunity_lab.kalshi_demo_stream_worker import ARCHIVE_NAME as OLD_ARCHIVE
from opportunity_lab.kalshi_stream_formula_comparison_v3 import Comparison
from opportunity_lab.kalshi_comparison_deadlines import comparison_status


def test_start_is_durable_and_never_resets_allowance(tmp_path):
    p=tmp_path/'start.json'
    assert recovery.lifetime_start(p,lambda:1000)==1000
    assert recovery.lifetime_start(p,lambda:9000)==1000


@pytest.mark.parametrize('at',[float('nan'),-1,1001,'1000'])
def test_invalid_lifetime_receipt_fails_closed(tmp_path,at):
    p=tmp_path/'start.json'
    p.write_text(json.dumps({'protocol':worker.PROTOCOL,'started_at':at}))
    with pytest.raises(ValueError):recovery.lifetime_start(p,lambda:1000)


def test_exhausted_rest_archive_is_readonly_no_more_fault_churn(tmp_path):
    p=tmp_path/'continuing-capture-v1.sqlite3'
    r=Recorder(p,now=1000,free_bytes=lambda:10**9)
    r.fault('capture_accounted_capacity');r.close()
    before=p.read_bytes()
    view=recovery.open_continuing_capture(p)
    view.quote({'not':'a market'});assert view.poll(None) is False
    view.fault('new fault must not mutate evidence');view.close()
    assert view.status()['new_inputs_active'] is False
    assert view.status()['state']=='retained_capacity_stop'
    assert p.read_bytes()==before


def test_retention_hashes_and_successor_paths_do_not_modify_prior_files(tmp_path):
    for n in (OLD_ARCHIVE,'stream-shadow-comparison-v2.sqlite3','continuing-capture-v1.sqlite3'):
        (tmp_path/n).write_bytes(b'old failures retained')
    values=recovery.retained_evidence(tmp_path)
    assert len(values)==3
    assert all(v['sha256']==hashlib.sha256(b'old failures retained').hexdigest() for v in values.values())
    assert worker.ARCHIVE_NAME=='stream-source-v4.sqlite3'
    assert worker.ARCHIVE_NAME!=OLD_ARCHIVE


def test_source_keeps_original48mib_cap_with_early_sealing_reserve(tmp_path):
    a=recovery.RecoveryArchive(tmp_path/'new.sqlite3',free_bytes=lambda:10**9)
    assert a.db.execute('PRAGMA max_page_count').fetchone()[0]==12288
    a.db.execute('CREATE TABLE padding(x BLOB)')
    a.db.execute('INSERT INTO padding VALUES(zeroblob(46137344))');a.db.commit()
    with pytest.raises(ValueError,match='archive_capacity'):a.begin(['A'],1000)
    a.close()


def test_v3_window_expires_truthfully_across_reopen_without_new_marks(tmp_path):
    now=[1000.];p=tmp_path/'comparison-v3.sqlite3'
    c=Comparison(p,clock=lambda:now[0],free_bytes=lambda:10**9)
    now[0]+=72*3600+1
    c.begin_epoch('e')
    assert c.observe('A',{},None,'hash') is False
    assert c.status()['stop_reason']=='admission_time_cap'
    assert comparison_status(c.db,now[0])['stop_reason']=='admission_time_cap'
    c.close()
    c=Comparison(p,clock=lambda:now[0],free_bytes=lambda:10**9)
    assert comparison_status(c.db,now[0])['state']=='capacity_stop'
    assert c.db.execute('SELECT count(*) FROM high_edge_v15_marks').fetchone()[0]==0
    c.close()
