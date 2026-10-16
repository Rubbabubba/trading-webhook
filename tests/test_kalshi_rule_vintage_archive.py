import copy,json,sqlite3
import pytest
from opportunity_lab import kalshi_rule_vintage_archive as archive
from opportunity_lab.kalshi_demo_market_data import DemoMarkets


def quote(at=101,rule='If official X exceeds 5, Yes.'):
    return {'environment':'demo','ticker':'X-E-T5','market_started_at':at,'market_observed_at':at+.1,'started_at':at+2,'observed_at':at+2.1,
            'market':{'ticker':'X-E-T5','event_ticker':'X-E','market_type':'binary','rules_primary':rule,'rules_secondary':'source A','updated_time':'1999-01-01T00:00:00Z'}}


def database():
    db=sqlite3.connect(':memory:',isolation_level=None);archive.init(db,now=100);return db


def test_versions_and_separate_receipts_preserve_change_and_reversion():
    db=database();q=quote();a=archive.capture(db,q)
    assert archive.capture(db,q)==a
    archive.capture(db,quote(110));b=archive.capture(db,quote(120,rule='Different source rule'))
    assert b!=a
    assert archive.capture(db,quote(130))==a
    s=archive.status(db)
    assert s['versions']==2 and s['receipts']==4
    assert db.execute('SELECT first_observed_at FROM rule_archive_versions WHERE version_hash=?',(a,)).fetchone()[0]==101.1
    assert s['historical_publication_verified'] is False


def test_restart_retains_original_rule_text(tmp_path):
    path=tmp_path/'rules.sqlite3';db=sqlite3.connect(path,isolation_level=None);archive.init(db,now=100)
    digest=archive.capture(db,quote());db.close()
    db=sqlite3.connect(path,isolation_level=None);archive.init(db,now=200)
    saved=json.loads(db.execute('SELECT fields_json FROM rule_archive_versions WHERE version_hash=?',(digest,)).fetchone()[0])
    assert saved['rules_primary']==quote()['market']['rules_primary']
    assert archive.status(db)['receipts']==1


@pytest.mark.parametrize('change',[{'market_started_at':99},{'market_observed_at':200},{'market_started_at':float('nan')},{'environment':'prod'},{'ticker':'wrong'}])
def test_prior_or_invalid_clocks_and_identity_never_archive(change):
    db=database()
    with pytest.raises(ValueError):archive.capture(db,quote()|change)
    assert archive.status(db)['receipts']==0


def test_protocol_drift_is_not_reinitialized_or_hidden():
    db=database();db.execute("UPDATE rule_archive_protocol SET detail='{}'")
    with pytest.raises(ValueError,match='protocol_changed'):archive.capture(db,quote())
    assert archive.status(db)['state']=='fault'


def test_capacity_keeps_old_evidence_and_reports_fault(monkeypatch):
    spec=copy.deepcopy(archive.PROTOCOL);spec['limits']['maximum_receipts']=1
    monkeypatch.setattr(archive,'PROTOCOL',spec)
    db=database();archive.capture(db,quote())
    with pytest.raises(ValueError,match='receipt_capacity'):archive.capture(db,quote(110))
    archive.record_fault(db,'receipt_capacity',at=120)
    assert archive.status(db)['state']=='fault' and archive.status(db)['receipts']==1


def test_quote_archives_existing_reads_without_order_or_extra_transport(monkeypatch):
    db=database();client=DemoMarkets();client.rule_archive_db=db;calls=[]
    def get(ticker,book=False,params=None):
        calls.append((ticker,book))
        if book:return {'orderbook_fp':{'yes_dollars':[],'no_dollars':[]}},103,103.1
        return {'market':quote()['market']|{'status':'active'}},101,101.1
    monkeypatch.setattr(client,'get',get)
    result=client.quote({'ticker':'X-E-T5'})
    assert calls==[('X-E-T5',False),('X-E-T5',True)]
    assert result['market_observed_at']==101.1 and result['observed_at']==103.1
    assert archive.status(db)['receipts']==1
    db.execute("UPDATE rule_archive_protocol SET detail='{}'")
    assert client.quote({'ticker':'X-E-T5'})['orderbook_fp']==result['orderbook_fp']
    assert archive.status(db)['state']=='fault'


def test_monitor_exposes_archive_failure():
    from opportunity_lab.kalshi_deterministic_monitor import faults
    assert 'rule_vintage_archive_fault' in faults({'evidence':{'rule_vintage_archive':{'state':'fault'}}},now=100)


def test_version_and_receipt_are_append_only():
    db=database();archive.capture(db,quote())
    for sql in ('DELETE FROM rule_archive_versions','DELETE FROM rule_archive_receipts',"UPDATE rule_archive_versions SET fields_json='{}'",'UPDATE rule_archive_receipts SET metadata_observed_at=0'):
        with pytest.raises(sqlite3.IntegrityError,match='append_only'):db.execute(sql)
    assert archive.status(db)['receipts']==1


def test_failed_receipt_insert_rolls_back_new_version():
    db=database()
    db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON rule_archive_receipts BEGIN SELECT RAISE(ABORT,'test_failure'); END")
    with pytest.raises(sqlite3.IntegrityError,match='test_failure'):archive.capture(db,quote())
    assert archive.status(db)['versions']==0 and archive.status(db)['receipts']==0
