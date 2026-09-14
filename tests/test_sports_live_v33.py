from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
import sqlite3
import pytest
from opportunity_lab.sports_live_books_v33 import Book, StreamBooks, auth_headers
from opportunity_lab.sports_live_coverage import core_markets, polling_batch, admission
from opportunity_lab.sports_live_execution import initial, decide
from opportunity_lab.sports_live_sync_v31 import audit
from opportunity_lab.sports_live_timing import Latency, schema, signal, update_markouts, expire_markouts
from opportunity_lab.sports_live_v33 import record_ok, market_view, observation, probe_step, open_db

NOW=datetime(2026,9,12,20,tzinfo=timezone.utc)


def obs(at=NOW, book_id=1, size=10):
    return {'snapshot':str(book_id),'latency_seconds':.5,'admission_ok':True,
            'model':{'probabilities':{'home':.85},'blockers':[],'completed':False},
            'markets':{'home':{'ticker':'E-H','valid':True,'quote':{'bid':.49,'ask':.50,'bid_size':size,'ask_size':size},
                'book_id':book_id,'book_received_at':at.isoformat(),'fee_coefficient':.07,'settlement_ok':False}}}


def enter(size=10):
    s=initial();decide(s,obs(),NOW,300,.04)
    a=decide(s,obs(NOW+timedelta(seconds=1),2,size),NOW+timedelta(seconds=1),300,.04)
    assert a[0]['action']=='buy'
    return s,a


def test_snapshot_delta_complement_and_gap():
    s=StreamBooks()
    snap={'type':'orderbook_snapshot','sid':1,'seq':10,'msg':{'market_ticker':'T','yes_dollars_fp':[['.4','10']],'no_dollars_fp':[['.5','20']]}}
    s.apply(snap,100)
    assert s.books['T'].quote()['ask']==.5
    assert s.books['T'].quote('no')['ask']==.6
    delta={'type':'orderbook_delta','sid':1,'seq':11,'msg':{'market_ticker':'T','side':'yes','price_dollars':'.4','delta_fp':'-5'}}
    s.apply(delta,101);assert s.books['T'].quote()['bid_size']==5
    delta['seq']=13
    with pytest.raises(ValueError,match='sequence_gap'):s.apply(delta,102)
    assert s.books['T'].quote() is None


@pytest.mark.parametrize('yes,no', [([['NaN',1]],[['.4',1]]),([['.7',1]],[['.4',1]]),([['.4',-1]],[['.4',1]])])
def test_rejects_bad_book(yes,no):
    with pytest.raises(ValueError):Book().snapshot(yes,no,0,1)


def test_delta_requires_snapshot_and_nonnegative_size():
    b=Book()
    with pytest.raises(ValueError):b.delta('yes','.4',1,0,1)
    b.snapshot([['.4',1]],[['.4',1]],0,1)
    with pytest.raises(ValueError):b.delta('yes','.4',-2,1,2)


def test_auth_signature_matches_kalshi_message(tmp_path):
    import base64
    from cryptography.hazmat.primitives.asymmetric import rsa,padding
    from cryptography.hazmat.primitives import hashes,serialization
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    path=tmp_path/'test.key'
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    h=auth_headers('test-key',path,123)
    key.public_key().verify(base64.b64decode(h['KALSHI-ACCESS-SIGNATURE']),b'123GET/trade-api/ws/v2',
       padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())


def test_core_families_and_held_market_preserved():
    def c(series,family,volume):return {'game':{'league':'ncaaf','event_id':'1'},'league':'ncaaf','series':series,'family':family,'market':{'status':'active','volume':volume}}
    cache={'h':c('KXNCAAFGAME','winner',1),'a':c('KXNCAAFGAME','winner',1),
           't1':c('KXNCAAFTOTAL','total',1),'t2':c('KXNCAAFTOTAL','total',10),'period':c('KXNCAAF1HTOTAL','total',100)}
    selected,missing=core_markets(cache,{'ncaaf_1'},['held'])
    assert selected=={'held','h','a','t2'} and missing['ncaaf_1']==['spread']


def test_exit_priority_and_new_coverage_reserved():
    chosen=polling_batch(['a','b','c','d','e'],{'a','b','c','d'}, {},100,4)
    assert chosen==['a','b','c','e']
    assert not admission(6,1,1) and not admission(1,6,1) and not admission(1,1,20)
    assert admission(1,1,1)


def test_old_play_is_not_rejuvenated_by_fresh_transport():
    m={'details':{'play_age_seconds':120},'probabilities':{'home':.7},'blockers':['old_or_inconsistent_play_timestamp']}
    a=audit({},m,NOW.isoformat(),NOW,{})
    assert a['classification']=='fresh_transport_old_play'
    assert m['blockers']==['old_or_inconsistent_play_timestamp']
    audit({},m,None,NOW,{})
    assert 'live_game_transport_over_15_seconds' in m['blockers']


def test_period_mismatch_stays_blocked():
    m={'details':{'situation':{'period':{'number':1}}},'probabilities':{},'blockers':[]}
    audit({'header':{'competitions':[{'status':{'period':2}}]}},m,NOW.isoformat(),NOW,{})
    assert 'probability_period_mismatch' in m['blockers']


def test_order_needs_post_arrival_fresh_book():
    s=initial();decide(s,obs(),NOW,300,.04)
    assert decide(s,obs(NOW+timedelta(seconds=.2),2),NOW+timedelta(seconds=.3),300,.04)[0]['action']=='wait'
    assert decide(s,obs(NOW+timedelta(seconds=.2),3),NOW+timedelta(seconds=1),300,.04)[0]['reason']=='waiting_post_arrival_book'
    assert decide(s,obs(NOW+timedelta(seconds=1),4),NOW+timedelta(seconds=7),300,.04)[0]['reason']=='post_arrival_book_stale'
    assert s['entries']==0


def test_partial_entry_cancels_unfilled_quantity():
    s,a=enter(3)
    assert s['position']['count']==3 and a[0]['unfilled_canceled']==7
    assert s['cash_cents']==100000-s['position']['cost']['total']


def test_partial_exit_cost_basis_and_account_conservation():
    s,_=enter();original=s['position']['cost']['total']
    at=NOW+timedelta(seconds=302)
    assert decide(s,obs(at,3,3),at,300,.04)[0]['action']=='stage_sell'
    a=decide(s,obs(at+timedelta(seconds=1),4,3),at+timedelta(seconds=1),300,.04)[0]
    assert a['action']=='partial_sell' and s['position']['count']==7
    assert s['position']['cost']['total']+a['position']['cost']['total']==original
    at+=timedelta(seconds=2);decide(s,obs(at,5),at,300,.04)
    a=decide(s,obs(at+timedelta(seconds=1),6),at+timedelta(seconds=1),300,.04)[0]
    assert a['action']=='sell' and s['position'] is None and s['exits']==1
    assert s['cash_cents']-100000==s['realized_cents']


def test_no_entry_without_service_capacity_or_signal():
    for key in ('admission','signal'):
        s=initial();o=obs()
        if key=='admission':o['admission_ok']=False
        else:o['model']['blockers']=['stale']
        assert decide(s,o,NOW,300,.04)[0]['action']=='wait'
        assert s['pending'] is None


def test_latency_uses_measured_samples():
    l=Latency();assert l.estimate() is None
    for x in [.1,.2,.3,.4,.7]:l.add(x)
    assert l.estimate()==.7
    l.add(float('nan'));assert len(l.samples)==5


def test_markouts_never_use_pre_target_or_missing_quotes():
    db=sqlite3.connect(':memory:');schema(db)
    m=obs()['markets']['home'];signal(db,'g','T','p1',100,m,.85,True)
    update_markouts(db,'T',104,m);assert db.execute('select count(*) from markouts').fetchone()[0]==0
    m=deepcopy(m);m['book_id']=2
    update_markouts(db,'T',106,m)
    row=json.loads(db.execute('select detail from markouts where seconds=5').fetchone()[0])
    assert row['status']=='observed' and row['modeled_net_cents']<0
    expire_markouts(db,170)
    assert db.execute('select count(*) from markouts').fetchone()[0]==4
    assert all(json.loads(r[0])['status']=='missing' for r in db.execute('select detail from markouts where seconds!=5'))


def test_transport_and_book_staleness():
    r={'response':{'headers':{},'error':None,'status':200,'duration':.2,'received_at':NOW.isoformat()}}
    assert record_ok(r,NOW) and not record_ok(r,NOW+timedelta(seconds=16))
    assert not market_view('missing',{}, {},NOW)['valid']


def test_paper_only_protocol_gate(tmp_path):
    with pytest.raises(ValueError,match='paper_only'):
        open_db(tmp_path,{'version':'sports_live_3.3','execution_enabled':True})


def test_probe_entry_and_exit_have_distinct_arrival_quotes():
    p={'state':'staged','stage_at':NOW.timestamp(),'latency':.5,'ask':.5,'book_id':1,'horizon':300}
    m=obs(NOW+timedelta(seconds=1),2)['markets']['home']
    probe_step(p,m,NOW+timedelta(seconds=1),.5);assert p['state']=='open'
    probe_step(p,m,NOW+timedelta(seconds=302),.5);assert p['state']=='exit_staged'
    m=obs(NOW+timedelta(seconds=303),3)['markets']['home']
    probe_step(p,m,NOW+timedelta(seconds=303),.5)
    assert p['state']=='closed' and p['net_cents']<0 and p['exit_delay_seconds']==2


def test_pending_expires_without_any_new_book():
    s=initial();decide(s,obs(),NOW,300,.04)
    assert decide(s,obs(),NOW+timedelta(seconds=31),300,.04)[0]['reason']=='confirmation_expired'
    assert s['pending'] is None


def test_nfl_stricter_spread_guard_preserved():
    s=initial();o=obs();m=o['markets']['home']
    m['quote']['bid']=.465;m['max_spread']=.03
    assert decide(s,o,NOW,300,.08)[0]['action']=='wait'


def test_runtime_offline_bootstrap_and_reports(tmp_path, monkeypatch):
    import hashlib
    import opportunity_lab.sports_live_v33 as live
    from opportunity_lab.sports_live_report_v33 import report
    monkeypatch.setattr(live,'utcnow',lambda:NOW)
    monkeypatch.setattr(live,'request',lambda url:{'raw':b'{}','received_at':NOW.isoformat(),
        'started_at':NOW.isoformat(),'duration':.1,'headers':{},'status':200,'error':None,'url':url})
    c={'league':'ncaaf','event_id':'1','game':'Away at Home','home_team_id':'h','away_team_id':'a',
       'kickoff':(NOW-timedelta(minutes=1)).isoformat(),'stop_at':(NOW+timedelta(hours=1)).isoformat(),
       'markets':{'home':'E-H','away':'E-A'},'market_event':'E','mapping_evidence':{}}
    source=tmp_path/'source';source.write_text('frozen fixture')
    config={'version':'sports_live_3.3','execution_enabled':False,'games':[c],
        'source_sha256':{str(source):hashlib.sha256(source.read_bytes()).hexdigest()},
        'stop_at':c['stop_at'],'metadata_paths':[],'max_pending':40,'max_probe_pending':12,'entry_margin':{'ncaaf':.04}}
    path=tmp_path/'manifest.json';path.write_text(json.dumps(config));output=tmp_path/'out'
    live.run(path,output,once=True)
    status=json.loads((output/'status.json').read_text())
    assert not status['errors'] and status['games'][0]['accounts']['300']['entries']==0
    report(output)
    assert (output/'games/ncaaf_1/report.json').exists()


@pytest.mark.parametrize('summary', [{}, {'situation':None}, {'situation':{'possession':None}}])
def test_nullable_period_and_possession_are_unknown(summary):
    m={'details':{'situation':{'period':None}},'probabilities':{},'blockers':['play_timestamp_missing']}
    result=audit(summary,m,NOW.isoformat(),NOW,{})
    assert result['state']['possession'] is None
    assert 'play_timestamp_missing' in m['blockers']


@pytest.mark.parametrize('sides', [{}, {'no_dollars_fp':[['.5','20']]}, {'yes_dollars_fp':None,'no_dollars_fp':[]}])
def test_empty_stream_side_is_not_a_disconnect_or_fill(sides):
    b=StreamBooks()
    b.apply({'type':'orderbook_snapshot','sid':1,'seq':1,'msg':{'market_ticker':'T',**sides}},100)
    assert b.books['T'].quote() is None
    b.apply({'type':'orderbook_snapshot','sid':1,'seq':2,'msg':{'market_ticker':'U','yes_dollars_fp':[['.4',10]],'no_dollars_fp':[['.5',10]]}},101)
    assert b.books['U'].quote()['ask']==.5
