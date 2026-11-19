from datetime import datetime,timezone,timedelta
import hashlib,json,sqlite3,time
import pytest
from opportunity_lab import kalshi_wallet_production as p
from opportunity_lab.kalshi_wallet_copy_worker import WalletCopy,client_order_id
from opportunity_lab.kalshi_wallet_monitor import trade_key
from test_kalshi_binary_journal import Exchange,quote


def grant(now):
    return dict(schema='kalshi_wallet_live_grant_v1',approval_id='owner-wallet-test',policy_hash=p.POLICY_HASH,
        account_key_sha256=hashlib.sha256(b'production-test').hexdigest(),evidence_sha256='a'*64,
        expires_at=(now+timedelta(days=1)).isoformat(),limits=dict(p.LIMITS))


class ProductionExchange(Exchange,p.WalletProductionClient):
    def __init__(self,journal):Exchange.__init__(self,journal);self.key_id='production-test'


def setup(tmp_path):
    now=datetime.now(timezone.utc);db=sqlite3.connect(':memory:',isolation_level=None)
    approved=[True]
    j=p.WalletProductionJournal(tmp_path/'production.db',grant(now),signals=db,clock=lambda:now,approval_verifier=lambda _:approved[0])
    x=ProductionExchange(j);broker=p.WalletProductionBroker(j,x)
    worker=p.ProductionWalletCopy(tmp_path,db,broker,clock=lambda:now.timestamp())
    db.execute('INSERT INTO cohort VALUES(?,?)',(now.date().isoformat(),json.dumps(['0x'+'1'*40])))
    return now,db,j,x,broker,worker,approved


def signal(db,now,side,outcome='yes',wallet='0x'+'1'*40,number=1):
    trade=dict(proxy_wallet=wallet,transaction_hash='0x'+f'{number:064x}',type='TRADE',condition_id='0x'+'a'*64,
        token_id='123',timestamp=int(now.timestamp()),side=side,price='.50',size='1')
    mapping=dict(ticker='GAME',side=outcome,event_ticker='EVENT')
    key=trade_key(trade);cid=client_order_id(key)
    db.execute('INSERT INTO signals VALUES(?,?,?,?,?,?)',(key,trade['timestamp'],side,json.dumps(trade),'submitting',json.dumps(mapping)))
    return key,cid


def production_quote(j,cid,key):
    q=quote(j,cid);q.update(environment='production',wallet_signal_id=key,fee_basis={'fee_multiplier':'1',
        'schema':'kalshi_fee_basis_probe_v1','schedule_id':p.POLICY['fee_schedule'],'event_ticker':'EVENT','fee_type':'quadratic'},
        market={'ticker':'GAME','event_ticker':'EVENT','market_type':'binary','status':'active'})
    return q


@pytest.mark.parametrize('outcome',['yes','no'])
def test_production_buy_restart_owned_sell_after_revocation(outcome,tmp_path):
    now,db,j,x,b,w,approved=setup(tmp_path)
    key,cid=signal(db,now,'BUY',outcome)
    j.reserve(cid,'GAME',1,50,2,outcome=outcome,action='buy',account_snapshot=b.snapshot())
    assert b.submit(cid,quote_snapshot=production_quote(j,cid,key))['filled']==1
    db.execute("UPDATE signals SET state='filled' WHERE id=?",(key,))
    for index in range(9):
        db.execute("INSERT INTO signals VALUES(?,?,'BUY','{}','ioc_no_fill',NULL)",(f'budget-{index}',int(now.timestamp())))
    path=j.path if hasattr(j,'path') else tmp_path/'production.db'
    j.close();approved[0]=False
    j=p.WalletProductionJournal(path,grant(now),signals=db,clock=lambda:now,approval_verifier=lambda _:approved[0])
    x.j=j;b=p.WalletProductionBroker(j,x)
    key,cid=signal(db,now,'SELL',outcome,number=2)
    j.reserve(cid,'GAME',1,50,2,outcome=outcome,action='sell',account_snapshot=b.snapshot())
    assert b.submit(cid,quote_snapshot=production_quote(j,cid,key))['filled']==1
    assert j.accounting()['positions']=={}
    assert x.posts==2
    assert b.reconcile_positions()['positions_match']
    j.close();db.close()


def test_missing_wallet_authority_never_initializes_broker_or_journal(tmp_path,monkeypatch):
    from opportunity_lab import kalshi_live_worker
    class NoGrant:
        def __init__(self,*args):pass
        def fetch(self):return None
        def report(self,result):pass
    monkeypatch.setattr(kalshi_live_worker,'OwnerAuthority',NoGrant)
    result=p.run(tmp_path,once=True)
    assert result['state']=='awaiting_owner_grant'
    assert result['new_entries'] is False
    assert not (tmp_path/'wallet-production-v1'/'journal.sqlite3').exists()


@pytest.mark.parametrize('failure',['revoked','foreign_owner','demo_quote','stale_quote','changed_source','fee_increase','unknown_write'])
def test_live_guards_never_retry_unknown_or_accept_bad_evidence(failure,tmp_path):
    now,db,j,x,b,w,approved=setup(tmp_path)
    key,cid=signal(db,now,'BUY')
    if failure=='revoked':approved[0]=False
    if failure=='foreign_owner':
        with pytest.raises(ValueError,match='owner_mismatch'):
            key,cid=signal(db,now,'SELL',wallet='0x'+'2'*40,number=2)
            j.reserve(cid,'GAME',1,50,2,outcome='yes',action='sell',account_snapshot=b.snapshot())
    elif failure=='revoked':
        with pytest.raises(ValueError,match='revoked'):
            j.reserve(cid,'GAME',1,50,2,outcome='yes',action='buy',account_snapshot=b.snapshot())
    else:
        j.reserve(cid,'GAME',1,50,2,outcome='yes',action='buy',account_snapshot=b.snapshot())
        q=production_quote(j,cid,key)
        if failure=='demo_quote':q['environment']='demo'
        if failure=='stale_quote':q['observed_at']-=30
        if failure=='fee_increase':q['fee_basis']['fee_multiplier']='10'
        if failure=='changed_source':
            trade=json.loads(db.execute('SELECT trade FROM signals WHERE id=?',(key,)).fetchone()[0]);trade['price']='.49'
            db.execute('UPDATE signals SET trade=? WHERE id=?',(json.dumps(trade),key))
        if failure=='unknown_write':x.timeout=True
        with pytest.raises((ValueError,TimeoutError)):b.submit(cid,quote_snapshot=q)
        if failure=='unknown_write':
            assert j.get(cid)['state']=='uncertain'
            assert w.quarantine_unmatched_entry(j.get(cid)) is False
            with pytest.raises(ValueError):b.submit(cid,quote_snapshot=q)
            assert x.posts==1
        else:assert x.posts==0
    j.close();db.close()
