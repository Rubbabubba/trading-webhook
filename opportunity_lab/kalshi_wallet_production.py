"""Isolated ranked-wallet production adapter; dormant without exact owner authority.

The Demo copier, inventory exceptions and negative-read quarantine are never
production authority. All mutations retain the binary journal's unknown-write
reservation, broker reconciliation and reduce-only owned sell semantics.
"""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import re
import sqlite3
from pathlib import Path

from .kalshi_binary_journal import BinaryJournal
from .kalshi_order_journal import Journal
from .kalshi_demo_broker import DemoClient
from .kalshi_live_execution import LiveBinaryJournal, LiveBinaryBroker, LiveClient, grant_hash
from .kalshi_wallet_copy_worker import WalletCopy, signal_key, executable, source_identity
from .kalshi_wallet_monitor import _read_quote, public_get, trade_key
from .kalshi_live_worker import OwnerAuthority

LIMITS = dict(capital_cents=10000, daily_loss_cents=500, total_loss_cents=1000,
              event_exposure_cents=210, portfolio_exposure_cents=525,
              order_debit_cents=105, order_contracts=1, max_positions=5,
              max_daily_buys=10, quote_age_seconds=5)
POLICY = dict(version='ranked_wallet_production_v1', signal_seconds=90,
              price_tolerance_cents=5, limits=LIMITS, ownership='wallet_condition_token',
              sizing='one_contract_ioc', unknown_write='reconcile_never_retry',
              fee_schedule='kalshi_binary_taker_2026-07-07_provisional',
              fee_schedule_reference='https://kalshi.com/docs/kalshi-fee-schedule.pdf')
POLICY_HASH = grant_hash(POLICY)


class WalletOwnerAuthority(OwnerAuthority):
    def __init__(self,url,token):
        from urllib.parse import urlparse
        parsed=urlparse(url)
        if (parsed.scheme!='https' or parsed.netloc!='personal-life-os-ia5e.onrender.com'
                or parsed.path!='/execution/kalshi/wallet/grant' or parsed.query or parsed.fragment):
            raise ValueError('dedicated_wallet_owner_authority_required')
        super().__init__(parsed._replace(path='/execution/kalshi/grant').geturl(),token)
        self.url=url

    def qualified(self,wallet):
        from urllib.request import Request
        from urllib.parse import urlparse
        from .wallet_qualification import validate_report
        try:
            url=urlparse(self.url)._replace(path="/execution/kalshi/wallet/selection").geturl()
            with self.opener.open(Request(url,headers={"Authorization":"Bearer "+self.token}),timeout=5) as response:raw=response.read(16001)
            if len(raw)>16000:return False
            value=json.loads(raw)
            if value.get("schema")!="kalshi_wallet_selection_v1":return False
            return wallet in validate_report(value.get("research"))["selected_wallets"]
        except Exception:return False

    def report(self,value):
        # Retain detailed wallet lifecycle locally; the hosted production
        # status protocol accepts the bounded account/strategy summary.
        return super().report({k:v for k,v in value.items() if k!='wallet_copy'})


def validate(grant):
    fields={'schema','approval_id','policy_hash','account_key_sha256','evidence_sha256','expires_at','limits'}
    if (not isinstance(grant,dict) or set(grant)!=fields
            or grant['schema']!='kalshi_wallet_live_grant_v1'
            or grant['policy_hash']!=POLICY_HASH or grant['limits']!=LIMITS
            or not re.fullmatch(r'[A-Za-z0-9_.:-]{4,120}',str(grant['approval_id']))):
        raise ValueError('exact_wallet_production_grant_required')
    for field in ('account_key_sha256','evidence_sha256'):
        if not re.fullmatch('[a-f0-9]{64}',str(grant[field])):raise ValueError('wallet_grant_binding_required')
    expiry=datetime.fromisoformat(grant['expires_at'].replace('Z','+00:00'))
    if expiry.tzinfo is None:raise ValueError('wallet_grant_expiry_required')
    return grant


class WalletProductionJournal(LiveBinaryJournal):
    def __init__(self,path,grant,*,signals,approval_verifier,clock=None,qualification_verifier=None):
        self.grant=validate(grant);self.signals=signals;self.approval_verifier=approval_verifier
        self.live_limits=grant['limits'];self._signal_context=None
        self.qualification_verifier=qualification_verifier or (lambda wallet:False)
        # An existing exact journal may manage owned exits after revocation.
        existing=Path(path).exists()
        if not callable(approval_verifier):raise ValueError('wallet_authority_verifier_required')
        if approval_verifier(grant_hash(grant)) is not True:
            if not existing:raise ValueError('verified_owner_approval_required')
            with sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True) as db:
                row=db.execute('SELECT detail FROM wallet_live_grant WHERE id=1').fetchone()
                if not row or json.loads(row[0])!=grant:raise ValueError('existing_wallet_recovery_grant_required')
        BinaryJournal.__init__(self,path,clock=clock,order_limit_cents=LIMITS['order_debit_cents'],
            capital_limit_cents=LIMITS['portfolio_exposure_cents'],daily_loss_cents=LIMITS['daily_loss_cents'])
        self.db.executescript('CREATE TABLE IF NOT EXISTS wallet_live_grant(id INTEGER PRIMARY KEY,detail TEXT NOT NULL);'
            'CREATE TABLE IF NOT EXISTS wallet_live_scope(client_id TEXT PRIMARY KEY,signal_id TEXT NOT NULL,event_id TEXT NOT NULL,ticker TEXT NOT NULL,action TEXT NOT NULL,reserve INTEGER NOT NULL);')
        encoded=json.dumps(grant,sort_keys=True)
        self.db.execute('INSERT OR IGNORE INTO wallet_live_grant VALUES(1,?)',(encoded,))
        if self.db.execute('SELECT detail FROM wallet_live_grant WHERE id=1').fetchone()[0]!=encoded:
            self.close();raise ValueError('wallet_live_grant_changed')

    def bind_environment(self,environment):
        if environment!='production':raise ValueError('wallet_production_journal_required')
        return Journal.bind_environment(self,environment)

    def source(self,cid):
        key=signal_key(cid)
        row=self.signals.execute('SELECT trade,mapping,state FROM signals WHERE id=?',(key,)).fetchone()
        if not row or not row[1] or row[2]!='submitting':raise ValueError('persisted_wallet_signal_required')
        trade,mapping=json.loads(row[0]),json.loads(row[1])
        if trade_key(trade)!=key or not 0<=self.clock().timestamp()-trade['timestamp']<=90:
            raise ValueError('wallet_signal_changed_or_expired')
        return key,trade,mapping

    def reserve(self,cid,ticker,contracts,price_cents,fee_reserve_cents,*,outcome,action,account_snapshot,order_mode='ioc'):
        if contracts!=1 or order_mode!='ioc':raise ValueError('wallet_one_contract_ioc_required')
        key,trade,mapping=self.source(cid)
        if (mapping['ticker']!=ticker or mapping['side']!=outcome or trade['side'].lower()!=action
                or not mapping.get('event_ticker')):raise ValueError('wallet_signal_order_scope_changed')
        if action=='sell':
            owners=[]
            for record in self.records():
                if record['intent']['action']=='buy' and record['filled'] and record['payload']['ticker']==ticker:
                    old=self.signals.execute('SELECT trade FROM signals WHERE id=?',(signal_key(record['payload']['client_order_id']),)).fetchone()
                    if old:
                        entry=json.loads(old[0])
                        if source_identity(entry)==source_identity(trade) and entry['timestamp']<=trade['timestamp']:owners.append(entry)
            active=[r for r in self.records() if r['intent']['action']=='buy' and r['filled'] and r['payload']['ticker']==ticker]
            latest=self.signals.execute('SELECT trade FROM signals WHERE id=?',
                (signal_key(active[-1]['payload']['client_order_id']),)).fetchone() if active else None
            if (not self.accounting()['positions'].get(ticker) or not latest
                    or source_identity(json.loads(latest[0]))!=source_identity(trade)
                    or not owners):raise ValueError('wallet_sell_owner_mismatch')
        self._signal_context=(cid,key,mapping['event_ticker'],ticker,action,price_cents+fee_reserve_cents)
        try:
            return BinaryJournal.reserve(self,cid,ticker,contracts,price_cents,fee_reserve_cents,
                outcome=outcome,action=action,account_snapshot=account_snapshot,order_mode=order_mode)
        finally:self._signal_context=None

    def _gate(self,intent,ticker,snapshot,*,exclude=None):
        if snapshot.get('account_key_sha256')!=self.grant['account_key_sha256']:
            raise ValueError('wallet_account_binding_mismatch')
        context=self._signal_context
        if context is None and exclude:
            row=self.db.execute('SELECT client_id,signal_id,event_id,ticker,action,reserve FROM wallet_live_scope WHERE client_id=?',(exclude,)).fetchone()
            context=tuple(row) if row else None
        if context is None:raise ValueError('wallet_signal_scope_missing')
        records=[r for r in self.records() if r['payload']['client_order_id']!=exclude]
        if any(r['state']!='terminal' for r in records):raise ValueError('wallet_serial_reconciliation_required')
        state=self.accounting(exclude=exclude)
        if intent['action']=='buy':
            if (self.approval_verifier(grant_hash(self.grant)) is not True
                    or self.clock()>=datetime.fromisoformat(self.grant['expires_at'].replace('Z','+00:00'))):
                raise ValueError('wallet_entry_authority_expired_or_revoked')
            _,trade,_=self.source(context[0])
            if self.qualification_verifier(trade['proxy_wallet']) is not True:raise ValueError('wallet_not_profit_qualified')
            cohort=self.signals.execute('SELECT wallets FROM cohort WHERE day=?',(self.clock().date().isoformat(),)).fetchone()
            if not cohort or trade['proxy_wallet'] not in json.loads(cohort[0]):raise ValueError('wallet_current_cohort_required')
            since=self.clock().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
            attempts=self.signals.execute("SELECT count(*) FROM signals s LEFT JOIN buy_attempts a ON a.signal_id=s.id "
                "WHERE s.side='BUY' AND s.id!=? AND coalesce(a.attempted_at,s.at)>=? "
                "AND s.state IN ('filled','ioc_no_fill','submitting','submission_unresolved')",(context[1],since)).fetchone()[0]
            if attempts>=LIMITS['max_daily_buys']:raise ValueError('wallet_daily_attempt_limit')
            if not records and snapshot['balance']['balance']<LIMITS['capital_cents']:
                raise ValueError('wallet_production_funding_required')
            if len(state['positions'])>=LIMITS['max_positions']:raise ValueError('wallet_position_limit')
            risk=state['capital_at_risk']*100+context[5]
            if (risk>LIMITS['daily_loss_cents']+min(0,state['daily_low']*100)
                    or risk>LIMITS['total_loss_cents']+min(0,state['realized']*100)):
                raise ValueError('wallet_loss_budget_exhausted')
            event_risk=sum(r[0] for r in self.db.execute("SELECT reserve FROM wallet_live_scope WHERE event_id=? AND action='buy' AND client_id!=?",(context[2],context[0])))
            if event_risk+context[5]>LIMITS['event_exposure_cents']:raise ValueError('wallet_event_exposure_limit')
        reserve=BinaryJournal._gate(self,intent,ticker,snapshot,exclude=exclude)
        self.db.execute('INSERT OR IGNORE INTO wallet_live_scope VALUES(?,?,?,?,?,?)',context)
        return reserve

    def mark_submission_started(self,cid,*,account_snapshot=None,quote_snapshot=None,demo_probe=False):
        key,trade,mapping=self.source(cid)
        if demo_probe or not isinstance(quote_snapshot,dict) or quote_snapshot.get('environment')!='production':
            raise ValueError('wallet_production_quote_required')
        price=executable(trade,mapping,quote_snapshot);record=self.get(cid)
        fee=quote_snapshot.get('fee_basis') or {}
        if (fee.get('schema')!='kalshi_fee_basis_probe_v1' or fee.get('schedule_id')!=POLICY['fee_schedule']
                or fee.get('event_ticker')!=mapping['event_ticker'] or fee.get('fee_type')!='quadratic'):
            raise ValueError('wallet_fresh_fee_identity_required')
        reserve=int((Decimal('1.75')*Decimal(str(fee['fee_multiplier']))).to_integral_value(rounding=ROUND_CEILING))
        if (quote_snapshot.get('wallet_signal_id')!=key or quote_snapshot['market'].get('event_ticker')!=mapping['event_ticker']
                or reserve>record['intent']['fee_cents']
                or (price>record['intent']['price_cents'] if trade['side']=='BUY' else price<record['intent']['price_cents'])):
            raise ValueError('wallet_fresh_quote_or_fee_changed')
        return BinaryJournal.mark_submission_started(self,cid,account_snapshot=account_snapshot,quote_snapshot=quote_snapshot)


class WalletProductionClient(LiveClient):
    def __init__(self,key_id,key_path,grant,*,approval_verifier,demo_key_id=None,recovery_only=False,**kwargs):
        validate(grant)
        if key_id==demo_key_id or hashlib.sha256(key_id.encode()).hexdigest()!=grant['account_key_sha256']:
            raise ValueError('separate_wallet_production_key_required')
        if not callable(approval_verifier):raise ValueError('wallet_authority_verifier_required')
        if not recovery_only and approval_verifier(grant_hash(grant)) is not True:
            raise ValueError('verified_owner_approval_required')
        DemoClient.__init__(self,key_id,key_path,**kwargs)


class WalletProductionBroker(LiveBinaryBroker):
    def __init__(self,journal,client):
        if not isinstance(journal,WalletProductionJournal) or not isinstance(client,WalletProductionClient):
            raise ValueError('isolated_wallet_production_broker_required')
        super().__init__(journal,client)


class ProductionWalletCopy(WalletCopy):
    max_submissions=10  # Production approval remains ten submissions, never the Demo research allowance.
    market_base=WalletProductionClient.BASE_URL

    def __init__(self,root,db,broker,*,read=public_get,clock=None):
        if not isinstance(broker,WalletProductionBroker):raise ValueError('wallet_production_broker_required')
        kwargs={'read':read}
        if clock is not None:kwargs['clock']=clock
        super().__init__(root,db,broker,**kwargs)

    def excluded_tickers(self):return set()  # No Demo inventory exceptions exist in production.

    def quote(self,mapping):return _read_quote(mapping,self.market_base,self.read,environment='production')

    def require_demo_hold(self):
        # This hook is called for owned sells as well as buys. Journal admission
        # requires fresh authority for buys; revocation never authorizes a blind
        # sell but still permits a reconciled, matching owned-position reduction.
        validate(self.journal.grant)

    def quarantine_unmatched_entry(self,record):return False


def run(root,*,once=False):
    """Separate deployment entrypoint. Missing/old strategy grants send no orders."""
    import os,time
    from .kalshi_live_worker import write_json,ProductionMarkets
    from .kalshi_process_lock import acquire
    root=Path(root)/'wallet-production-v1';root.mkdir(parents=True,exist_ok=True)
    authority=WalletOwnerAuthority(os.environ.get('LIFE_OS_KALSHI_WALLET_GRANT_URL',
        'https://personal-life-os-ia5e.onrender.com/execution/kalshi/wallet/grant'),os.environ.get('LIFE_OS_KALSHI_LIVE_TOKEN',''))
    with acquire(root/'worker.lock'):
        worker=journal=db=None;last_preflight=0
        try:
            while True:
                try:
                    if worker is None:
                        cached=root/'owner-grant.json'
                        grant=json.loads(cached.read_text()) if cached.exists() else authority.fetch()
                        if grant is None:
                            if (os.environ.get('KALSHI_LIVE_API_KEY_ID') and os.environ.get('KALSHI_LIVE_PRIVATE_KEY_PATH')
                                    and time.monotonic()-last_preflight>=60):
                                last_preflight=time.monotonic()
                                from .kalshi_live_preflight import run as preflight
                                proof=preflight(os.environ['KALSHI_LIVE_API_KEY_ID'],os.environ['KALSHI_LIVE_PRIVATE_KEY_PATH'],ProductionMarkets(),
                                    progress_file=root/'preflight-catalog.json')
                                write_json(root/'preflight.json',proof);authority.report(proof)
                            result={'state':'awaiting_owner_grant','new_entries':False}
                        else:
                            validate(grant)
                            db=sqlite3.connect(root/'signals.sqlite3',isolation_level=None)
                            recovering=(root/'journal.sqlite3').exists()
                            journal=WalletProductionJournal(root/'journal.sqlite3',grant,signals=db,approval_verifier=authority.verify,qualification_verifier=authority.qualified)
                            client=WalletProductionClient(os.environ.get('KALSHI_LIVE_API_KEY_ID',''),os.environ.get('KALSHI_LIVE_PRIVATE_KEY_PATH',''),
                                grant,approval_verifier=authority.verify,recovery_only=recovering,demo_key_id=os.environ.get('KALSHI_DEMO_API_KEY_ID'))
                            worker=ProductionWalletCopy(root,db,WalletProductionBroker(journal,client))
                            write_json(cached,grant)
                            result={'state':'running','new_entries':False,'wallet_copy':worker.tick()}
                    else:result={'state':'running','new_entries':False,'wallet_copy':worker.tick()}
                    if worker:
                        result['new_entries']=authority.verify(grant_hash(journal.grant)) and datetime.now(timezone.utc)<datetime.fromisoformat(journal.grant['expires_at'].replace('Z','+00:00'))
                        result.update(open_positions=len(journal.accounting()['positions']),unresolved_orders=sum(r['state']!='terminal' for r in journal.records()))
                        ledger=journal.accounting()
                        result.update(net_pnl_cents=round(float(ledger['realized']*100),2),fees_cents=round(float(ledger['fees']*100),2))
                except Exception as error:
                    result={'state':'blocked','new_entries':False,'error_type':type(error).__name__}
                    if worker is None:
                        if journal:journal.close();journal=None
                        if db:db.close();db=None
                result.update(schema='kalshi_live_status_v1',environment='production',strategy_id=POLICY['version'],
                    generated_at=datetime.now(timezone.utc).isoformat(),ai_tokens=0,ai_cost_usd=0)
                write_json(root/'status.json',result)
                try:authority.report(result)
                except Exception:pass
                if once:return result
                time.sleep(8)
        finally:
            if journal:journal.close()
            if db:db.close()


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser();parser.add_argument('--root',required=True);parser.add_argument('--once',action='store_true')
    args=parser.parse_args();run(args.root,once=args.once)
