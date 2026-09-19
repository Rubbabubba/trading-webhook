"""Atomic explicit-outcome journal, restricted to demo rehearsals."""
import json
from fractions import Fraction

from .kalshi_order_journal import Journal
from .kalshi_order_direction import terms
from .kalshi_binary_accounting import replay_binary, check_budget, reconcile_positions


class BinaryJournal(Journal):
    environment = 'demo'
    def __init__(self, path, *, clock=None, order_limit_cents=500,
                 capital_limit_cents=5000, daily_loss_cents=1000):
        super().__init__(path, clock=clock)
        if any(type(v) is not int or v<=0 for v in (order_limit_cents,capital_limit_cents,daily_loss_cents)):
            self.close(); raise ValueError('invalid_binary_policy')
        if order_limit_cents>capital_limit_cents:
            self.close(); raise ValueError('invalid_binary_policy')
        self.db.executescript('CREATE TABLE IF NOT EXISTS economic_intents(id TEXT PRIMARY KEY,detail TEXT NOT NULL);'
                             'CREATE TABLE IF NOT EXISTS binary_policy(id INTEGER PRIMARY KEY,detail TEXT NOT NULL);'
                             'CREATE TABLE IF NOT EXISTS abandoned_intents('
                             'id TEXT PRIMARY KEY,payload TEXT NOT NULL,intent TEXT NOT NULL);'
                             'CREATE TABLE IF NOT EXISTS uncertain_quarantine('
                             'id TEXT PRIMARY KEY,payload TEXT NOT NULL,intent TEXT NOT NULL,'
                             'evidence TEXT NOT NULL,quarantined_at REAL NOT NULL);')
        try:
            self.bind_environment(self.environment)
            if self.db.execute('SELECT 1 FROM intents LEFT JOIN economic_intents USING(id) WHERE economic_intents.id IS NULL').fetchone():
                raise ValueError('cannot_reinterpret_existing_journal')
            self.policy=dict(order_limit_cents=order_limit_cents,capital_limit_cents=capital_limit_cents)
            encoded=json.dumps(dict(self.policy,daily_loss_cents=daily_loss_cents),sort_keys=True)
            self.db.execute('INSERT OR IGNORE INTO binary_policy VALUES(1,?)',(encoded,))
            if self.db.execute('SELECT detail FROM binary_policy WHERE id=1').fetchone()[0]!=encoded:
                raise ValueError('binary_policy_changed')
            self.configure_risk(daily_loss_cents)
        except Exception:
            self.close();raise

    def bind_environment(self, environment):
        if environment!='demo':raise ValueError('binary_demo_only')
        return super().bind_environment(environment)

    def get(self, client_id):
        record=super().get(client_id)
        row=self.db.execute('SELECT detail FROM economic_intents WHERE id=?',(client_id,)).fetchone()
        if row is None:raise ValueError('economic_intent_missing')
        record['intent']=json.loads(row[0]);return record

    def accounting(self, *, exclude=None):
        own=not self.db.in_transaction
        if own:self.db.execute('BEGIN')
        try:
            records=[r for r in self.records() if r['payload']['client_order_id']!=exclude]
            evidence={k:json.loads(v) for k,v in self.db.execute('SELECT client_id,detail FROM broker_evidence')}
            settlements=[json.loads(v) for v, in self.db.execute('SELECT detail FROM settlements')]
            return replay_binary(records,evidence,as_of=self.clock(),settlements=settlements)
        finally:
            if own:self.db.execute('ROLLBACK')

    def validate_reconciliation(self):
        self.accounting()

    def _snapshot(self, snapshot, state):
        import math
        if not isinstance(snapshot,dict) or snapshot.get('environment')!=self.environment:
            raise ValueError('demo_snapshot_required')
        start,end=snapshot.get('started_at'),snapshot.get('observed_at')
        if (any(type(v) not in (int,float) or not math.isfinite(v) for v in (start,end))
                or not 0<=end-start<=30 or not 0<=self.clock().timestamp()-end<=60):
            raise ValueError('stale_account_snapshot')
        cash=snapshot['balance']['balance']
        if type(cash) is not int or cash<0 or snapshot['resting_orders']!=[]:
            raise ValueError('invalid_cash_or_external_orders')
        reconcile_positions(state,snapshot['positions'])
        quarantine=snapshot.get('quarantined_reserve_cents',0)
        if type(quarantine) is not int or quarantine not in (0,60) or (quarantine and self.environment!='demo'):
            raise ValueError('invalid_quarantine_reserve')
        if cash<quarantine:raise ValueError('quarantine_cash_shortfall')
        return cash-quarantine

    def _gate(self, intent, ticker, snapshot, *, exclude=None):
        if self.db.execute('SELECT stopped FROM controls WHERE id=1').fetchone()[0]:
            raise ValueError('stopped')
        if self.db.execute("SELECT 1 FROM intents WHERE state='uncertain'").fetchone():
            raise ValueError('unreconciled_submission')
        if self.db.execute('SELECT 1 FROM settlements WHERE ticker=?',(ticker,)).fetchone():
            raise ValueError('market_already_settled')
        state=self.accounting(exclude=exclude)
        cash=self._snapshot(snapshot,state)
        limit=self.db.execute('SELECT daily_loss_cents FROM risk_policy WHERE id=1').fetchone()[0]
        if state['daily_low']<=-Fraction(limit,100):
            self.db.execute('INSERT OR IGNORE INTO risk_stops VALUES(?)',(state['day'],))
        if intent['action']=='buy' and self.db.execute('SELECT 1 FROM risk_stops WHERE day=?',(state['day'],)).fetchone():
            self.db.execute('COMMIT');raise ValueError('daily_loss_stop')
        policy=dict(self.policy)
        policy['capital_limit_cents']-=snapshot.get('quarantined_reserve_cents',0)
        budget_intent={key:value for key,value in intent.items() if key!='order_mode'}
        return check_budget(state,ticker=ticker,cash_cents=cash,**budget_intent,**policy)

    def reserve(self, client_id, ticker, contracts, price_cents, fee_reserve_cents,
                *, outcome, action, account_snapshot, order_mode='ioc'):
        if not isinstance(client_id,str) or not client_id.strip() or not isinstance(ticker,str) or not ticker.strip():
            raise ValueError('invalid_intent_identity')
        if order_mode not in ('ioc','post_only_gtc'):
            raise ValueError('invalid_order_mode')
        if order_mode=='post_only_gtc' and (action!='buy' or contracts!=1):
            raise ValueError('acceptance_order_must_be_one_contract_buy')
        intent=dict(outcome=outcome,action=action,count=contracts,price_cents=price_cents,
                    fee_cents=fee_reserve_cents,order_mode=order_mode)
        wire=terms(outcome,action,price_cents)
        payload=dict(ticker=ticker,client_order_id=client_id,side=wire['side'],price=wire['price'],
                     count=f'{contracts}.00',reduce_only=wire['reduce_only'],subaccount=0,
                     time_in_force='immediate_or_cancel',self_trade_prevention_type='taker_at_cross',cancel_order_on_pause=True)
        if order_mode=='post_only_gtc':
            payload.update(time_in_force='good_till_canceled',post_only=True)
        encoded=json.dumps(intent,sort_keys=True);body=json.dumps(payload,sort_keys=True)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            old=self.db.execute('SELECT payload FROM intents WHERE id=?',(client_id,)).fetchone()
            if old:
                if old!=(body,) or self.db.execute('SELECT detail FROM economic_intents WHERE id=?',(client_id,)).fetchone()!=(encoded,):
                    raise ValueError('client_id_reused_for_different_intent')
                self.db.execute('COMMIT');return False
            reserve=self._gate(intent,ticker,account_snapshot)
            self.db.execute("INSERT INTO intents(id,payload,reserve,state) VALUES(?,?,?,'reserved')",(client_id,body,reserve))
            self.db.execute('INSERT INTO economic_intents VALUES(?,?)',(client_id,encoded))
            self.db.execute('COMMIT');return True
        except Exception:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def abandon_reserved(self, client_id):
        """Close a definitely unsent intent; uncertainty is never eligible."""
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row=self.db.execute(
                "SELECT payload FROM intents WHERE id=? AND state='reserved' AND broker_id IS NULL AND filled=0",
                (client_id,)).fetchone()
            intent=self.db.execute('SELECT detail FROM economic_intents WHERE id=?',(client_id,)).fetchone()
            if row is None or intent is None:raise ValueError('reserved_abandon_not_allowed')
            self.db.execute('INSERT INTO abandoned_intents VALUES(?,?,?)',(client_id,row[0],intent[0]))
            self.db.execute('DELETE FROM economic_intents WHERE id=?',(client_id,))
            self.db.execute('DELETE FROM intents WHERE id=?',(client_id,))
            self.validate_reconciliation();self.db.execute('COMMIT')
            return {'client_order_id':client_id,'state':'abandoned','filled':0}
        except Exception:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def quarantine_uncertain_zero_fill(self, client_id, evidence, *, minimum_age_seconds=300,
                                       minimum_observations=2,
                                       minimum_observation_span_seconds=60):
        """Archive an old demo-only uncertainty after exhaustive negative evidence.

        This is intentionally narrower than reconciliation: it never invents an
        exchange order or broker ID.  The original intent, submission quote and
        bounded proof remain durable for later audit.
        """
        required_zero = ('current_exact_orders','historical_exact_orders',
                         'ticker_current_fills','ticker_historical_fills',
                         'ticker_positions','all_positions','all_resting_orders')
        observations=evidence.get('negative_observations') if isinstance(evidence,dict) else None
        if (self.environment != 'demo' or not isinstance(evidence,dict)
                or evidence.get('environment') != 'demo'
                or any(evidence.get(key) != 0 for key in required_zero)
                or type(evidence.get('observed_at')) not in (int,float)
                or not isinstance(observations,list) or len(observations)<minimum_observations
                or any(not isinstance(item,dict) or item.get('environment')!='demo'
                       or type(item.get('observed_at')) not in (int,float)
                       or any(item.get(key)!=0 for key in required_zero)
                       for item in observations)):
            raise ValueError('uncertain_quarantine_evidence_incomplete')
        now=self.clock().timestamp()
        observed_times=[item['observed_at'] for item in observations]
        if (not 0<=now-evidence['observed_at']<=120
                or observed_times!=sorted(observed_times)
                or observed_times[-1]-observed_times[0]<minimum_observation_span_seconds
                or evidence['observed_at']!=observed_times[-1]):
            raise ValueError('uncertain_quarantine_evidence_stale')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            row=self.db.execute(
                "SELECT payload FROM intents WHERE id=? AND state='uncertain' "
                "AND broker_id IS NULL AND filled=0",(client_id,)).fetchone()
            intent=self.db.execute('SELECT detail FROM economic_intents WHERE id=?',(client_id,)).fetchone()
            quote=self.db.execute('SELECT detail FROM submission_quotes WHERE client_id=?',(client_id,)).fetchone()
            if row is None or intent is None or quote is None:
                raise ValueError('uncertain_quarantine_not_allowed')
            economic=json.loads(intent[0]); submitted=json.loads(quote[0]).get('observed_at')
            if (economic.get('order_mode')!='post_only_gtc'
                    or type(submitted) not in (int,float) or now-submitted<minimum_age_seconds):
                raise ValueError('uncertain_quarantine_not_allowed')
            self.db.execute('INSERT INTO uncertain_quarantine VALUES(?,?,?,?,?)',
                            (client_id,row[0],intent[0],json.dumps(evidence,sort_keys=True),now))
            self.db.execute('DELETE FROM economic_intents WHERE id=?',(client_id,))
            self.db.execute('DELETE FROM intents WHERE id=?',(client_id,))
            self.validate_reconciliation();self.db.execute('COMMIT')
            return {'client_order_id':client_id,'state':'quarantined_zero_fill','filled':0}
        except Exception:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise

    def mark_submission_started(self, client_id, *, account_snapshot=None, quote_snapshot=None, demo_probe=False):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            record=self.get(client_id)
            if record['state']!='reserved':raise ValueError('submission_not_allowed')
            self._gate(record['intent'],record['payload']['ticker'],account_snapshot,exclude=client_id)
            # The existing depth validator uses YES-book prices for either direction.
            from .kalshi_quote_guard import validate
            if not isinstance(quote_snapshot,dict) or quote_snapshot.get('environment')!=self.environment:
                raise ValueError('demo_quote_required')
            if demo_probe:
                intent=record['intent'];market=quote_snapshot.get('market',{})
                if (self.environment!='demo' or intent['outcome']!='no' or intent['action']!='buy'
                        or intent['count']!=1 or intent['price_cents']!=1
                        or market.get('ticker')!=record['payload']['ticker'] or market.get('status')!='active'
                        or market.get('market_type')!='binary' or quote_snapshot.get('ticker')!=market['ticker']):
                    raise ValueError('invalid_demo_acceptance_probe')
                import math
                start,end=quote_snapshot.get('started_at'),quote_snapshot.get('observed_at')
                if (any(type(v) not in (int,float) or not math.isfinite(v) for v in (start,end))
                        or not 0<=end-start<=2 or not 0<=self.clock().timestamp()-end<=5):
                    raise ValueError('stale_demo_probe')
                quote_snapshot=dict(quote_snapshot,probe_kind='one_cent_no_ioc_acceptance_only')
            elif record['intent'].get('order_mode','ioc')=='post_only_gtc':
                from .kalshi_resting_order_guard import validate
                validate(record['payload'],quote_snapshot,now=self.clock(),environment=self.environment)
            else:
                validate(record['payload'],quote_snapshot,now=self.clock(),environment=self.environment)
            self.db.execute('INSERT INTO submission_quotes VALUES(?,?)',(client_id,json.dumps(quote_snapshot,sort_keys=True)))
            self.db.execute("UPDATE intents SET state='uncertain' WHERE id=?",(client_id,))
            self.db.execute('COMMIT');return record['payload']
        except Exception:
            if self.db.in_transaction:self.db.execute('ROLLBACK')
            raise
