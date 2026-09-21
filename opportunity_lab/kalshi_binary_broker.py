"""Demo binary adapter. No production transport; no automatic retry of writes."""
from decimal import Decimal
import time

from .kalshi_demo_broker import DemoBroker, CREATE, check_exchange, identifier, quantity
from .kalshi_binary_accounting import reconcile_positions


class BinaryDemoBroker(DemoBroker):
    def __init__(self, journal, client, *, unresolved_journals=(), quarantine_markets=None):
        from .kalshi_binary_journal import BinaryJournal
        if not isinstance(journal,BinaryJournal):raise ValueError('binary_journal_required')
        super().__init__(journal,client)
        self.unresolved_journals=tuple(unresolved_journals)
        self.quarantine_markets=quarantine_markets
        self.quarantined_reserve_cents=0

    def _check_other_journals(self):
        import sqlite3
        from pathlib import Path
        from .kalshi_demo_broker import DemoClient
        import json
        self.quarantined_reserve_cents=0
        paths=list(self.unresolved_journals)
        # A new journal must never sidestep this project's original unresolved
        # demo attempt. Fake clients in local tests do not touch account state.
        original=Path(__file__).resolve().parents[1]/'sports_paper/demo_execution_20260914/journal.sqlite3'
        if isinstance(self.client,DemoClient) and original.exists():paths.append(original)
        for path in set(Path(p).resolve() for p in paths):
            absolute=Path(path).resolve()
            db=sqlite3.connect(absolute.as_uri()+'?mode=ro',uri=True)
            try:
                rows=db.execute("SELECT payload,reserve,state,filled,broker_id FROM intents WHERE state!='terminal'").fetchall()
                for row in rows:
                    if self.journal.environment!='demo':raise ValueError('earlier_demo_intent_unresolved')
                    from .kalshi_demo_quarantine import validate
                    from .kalshi_demo_market_data import DemoMarkets
                    markets=getattr(self,'quarantine_markets',None)
                    if markets is None:
                        if type(self.client) is not DemoClient or self.client.BASE_URL!=DemoClient.BASE_URL:
                            raise ValueError('earlier_demo_intent_unresolved')
                        markets=DemoMarkets();self.quarantine_markets=markets
                    record=dict(payload=json.loads(row[0]),reserve=row[1],state=row[2],filled=row[3],broker_id=row[4])
                    # Check for an audit before making any market read.
                    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='demo_quarantine'").fetchone():
                        raise ValueError('earlier_demo_intent_unresolved')
                    market,_,_=markets.get(record['payload']['ticker'])
                    self.quarantined_reserve_cents+=validate(db,record,key_id=self.client.key_id,
                        market=market['market'],now=self.journal.clock().timestamp())
            finally:db.close()

    def snapshot(self):
        self._check_other_journals()
        start=self.journal.clock().timestamp()
        balance=self.client.request('GET','/portfolio/balance',params={'subaccount':0})
        positions=self.client.pages('/portfolio/positions','market_positions',subaccount=0,count_filter='position')
        orders=self.client.pages('/portfolio/orders','orders',subaccount=0,status='resting')
        return dict(environment=self.journal.environment,started_at=start,observed_at=self.journal.clock().timestamp(),balance=balance,
                    positions=positions,resting_orders=orders,quarantined_reserve_cents=self.quarantined_reserve_cents)

    def submit(self, client_id, *, quote_snapshot=None, quote_provider=None, demo_probe=False):
        if self.journal.get(client_id)['state']!='reserved':raise ValueError('submission_not_allowed')
        check_exchange(self.client)
        snapshot=self.snapshot()
        if quote_provider is not None:
            if quote_snapshot is not None or not callable(quote_provider):
                raise ValueError('invalid_quote_provider')
            quote_snapshot=quote_provider(self.journal.get(client_id)['payload'])
        payload=self.journal.mark_submission_started(client_id,account_snapshot=snapshot,quote_snapshot=quote_snapshot,demo_probe=demo_probe)
        ack=self.client.request('POST',CREATE,body=payload)
        if ack.get('client_order_id')!=client_id:raise ValueError('create_identity_mismatch')
        filled,remaining=quantity(ack.get('fill_count')),quantity(ack.get('remaining_count'))
        if filled+remaining>quantity(payload['count']):raise ValueError('invalid_create_counts')
        self.journal.acknowledge(client_id,identifier(ack.get('order_id')),filled)
        return self.refresh(client_id)

    def validate_legacy_direction(self, order, payload):
        from .kalshi_order_direction import terms
        if 'side' in order or 'action' in order:
            if terms(order.get('side'),order.get('action'),50)['side']!=payload['side']:
                raise ValueError('order_identity_mismatch')

    def order_cost_basis(self, record, order, gross, total):
        # The observed V2 ask/NO fill reports NO cost even when legacy order
        # metadata says side=yes/action=sell. Prices on fills remain YES prices.
        direction=order.get('book_side')
        if direction not in ('bid','ask') or order.get('outcome_side')!=('yes' if direction=='bid' else 'no'):
            raise ValueError('order_cost_representation_unverified')
        return gross if direction=='bid' else Decimal(total)-gross

    def reserved_cost(self, record, gross, fees, total):
        intent=record['intent']
        economic=gross if intent['outcome']=='yes' else Decimal(total)-gross
        return economic+fees if intent['action']=='buy' else fees

    def reconcile_positions(self, *, allow_reserved=False):
        allowed={'terminal','reserved'} if allow_reserved else {'terminal'}
        if any(r['state'] not in allowed for r in self.journal.records()):raise ValueError('orders_not_terminal')
        rows=self.client.pages('/portfolio/positions','market_positions',subaccount=0,count_filter='position')
        try:
            reconcile_positions(self.journal.accounting(),rows)
            if self.client.pages('/portfolio/orders','orders',subaccount=0,status='resting'):
                raise ValueError('external_resting_orders')
        except Exception:
            self.journal.stop();raise
        return dict(positions_match=True,position_count=len(rows))

    def reconcile_settlements(self):
        """Record settlements for this ledger without claiming account-wide history."""
        tickers = {r['payload']['ticker'] for r in self.journal.records()}
        matches = {}
        for row in self.client.pages('/portfolio/settlements', 'settlements', subaccount=0):
            ticker = row.get('ticker')
            if ticker not in tickers:
                continue
            if ticker in matches and matches[ticker] != row:
                raise ValueError('duplicate_settlement')
            matches[ticker] = row
        for row in matches.values():
            self.journal.record_settlement(row)
        return dict(reconciled_settlements=len(matches))
