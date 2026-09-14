"""Settlement-capable shadow engine for the next protocol; v1 stays frozen."""
import json
import math
import time
from decimal import Decimal, InvalidOperation

from .kalshi_shadow import Shadow
from .kalshi_shadow_live import Coordinator, epoch, identity
from .kalshi_risk import amount


def one_contract_frame(frame):
    """Preserve fractional depth; cap larger sizes at the one-contract requirement."""
    books = {}
    for side in ('yes_dollars', 'no_dollars'):
        levels = frame['orderbook_fp'][side]
        if not isinstance(levels, list):
            raise ValueError('invalid_book')
        books[side] = []
        for price, size in levels:
            if not isinstance(size, str) or len(size) > 40:
                raise ValueError('invalid_depth')
            try:
                depth = Decimal(size)
            except InvalidOperation:
                raise ValueError('invalid_depth') from None
            if not depth.is_finite() or depth <= 0:
                raise ValueError('invalid_depth')
            books[side].append([price, str(min(depth, Decimal(1)))])
    return dict(frame, orderbook_fp=books)


class SettlementShadow(Shadow):
    def __init__(self, path, config):
        super().__init__(path, config)
        self.db.execute('CREATE TABLE IF NOT EXISTS verified_settlements(ticker TEXT PRIMARY KEY,evidence TEXT,action TEXT)')

    def step(self, frame, *, now, signal=None):
        if self.db.execute('SELECT 1 FROM verified_settlements WHERE ticker=?', (frame.get('ticker'),)).fetchone():
            raise ValueError('market_already_finalized')
        return super().step(one_contract_frame(frame), now=now, signal=signal)

    def settle(self, market, expected, *, received_at, now):
        """Consume fresh public market metadata, never an orderbook or forecast.

        expected must be the original persisted discovery identity. Only $1
        binary YES/NO payouts are supported. Scalar/void cases remain unresolved.
        """
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in (received_at, now)) or not 0 <= now-received_at <= 5:
            raise ValueError('stale_settlement')
        if not expected.get('ticker') or not expected.get('event_ticker') or not expected.get('rules_primary'):
            raise ValueError('missing_original_identity')
        if identity(market) != identity(expected):
            raise ValueError('settlement_identity_changed')
        if market.get('status') != 'finalized' or market.get('market_type') != 'binary' or market.get('result') not in ('yes', 'no'):
            raise ValueError('settlement_not_final_binary')
        settled_at = epoch(market['settlement_ts'])
        if settled_at > received_at:
            raise ValueError('future_settlement')
        yes_value = amount(market['settlement_value_dollars'])
        if amount(market['notional_value_dollars']) != 1 or yes_value != (1 if market['result'] == 'yes' else 0):
            raise ValueError('inconsistent_binary_payout')
        ticker = market['ticker']
        evidence = json.dumps(dict(identity=identity(market), settled_at=settled_at,
                                   result=market['result'], yes_value=int(yes_value)), sort_keys=True)
        self.db.execute('BEGIN IMMEDIATE')
        try:
            old = self.db.execute('SELECT evidence,action FROM verified_settlements WHERE ticker=?', (ticker,)).fetchone()
            if old:
                if old[0] != evidence:
                    raise ValueError('settlement_conflict')
                self.db.execute('COMMIT')
                return dict(json.loads(old[1]), replay=True)
            state = self.state(); position = state['position']; pending = state['pending']
            active = position or pending
            if active and active['ticker'] != ticker:
                raise ValueError('settlement_wrong_position')
            if state['last_at'] is not None and received_at <= state['last_at']:
                raise ValueError('non_increasing_settlement')
            if position and settled_at < position['opened_at']:
                raise ValueError('settlement_before_entry')
            action = dict(action='finalized_without_position', ticker=ticker,
                          at=received_at, settled_at=settled_at, execution_enabled=False)
            if position:
                payout = 100 if position['side'] == market['result'] else 0
                net = payout-position['basis']
                state['cash_cents'] += payout; state['realized_cents'] += net
                action.update(action='settlement', side=position['side'], proceeds_cents=payout, net_cents=net)
            elif pending:
                action['action'] = 'unfilled_entry_cancelled'
            state.update(position=None, pending=None, last_at=received_at)
            self.db.execute('UPDATE state SET detail=? WHERE id=1', (json.dumps(state),))
            self.db.execute('INSERT INTO actions(detail) VALUES(?)', (json.dumps(action),))
            self.db.execute('INSERT INTO verified_settlements VALUES(?,?,?)', (ticker, evidence, json.dumps(action)))
            self.db.execute('COMMIT')
            return action
        except Exception:
            self.db.execute('ROLLBACK')
            raise


class SettlementCoordinator(Coordinator):
    def observe(self, frame, market, series, *, now):
        return super().observe(one_contract_frame(frame), market, series, now=now)

    def poll(self, client, ticker, *, paused=False):
        """Resolve finalized markets before requesting potentially absent books."""
        payload, at, _ = client.get('markets', ticker)
        market = payload['market']
        if market.get('status') == 'finalized':
            return self.engine.settle(market, self.universe[ticker]['market'],
                                      received_at=at, now=time.time())
        if market.get('status') != 'active':
            return {'action': 'awaiting_market', 'ticker': ticker, 'execution_enabled': False}
        series, _, _ = client.get('series', self.universe[ticker]['series']['ticker'])
        book, at, rid = client.get('markets', ticker, book=True)
        frame = dict(book, ticker=ticker, received_at=at, book_id=rid)
        if paused:
            frame['signal_valid'] = False
        return self.observe(frame,
                            market, series['series'], now=time.time())
