"""Scoped unavailable snapshots; no inference that omitted depth equals zero."""
from .kalshi_demo_stream_source import Books, SourceFault, levels, number
import uuid


class AvailabilityBooks(Books):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.incomplete = {}

    def _apply(self, p, at):
        msg = p.get('msg', {}) if isinstance(p, dict) else {}
        kind = p.get('type') if isinstance(p, dict) else None
        ticker = msg.get('market_ticker') if isinstance(msg, dict) else None
        partial = (kind == 'orderbook_snapshot' and isinstance(msg, dict)
                   and bool({'yes_dollars_fp','no_dollars_fp'} & set(msg))
                   and not {'yes_dollars_fp','no_dollars_fp'} <= set(msg))
        if kind=='orderbook_snapshot' and ticker in self.incomplete and msg.get('market_id')!=self.incomplete[ticker]:
            raise SourceFault('market_identity_changed')
        if partial or kind == 'orderbook_delta' and ticker in self.incomplete:
            try:uuid.UUID(msg.get('market_id'))
            except (ValueError,TypeError,AttributeError):raise SourceFault('invalid_market_identity') from None
            if partial:
                # Supplied malformed, oversized or null ladders still fail closed.
                for side in ('yes_dollars_fp','no_dollars_fp'):
                    if side in msg:levels(msg[side])
            else:
                if msg.get('market_id') != self.incomplete[ticker]:
                    raise SourceFault('market_identity_changed')
                price,delta=number(msg.get('price_dollars')),number(msg.get('delta_fp'))
                if msg.get('side') not in {'yes','no'} or not 0<price<1 or abs(delta)>1000000000:
                    raise SourceFault('invalid_delta')
            # The parent validates receipt/server clocks, subscription sequence,
            # target identity and UUID, then clears this symbol's stale book.
            # This projection is for unavailable-state validation only. Archive
            # bytes remain the exact original message; no synthetic book is stored.
            projected={**p,'type':'orderbook_snapshot','msg':{
                'market_ticker':ticker,'market_id':msg.get('market_id')}}
            result=super()._apply(projected,at)
            self.incomplete[ticker]=msg.get('market_id')
            return {**result,'availability_reason':'incomplete_snapshot_side',
                    'original_kind':kind}
        result=super()._apply(p,at)
        if kind=='orderbook_snapshot' and result['kind']=='orderbook_snapshot':
            self.incomplete.pop(ticker,None)
        return result
