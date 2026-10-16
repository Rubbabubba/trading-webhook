"""Conservative conditional resting-order model; never authentic owned fills."""
from decimal import Decimal
from itertools import groupby
import math


def outcome_view(view, side):
    if view is None:
        return None
    if side=='yes':
        return dict(bid=view['yes_bid'], ask=view['yes_ask'],
                    bid_size=view['yes_bid_size'], ask_size=view['no_bid_size'])
    return dict(bid=100-view['yes_ask'], ask=100-view['yes_bid'],
                bid_size=view['no_bid_size'], ask_size=view['yes_bid_size'])


def classify(attempt, signal, activation_view, tape, *, complete):
    if signal is None:
        return dict(state='benchmark_no_trade', owned_fill=False)
    side = signal['outcome']; limit = signal['price_cents']
    result = dict(side=side, limit=limit, owned_fill=False, actual_orders=0, actual_fills=0)
    quote = outcome_view(activation_view, side)
    if quote is None:
        return dict(result, state='activation_unobserved')
    if not quote['bid']<=limit<quote['ask']:
        return dict(result, state='activation_quote_rejected', activation_quote=quote)
    result.update(activation_quote=quote,
        displayed_ahead_size=quote['bid_size'] if limit==quote['bid'] else 0,
        queue_independently_verified=False, modeled_entry_cost=limit+3)
    if not complete:
        return dict(result, state='tape_incomplete_uncertain_possible_fill')
    potential=[]
    for row in tape:
        if row['ticker']!=attempt['ticker']:
            raise ValueError('tape_ticker_mismatch')
        price=Decimal(row[side+'_price'])*100
        if (row['taker_outcome_side']!=side and price<=limit
                and attempt['activation_at']-1<=row['at']<=attempt['cancel_ack_at']+1):
            potential.append(row)
    potential.sort(key=lambda r:(r['at'],r['trade_id']))
    for at, grouped in groupby(potential,key=lambda r:r['at']):
        rows=list(grouped)
        # Do not skip a possible earlier partial/touch or clock-boundary fill
        # and select a later trade because it gives a more convenient result.
        safe=(attempt['activation_at']+1<=at<=attempt['cancel_request_at']-1
            and all(Decimal(r['count'])>=1 and Decimal(r[side+'_price'])*100<=limit-1 for r in rows))
        evidence=dict(public_trade_ids=[r['trade_id'] for r in rows], observed_trade_at=at)
        if not safe:
            return dict(result,state='uncertain_possible_fill',**evidence)
        return dict(result,state='conditional_trade_through',entry_at=at,
                    modeled_entry_price=limit,**evidence)
    return dict(result,state='no_qualifying_public_trade')


def exit_observation(path, horizon, view, at):
    due=path['entry_at']+horizon
    quote=outcome_view(view,path['side'])
    if quote is None or not due<=at<=due+5 or quote['bid_size']<1:
        return dict(state='unresolved_exit',observed_at=at)
    return dict(state='conditional_exit',at=at,bid=quote['bid'],
                net_proceeds_before_extra_stress=max(0,math.floor(quote['bid'])-1)-2)


def summarize(paths):
    entered=[p for p in paths if p['state']=='conditional_trade_through']
    uncertain=[p for p in paths if 'uncertain_possible_fill' in p['state']]
    totals={}
    for h in (5,30,300):
        complete=[p for p in entered if p['exits'][str(h)]['state']=='conditional_exit']
        unknown=[p for p in entered if p not in complete]+uncertain
        totals[str(h)]=dict(complete_conditional_roundtrips=len(complete),unresolved_paths=len(unknown),
            scenarios={str(s):dict(
                complete_net_cents=sum(p['exits'][str(h)]['net_proceeds_before_extra_stress']-p['modeled_entry_cost']-2*s for p in complete) if complete else None,
                full_principal_stress_cents=-sum(p['modeled_entry_cost']+s for p in unknown) if unknown else None,
                combined_cents=(sum(p['exits'][str(h)]['net_proceeds_before_extra_stress']-p['modeled_entry_cost']-2*s for p in complete)-sum(p['modeled_entry_cost']+s for p in unknown)) if entered or uncertain else None,
                session_cluster_lcb_cents=None) for s in (0,2,5)})
    return dict(conditional_entries=len(entered),uncertain_possible_fills=len(uncertain),
                complete_all_horizons=sum(all(p['exits'][str(h)]['state']=='conditional_exit' for h in (5,30,300)) for p in entered),horizons=totals)
