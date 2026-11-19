"""Read-only bounded admission and reconciled-journal reporting."""
import re

COUNTS=('buy_attempts_today','remaining_buy_attempts','max_daily_buys','max_open_positions',
        'entries_filled','exits_filled','settlements','unresolved_orders','sell_attempts','sell_signals_observed','filled_buys_today','max_daily_filled_buys')
MONEY=('realized_net_usd','daily_realized_net_usd','fees_usd','open_basis_usd','pending_reserves_usd')


def normalize(value):
    if not isinstance(value,dict):return None
    state=value.get('state')
    if state not in {'reconciled','pending_reconciliation','unavailable'}:return None
    result={'state':state}
    for key in COUNTS:
        v=value.get(key);result[key]=v if type(v) is int and 0<=v<=10**9 else None
    for key in MONEY:
        v=value.get(key);result[key]=v if isinstance(v,str) and re.fullmatch(r'-?\d{1,12}(?:\.\d{1,20})?',v) else None
    for key in ('budget_session_utc','pnl_session_central'):
        v=value.get(key);result[key]=v if isinstance(v,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',v) else None
    allowed={'admitted','daily_attempt_limit','open_position_limit','pending_reconciliation','unavailable','daily_filled_entry_limit','submission_cooldown'}
    admission=value.get('admission');result['admission']=admission if admission in allowed else 'unavailable'
    for key,allowed in {
        'sell_admission':{'pending_reconciliation','eligible_owned_sells_only'},
        'sell_verification':{'awaiting_broker_resolution','broker_fill_verified',
            'eligible_sell_attempt_unfilled','awaiting_matching_owned_wallet_sell'},
    }.items():
        v=value.get(key);result[key]=v if isinstance(v,str) and v in allowed else None
    if result['sell_verification']=='broker_fill_verified' and not (
            result['state']=='reconciled' and result['unresolved_orders']==0
            and result['exits_filled'] is not None and result['sell_attempts'] is not None
            and 0<result['exits_filled']<=result['sell_attempts']):
        result['sell_verification']=None
    if result['sell_admission']=='eligible_owned_sells_only' and result['unresolved_orders']!=0:
        result['sell_admission']=None
    return result
