"""Explicit rule implication only. Ticker resemblance is never a relationship."""
from decimal import Decimal,InvalidOperation
from datetime import datetime
import re,json,hashlib,math
NUM=r'-?[0-9]+(?:\.[0-9]+)?'
CLOCK=r'(?:[1-9]|1[0-2])(?::[0-5][0-9])? (?:AM|PM) (?:EDT|EST)'
DATE=r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (?:[1-9]|[12][0-9]|3[01]), 20[0-9]{2}'
INDEX=r'(?:Bitcoin Real-Time Index \(BRTI\)|Ethereum Real-Time Index \(ERTI\)|Ripple-Dollar Real Time Index \(XRPUSD_RTI\)|SOLUSD_RTI|BNBUSDRTI|DOGEUSDRTI|ZECUSDRTI)'
NAME=r"[A-Za-z0-9 '&./()\-]+"
PATTERNS=[
 ('continuous',re.compile(r"^If the simple average of the sixty seconds of CF Benchmarks' "+INDEX+r' before (?P<clock>'+CLOCK+r') is above \$?(?P<strike>'+NUM+r') at (?P=clock) on '+DATE+r', then the market resolves to Yes\.$')),
 ('runs',re.compile(r'^If '+NAME+r' collectively score more (?:than )?(?P<strike>'+NUM+r') runs in the '+NAME+r' professional baseball game originally scheduled for '+DATE+r' at '+CLOCK+r', then the market resolves to Yes\.$')),
 ('integer_runs',re.compile(r'^If '+NAME+r' scores (?P<strike>[0-9]+)\+ runs in the '+NAME+r' professional baseball game originally scheduled for '+DATE+r' at '+CLOCK+r', then the market resolves to Yes\.$')),
]

def number(value):
    try:d=Decimal(str(value))
    except (InvalidOperation,ValueError,TypeError):raise ValueError('invalid_strike') from None
    if not d.is_finite():raise ValueError('invalid_strike')
    return d

def identity(m):
    if not isinstance(m.get('ticker'),str) or not m['ticker'] or m['ticker'].startswith('KXMVE'):raise ValueError('missing_or_unsupported_ticker')
    if m.get('market_type')!='binary' or m.get('strike_type')!='greater' or m.get('cap_strike') is not None or m.get('mve_collection_ticker') or m.get('exchange_index',0)!=0:raise ValueError('unsupported_contract')
    primary=m.get('rules_primary')
    if not isinstance(primary,str):raise ValueError('missing_rule')
    if re.search(r'\b(?:not|unless|except|instead|or|neither|never|false)\b',primary,re.I):raise ValueError('ambiguous_rule_logic')
    matches=[(kind,p.fullmatch(primary)) for kind,p in PATTERNS if p.fullmatch(primary)]
    if len(matches)!=1:raise ValueError('unsupported_rule_template')
    kind,match=matches[0];printed=number(match.group('strike'));strike=number(m.get('floor_strike'))
    if strike!=(printed-Decimal('.5') if kind=='integer_runs' else printed):raise ValueError('metadata_rule_strike_disagreement')
    context={k:m.get(k) for k in ('event_ticker','rules_secondary','occurrence_datetime','expiration_time')}
    if any(not isinstance(v,str) or not v for v in context.values()):raise ValueError('missing_rule_context')
    for k in ('occurrence_datetime','expiration_time'):
        try:t=datetime.fromisoformat(context[k].replace('Z','+00:00'))
        except ValueError:raise ValueError('invalid_rule_time') from None
        if t.tzinfo is None:raise ValueError('timezone_required')
    context.update(template=primary[:match.start('strike')]+'<STRIKE>'+primary[match.end('strike'):],template_kind=kind,exchange_index=0)
    raw=json.dumps(context,sort_keys=True,separators=(',',':'))
    return hashlib.sha256(raw.encode()).hexdigest(),strike

def relation(low,high):
    a,x=identity(low);b,y=identity(high)
    if a!=b or x>=y or low.get('ticker')==high.get('ticker'):raise ValueError('no_threshold_implication')
    return dict(relationship_id=a,event_id=low['event_ticker'],low_ticker=low['ticker'],high_ticker=high['ticker'],low_strike=str(x),high_strike=str(y),ordinary_binary_minimum_payout_cents=100,actual_orders=0,fill_assumed=False,execution_enabled=False,administrative_cancellation_payoff_verified=False)

def trading_window(m,t):
    if type(t) not in (int,float) or not math.isfinite(t):return False
    try:
        opened=datetime.fromisoformat(m['open_time'].replace('Z','+00:00'))
        cuts=[datetime.fromisoformat(m[k].replace('Z','+00:00')) for k in ('close_time','occurrence_datetime','expected_expiration_time') if m.get(k)]
    except (ValueError,TypeError,KeyError):return False
    if not cuts or any(x.tzinfo is None for x in [opened,*cuts]):return False
    return opened.timestamp()<=t<min(cuts).timestamp()
