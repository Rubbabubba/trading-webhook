import pytest
from research.structural_rules_20261016.matcher import identity,relation,trading_window

def market(strike='76.9999',dollar='$'):
    return dict(ticker='A-'+strike,event_ticker='E',market_type='binary',strike_type='greater',floor_strike=strike,cap_strike=None,rules_primary=f"If the simple average of the sixty seconds of CF Benchmarks' SOLUSD_RTI before 5 PM EDT is above {dollar}{strike} at 5 PM EDT on Jul 19, 2026, then the market resolves to Yes.",rules_secondary='Same source and final averaging rule.',occurrence_datetime='2026-07-19T21:05:00Z',expiration_time='2026-07-26T21:00:00Z',close_time='2026-07-19T21:00:00Z',open_time='2026-07-18T00:00:00Z')

def test_exact_currency_rules_and_strict_implication():
    a=market();b=market('77.9999');assert relation(a,b)['ordinary_binary_minimum_payout_cents']==100
    with pytest.raises(ValueError):relation(b,a)
    with pytest.raises(ValueError):relation(a,a)
    for changed in [dict(b,rules_secondary='Different averaging source'),dict(b,occurrence_datetime='2026-07-20T21:05:00Z'),dict(b,rules_primary=b['rules_primary'].replace('SOLUSD_RTI','Coinbase price'))]:
        with pytest.raises(ValueError):relation(a,changed)
    with pytest.raises(ValueError):identity(dict(a,rules_primary=a['rules_primary'].replace('on Jul 19','unless false on Jul 19')))
    with pytest.raises(ValueError):identity(dict(a,rules_primary=a['rules_primary'].replace('at 5 PM','at 6 PM')))
    with pytest.raises(ValueError):identity(dict(a,ticker=None))

def test_integer_runs_metadata_equivalence_and_ambiguous_rule_rejection():
    m=market();m.update(floor_strike='1.5',rules_primary='If Seattle scores 2+ runs in the San Francisco vs Seattle professional baseball game originally scheduled for Jul 19, 2026 at 4:10 PM EDT, then the market resolves to Yes.')
    assert str(identity(m)[1])=='1.5'
    with pytest.raises(ValueError):identity(dict(m,floor_strike='2'))
    with pytest.raises(ValueError):identity(dict(m,rules_primary=m['rules_primary'].replace('scores','does not score')))
    with pytest.raises(ValueError):identity(dict(m,rules_primary=m['rules_primary']+' Or another condition.'))
    m.update(floor_strike='12.5',rules_primary="If Washington and A's collectively score more 12.5 runs in the Washington vs A's professional baseball game originally scheduled for Jul 18, 2026 at 10:05 PM EDT, then the market resolves to Yes.")
    assert str(identity(m)[1])=='12.5'
    with pytest.raises(ValueError):identity(dict(m,floor_strike='13'))

def test_different_actual_close_times_require_both_leg_windows():
    a=market();b=market('77.9999');b['close_time']='2026-07-19T20:59:00Z'
    assert relation(a,b)
    assert trading_window(a,1784494770) and not trading_window(b,1784494770)
    assert not trading_window(dict(a,open_time='2026-07-18T00:00:00'),1784494770)
    assert not trading_window(a,float('nan'))
