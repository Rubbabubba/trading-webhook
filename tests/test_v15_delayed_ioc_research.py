from fractions import Fraction
from research.v15_delayed_ioc_20261016.replay import decision, step
from opportunity_lab.kalshi_maker_v12 import shadow_decision
from research.v15_delayed_ioc_20261016.review import account
from research.v15_delayed_ioc_20261016.trade_contract import validate
import pytest


def frame():
    return dict(received_at=1000, orderbook_fp={
        'yes_dollars': [['0.40', '9']], 'no_dollars': [['0.50', '3']]})


def view(**kw):
    return dict(ask=41, bid=39, ask_depth=3, bid_depth=3, **kw)


def test_frozen_decision_and_same_observation_benchmarks():
    f = frame(); signals, reason = decision([], f)
    frozen, _ = shadow_decision([], f)
    assert reason == 'signal'
    assert signals['frozen_v12'] == frozen
    assert signals['v15_ioc']['price_cents'] == frozen['price_cents'] + 2 == 42
    assert signals['frozen_v10'] is None
    assert f == frame()


def test_native_depth_limit_is_not_clipped_or_relaxed():
    f = frame()
    f['orderbook_fp']['yes_dollars'][0][1] = '1000001'
    assert decision([], f) == (None, 'unsupported_native_accounting_amount')
    assert f['orderbook_fp']['yes_dollars'][0][1] == '1000001'


def test_marketability_requires_slippage_inside_original_limit():
    p = dict(limit=42, state='awaiting_entry')
    step(p, 'entry', 1001, 1001, view())
    assert p['state'] == 'conditional_entry' and p['entry_cost_price'] == 42
    rejected = dict(limit=42, state='awaiting_entry')
    step(rejected, 'entry', 1001, 1001, {**view(), 'ask': 42})
    assert rejected['state'] == 'entry_limit_not_marketable_after_slippage'


def test_no_late_entry_or_retry_or_queue_fill_inference():
    for v, at in [(None, 1001), (view(), 1002.001)]:
        p = dict(limit=42)
        step(p, 'entry', 1001, at, v)
        assert p['state'] == 'entry_unavailable_or_late'
        assert 'entry_at' not in p


def test_exit_uses_own_bid_and_separate_counterfactual_horizons():
    p = dict(limit=42)
    step(p, 'entry', 1001, 1001.5, view())
    step(p, '5', 1006.5, 1006.5, {**view(), 'bid': 49.9})
    step(p, '30', 1031.5, 1036.5001, view())
    assert p['exits']['5']['proceeds_price'] == 48
    assert p['exits']['30']['state'] == 'unresolved_exit'
    assert p['entry_at'] == 1001.5


def test_size_and_fractional_cent_guards():
    p = dict(limit=42)
    step(p, 'entry', 1, 1, {**view(), 'ask_depth': .5})
    assert p['state'] == 'entry_insufficient_depth'
    p = dict(limit=42)
    step(p, 'entry', 1, 1, {**view(), 'ask': 41.01})
    assert p['state'] == 'entry_limit_not_marketable_after_slippage'


def test_independent_cash_accounting_preserves_missing_exits_and_unfilled_nulls():
    p = dict(limit=42)
    step(p, 'entry', 1, 1, view())
    step(p, '5', 6, 6, {**view(), 'bid': 50})
    step(p, '30', 31, 37, view())
    step(p, '300', 301, 301, None)
    result = account([p, {'state': 'entry_limit_not_marketable_after_slippage'}])
    assert result['conditional_entries'] == 1
    assert result['horizons']['5']['scenarios']['0']['complete_net_cents'] == 3
    assert result['horizons']['5']['scenarios']['2']['complete_net_cents'] == -1
    assert result['horizons']['30']['scenarios']['0']['combined_with_unresolved_stress_cents'] == -44
    assert result['complete_all_horizons'] == 0
    no_entry = account([{'state': 'entry_limit_not_marketable_after_slippage'}])
    assert no_entry['horizons']['5']['gross_cents'] is None
    assert no_entry['horizons']['5']['scenarios']['0']['complete_net_cents'] is None


def public_trade(**changes):
    return dict(trade_id='synthetic', ticker='T', count_fp='1.00',
        yes_price_dollars='0.40', no_price_dollars='0.60',
        taker_outcome_side='no', taker_book_side='ask', taker_side='no',
        created_time='2026-07-19T03:00:30Z', is_block_trade=False) | changes


def test_public_trade_canonical_direction_and_explicit_price_scales():
    row = validate(public_trade(), 1784430024, 1784430084)
    assert row['taker_outcome_side']=='no' and row['yes_price']=='0.40'
    assert row['no_price']=='0.60' and row['owned_fill'] is False


@pytest.mark.parametrize('changes', [dict(taker_book_side='bid'), dict(taker_side='yes'),
    dict(is_block_trade=True), dict(count_fp='NaN'), dict(no_price_dollars='0.40'),
    dict(created_time='2026-07-19T03:05:30Z')])
def test_ambiguous_or_invalid_trade_never_becomes_fill_evidence(changes):
    with pytest.raises((ValueError, ArithmeticError)):
        validate(public_trade(**changes), 1784430024, 1784430084)
