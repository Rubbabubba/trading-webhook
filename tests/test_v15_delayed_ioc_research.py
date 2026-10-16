from fractions import Fraction
from research.v15_delayed_ioc_20261016.replay import decision, step
from opportunity_lab.kalshi_maker_v12 import shadow_decision


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
