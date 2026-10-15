from evaluate import hourly_max,probability,fee_cents,decision_quote,choose
import pytest


def test_hourly_day_requires_24_unique_finite_fahrenheit_values():
    p={'timezone':'America/New_York','hourly_units':{'temperature_2m':'°F'},'hourly':{'time':[f'2026-06-01T{i:02}:00' for i in range(24)],'temperature_2m':list(range(24))}}
    assert hourly_max(p,'2026-06-01')==23
    p['hourly']['time'][23]=p['hourly']['time'][22]
    with pytest.raises(ValueError): hourly_max(p,'2026-06-01')


def test_normal_proxy_and_integer_boundaries():
    assert probability(70,(float('-inf'),70))==0.5
    assert 0<probability(70,(69.5,71.5))<1
    assert fee_cents(.5)==2 and fee_cents(.9)==1


def test_no_late_or_last_trade_substitute():
    p={'candlesticks':[{'end_period_ts':59,'yes_bid':{'close':'.3'},'yes_ask':{'close':'.35'},'price':{'close':'.32'}}]}
    with pytest.raises(ValueError):decision_quote(p,60)


def test_selection_has_no_outcomes_and_uses_no_ask():
    rows=[{'ticker':'X','bid':.8,'ask':.85,'model_probabilities':[.1,.12]}]
    selected=choose(rows)
    assert len(selected)==1 and selected[0]['side']=='no'
    assert abs(selected[0]['entry_price_proxy']-.2)<1e-10
    assert selected[0]['fill_assumed'] is False


def test_both_models_must_meet_edge():
    assert choose([{'ticker':'X','bid':.3,'ask':.35,'model_probabilities':[.6,.4]}])==[]
