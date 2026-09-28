import pytest
from opportunity_lab.kalshi_strategy_evaluation import evaluate


def rows(events=30, per=4, net=5):
    return [dict(event_id=f'e{i}',episode_id=f'e{i}-{j}',
        closed_at=f'2026-09-{1+i%15:02d}T{j%24:02d}:{j//24:02d}:00+00:00',net_cents=net,
        fees_cents=1,slippage_cents=1) for i in range(events) for j in range(per)]


def test_positive_independent_sample_passes_registered_screen():
    result=evaluate(rows())
    assert result['release_screen_passed']
    assert result['stressed_net_cents']==360
    assert result['event_cluster_mean_95_lower_cents']==5


def test_correlated_volume_does_not_replace_event_count():
    result=evaluate(rows(events=1,per=100,net=20))
    assert not result['gates']['minimum_events']
    assert not result['release_screen_passed']


def test_cost_stress_can_reject_nominal_profit():
    result=evaluate(rows(net=1))
    assert result['positive_net'] if 'positive_net' in result else result['gates']['positive_net']
    assert not result['gates']['positive_stressed_net']


def test_drawdown_uses_chronological_episode_order():
    data=rows(events=30,per=4,net=5)
    data[1]['net_cents']=-20
    assert evaluate(data)['maximum_drawdown_cents']>=20


@pytest.mark.parametrize('change,error',[
    ({'episode_id':'e0-0'},'duplicate_episode'),
    ({'closed_at':'2026-01-01'},'timezone_required'),
    ({'fees_cents':-1},'negative_cost'),
    ({'net_cents':float('nan')},'invalid_net_cents')])
def test_invalid_evidence_fails_closed(change,error):
    data=rows();data[1].update(change)
    with pytest.raises(ValueError,match=error):evaluate(data)
