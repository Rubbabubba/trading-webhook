from tools.analyze_sports_strategy_evidence import summarize


def test_cost_attribution_recovers_gross_before_cost():
    report = {'games': [{'slug': 'nfl_1', 'league': 'nfl', 'accounts': {
        '300': {'entries': 2, 'realized_cents': -20, 'fees_cents': 15, 'slippage_cents': 10}
    }}]}
    row = summarize(report)['nfl']['300']
    assert row['traded_games'] == 1
    assert row['modeled_transaction_cost_cents'] == 25
    assert row['gross_before_modeled_cost_cents'] == 5
