from opportunity_lab.sports_dynamic_mapping import schedule_events


def test_tennis_parser_includes_non_us_open_tournaments():
    payload = {'events': [{'id': 't', 'name': 'Tokyo Open', 'groupings': [{
        'grouping': {'slug': 'mens-singles'},
        'competitions': [{'id': 'm1', 'date': '2026-09-29T12:00:00Z', 'competitors': [
            {'athlete': {'displayName': 'Player A'}},
            {'athlete': {'displayName': 'Player B'}},
        ]}],
    }]}]}
    rows = schedule_events(payload, 'atp')
    assert rows[0]['id'] == 'm1'
    assert rows[0]['tournament_name'] == 'Tokyo Open'


def test_tennis_parser_keeps_gender_groups_separate():
    payload = {'events': [{'id': 't', 'name': 'Open', 'groupings': [
        {'grouping': {'slug': 'mens-singles'}, 'competitions': []},
        {'grouping': {'slug': 'womens-singles'}, 'competitions': [{
            'id': 'w1', 'date': '2026-09-29T12:00:00Z',
            'competitors': [{'athlete': {'displayName': 'A'}}, {'athlete': {'displayName': 'B'}}],
        }]},
    ]}]}
    assert schedule_events(payload, 'atp') == []
    assert schedule_events(payload, 'wta')[0]['id'] == 'w1'
