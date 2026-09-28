from datetime import datetime, timedelta, timezone

from opportunity_lab.sports_persistent_passive import evaluate, initial, position_action


CONFIG = {
    'contracts': 1,
    'tick_size': .01,
    'improve_by': .01,
    'max_spread': .04,
    'minimum_stressed_edge': .08,
    'adverse_selection_stress': .02,
    'minimum_confirmation_seconds': 15,
    'contradictory_states_for_exit': 2,
}
NOW = datetime(2026, 9, 28, tzinfo=timezone.utc)


def observation(signature='play-1', probability=.72, blockers=None):
    return {
        'admission_ok': True,
        'model': {'signature': signature, 'probabilities': {'home': probability, 'away': 1-probability},
                  'blockers': blockers or [], 'completed': False},
        'markets': {
            'home': {'ticker': 'HOME', 'valid': True, 'maker_fee_coefficient': .0175,
                     'quote': {'bid': .55, 'ask': .57}},
            'away': {'ticker': 'AWAY', 'valid': True, 'maker_fee_coefficient': .0175,
                     'quote': {'bid': .43, 'ask': .45}},
        },
    }


def test_requires_two_distinct_persistent_play_states_and_is_post_only():
    state = initial()
    assert evaluate(state, observation(), NOW, CONFIG)['reason'] == 'awaiting_second_distinct_play'
    assert evaluate(state, observation(), NOW + timedelta(seconds=20), CONFIG)['reason'] == 'awaiting_second_distinct_play'
    result = evaluate(state, observation('play-2'), NOW + timedelta(seconds=20), CONFIG)
    assert result['action'] == 'shadow_post_only_signal'
    assert result['signal']['contracts'] == 1
    assert result['signal']['post_only'] is True
    assert result['signal']['limit_price'] < .57
    assert evaluate(state, observation('play-3'), NOW + timedelta(seconds=40), CONFIG)['reason'] == 'one_signal_per_event'


def test_signal_does_not_survive_source_failure_or_side_flip():
    state = initial()
    evaluate(state, observation(), NOW, CONFIG)
    assert evaluate(state, observation('play-2', blockers=['stale']), NOW + timedelta(seconds=20), CONFIG)['reason'] == 'source_or_mapping_gate'
    assert evaluate(state, observation('play-3'), NOW + timedelta(seconds=40), CONFIG)['reason'] == 'awaiting_second_distinct_play'


def test_feed_failure_freezes_position_instead_of_forcing_taker_exit():
    broken = observation(blockers=['transport_failure'])
    assert position_action({'side': 'home'}, broken, 99, CONFIG) == {
        'action': 'hold', 'reason': 'feed_or_mapping_failure_freeze'}


def test_exit_requires_two_fresh_contradictory_states():
    position = {'side': 'home'}
    assert position_action(position, observation(), 1, CONFIG)['action'] == 'hold'
    assert position_action(position, observation(), 2, CONFIG)['action'] == 'stage_post_only_exit'
