from copy import deepcopy
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import sqlite3

import pytest

from opportunity_lab.sports_research_execution import initial, decide, entry, cost
from opportunity_lab.sports_research_models_v11 import outcomes, fit_prior, tennis_probability, estimate
from opportunity_lab.sports_research_mapping import match
from opportunity_lab.sports_paper_research_v11 import Archive, fresh, open_paper, build_observation
from opportunity_lab.college_football_paper import timestamp

NOW = datetime(2026, 9, 12, 15, tzinfo=timezone.utc)


def observation():
    markets = {s: {'ticker': s, 'book_id': 1, 'valid': True, 'fee_coefficient': .07,
        'settlement_ok': False, 'quote': {'ask': .5, 'bid': .49, 'ask_size': 100, 'bid_size': 100}}
        for s in ('home', 'away', 'draw')}
    return {'snapshot': 'a', 'markets': markets, 'model': {'probabilities': {'home': .8, 'away': .1, 'draw': .1},
            'blockers': [], 'completed': False}}


def next_quote(obs, n):
    obs['snapshot'] = str(n)
    for m in obs['markets'].values():
        m['book_id'] = n


def enter():
    state, obs = initial(), observation()
    assert decide(state, obs, NOW, 300, .06)[0]['action'] == 'stage_buy'
    next_quote(obs, 2)
    assert decide(state, obs, NOW + timedelta(seconds=60), 300, .06)[0]['action'] == 'buy'
    return state, obs


def test_confirmation_requires_new_selected_book_not_other_updates():
    state, obs = initial(), observation()
    decide(state, obs, NOW, 300, .06)
    obs['snapshot'] = 'new_scoreboard'
    obs['markets']['away']['book_id'] = 2
    assert decide(state, obs, NOW + timedelta(seconds=60), 300, .06)[0]['reason'] == 'waiting_new_quote'
    assert state['entries'] == 0


def test_price_worsening_and_missing_liquidity_cancel_buy():
    state, obs = initial(), observation()
    decide(state, obs, NOW, 300, .06)
    next_quote(obs, 2)
    obs['markets']['home']['quote']['ask'] = .6
    assert decide(state, obs, NOW + timedelta(seconds=60), 300, .06)[0]['action'] == 'cancel'
    assert state['cash_cents'] == 100000
    obs['markets']['home']['quote']['ask_size'] = 0
    assert entry(obs['markets']['home'], .9, 100000, .06)[0] is None


def test_time_exit_cash_fees_and_drawdown_reconcile():
    state, obs = enter()
    assert state['entries'] == 1
    next_quote(obs, 3)
    assert decide(state, obs, NOW + timedelta(seconds=361), 300, .06)[0]['reason'] == 'holding_limit'
    next_quote(obs, 4)
    action = decide(state, obs, NOW + timedelta(seconds=421), 300, .06)[0]
    assert action['action'] == 'sell'
    assert state['cash_cents'] == 100000 + state['realized_cents']
    assert state['realized_cents'] == -10 - state['fees_cents'] - state['slippage_cents']
    assert state['drawdown_cents'] >= -state['realized_cents']


def test_horizons_are_separate_and_fifteen_minute_does_not_exit_at_five():
    state, obs = enter()
    next_quote(obs, 3)
    assert decide(state, obs, NOW + timedelta(seconds=361), 900, .06)[0]['action'] == 'hold'


def test_unavailable_exit_not_fabricated_and_settlement_once():
    state, obs = enter()
    next_quote(obs, 3)
    obs['markets']['home']['valid'] = False
    action = decide(state, obs, NOW + timedelta(seconds=400), 300, .06)[0]
    assert action['reason'] == 'exit_liquidity_unavailable' and state['position']
    assert state['equity_cents'] is None
    obs['markets']['home'].update(settlement_ok=True, result='yes')
    assert decide(state, obs, NOW + timedelta(seconds=460), 300, .06)[0]['action'] == 'settle'
    balance = state['cash_cents']
    obs['model']['blockers'] = ['finished']
    decide(state, obs, NOW + timedelta(seconds=520), 300, .06)
    assert state['cash_cents'] == balance and state['exits'] == 1


def test_draw_is_own_probability_and_pause_cancels_pending():
    state, obs = initial(), observation()
    obs['model']['probabilities'] = {'home': .1, 'away': .1, 'draw': .8}
    assert decide(state, obs, NOW, 300, .06)[0]['side'] == 'draw'
    decide(state, obs, NOW + timedelta(seconds=60), 300, .06, paused=True)
    assert state['pending'] is None and state['entries'] == 0


def test_stale_signal_blocks_entries():
    state, obs = initial(), observation()
    obs['model']['blockers'] = ['stale']
    assert decide(state, obs, NOW, 300, .06)[0]['reason'] == 'entry_gate'
    obs['model']['blockers'] = []
    obs['markets']['home']['valid'] = False
    assert decide(state, obs, NOW, 300, .06)[0]['action'] == 'wait'


def test_poisson_sum_symmetry_time_and_goal_direction():
    p = outcomes(1.4, 1.4)
    assert sum(p.values()) == pytest.approx(1)
    assert p['home'] == pytest.approx(p['away'])
    assert outcomes(1.4, 1.4, 1)['home'] > p['home']
    assert outcomes(0, 0) == {'home': 0, 'draw': 1, 'away': 0}
    assert outcomes(.01, .01, 1)['home'] > .98


def test_soccer_fit_recovers_prior_without_future_inputs():
    prior = outcomes(1.8, 1.0)
    fit = fit_prior('epl', prior)
    assert fit['home_rate'] == pytest.approx(1.8, abs=.03)
    assert fit['away_rate'] == pytest.approx(1, abs=.03)


def test_tennis_best_of_sets_and_prior():
    assert tennis_probability(0, 0, 0, 0, 3, .5) == pytest.approx(.5)
    assert tennis_probability(1, 0, 0, 0, 3, .5) == pytest.approx(.75)
    assert tennis_probability(1, 0, 0, 0, 5, .5) == pytest.approx(.6875)
    assert tennis_probability(0, 0, 5, 0, 3, .5) > .5
    fit = fit_prior('atp', {'home': .7, 'away': .3})
    assert tennis_probability(0, 0, 0, 0, 5, fit['game_probability']) == pytest.approx(.7, abs=1e-6)


def config():
    return {'league': 'epl', 'event_id': '1', 'home_team_id': 'h', 'away_team_id': 'a'}


def competition():
    return {'id': '1', 'status': {'clock': 1800, 'type': {'state': 'in', 'name': 'STATUS_IN_PROGRESS'}},
            'competitors': [{'id': 'h', 'homeAway': 'home', 'score': '1'}, {'id': 'a', 'homeAway': 'away', 'score': '0'}]}


def test_identity_missing_anchor_late_soccer_and_red_card():
    c = competition()
    assert estimate(config(), {}, c, {}, None, NOW)['blockers'] == ['no_prospective_pregame_anchor']
    c['competitors'][0]['id'] = 'WRONG'
    with pytest.raises(ValueError, match='identity'):
        estimate(config(), {}, c, {}, {}, NOW)
    c = competition()
    c['status']['clock'] = 5400
    with pytest.raises(ValueError, match='Late'):
        estimate(config(), {}, c, {}, {'home_rate': 1, 'away_rate': 1}, NOW)
    c = competition()
    c['competitors'][0]['statistics'] = [{'name': 'redCards', 'displayValue': '1'}]
    with pytest.raises(ValueError, match='Red-card'):
        estimate(config(), {}, c, {}, {'home_rate': 1, 'away_rate': 1}, NOW)


def test_raw_archive_hash_failure_and_http_age(tmp_path):
    path = tmp_path / 'raw.db'
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE responses(id INTEGER,game TEXT,endpoint TEXT,metadata TEXT,sha256 TEXT,body_gzip BLOB)')
    meta = {'status': 200, 'duration': .1, 'received_at': NOW.isoformat(), 'headers': {'Age': '0'}}
    db.execute('INSERT INTO responses VALUES(1,?,?,?,?,?)', ('epl', 'scoreboard', json.dumps(meta), 'bad', gzip.compress(b'{}')))
    db.commit(); db.close()
    reader = Archive(path); reader.refresh()
    assert not fresh(reader.cache['epl', 'scoreboard'], NOW)
    record = {'metadata': meta}
    assert fresh(record, NOW)
    assert not fresh(record, NOW + timedelta(seconds=121))
    meta['headers']['Age'] = 'nan'
    assert not fresh(record, NOW)


def test_paper_cannot_enable_real_execution(tmp_path):
    with pytest.raises(ValueError, match='Paper-only'):
        open_paper(tmp_path/'paper.db', {'version': 'sports_paper_research_1.1', 'execution_enabled': True})


def test_mapping_rejects_duplicate_and_wrong_date():
    event = {'id': '1', 'name': 'A at H', 'date': '2026-09-12T15:00Z', 'competitions': [
        {'competitors': [{'id': 'h', 'homeAway': 'home', 'team': {'displayName': 'H'}},
                         {'id': 'a', 'homeAway': 'away', 'team': {'displayName': 'A'}}]}]}
    markets = [{'ticker': 'KX-26SEP12AH-'+s, 'event_ticker': 'KX-26SEP12AH', 'yes_sub_title': s, 'market_type': 'binary'} for s in ('H','A')]
    source = {'series':'KX', 'sport':'football/college-football'}
    assert match(event, markets, 'ncaaf', source)
    assert match(event, markets + [markets[0]], 'ncaaf', source) is None
    event['date'] = '2026-09-13T15:00Z'
    assert match(event, markets, 'ncaaf', source) is None


def test_mlb_reference_score_timestamp_and_extra_innings_gates():
    c = competition()
    conf = {**config(), 'league': 'mlb', 'reference_game_pk': 123}
    ref = {'gamePk': 123, 'gameData': {'status': {'abstractGameState': 'Live'}}, 'liveData': {
        'linescore': {'currentInning': 3, 'outs': 1, 'inningHalf': 'Top', 'teams': {'home': {'runs': 1}, 'away': {'runs': 0}}, 'offense': {}},
        'plays': {'currentPlay': {'playEvents': [{'endTime': NOW.isoformat()}]}}}}
    result = estimate(conf, {}, c, ref, {'strength': 1}, NOW)
    assert result['probabilities']['home'] > .5 and sum(result['probabilities'].values()) == pytest.approx(1)
    ref['liveData']['linescore']['teams']['home']['runs'] = 2
    with pytest.raises(ValueError, match='scores disagree'):
        estimate(conf, {}, c, ref, {'strength': 1}, NOW)
    ref['liveData']['linescore']['teams']['home']['runs'] = 1
    with pytest.raises(ValueError, match='timestamp'):
        estimate(conf, {}, c, ref, {'strength': 1}, NOW + timedelta(seconds=91))
    ref['liveData']['linescore']['currentInning'] = 10
    with pytest.raises(ValueError, match='Extra innings'):
        estimate(conf, {}, c, ref, {'strength': 1}, NOW)


def test_pregame_anchor_is_never_backfilled_after_start():
    from types import SimpleNamespace
    def record(data, id=1):
        return {'id': id, 'data': data, 'metadata': {'status': 200, 'duration': .1, 'received_at': NOW.isoformat(), 'headers': {}}}
    comp = competition()
    comp['status']['type'] = {'state': 'pre', 'name': 'STATUS_SCHEDULED'}
    for team in comp['competitors']:
        team.pop('score', None)
    conf = {**config(), 'archive': 'expansion', 'series': 'KXEPLGAME', 'market_event': 'E',
            'kickoff': (NOW + timedelta(minutes=30)).isoformat(), 'stop_at': (NOW + timedelta(hours=12)).isoformat(),
            'markets': {'home': 'E-H', 'away': 'E-A', 'draw': 'E-T'},
            'mapping_evidence': {'markets': {s: {'rules_primary': 'rules', 'rules_secondary': ''} for s in ('home','away','draw')}}}
    cache = {('epl','scoreboard'): record({'events': [{'id':'1','competitions':[comp]}]}),
             ('epl_1','summary'): record({'header': {'competitions':[comp]}}),
             ('epl','fees'): record({'series': {'ticker':'KXEPLGAME', 'fee_type':'quadratic', 'fee_multiplier':1}})}
    market_list=[]
    for side,ticker in conf['markets'].items():
        market_list.append({'ticker':ticker,'event_ticker':'E','market_type':'binary','status':'active','rules_primary':'rules','rules_secondary':''})
        price = {'home': .44, 'away': .27, 'draw': .27}[side]
        cache['E',ticker+'_book'] = record({'orderbook_fp': {'yes_dollars': [[str(price),'100']], 'no_dollars': [[str(1-price-.02),'100']]}})
    cache['epl','market_list'] = record(market_list)
    archives = {'expansion':SimpleNamespace(cache=cache)}
    obs,anchor,memory = build_observation(conf,archives,None,{},NOW)
    assert anchor and timestamp(anchor['observed_at']) < timestamp(conf['kickoff'])
    conf['kickoff'] = (NOW - timedelta(seconds=1)).isoformat()
    assert build_observation(conf,archives,None,{},NOW)[1] is None


def test_ledger_restart_preserves_accounts_and_rejects_protocol_change(tmp_path, monkeypatch):
    import opportunity_lab.sports_paper_research_v11 as research
    monkeypatch.setattr(research, 'ROOT', tmp_path)
    source = tmp_path/'source.py'; source.write_text('paper only')
    protocol = {'version':'sports_paper_research_1.1','execution_enabled':False,
                'source_sha256':{'source.py':hashlib.sha256(source.read_bytes()).hexdigest()}}
    path=tmp_path/'paper.db'
    db = open_paper(path,protocol)
    state,obs=enter()
    with db:
        db.execute('INSERT INTO accounts VALUES(?,?,?)',('game',300,json.dumps(state)))
    db.close()
    db=open_paper(path,protocol)
    assert json.loads(db.execute('SELECT state FROM accounts').fetchone()[0]) == state
    db.close()
    with pytest.raises(ValueError, match='manifest mismatch'):
        open_paper(path,{**protocol,'changed_setting':True})
    source.write_text('changed')
    with pytest.raises(ValueError,match='Frozen source changed'):
        open_paper(path,protocol)


@pytest.mark.parametrize('league', ['epl', 'mls'])
@pytest.mark.parametrize('phase', ['STATUS_FIRST_HALF', 'STATUS_SECOND_HALF', 'STATUS_IN_PROGRESS'])
def test_soccer_active_half(league, phase):
    c = competition()
    c['status']['type']['name'] = phase
    result = estimate({**config(), 'league': league}, {}, c, {}, {'home_rate': 1.4, 'away_rate': 1.1}, NOW)
    assert not result['blockers']
    assert sum(result['probabilities'].values()) == pytest.approx(1)


@pytest.mark.parametrize('phase', ['STATUS_HALFTIME', 'STATUS_SUSPENDED', 'STATUS_EXTRA_TIME', 'UNKNOWN'])
def test_soccer_inactive_remains_blocked(phase):
    c = competition()
    c['status']['type']['name'] = phase
    assert estimate(config(), {}, c, {}, {'home_rate': 1, 'away_rate': 1}, NOW)['blockers'] == ['not_active_play']


def test_soccer_pregame_optional_scores_live_scores_required():
    from copy import deepcopy
    from opportunity_lab.sports_paper_research_v11 import soccer_sources_agree
    c = competition()
    c['status']['type'] = {'state': 'pre', 'name': 'STATUS_SCHEDULED'}
    for team in c['competitors']:
        team['score'] = '0'
    s = deepcopy(c)
    for team in s['competitors']:
        team.pop('score')
    assert soccer_sources_agree(c, s, config())
    c['competitors'][0]['score'] = '1'
    assert not soccer_sources_agree(c, s, config())
    c['status']['type']['state'] = 'in'
    assert not soccer_sources_agree(c, s, config())
    s['status']['type']['state'] = 'in'
    with pytest.raises((ValueError, TypeError)):
        soccer_sources_agree(c, s, config())
    s = deepcopy(c)
    s['competitors'][0]['score'] = '2'
    assert not soccer_sources_agree(c, s, config())
    s['competitors'][0]['id'] = 'wrong'
    assert not soccer_sources_agree(c, s, config())


def test_reports_are_bounded(monkeypatch):
    import opportunity_lab.sports_paper_research_v11 as r
    calls = []
    monkeypatch.setattr(r, 'report_game', lambda db, slug, output: calls.append(slug))
    pending = dict.fromkeys(['a', 'b', 'c'])
    r.drain_report(None, pending, None)
    assert calls == ['a'] and list(pending) == ['b', 'c']
