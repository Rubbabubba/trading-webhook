"""Uncalibrated research models. Pregame market anchors are not independent edge."""
from functools import lru_cache
import math

from .college_football_paper import timestamp
from .sports_paper_v2 import parse_game


def poisson(mu):
    values = [math.exp(-mu)]
    for n in range(1, 45):
        values.append(values[-1] * mu / n)
    if 1 - sum(values) > 1e-8:
        raise ValueError('Poisson tail exceeds tolerance')
    return values


def outcomes(home_mu, away_mu, lead=0):
    home, away = poisson(home_mu), poisson(away_mu)
    # Prefix sums keep fitting inexpensive without truncating meaningful mass.
    cumulative = [0.0]
    for p in away:
        cumulative.append(cumulative[-1] + p)
    win = draw = 0.0
    for n, p in enumerate(home):
        target = n + lead
        win += p * cumulative[min(len(away), max(0, target))]
        if 0 <= target < len(away):
            draw += p * away[target]
    return {'home': win, 'draw': draw, 'away': max(0, 1 - win - draw)}


def fit_soccer(prior):
    def error(h, a):
        result = outcomes(h, a)
        return sum((result[s] - prior[s]) ** 2 for s in prior)
    _, h, a = min((error(h / 5, a / 5), h / 5, a / 5)
                  for h in range(1, 26) for a in range(1, 26))
    _, h, a = min((error(h + i / 40, a + j / 40), h + i / 40, a + j / 40)
                  for i in range(-4, 5) for j in range(-4, 5))
    residual = max(abs(outcomes(h, a)[s] - prior[s]) for s in prior)
    if residual > .04:
        raise ValueError('Pregame soccer probabilities incompatible with Poisson model')
    return {'home_rate': h, 'away_rate': a, 'fit_max_error': residual}


@lru_cache(maxsize=50000)
def set_probability(h, a, g):
    if max(h, a) >= 6 and abs(h - a) >= 2 or max(h, a) == 7:
        return float(h > a)
    if (h, a) == (6, 6):
        return g  # Frozen approximation: tiebreak win chance equals game chance.
    return g * set_probability(h + 1, a, g) + (1 - g) * set_probability(h, a + 1, g)


def tennis_probability(sets_h, sets_a, games_h, games_a, best_of, g):
    target = best_of // 2 + 1
    new_set = set_probability(0, 0, g)
    @lru_cache(None)
    def future(h, a):
        if h >= target or a >= target:
            return float(h >= target)
        return new_set * future(h + 1, a) + (1 - new_set) * future(h, a + 1)
    if sets_h >= target or sets_a >= target:
        return float(sets_h >= target)
    p = set_probability(games_h, games_a, g)
    return p * future(sets_h + 1, sets_a) + (1 - p) * future(sets_h, sets_a + 1)


def fit_prior(league, prior):
    if league in ('epl', 'mls'):
        return fit_soccer(prior)
    low, high = (.1, .9) if league in ('atp', 'wta') else (.2, 5.0)
    for _ in range(36):
        mid = (low + high) / 2
        if league in ('atp', 'wta'):
            p = tennis_probability(0, 0, 0, 0, 5 if league == 'atp' else 3, mid)
        else:
            result = outcomes(4.5 * mid, 4.5 / mid)
            p = result['home'] + result['draw'] / 2
        if p < prior['home']:
            low = mid
        else:
            high = mid
    return {'game_probability': (low + high) / 2} if league in ('atp', 'wta') else {'strength': (low + high) / 2}


def participants(competition):
    return {c.get('homeAway'): c for c in competition.get('competitors', [])}


def identity(competition, config):
    sides = participants(competition)
    return str(competition.get('id')) == config['event_id'] and all(
        str(sides.get(s, {}).get('id')) == config[s + '_team_id'] for s in ('home', 'away'))


def score(value):
    if isinstance(value, dict):
        value = value.get('value', value.get('displayValue'))
    result = float(value)
    if not math.isfinite(result) or result < 0 or not result.is_integer():
        raise ValueError('Invalid score')
    return int(result)


def estimate(config, summary, competition, reference, anchor, now):
    """Return probabilities plus explicit state/identity gates, without repairing timestamps."""
    league = config['league']
    if not identity(competition, config):
        raise ValueError('Game or participant identity mismatch')
    kind = competition.get('status', {}).get('type', {})
    result = {'probabilities': {}, 'completed': bool(kind.get('completed')),
              'phase': kind.get('name'), 'state': kind.get('state'), 'blockers': [], 'signature': None}
    if result['completed'] or kind.get('state') != 'in' or kind.get('name') != 'STATUS_IN_PROGRESS':
        result['blockers'].append('not_active_play')
        return result
    if league == 'ncaaf':
        parsed = parse_game(summary, {**config, 'max_signal_age_seconds': 90}, now)
        result.update(blockers=parsed['blockers'], signature=str(parsed['play_id']), details=parsed)
        if parsed['home_probability'] is not None:
            p = parsed['home_probability']
            result['probabilities'] = {'home': p, 'away': 1 - p}
        return result
    if not anchor:
        result['blockers'].append('no_prospective_pregame_anchor')
        return result
    teams = participants(competition)
    if league in ('epl', 'mls'):
        status = competition['status']
        clock = status.get('clock')
        if clock is None:
            # ESPN soccer displayClock is elapsed minutes, sometimes 45'+2'.
            import re
            pieces = re.fullmatch(r"(\d+)(?:'|′)?(?:\+(\d+)(?:'|′)?)?", status.get('displayClock', ''))
            if not pieces:
                raise ValueError('Soccer elapsed clock unavailable')
            clock = 60 * (int(pieces[1]) + int(pieces[2] or 0))
        minutes = float(clock) / 60
        if not math.isfinite(minutes) or not 0 <= minutes < 90:
            raise ValueError('Late/stoppage/extra-time model unsupported')
        for team in teams.values():
            for stat in team.get('statistics', []):
                if 'redcard' in stat.get('name', '').lower() and float(stat.get('displayValue', 0)):
                    raise ValueError('Red-card model unsupported')
        scores = {s: score(teams[s]['score']) for s in teams}
        fraction = (90 - minutes) / 90
        result.update(probabilities=outcomes(anchor['home_rate'] * fraction, anchor['away_rate'] * fraction,
                                             scores['home'] - scores['away']),
                      signature=str((minutes, scores)), details={'minutes': minutes, 'scores': scores})
    elif league == 'mlb':
        if str(reference.get('gamePk')) != str(config['reference_game_pk']):
            raise ValueError('MLB reference identity mismatch')
        if reference.get('gameData', {}).get('status', {}).get('abstractGameState') != 'Live':
            raise ValueError('MLB reference not live')
        lines = reference['liveData']['linescore']
        inning, outs, half = int(lines['currentInning']), int(lines['outs']), lines['inningHalf']
        if not 1 <= inning <= 9 or not 0 <= outs < 3 or half not in ('Top', 'Bottom'):
            raise ValueError('Extra innings or inning transition unsupported')
        scores = {s: int(lines['teams'][s]['runs']) for s in ('home', 'away')}
        if any(scores[s] != score(teams[s]['score']) for s in scores):
            raise ValueError('MLB source scores disagree')
        play = reference['liveData']['plays']['currentPlay']
        times = [p.get('endTime') for p in play.get('playEvents', []) if p.get('endTime')]
        wallclock = times[-1] if times else play.get('about', {}).get('endTime')
        if not wallclock or not -5 <= (now - timestamp(wallclock)).total_seconds() <= 90:
            raise ValueError('MLB reference play timestamp stale/missing')
        if half == 'Bottom' and inning == 9 and scores['home'] > scores['away']:
            raise ValueError('Walkoff transition')
        home_outs = 27 - (inning - 1) * 3 - (outs if half == 'Bottom' else 0)
        away_outs = 27 - (inning - 1) * 3 - (3 if half == 'Bottom' else outs)
        strength = anchor['strength']
        h, a = home_outs / 6 * strength, away_outs / 6 / strength
        offense = lines.get('offense', {})
        base_bonus = sum(weight for base, weight in (('first', .2), ('second', .4), ('third', .6)) if base in offense)
        base_bonus *= (3 - outs) / 3
        if half == 'Top':
            a += base_bonus
        else:
            h += base_bonus
        p = outcomes(h, a, scores['home'] - scores['away'])
        result.update(probabilities={'home': p['home'] + p['draw'] / 2, 'away': p['away'] + p['draw'] / 2},
                      signature=str((inning, half, outs, scores, wallclock)),
                      details={'inning': inning, 'half': half, 'outs': outs, 'scores': scores, 'play_timestamp': wallclock})
    else:
        best_of = 5 if league == 'atp' else 3
        sets = {'home': 0, 'away': 0}
        rows = {s: teams[s].get('linescores', []) for s in teams}
        if len(rows['home']) != len(rows['away']) or not rows['home'] or len(rows['home']) > best_of:
            raise ValueError('Tennis set scores unavailable')
        gh = ga = 0
        for i, (home, away) in enumerate(zip(rows['home'], rows['away'])):
            gh, ga = score(home['value']), score(away['value'])
            if max(gh, ga) > 7 or (max(gh, ga) == 7 and min(gh, ga) not in (5, 6)):
                raise ValueError('Unsupported tennis set score')
            finished = max(gh, ga) >= 6 and abs(gh - ga) >= 2 or max(gh, ga) == 7
            if finished:
                sets['home' if gh > ga else 'away'] += 1
                gh = ga = 0
            elif i != len(rows['home']) - 1:
                raise ValueError('Unfinished prior tennis set')
        if max(sets.values()) >= best_of // 2 + 1:
            raise ValueError('Match completion transition')
        p = tennis_probability(sets['home'], sets['away'], gh, ga, best_of, anchor['game_probability'])
        result.update(probabilities={'home': p, 'away': 1 - p}, signature=str((sets, gh, ga)),
                      details={'sets': sets, 'games': [gh, ga], 'best_of': best_of,
                               'limitation': 'Game-level only; ignores current points and server; uncalibrated'})
    if any(not math.isfinite(p) or not 0 <= p <= 1 for p in result['probabilities'].values()):
        raise ValueError('Invalid model probability')
    return result
