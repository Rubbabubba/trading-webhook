"""Fresh transport is distinct from game progress. Existing safety gates remain."""
import hashlib
import json
from .college_football_paper import timestamp


def audit(summary, model, received_at, now, memory):
    c = (summary.get('header', {}).get('competitions') or [{}])[0]
    status = c.get('status', {})
    teams = {t.get('homeAway'): t for t in c.get('competitors', [])}
    details = model.get('details', {})
    state = {'scores': {s: t.get('score') for s,t in teams.items()},
             'period': status.get('period'), 'clock': status.get('displayClock'),
             'possession': (summary.get('situation') or {}).get('possession'),
             'play_id': details.get('play_id'), 'probabilities': model.get('probabilities')}
    signature = hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
    if memory.get('sync_signature') != signature:
        memory.update(sync_signature=signature, state_changed_at=now.isoformat())
    try:
        transport_age = (now-timestamp(received_at)).total_seconds()
    except (ValueError,TypeError,AttributeError):
        transport_age = None
    blockers = model['blockers']
    if transport_age is None or not 0 <= transport_age <= 15:
        blockers.append('live_game_transport_over_15_seconds')
    situation = details.get('situation', {})
    period = (situation.get('period') or {}).get('number')
    if period is not None and status.get('period') != period:
        blockers.append('probability_period_mismatch')
    age = details.get('play_age_seconds')
    label = ('transport_unavailable' if transport_age is None or transport_age > 15 else
             'inconsistent_state' if any('mismatch' in b for b in blockers) else
             'fresh_transport_old_play' if age is not None and age > 90 else 'current')
    return {'transport_age_seconds':transport_age, 'last_state_change_at':memory.get('state_changed_at'),
            'play_age_seconds':age, 'classification':label, 'state':state,
            'note':'Old plays are diagnosed separately; no timestamps repaired or stale probabilities admitted.'}
