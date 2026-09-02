import app
from fastapi import HTTPException


def test_release_transition_allows_direct_paper_to_live_guarded():
    assert app._release_transition_allowed('paper', 'live_guarded') is True


def test_manual_live_guarded_promotion_can_arm_despite_regime_and_proof_blockers(monkeypatch):
    original_state = app.RELEASE_STATE
    try:
        monkeypatch.setattr(app, 'RELEASE_PROMOTION_REQUIRE_READINESS', True)
        monkeypatch.setattr(app, 'RELEASE_PROMOTION_MANUAL_ARM_ALLOWED', True)
        monkeypatch.setattr(app, 'SYSTEM_RELEASE_STAGE', 'live_guarded')
        app.RELEASE_STATE = {
            'configured_stage': 'paper',
            'current_stage': 'paper',
            'approval_status': 'not_required',
            'approval_armed': False,
            'history': [],
        }

        def fake_snapshot(include_gate=True):
            return {
                'promotion_targets': {
                    'live_guarded': {
                        'ready': False,
                        'unmet_conditions': [
                            'regime_not_favorable',
                            'insufficient_entry_events',
                            'insufficient_exit_events',
                        ],
                        'arm_ready': True,
                        'arm_unmet_conditions': [],
                    }
                }
            }

        monkeypatch.setattr(app, '_release_workflow_snapshot', fake_snapshot)
        monkeypatch.setattr(app, 'persist_release_state', lambda reason='': True)
        result = app.release_stage_transition('live_guarded', actor='test', reason='arm_before_regime')
        assert app.RELEASE_STATE['current_stage'] == 'live_guarded'
        assert app.RELEASE_STATE['approval_armed'] is True
        assert result['promotion_targets']['live_guarded']['arm_ready'] is True
    finally:
        app.RELEASE_STATE = original_state


def test_manual_live_guarded_promotion_still_blocks_hard_safety_failures(monkeypatch):
    monkeypatch.setattr(app, 'RELEASE_PROMOTION_REQUIRE_READINESS', True)
    monkeypatch.setattr(app, 'RELEASE_PROMOTION_MANUAL_ARM_ALLOWED', True)
    preflight = {
        'unmet_conditions': [
            'regime_not_favorable',
            'kill_switch_on',
            'continuity_issues_present',
        ]
    }
    unmet = app._manual_promotion_unmet_conditions('live_guarded', preflight)
    assert 'kill_switch_on' in unmet
    assert 'continuity_issues_present' in unmet
    assert 'regime_not_favorable' not in unmet
