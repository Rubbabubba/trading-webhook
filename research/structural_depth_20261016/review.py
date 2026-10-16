"""Keep counterfactual orderings separate; no phantom zero-return trade."""
from pathlib import Path
from collections import Counter
import json,hashlib
ROOT=Path(__file__).parent

def run():
    p=ROOT/'replay-result.json';r=json.loads(p.read_text());attempts=r['attempts']
    if len({a['event_id'] for a in attempts})!=len(attempts):raise ValueError('event_dedup_failed')
    summaries={}
    for label in ('low_yes_first','high_no_first'):
        paths=[a['orders'][label] for a in attempts];scenarios={}
        for stress in ('0','2','5'):
            bundle=[s['normal_min_net_cents'][stress] for s in paths if s['state']=='conditional_normal_bundle']
            orphan=[s['modeled_net_cents'][stress] for s in paths if s['state']=='conditional_orphan_liquidation']
            unknown=[s['full_principal_loss_stress_cents'][stress] for s in paths if 'full_principal_loss_stress_cents' in s]
            scenarios[stress]=dict(conditional_bundle_normal_min_cents=sum(bundle) if bundle else None,conditional_orphan_liquidation_cents=sum(orphan) if orphan else None,unresolved_principal_stress_cents=sum(unknown) if unknown else None,combined_conditional_with_unresolved_stress_cents=sum(bundle)+sum(orphan)+sum(unknown) if paths else None,session_cluster_lcb_cents=None)
        summaries[label]=dict(states=dict(Counter(s['state'] for s in paths)),scenarios=scenarios)
    report=dict(schema='structural_delayed_state_review_v2',replay_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),complete_registered_window=r['complete_registered_window'],source_fault=r['source_fault'],validated_pairs=r['validated_pairs'],decision_window=r['decision_window'],counts=r['counts'],opportunity_event_attempts=len(attempts),orderings=summaries,actual_orders=0,actual_fills=0,realized_pnl_cents=None,profitability_proven=False,promotion_ready=False,execution_enabled=False,decision='do_not_advance_no_validated_executable_edge',blockers=['one_source_date_not_independent_date_holdout','no_authentic_broker_paired_fills','historical_rule_publication_unverified','administrative_outcome_payoffs_unverified','event_netting_and_cash_reconciliation_unverified','series_specific_historical_fee_terms_unverified','seed_metadata_scope_not_full_universe'],note='No attempts is absence of cost-qualified opportunities in this bounded cohort, not zero-return trading or proof of market-wide absence.')
    with (ROOT/'review.json').open('x') as f:json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps(report))
if __name__=='__main__':run()
