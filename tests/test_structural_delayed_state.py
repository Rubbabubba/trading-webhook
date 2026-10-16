from research.structural_depth_20261016.replay import step,net

def attempt(first='low'):
    return dict(first=first,decision_asks=dict(low=30,high=40),state='awaiting_first')

def views(low=30,high=40):return dict(low=dict(ask=low,bid=low-2),high=dict(ask=high,bid=high-2))

def test_sequential_latency_and_all_costs_without_actual_fills():
    s=attempt();assert step(s,1.5,1,views())==2.5
    assert step(s,2.5,2.5,views()) is None
    assert s['state']=='conditional_normal_bundle'
    assert s['normal_min_net_cents']=={'0':24,'2':20,'5':14}
    assert s['second_at']-s['first_at']>=1

def test_late_first_and_price_chase_never_become_entries():
    s=attempt();step(s,2.01,1,views());assert s['state']=='first_unobserved_or_unavailable' and 'first_price' not in s
    s=attempt();step(s,1,1,views(low=31));assert s['state']=='first_price_guard_rejected'
    s=attempt();step(s,1,1,dict(low=views()['low'],high=None));assert 'first_price' not in s

def test_second_failure_preserves_orphan_unwind_costs_and_unknowns():
    s=attempt();step(s,1,1,views());assert step(s,2,2,views(high=41))==3
    step(s,3,3,dict(low=dict(ask=30,bid=28),high=None))
    assert s['state']=='conditional_orphan_liquidation' and s['modeled_net_cents']=={'0':-8,'2':-12,'5':-18}
    s=attempt();step(s,1,1,views());step(s,2,2,views(high=41));step(s,3,3,dict(low=None,high=None))
    assert s['state']=='orphan_unwind_unresolved' and s['full_principal_loss_stress_cents']=={'0':-33,'2':-35,'5':-38}

def test_alternate_order_is_separate_counterfactual_and_cannot_skip_delays():
    s=attempt('high');assert step(s,1,1,views())==2
    assert s['first_price']==40
    step(s,3.01,2,views());assert s['state']=='awaiting_orphan_unwind'
    assert net(30,40,2)==20
