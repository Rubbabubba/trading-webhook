from opportunity_lab.matched_promotions import evaluate_promotions


def test_bonus_bet_equalizes_outcomes_and_stays_manual():
    result = evaluate_promotions([{"name": "Example", "type": "bonus_bet", "bonus_amount": 100,
        "promo_decimal_odds": 3, "hedge_decimal_odds": 2, "hedge_commission_rate": 0,
        "texas_available": True, "account_eligible": True, "terms_confirmed": True}])
    row = result["offers"][0]
    assert row["recommended_hedge_stake"] == 100
    assert row["guaranteed_profit"] == 100
    assert row["eligible"] is True
    assert result["execution_enabled"] is False


def test_unconfirmed_jurisdiction_blocks_positive_offer():
    row = evaluate_promotions([{"name": "Example", "type": "deposit_match", "deposit_amount": 100,
        "match_rate": .5, "maximum_bonus": 50, "estimated_conversion_rate": .8,
        "wagering_requirement_multiple": 1, "estimated_wagering_loss_rate": .05}])["offers"][0]
    assert row["guaranteed_profit"] > 0
    assert row["eligible"] is False
    assert "texas_availability_not_confirmed" in row["blockers"]
