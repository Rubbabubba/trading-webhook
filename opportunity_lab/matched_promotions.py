"""Manual-assist economics for promotions and matched betting."""

from __future__ import annotations


def evaluate_promotions(offers: list[dict]) -> dict:
    rows = []
    for offer in offers:
        kind = str(offer.get("type") or "").strip().lower()
        try:
            if kind == "bonus_bet":
                row = _bonus_bet(offer)
            elif kind == "deposit_match":
                row = _deposit_match(offer)
            else:
                row = {"name": offer.get("name"), "type": kind or None, "valid": False,
                       "error": "type_must_be_bonus_bet_or_deposit_match"}
        except (TypeError, ValueError, ZeroDivisionError) as exc:
            row = {"name": offer.get("name"), "type": kind or None, "valid": False, "error": str(exc)}
        row["execution_enabled"] = False
        rows.append(row)
    valid = [row for row in rows if row.get("valid")]
    valid.sort(key=lambda row: float(row.get("guaranteed_profit") or -1e99), reverse=True)
    return {"strategy": "matched_promotions_manual_assist", "offer_count": len(rows),
            "valid_offer_count": len(valid), "positive_offer_count": sum(row["guaranteed_profit"] > 0 for row in valid),
            "offers": valid + [row for row in rows if not row.get("valid")],
            "research_only": True, "execution_enabled": False}


def _bonus_bet(offer: dict) -> dict:
    bonus = _positive(offer, "bonus_amount")
    promo_odds = _odds(offer, "promo_decimal_odds")
    hedge_odds = _odds(offer, "hedge_decimal_odds")
    commission = _fraction(offer.get("hedge_commission_rate") or 0, "hedge_commission_rate")
    qualifying_loss = _nonnegative(offer.get("qualifying_loss") or 0, "qualifying_loss")
    withdrawal_cost = _nonnegative(offer.get("withdrawal_cost") or 0, "withdrawal_cost")
    effective_hedge_profit = (hedge_odds - 1) * (1 - commission)
    hedge_stake = bonus * (promo_odds - 1) / (1 + effective_hedge_profit)
    promo_win = bonus * (promo_odds - 1) - hedge_stake
    hedge_win = hedge_stake * effective_hedge_profit
    gross = min(promo_win, hedge_win)
    profit = gross - qualifying_loss - withdrawal_cost
    return _finalize(offer, "bonus_bet", bonus, hedge_stake, gross, profit,
                     {"promo_win_profit_before_costs": promo_win, "hedge_win_profit_before_costs": hedge_win,
                      "stake_returned_on_bonus": False})


def _deposit_match(offer: dict) -> dict:
    deposit = _positive(offer, "deposit_amount")
    match_rate = _fraction(offer.get("match_rate"), "match_rate")
    cap = _positive(offer, "maximum_bonus")
    bonus = min(deposit * match_rate, cap)
    conversion = _fraction(offer.get("estimated_conversion_rate"), "estimated_conversion_rate")
    wagering = _nonnegative(offer.get("wagering_requirement_multiple") or 0, "wagering_requirement_multiple")
    friction_rate = _fraction(offer.get("estimated_wagering_loss_rate") or 0, "estimated_wagering_loss_rate")
    withdrawal_cost = _nonnegative(offer.get("withdrawal_cost") or 0, "withdrawal_cost")
    wagering_volume = bonus * wagering
    wagering_cost = wagering_volume * friction_rate
    gross = bonus * conversion
    profit = gross - wagering_cost - withdrawal_cost
    return _finalize(offer, "deposit_match", deposit, 0.0, gross, profit,
                     {"bonus_amount": bonus, "required_wagering_volume": wagering_volume,
                      "estimated_wagering_cost": wagering_cost})


def _finalize(offer, kind, capital, hedge_stake, gross, profit, details):
    jurisdiction = offer.get("texas_available") is True
    account = offer.get("account_eligible") is True
    terms = offer.get("terms_confirmed") is True
    blockers = []
    if not jurisdiction: blockers.append("texas_availability_not_confirmed")
    if not account: blockers.append("account_eligibility_not_confirmed")
    if not terms: blockers.append("promotion_and_settlement_terms_not_confirmed")
    if profit <= 0: blockers.append("not_profitable_after_modeled_friction")
    return {"name": offer.get("name"), "venue": offer.get("venue"), "type": kind, "valid": True,
            "capital_or_bonus_amount": round(capital, 2), "recommended_hedge_stake": round(hedge_stake, 2),
            "gross_conversion_value": round(gross, 2), "guaranteed_profit": round(profit, 2),
            "roi_on_input_amount_pct": round(profit / capital * 100, 4), "details": {key: round(value, 4) if isinstance(value, float) else value for key, value in details.items()},
            "eligible": not blockers, "blockers": blockers}


def _positive(row, key):
    value = float(row.get(key))
    if value <= 0: raise ValueError(f"{key}_must_be_positive")
    return value


def _nonnegative(value, key):
    value = float(value)
    if value < 0: raise ValueError(f"{key}_cannot_be_negative")
    return value


def _fraction(value, key):
    value = float(value)
    if not 0 <= value <= 1: raise ValueError(f"{key}_must_be_between_zero_and_one")
    return value


def _odds(row, key):
    value = float(row.get(key))
    if value <= 1: raise ValueError(f"{key}_must_exceed_one")
    return value
