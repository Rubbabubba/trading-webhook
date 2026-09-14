"""Fail-closed qualification matrix for the paper-only intraday system."""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from regime_intraday_option_replay import replay_option_execution
from regime_intraday_validation import broker_promotion_evidence, entry_execution_analysis, paper_fill_reconciliation


def _quote(minute: int, *, long_bid: float = 1.00, long_ask: float = 1.02,
           short_bid: float = .42, short_ask: float = .44, **flags: Any) -> dict[str, Any]:
    return {"timestamp": f"2026-09-01T14:{minute:02d}:00Z", "long_bid": long_bid, "long_ask": long_ask,
            "short_bid": short_bid, "short_ask": short_ask, **flags}


def production_scenario_matrix() -> list[dict[str, Any]]:
    """Production-equivalent execution cases with explicit expected outcomes."""
    base = {"data_source": "indicative", "entry_limit_debit": .65, "max_leg_spread_pct": .08,
            "stop_loss_fraction": .5, "option_stop_confirmations": 2, "roundtrip_fees_dollars": 1.30}
    cases = [
        ("entry_target_close", [_quote(0), _quote(1, long_bid=1.35, long_ask=1.37, short_bid=.44, short_ask=.46, underlying_target_hit=True)], {}, "closed", "underlying_target"),
        ("confirmed_option_stop", [_quote(0), _quote(1, long_bid=.72, long_ask=.74, short_bid=.42, short_ask=.44), _quote(2, long_bid=.70, long_ask=.72, short_bid=.42, short_ask=.44), _quote(3, long_bid=.69, long_ask=.71, short_bid=.42, short_ask=.44)], {}, "closed", "confirmed_option_stop"),
        ("immediate_underlying_stop", [_quote(0), _quote(1, long_bid=.80, long_ask=.82, short_bid=.42, short_ask=.44, underlying_stop_hit=True)], {}, "closed", "underlying_stop"),
        ("entry_never_marketable", [_quote(0, long_bid=1.18, long_ask=1.20)], {}, "no_fill", "entry_not_marketable"),
        ("partial_entry", [_quote(0)], {"broker_entry_status": "partially_filled"}, "requires_attention", "partial_fill"),
        ("broker_rejection", [_quote(0)], {"broker_entry_status": "rejected"}, "rejected", "broker_rejected"),
        ("wide_spread_rejection", [_quote(0, long_bid=.80, long_ask=1.02)], {}, "rejected", "wide_leg_spread"),
        ("missing_exit_quote", [_quote(0), _quote(1, long_bid=0, short_ask=0, underlying_stop_hit=True)], {}, "exit_requires_attention", "exit_quote_unavailable"),
        ("end_of_day_liquidation", [_quote(0), _quote(1, long_bid=.98, long_ask=1, short_bid=.42, short_ask=.44, end_of_day=True)], {}, "closed", "end_of_day"),
        ("one_minute_entry_latency", [_quote(0, long_ask=1.20), _quote(1), _quote(2, long_bid=1.20, long_ask=1.22, short_bid=.42, short_ask=.44, underlying_target_hit=True)], {"entry_latency_minutes": 1}, "closed", "underlying_target"),
        ("two_minute_exit_latency_unfilled", [_quote(0), _quote(1, underlying_stop_hit=True)], {"exit_latency_minutes": 2}, "exit_unfilled", "underlying_stop"),
        ("single_stop_confirmation", [_quote(0), _quote(1, long_bid=.72, long_ask=.74, short_bid=.42, short_ask=.44), _quote(2, long_bid=.71, long_ask=.73, short_bid=.42, short_ask=.44)], {"option_stop_confirmations": 1}, "closed", "confirmed_option_stop"),
    ]
    return [{"name": name, "case": {**base, "quotes": quotes, **overrides}, "expected_status": status, "expected_reason": reason}
            for name, quotes, overrides, status, reason in cases]


def run_mechanical_matrix() -> dict[str, Any]:
    rows = []
    for scenario in production_scenario_matrix():
        result = replay_option_execution(scenario["case"])
        passed = result.get("status") == scenario["expected_status"] and result.get("reason") == scenario["expected_reason"]
        rows.append({"name": scenario["name"], "passed": passed, "expected_status": scenario["expected_status"],
                     "actual_status": result.get("status"), "expected_reason": scenario["expected_reason"],
                     "actual_reason": result.get("reason")})
    return {"case_count": len(rows), "passed_count": sum(row["passed"] for row in rows),
            "failed_count": sum(not row["passed"] for row in rows), "passed": all(row["passed"] for row in rows), "cases": rows}


def run_randomized_execution_invariants(*, trials: int = 2000, seed: int = 20260914) -> dict[str, Any]:
    """Stress replay accounting and fail-closed states across deterministic random paths."""
    rng = random.Random(seed)
    violations = []
    statuses: dict[str, int] = {}
    for index in range(max(100, min(10000, int(trials)))):
        quote_count = rng.randint(1, 8)
        quotes = []
        for minute in range(quote_count):
            short_bid = rng.uniform(.20, .80)
            short_ask = short_bid + rng.uniform(.01, .12)
            long_bid = short_ask + rng.uniform(.01, .80)
            long_ask = long_bid + rng.uniform(.01, .15)
            flags = {}
            if minute > 0 and rng.random() < .08:
                flags[rng.choice(["underlying_stop_hit", "underlying_target_hit", "end_of_day"])] = True
            if minute > 0 and rng.random() < .04:
                long_bid = 0
            quotes.append(_quote(minute, long_bid=long_bid, long_ask=long_ask, short_bid=short_bid, short_ask=short_ask, **flags))
        case = {"data_source": "synthetic", "quotes": quotes, "entry_limit_debit": rng.uniform(.20, 1),
                "max_leg_spread_pct": rng.uniform(.04, .20), "entry_latency_minutes": rng.randint(0, 2),
                "exit_latency_minutes": rng.randint(0, 2), "option_stop_confirmations": rng.randint(1, 3),
                "stop_loss_fraction": rng.uniform(.25, .80), "slippage_per_side": rng.choice([0, .01, .02]),
                "roundtrip_fees_dollars": rng.choice([0, 1.30, 2.60])}
        result = replay_option_execution(case)
        status = str(result.get("status") or "missing")
        statuses[status] = statuses.get(status, 0) + 1
        problems = []
        if result.get("live_submission") is True:
            problems.append("live_submission_exposed")
        if status == "closed":
            if min(float(result.get("entry_debit") or 0), float(result.get("exit_credit") or 0)) <= 0:
                problems.append("closed_without_positive_fills")
            expected_net = round(float(result.get("gross_pnl_dollars") or 0) - float(result.get("fees_dollars") or 0), 2)
            if expected_net != result.get("net_pnl_dollars"):
                problems.append("net_pnl_not_gross_less_fees")
        if status in {"no_fill", "rejected"} and result.get("net_pnl_dollars") is not None:
            problems.append("unfilled_order_has_realized_pnl")
        if problems and len(violations) < 50:
            violations.append({"trial": index, "status": status, "problems": problems})
    return {"seed": seed, "trial_count": max(100, min(10000, int(trials))), "passed": not violations,
            "violation_count": len(violations), "status_counts": statuses, "violations": violations}


def qualification_report(ledger: dict[str, Any], *, readiness: dict[str, Any] | None = None,
                         trials: int = 2000) -> dict[str, Any]:
    mechanical = run_mechanical_matrix()
    invariants = run_randomized_execution_invariants(trials=trials)
    fills = paper_fill_reconciliation(ledger, estimated_round_trip_fees_dollars=1.30)
    entries = entry_execution_analysis(ledger)
    evidence = broker_promotion_evidence(ledger, minimum_roundtrips=30, target_roundtrips=50,
                                         estimated_round_trip_fees_dollars=1.30)
    paper_blockers = []
    if not mechanical["passed"]:
        paper_blockers.append("mechanical_scenario_matrix_failed")
    if not invariants["passed"]:
        paper_blockers.append("randomized_execution_invariant_failed")
    if readiness and readiness.get("paper_blockers"):
        paper_blockers.extend(f"runtime:{item}" for item in readiness["paper_blockers"])
    live_blockers = [*paper_blockers, *evidence["blockers"], "actual_opra_execution_evidence_required",
                     "live_order_transport_hard_closed", "explicit_human_capital_promotion_required"]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(), "qualification_version": "v1",
        "mechanical_matrix": mechanical, "randomized_invariants": invariants,
        "paper_execution": {"reconciliation": fills, "entry_execution": entries},
        "promotion_evidence": evidence, "runtime_readiness": readiness,
        "paper_production_qualified": not paper_blockers, "paper_blockers": list(dict.fromkeys(paper_blockers)),
        "live_capital_qualified": False, "live_blockers": list(dict.fromkeys(live_blockers)),
        "data_limitations": ["Free indicative option quotes are not actual OPRA BBO.",
                             "Alpaca paper fills do not model market impact, information leakage, latency slippage, or queue position."],
        "live_submission": False,
    }
