
# PATCH 120 — Profitability Recalibration

PATCH_VERSION = "patch-120-profitability-recalibration-phase-a"

def recalibrate_candidate_filters(candidate):
    # Lower ATR threshold (example logic)
    if candidate.get("atr_pct", 0) < 0.8:  # previously higher
        return False, "atr_too_low"

    # Relax breakout distance slightly
    if candidate.get("breakout_distance_pct", 0) < -8.0:
        return False, "too_far_below_breakout"

    return True, None
