# Patch 159 — candidate rejection truth on exception/fallback paths

- fixes dashboard exposure-capacity view to include `portfolio_exposure_remaining`
- annotates `portfolio_exposure_limit` rejection reasons with over-cap amount and basis when available
- unifies dashboard candidate/rejection rendering onto the effective scan summary, including fallback to latest completed scan after `scan_exception` / `dispatch_failure`
- uses the effective scan summary for failure decomposition on dashboard so sections agree on the same candidate set
- updates dashboard hero pill to Patch 159

No strategy, execution, sizing, or accounting behavior changed.
