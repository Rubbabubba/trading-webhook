Patch 084: paper proof control alignment

- Adds paper proof gate snapshot so proof capture uses paper-session controls instead of guarded-live controls.
- Exposes paper_proof_eligible, paper_orders_permitted, and paper_proof_unmet_conditions on release and live readiness diagnostics.
- Aligns /diagnostics/proof_capture_plan blockers and arming gaps with paper execution reality.
- Preserves guarded live gating for live promotion logic; this patch only fixes paper proof truth.
