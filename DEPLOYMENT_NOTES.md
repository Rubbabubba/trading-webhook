# Patch 079 – Preview plan activation

This patch builds on patch 078 and activates synthetic preview plan creation for current runtime preview selections without allowing order submission.

What changed:
- Selected symbols in current runtime preview now materialize as preview-only plans in runtime truth and paper execution proof diagnostics.
- Pipeline guardrails now show selected -> planned progression in preview mode without creating real orders.
- Coverage now reflects preview-selected/planned symbols so diagnostics match actual runtime preview state.

Safety:
- No broker orders are submitted.
- No live paper execution permission logic was widened.
- Preview plans are diagnostic-only and marked preview_only=true.
