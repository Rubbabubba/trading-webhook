# Patch 160 — candidate sizing truth alignment

- adds cap-aware candidate sizing truth helper shared by scan and diagnostics paths
- candidate estimated_qty is clipped for truth/reporting against remaining portfolio and symbol exposure capacity
- stores sizing_truth with raw qty, effective qty, projected notional, remaining capacity, and basis
- rejection text can now show basis and qty clipping context for portfolio exposure rejections
