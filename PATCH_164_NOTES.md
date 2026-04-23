# Patch 164

- Fixes portfolio cap comparator so candidates are blocked only when projected notional exceeds remaining capacity for the active cap mode.
- Leaves sizing, ranking, and execution behavior unchanged.
- Removes duplicate portfolio_exposure_limit selection blocker append.
