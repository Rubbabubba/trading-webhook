# Patch 156 — Dashboard Hotfix + Namespace Sweep

Built from Patch 154 baseline.

## Purpose
Repair the dashboard/release-gate regression caused by using `time.time()` and `time.sleep()` while `time` was imported from `datetime`.

## Changes
- Replaced unsafe `time.sleep(...)` calls with `_time.sleep(...)`
- Replaced unsafe `time.time()` calls with `_time.time()`
- Swept `app.py` for direct `time.` runtime calls to ensure no remaining namespace-collision cases
- Updated patch version string to `patch-156-dashboard-hotfix-namespace-sweep`

## No Drift / No Regression
No changes to entry logic, scanner logic, exit logic, release gating rules, or performance accounting behavior.
