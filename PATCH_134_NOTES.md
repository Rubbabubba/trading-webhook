# Patch 134 Notes

## Patch Version
`patch-134-scan-truth-alignment-observability-fix`

## Goal
Align dashboard and freshness surfaces with authoritative scan truth when the latest stored LAST_SCAN is stale on `scan_exception`.

## Changes
- Added aligned last-scan display helper that prefers recent authoritative scan status when available.
- Updated freshness snapshot to expose `aligned_last_scan`.
- Deferred `paper_lifecycle` freshness when there has been no same-session lifecycle event yet, preventing false `freshness_degraded` warnings.
- Updated dashboard Last scan card to show aligned scan truth and source.

## Non-Goals
- No entry logic changes
- No exit logic changes
- No release-gating changes
- No position sizing changes
- No correlation rule changes

## Expected Result
- Dashboard no longer misleadingly emphasizes `scan_exception` when runtime scan truth is healthy.
- Freshness warning should clear when the only stale artifact is lack of same-session paper lifecycle activity.
- Current live candidate behavior remains unchanged.
