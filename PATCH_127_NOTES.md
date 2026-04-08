PATCH 127 NOTES

Goal:
Fix release-stage authority so live env configuration can actually enable fully live trading/exits without deleting persisted release state.

Changes:
- RELEASE_STAGE now recognized alongside SYSTEM_RELEASE_STAGE
- Adds explicit support for release stage `live`
- When configured stage is `live`, env wins over persisted `live_guarded` state
- Live activation now arms for both `live_guarded` and `live`
- Transition and promotion helpers recognize `live`

No changes:
- scanner logic
- strategy selection
- reconcile
- entry/exit order logic beyond release gating
