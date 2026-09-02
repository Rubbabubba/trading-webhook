# Archive

This folder contains historical artifacts that are not part of the active runtime path.

Archived files are kept for forensic/debug reference while the production swing system is being cleaned up and modularized.

## Folders

- `legacy_patch_notes/` - old patch note files kept for history.
- `legacy_roadmaps/` - superseded strategy and migration roadmaps.
- `legacy_workers/` - redundant worker entry points no longer used by Render.
- `operator_review_snapshots/` - old endpoint snapshots and handoff notes.
- `tools/` - one-off local utility scripts that are not part of Render runtime.

Nothing in this folder should be imported by production code.
