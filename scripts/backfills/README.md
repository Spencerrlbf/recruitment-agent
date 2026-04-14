# `scripts/backfills/`

Use this directory for long-running migration and rebuild entrypoints.

Expectations:
- deterministic ordering
- resumable progress through `scripts/checkpoints/`
- idempotent writes so reruns are safe
- explicit dry-run support before real writes

Naming convention:
- `NN_scope_backfill.py`
- examples: `01_companies_backfill.py`, `20_rebuild_candidate_chunks.py`
- keep one primary data movement concern per script

Start from `backfill_template.py` when adding a new job.
