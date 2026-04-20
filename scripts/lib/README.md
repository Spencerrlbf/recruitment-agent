# `scripts/lib/`

Use this directory for shared Python helpers that support migrations, backfills, validation runners, and report generation.

What belongs here:
- checkpoint helpers
- shared database/session helpers
- reusable serialization and report-writing utilities

Naming convention:
- `lower_snake_case.py`
- keep each module reusable across multiple scripts
- keep business logic in the calling script, not in generic helpers

Current shared helpers in this directory include:
- `checkpoint.py` for atomic JSON checkpoint state
- `psql.py` for lightweight `psql`-backed database access without extra Python dependencies
