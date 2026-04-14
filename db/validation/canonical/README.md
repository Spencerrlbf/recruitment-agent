# `db/validation/canonical/`

Use this directory for SQL that validates the canonical layer after migrations and backfills land.

Expected check types:
- row-count reconciliation
- orphan detection
- duplicate detection
- coverage and completeness checks

Naming convention:
- `NN_check_name.sql`
- keep numeric prefixes in execution order
- keep filenames short and specific, for example `01_row_counts.sql`

Outputs from these checks should be written to `reports/qa/` when they become real validation runs.
