# `db/validation/canonical/`

Use this directory for SQL that validates the canonical layer after migrations and backfills land.

This starts with schema-contract checks for Task 3 and can later expand into data-quality checks after backfills exist.

Expected check types:
- table, extension, helper, and trigger existence
- PK/FK, uniqueness, and index checks
- smoke tests that exercise canonical inserts and constraint behavior
- later row-count, orphan, duplicate, and coverage checks after backfill work exists

Naming convention:
- `NN_check_name.sql`
- keep numeric prefixes in execution order
- keep filenames short and specific, for example `01_row_counts.sql`

Outputs from these checks should be written to `reports/qa/` when they become real validation runs.
