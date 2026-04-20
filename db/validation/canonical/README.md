# `db/validation/canonical/`

Use this directory for SQL that validates the canonical layer after migrations and backfills land.

This starts with schema-contract checks for Task 3 and expands in Task 5 into canonicalization support checks and semantic rule-behavior tests before later data-quality checks arrive with backfills.

Expected check types:
- table, extension, helper, trigger, and support-object existence
- PK/FK, uniqueness, and index checks
- semantic canonicalization behavior checks for normalization, dedupe decisions, source precedence, and ambiguity handling
- smoke tests that exercise canonical inserts and constraint behavior
- later row-count, orphan, duplicate, and coverage checks after backfill work exists

Naming convention:
- `NN_check_name.sql`
- keep numeric prefixes in execution order
- keep filenames short and specific, for example `01_row_counts.sql`

Outputs from these checks should be written to `reports/qa/` when they become real validation runs.
