# `db/migrations/`

Use this directory for forward-only schema migrations.

What belongs here:
- DDL files that create or alter schema objects
- index and constraint changes that are part of an ordered migration
- migration comments that explain non-obvious rollout sequencing

What does not belong here:
- validation SQL
- backfill scripts
- generated checkpoints
- QA or evaluation reports

Naming convention:
- `YYYYMMDDHHMMSS__short_description.sql`
- use lowercase snake case after the double underscore
- keep one migration concern per file where practical

Task 2 note:
- this scaffold intentionally does not add domain-table DDL yet
