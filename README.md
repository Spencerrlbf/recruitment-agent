# Recruitment AI Platform

Clean project workspace for rebuilding the candidate database and AI search stack.

This repo is intentionally separate from the legacy source workspace:
- Legacy source workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/Recruitment-Matching`
- New product workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/recruitment-ai-platform`

## Goals
- Create a normalized candidate and company data model.
- Backfill from the legacy Supabase project without mutating legacy tables.
- Build semantic search over clean candidate profile and experience data.
- Move the app and scripts onto a git-friendly codebase.

## Initial Structure
- `app/`: future API, workers, or UI code
- `db/migrations/`: SQL migrations for the new schema
- `db/validation/`: validation SQL and QA checks
- `docs/`: migration docs and schema notes
- `scripts/`: backfill and maintenance scripts
- `TASKS.md`: ordered implementation checklist

## Current Working Approach
- Keep the old Supabase project as the migration source.
- Create `v2` tables inside the same Supabase project first.
- Backfill into `v2`.
- Switch read paths after validation.
- Archive legacy structures later.

## Starting Documents
- [TASKS.md](./TASKS.md)
- [docs/legacy-data-model.md](./docs/legacy-data-model.md)
- [docs/target-data-model.md](./docs/target-data-model.md)
