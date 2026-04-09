# Candidate DB Cleanup And AI Search Migration

This file breaks the migration into small tasks that can be implemented one at a time.

Rules for this project:
- Build the new model inside the current Supabase project.
- Do not mutate or delete legacy tables during the migration.
- Keep candidate IDs stable where possible.
- Prefer deterministic backfills and idempotent scripts.
- Validate each phase before moving to the next one.

## [ ] Task 1: Lock The Target Data Model
- Define the exact `v2` tables to build:
  - `candidate_profiles_v2`
  - `candidate_emails_v2`
  - `companies_v2`
  - `candidate_experiences_v2`
  - `candidate_search_documents`
- Decide which legacy fields map into each new table.
- Decide which legacy tables are out of scope for phase 1.
- Write the field mapping doc before any DDL is created.

Done when:
- There is a reviewed mapping from legacy tables and JSON fields to each `v2` table.
- We agree on required unique constraints and foreign keys.

## [ ] Task 2: Create Migration Scaffolding
- Create a folder for migration code and SQL.
- Add a repeatable migration runner script or command entrypoint.
- Add a small checkpoint strategy so long backfills can resume safely.
- Add a place for validation queries and QA reports.

Done when:
- There is a clear folder structure for SQL, backfill scripts, and validation.
- A backfill can be resumed without reprocessing everything.

## [ ] Task 3: Create The New Tables And Constraints
- Create the new `v2` tables in the same project.
- Add primary keys, foreign keys, timestamps, and uniqueness rules.
- Add indexes needed for candidate lookup, company lookup, and candidate-company joins.
- Keep old tables untouched.

Done when:
- All new tables exist.
- Constraints compile successfully.
- We can insert test rows into the new tables.

## [ ] Task 4: Add Canonicalization Rules
- Define how emails are normalized.
- Define how company identity is resolved:
  - `linkedin_id` first
  - then `linkedin_username`
  - then canonical LinkedIn URL
  - then normalized company name fallback
- Define how dates and current-role flags are normalized from LinkedIn experience JSON.

Done when:
- We have deterministic normalization rules for emails, companies, and experience dates.
- These rules are implemented in reusable code or SQL helpers.

## [ ] Task 5: Seed `companies_v2` From Existing `companies`
- Copy the existing `companies` table into `companies_v2`.
- Deduplicate on the canonical identity rules.
- Preserve source metadata that helps with traceability.
- Flag weak company identities that were created from name-only fallback.

Done when:
- `companies_v2` is populated from the existing canonical company data.
- Duplicate company rows are collapsed according to the agreed identity rules.

## [ ] Task 6: Backfill `candidate_profiles_v2`
- Copy one row per candidate from legacy `candidates`.
- Keep only stable profile attributes in the profile table.
- Do not keep full experience JSON as the source of truth in the new profile table.
- Preserve raw source references needed for debugging.

Done when:
- `candidate_profiles_v2` row count matches legacy candidate count.
- Candidate IDs are stable and usable for downstream backfills.

## [ ] Task 7: Backfill `candidate_emails_v2`
- Copy rows from legacy `candidate_emails`.
- Normalize email casing and whitespace.
- Mark one primary email per candidate where possible.
- Enforce dedupe rules on candidate-email pairs.

Done when:
- `candidate_emails_v2` is populated.
- No duplicate normalized email rows exist for the same candidate.
- Verification metadata is preserved.

## [ ] Task 8: Backfill `candidate_experiences_v2`
- Loop over each candidate’s `work_experience`.
- If `work_experience` is missing, fall back to `linkedin_data.data.experience`.
- Resolve or create a company in `companies_v2` for each experience.
- Insert one experience row per candidate experience entry.
- Store the raw source payload for traceability.

Done when:
- Experience rows are populated for enriched candidates.
- Each experience row links to a valid candidate and company row.
- Current roles are represented by `is_current`, not by candidate-level text fields alone.

## [ ] Task 9: Derive Candidate Current Role Fields
- Compute candidate-level current company and current title from `candidate_experiences_v2`.
- Populate cached current-role fields on `candidate_profiles_v2` if we decide to keep them.
- Make experience rows the source of truth and candidate-level fields derived values.

Done when:
- Current company and title are consistent with the latest current experience row.
- The profile table no longer relies on legacy denormalized current-role fields.

## [ ] Task 10: Build Validation And QA Checks
- Add row-count checks across old and new tables.
- Add orphan checks for candidate and company foreign keys.
- Add duplicate checks for companies and emails.
- Add spot-check queries for current company, title, and experience timelines.
- Produce a migration QA report.

Done when:
- We can prove the backfill is structurally correct.
- Known edge cases are listed and measurable.

## [ ] Task 11: Build AI Search Documents
- Create `candidate_search_documents`.
- Aggregate searchable text from:
  - candidate profile
  - experience titles
  - experience descriptions
  - company names
  - education
  - skills
- Keep this table optimized for semantic search and filtering.

Done when:
- Each candidate has a search document row or a defined reason why not.
- Search text is generated from normalized `v2` tables, not from legacy blobs.

## [ ] Task 12: Add Embeddings And Retrieval Indexes
- Add the vector column and supporting indexes for semantic search.
- Generate embeddings for each search document.
- Add filters for structured retrieval such as location, years of experience, current company, and prior company.

Done when:
- Semantic search can rank candidates from the normalized data model.
- Structured filters can be combined with vector search.

## [ ] Task 13: Switch Read Paths To `v2`
- Update scripts and app queries to read from the new tables.
- Keep legacy tables in place during the transition.
- Compare old and new results for a sample of candidate search flows.

Done when:
- Candidate retrieval and search use the `v2` model.
- Legacy tables are no longer required for normal reads.

## [ ] Task 14: Archive Legacy Tables
- Mark old tables as deprecated.
- Remove or freeze write paths that still target legacy tables.
- Archive old tables only after the new read path is stable.

Done when:
- The application no longer depends on legacy candidate/company structure.
- We have a safe rollback plan and an archive decision for old tables.
