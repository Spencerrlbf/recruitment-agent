# Legacy Candidate Data Model Notes

This document captures the relevant parts of the current source database before the migration.

Source workspace:
- `/Users/spencerbarton-fisher/Mac-Mini-Projects/Recruitment-Matching`

Source artifacts used:
- `schema.types.ts`
- direct API inspection of representative candidate and company rows

## Relevant Legacy Tables

### `candidates`
- One very wide row per candidate.
- Contains both profile fields and denormalized LinkedIn payloads.
- Important fields observed:
  - identity: `id`, `full_name`, `first_name`, `last_name`, `linkedin_username`, `linkedin_url`
  - profile: `headline`, `profile_summary`, `location`, `current_title`, `current_company`
  - search-ish fields: `all_skills_text`, `top_skills`, `education_*`, `years_of_experience`
  - raw payloads: `work_experience`, `linkedin_data`, `previous_companies`
- Problem:
  - this table is currently mixing canonical profile data and raw nested experience data

### `candidate_emails`
- Already normalized in the right direction
- One candidate can have many email rows
- Carries verification metadata
- Should be migrated to `candidate_emails_v2` with stronger normalization and dedupe

### `companies`
- Already partly canonical
- Has usable LinkedIn identity fields:
  - `linkedin_id`
  - `linkedin_username`
  - `linkedin_url`
- Also stores enrichment fields like `description`, `industries`, `staff_count`, `website`
- Should be reused as a seed source, then cleaned into `companies_v2`

### `candidate_company_history`
- Exists today but appears too thin to be the future source of truth
- Important fields:
  - `candidate_id`
  - `company_id`
  - `raw_company_name`
  - `position_title`
  - `start_date`
  - `end_date`
  - `is_current`
- This should be replaced by a stronger `candidate_experiences_v2` table

### `company_staging`
- Appears to be a transitional extraction table from candidate history into companies
- Useful as a migration reference, but not a final product table

## Key Migration Insight
- Candidate work history already exists in the current database.
- We do not need fresh API calls to reconstruct experience and company relationships.
- The migration should primarily normalize existing `candidates.work_experience` and fallback to `candidates.linkedin_data.data.experience`.

## Recommended Phase 1 Scope
- In scope:
  - candidate profiles
  - candidate emails
  - companies
  - candidate experiences
  - semantic search documents
- Out of scope for the first pass:
  - recruiter workflow tables
  - conversations/messages
  - applications
  - matching tables

## Suggested Source Of Truth After Migration
- `candidate_profiles_v2`: stable candidate profile data
- `candidate_emails_v2`: candidate contact methods
- `companies_v2`: canonical companies
- `candidate_experiences_v2`: canonical candidate-to-company experience rows
- `candidate_search_documents`: semantic search surface for retrieval
