# Recruiting Intelligence System Tasks

This file turns the product spec into an execution sequence. The goal is to build an automated recruiting agent, not just a one-time data migration.

Rules for this project:
- Build the new model inside the current Supabase project first.
- Do not mutate or delete legacy tables during the migration.
- Keep candidate IDs stable where possible.
- Prefer deterministic, idempotent, resumable scripts and workers.
- Generate embeddings automatically on the relevant insert or upload event.
- Persist recruiter feedback so ranking decisions can improve over time.
- Validate each phase before moving to the next one.
- Treat LinkedIn data as the baseline searchable source for every candidate.
- Treat resumes as higher-signal search evidence when present, but do not penalize candidates who only have LinkedIn data.
- Treat recruiter notes and transcript-derived signals as supplemental evidence; raw notes are lower-trust than approved structured summaries.
- Keep `candidate_search_documents` as an aggregate candidate summary/cache, not the only retrieval surface.
- Preserve the JD signal in ranking at all times, even when reference-candidate similarity becomes strong.
- Route candidates with unknown hard-filter values into a needs-screening bucket instead of excluding them by default.

## Phase 1: Canonical Candidate Data And Retrieval Foundation

## [x] Task 1: Lock The Canonical And Retrieval Data Model
- Finalize the exact `v2` canonical tables:
  - `candidate_profiles_v2`
  - `candidate_emails_v2`
  - `companies_v2`
  - `candidate_experiences_v2`
- Finalize the retrieval-layer tables and boundaries:
  - `candidate_source_documents`
  - `candidate_search_chunks`
  - `candidate_chunk_embeddings`
  - `candidate_search_documents`
- Document which fields are canonical, derived/cache, retrieval-only, or explicitly out of scope.
- Document how LinkedIn, resumes, recruiter notes, transcript summaries, and future candidate artifacts flow into the retrieval layer.

Done when:
- The schema contract is documented.
- Source-of-truth rules, foreign keys, uniqueness rules, and provenance rules are agreed.
- `candidate_search_documents` is explicitly defined as an aggregate summary/cache rather than the only searchable vector surface.

## [x] Task 2: Create Migration, Checkpoint, And Validation Scaffolding
- Create or confirm the repo structure for implementation support:
  - `db/migrations/`
  - `db/validation/canonical/`
  - `db/validation/retrieval/`
  - `scripts/backfills/`
  - `scripts/lib/`
  - `scripts/checkpoints/`
  - `reports/qa/`
  - `reports/retrieval_eval/`
- Add lightweight README files or equivalent docs explaining what belongs in each location and the naming conventions for:
  - migration files
  - validation files
  - backfill scripts
  - checkpoint files
  - QA and retrieval-evaluation report artifacts
- Implement a shared file-based JSON checkpoint helper under `scripts/lib/` for long-running backfills and rebuilds.
- Store generated checkpoint files under `scripts/checkpoints/` using atomic writes and resumable progress fields.
- Add a reusable backfill script template that demonstrates:
  - dry-run mode
  - batch iteration
  - checkpoint load/save
  - resumable execution
  - idempotent rerun structure
- Add placeholder validation scaffolding for:
  - row-count checks
  - orphan checks
  - duplicate checks
  - coverage checks for source documents, chunks, and embeddings
- Add `.gitignore` rules for generated checkpoint files and generated report artifacts while preserving the directory structure in git.
- Keep this task scaffolding-only:
  - do not create canonical tables yet
  - do not create retrieval tables yet
  - do not create job tables yet
  - do not implement domain backfill logic yet

Done when:
- The repo has a documented place for migrations, validation SQL, backfill scripts, shared helpers, checkpoint files, QA reports, and retrieval-evaluation artifacts.
- A shared checkpoint helper exists and supports safe resumable file-based JSON checkpoints.
- A reusable backfill template exists and demonstrates dry-run, batching, checkpoint resume, and idempotent structure.
- Placeholder validation scaffolding exists for canonical and retrieval QA work.
- Generated checkpoint and report artifacts are ignored appropriately by git.
- No domain table DDL or real backfill business logic has been introduced in this task.

## [x] Task 3: Create Canonical Tables, Constraints, And Canonical Validation
- Follow `SCHEMA_CONTRACT.md` exactly for **Layer A — Canonical candidate/company data only**.
- Create only these canonical tables:
  - `candidate_profiles_v2`
  - `candidate_emails_v2`
  - `companies_v2`
  - `candidate_experiences_v2`
- This task is **schema DDL only** for canonical tables.
- Do **not** create retrieval-layer tables in this task, including:
  - `candidate_source_documents`
  - `candidate_search_chunks`
  - `candidate_chunk_embeddings`
  - `candidate_search_documents`
- Do **not** create job or match-state tables in this task.
- Add required canonical-table extensions and shared helpers needed by the schema contract, including:
  - `citext` if used by canonical email columns
  - UUID/default-generation support if used by new canonical rows
  - any shared `updated_at` trigger/helper if timestamps are DB-managed
- Create the canonical tables with the exact columns, nullability, PKs, FKs, on-delete behavior, partial unique constraints, and indexes defined in `SCHEMA_CONTRACT.md`.
- Preserve canonical/retrieval boundaries:
  - canonical tables must remain valid without retrieval artifacts
  - `current_*` profile fields and `experience_years` remain derived/cache fields
  - work-history truth remains in `candidate_experiences_v2`
- Keep legacy tables untouched:
  - no mutation
  - no deletion
  - no renames
  - no data copy/backfill yet
- Add canonical schema validation SQL under `db/validation/canonical/` for:
  - table existence
  - PK/FK existence
  - required unique and partial-unique constraints
  - required non-vector indexes
- Add a canonical schema smoke test or validation script that proves:
  - a minimal `candidate_profiles_v2` insert works without any retrieval rows
  - `candidate_emails_v2` uniqueness and one-primary-email rules behave correctly
  - `companies_v2` allows name-fallback rows without enforcing uniqueness on normalized name alone
  - `candidate_experiences_v2` can insert valid experience rows linked to a candidate even when `company_id` is null
- If any canonical DDL detail is ambiguous or missing in `SCHEMA_CONTRACT.md`, stop and update the contract instead of inventing schema behavior.

Done when:
- The four canonical Layer A tables exist and compile successfully.
- Canonical DDL matches `SCHEMA_CONTRACT.md` without introducing retrieval or job tables.
- Required canonical extensions/helpers are present.
- PKs, FKs, delete behaviors, unique rules, partial unique constraints, and canonical indexes exist as specified.
- Canonical validation SQL exists under `db/validation/canonical/`.
- Smoke inserts prove the canonical tables work independently of retrieval artifacts.
- No retrieval tables, vector columns, pgvector indexes, backfill logic, or legacy-table mutations were introduced in this task.

Done when:
- The tables compile successfully.
- Test inserts work.
- Canonical tables do not depend on retrieval artifacts to remain valid.

## [x] Task 4: Create Candidate Retrieval Corpus Tables, Constraints, And Retrieval Validation
- Follow `SCHEMA_CONTRACT.md` exactly for **Layer B — Candidate retrieval corpus only**.
- Create only these candidate retrieval tables:
  - `candidate_source_documents`
  - `candidate_search_chunks`
  - `candidate_chunk_embeddings`
  - `candidate_search_documents`
- This task is **schema DDL only** for candidate retrieval storage.
- Do **not** create job-side retrieval or match-state tables in this task, including:
  - `jobs`
  - `job_source_documents`
  - `job_search_chunks`
  - `job_chunk_embeddings`
  - `job_reference_candidates`
  - `job_candidates`
  - `job_rejection_memory`
- Do **not** implement candidate source-document backfill, chunking logic, embedding-generation jobs, or aggregate search-document rebuild jobs in this task.
- Do **not** add recruiter-assessment tables in this task.
- Confirm required vector support exists before creating embedding columns and ANN indexes:
  - ensure `pgvector` support is available for `candidate_chunk_embeddings.embedding`
  - if vector-extension setup is needed and was not already added in Task 3, add only the minimum required extension setup for this task
- Create the retrieval corpus tables with the exact columns, nullability, PKs, FKs, on-delete behavior, versioning fields, trust/source metadata, and indexes defined in `SCHEMA_CONTRACT.md`.
- Preserve canonical/retrieval boundaries:
  - retrieval tables depend on canonical candidate tables from Task 3
  - embeddings live only in `candidate_chunk_embeddings`
  - do **not** add embeddings to canonical tables
  - do **not** add an embedding column to `candidate_search_documents`
  - keep `candidate_search_documents` as an aggregate summary/cache, not the primary retrieval surface
- Add retrieval-support indexes exactly where the schema contract calls for them:
  - FK/supporting indexes on source documents, chunks, and embeddings
  - ANN index only for the active retrieval model/dimension combination
  - do **not** create mixed-dimension ANN indexes or guess additional vector-index strategies not documented in the contract
- Keep legacy tables untouched:
  - no mutation
  - no deletion
  - no renames
  - no data copy/backfill yet
- Add retrieval schema validation SQL under `db/validation/retrieval/` for:
  - table existence
  - PK/FK existence
  - required uniqueness constraints
  - required non-vector indexes
  - required vector-index presence only where the active model/dimension combination supports it
- Add a retrieval schema smoke test or validation script that proves:
  - one candidate can have multiple `candidate_source_documents`
  - one source document can have multiple `candidate_search_chunks`
  - one candidate can have multiple chunk embeddings
  - a candidate can support LinkedIn-only evidence without requiring resume or note rows
  - a candidate can later support LinkedIn + resume + recruiter-note evidence without schema changes
  - `candidate_search_documents` can exist as a one-row-per-candidate aggregate cache without acting as the only embedding surface
- If any retrieval-table detail is ambiguous or missing in `SCHEMA_CONTRACT.md`, stop and update the contract instead of inventing schema behavior.

Done when:
- The four Layer B candidate retrieval tables exist and compile successfully.
- Retrieval DDL matches `SCHEMA_CONTRACT.md` without introducing job-side retrieval, match-state, or recruiter-assessment tables.
- Required vector support exists for `candidate_chunk_embeddings`.
- PKs, FKs, delete behaviors, versioning fields, uniqueness rules, trust/source metadata, and retrieval-support indexes exist as specified.
- `candidate_search_documents` is implemented as an aggregate summary/cache and does not carry the primary retrieval embedding surface.
- Retrieval validation SQL exists under `db/validation/retrieval/`.
- Smoke inserts prove the schema supports multiple documents, multiple chunks, and multiple embeddings per candidate.
- No backfill logic, chunking logic, embedding-generation jobs, job tables, or legacy-table mutations were introduced in this task.

Done when:
- The retrieval corpus can store multiple documents and multiple embeddings per candidate.
- A candidate can have LinkedIn-only evidence, LinkedIn plus resume evidence, and recruiter-note evidence without schema changes.

## [x] Task 5: Implement Canonicalization Rules
- Normalize emails into deterministic comparison keys while preserving raw values.
- Normalize LinkedIn identity fields for both candidates and companies:
  - usernames
  - canonical URLs
  - normalized URLs used for matching
- Resolve canonical companies by strong identity first and name fallback last.
- Normalize company names for fallback matching only; preserve raw display/source names separately.
- Normalize experience dates, date precision, current-role flags, and raw company names.
- Define duplicate-decision rules before any backfill writes land:
  - `candidate_profiles_v2`: treat rows as the same candidate only on stable legacy candidate UUID reuse or strong LinkedIn identity; do **not** auto-merge candidates by name/title/location similarity alone
  - `candidate_emails_v2`: treat rows as duplicates on normalized email
  - `companies_v2`: resolve duplicates by precedence:
    1. `linkedin_id`
    2. `linkedin_username`
    3. `linkedin_url_normalized`
    4. normalized-name fallback only when strong identity is absent and no contradictory evidence exists
  - `candidate_experiences_v2`: treat rows as duplicates on deterministic per-candidate source hash built from normalized experience identity fields
  - `candidate_source_documents`: define when an incoming row is:
    - the same logical document and should be a no-op
    - a new version of an existing logical document and should supersede the older active row
    - a genuinely new parallel document that should remain separate
  - `candidate_search_chunks`: treat duplicates within a document version by stable document identity plus chunk position/logical key
  - `candidate_chunk_embeddings`: treat duplicates on chunk identity plus embedding model/version identity
  - `candidate_search_documents`: treat this as a rebuildable one-row-per-candidate aggregate cache, not a merge target
- Define source-precedence rules so lower-trust or weaker-identity inputs do not overwrite stronger evidence without an explicit rule.
- Define ambiguity handling:
  - ambiguous candidate or company matches must be logged and skipped or routed to manual review
  - ambiguous matches must **not** be auto-merged silently
- Preserve raw values and provenance wherever normalized or derived values are introduced.

Done when:
- Canonicalization behavior is deterministic and idempotent.
- Helpers exist in reusable SQL or application code.
- Duplicate-decision rules are explicit for candidate profiles, emails, companies, experiences, source documents, chunks, embeddings, and aggregate search rows.
- Source-precedence rules are explicit and reproducible.
- Ambiguous match handling is explicit and measurable.
- Source-document logic can distinguish no-op duplicates from new versions.
- Raw source values remain available alongside normalized/derived values where needed for traceability.
- Canonicalization is independent from chunking and embedding generation.

## [ ] Task 6a: Implement Company Backfill Script And Preflight Validation
- Scope for this task is **only** the legacy `companies` table -> `companies_v2`.
- Do **not** read from `company_staging` in this task.
- Do **not** create `companies_v2` rows from candidate profile or experience payloads in this task.
- Build a dedicated checkpoint-aware backfill entrypoint under `scripts/backfills/` for the legacy `companies` -> `companies_v2` migration.
- Do **not** run any real write backfill in this task beyond dry-run and duplicate-validation fixtures.
- Follow `SCHEMA_CONTRACT.md` exactly for the destination schema. `companies_v2` columns used by this backfill are:
  - `name text not null`
  - `normalized_name text not null`
  - `linkedin_id text null`
  - `linkedin_username text null`
  - `linkedin_url text null`
  - `linkedin_url_normalized text null`
  - `website text null`
  - `description text null`
  - `industries text[] null`
  - `specialties text[] null`
  - `company_type text null`
  - `staff_count integer null`
  - `staff_count_range text null`
  - `headquarters_city text null`
  - `headquarters_country text null`
  - `logo_url text null`
  - `enrichment_status text null`
  - `last_enrichment_sync timestamptz null`
  - `data_source text not null`
  - `identity_basis text not null`
  - `source_record_refs jsonb null`
- Respect the canonical uniqueness and matching rules for `companies_v2`:
  - partial unique on `linkedin_id` where not null
  - partial unique on `linkedin_username` where not null
  - partial unique on `linkedin_url_normalized` where not null
  - index on `normalized_name`
  - do **not** enforce uniqueness on `normalized_name` alone
- Map legacy `companies` values into `companies_v2` as follows unless `SCHEMA_CONTRACT.md` is updated first:
  - `name` <- legacy company display name
  - `normalized_name` <- normalized `name`
  - `linkedin_id` <- legacy `linkedin_id`
  - `linkedin_username` <- normalized legacy `linkedin_username`
  - `linkedin_url` <- legacy `linkedin_url`
  - `linkedin_url_normalized` <- normalized legacy `linkedin_url`
  - `website` <- cleaned legacy `website`
  - `description` <- legacy `description`
  - `industries` <- legacy `industries`
  - `specialties` <- legacy `specialties` when present, otherwise `null`
  - `company_type` <- legacy `company_type` when present, otherwise `null`
  - `staff_count` <- legacy `staff_count`
  - `staff_count_range` <- derived from `staff_count`
  - `headquarters_city` <- legacy headquarters city when present, otherwise `null`
  - `headquarters_country` <- legacy headquarters country when present, otherwise `null`
  - `logo_url` <- legacy `logo_url` when present, otherwise `null`
  - `enrichment_status` and `last_enrichment_sync` <- carry over legacy values when present and contract-compatible, otherwise leave `null`
  - `data_source` <- `'legacy_backfill'`
  - `identity_basis` <- strongest populated identity on the stored row: `linkedin_id`, then `linkedin_username`, then `linkedin_url`, otherwise `name`
  - `source_record_refs` <- raw legacy provenance including the source table name, legacy row id, raw company name, raw website, and any raw LinkedIn identifiers used during matching
- Normalize and clean values before matching or writing:
  - trim whitespace and convert blank strings to `null`
  - normalize company names with the canonical company-name helper
  - normalize LinkedIn usernames and URLs with the canonicalization helpers from Task 5
  - normalize `website` to scheme + host only; drop path, query string, fragment, and trailing slash (for example `http://www.youtube.com/jobs` -> `http://www.youtube.com`)
  - preserve important raw source values inside `source_record_refs` for traceability
- Use the Task 5 canonicalization helpers as the matching contract for this task.
- For each legacy company row, resolve identity in this precedence order:
  - exact `linkedin_id`
  - exact normalized `linkedin_username`
  - exact normalized `linkedin_url_normalized`
  - normalized-name fallback only when strong LinkedIn identity is absent
- Handle resolver outcomes explicitly:
  - `match_existing` -> update one existing `companies_v2` row using field-level precedence rules
  - `create_new` -> insert one new `companies_v2` row
  - `ambiguous` -> record the ambiguity, skip the write, and continue
- Never auto-merge on ambiguous matches.
- Never overwrite stronger non-null stored identity values with weaker incoming values.
- Lower-precedence inputs may fill blank nullable fields, but equal-precedence reruns must preserve the stored value so reruns remain idempotent.
- `source_record_refs` updates must be deduplicated so reruns do not append the same legacy provenance twice.
- Optimize the backfill for large legacy company volume:
  - process strong-identity rows first, then name-only fallback rows
  - use deterministic batching and a stable monotonic cursor from the legacy `companies` table
  - keep writes set-based or batched where possible instead of row-by-row chatty inserts
  - advance checkpoints only after successful durable batch writes
  - support `--dry-run` without mutating destination rows or checkpoint state
- Complete a required preflight validation pass in this task:
  - run a deterministic `--dry-run` on the first 100 legacy `companies` rows that would be processed by the real script order
  - generate a QA report for that 100-row dry-run and review it
  - create controlled duplicate-validation fixtures in a safe development or sandbox environment instead of mutating the real legacy source table
  - verify duplicate handling for all identity paths:
    - duplicate `linkedin_id`
    - duplicate `linkedin_username`
    - duplicate `linkedin_url_normalized`
    - name-only fallback duplicate on `normalized_name`
    - conflicting strong-identity inputs that must resolve to `ambiguous`
  - confirm the duplicate-validation fixtures produce the expected resolver outcomes and do not silently create duplicate canonical companies
- Emit a QA report for Task 6a under `reports/qa/` with counts for:
  - rows read
  - rows normalized
  - rows that would be inserted
  - rows that would be matched/updated
  - rows skipped
  - ambiguous rows
  - duplicate-reduction totals by strong-identity match vs name-fallback match
  - preflight 100-row dry-run findings
  - duplicate-validation fixture outcomes by identity path

Done when:
- A dedicated `companies` backfill script exists under `scripts/backfills/`.
- The script supports deterministic ordering, resumable checkpoints, safe reruns, and `--dry-run` mode.
- The script processes only the legacy `companies` table for this task.
- A deterministic `--dry-run` on the first 100 legacy `companies` rows has been completed and reviewed.
- Controlled duplicate-validation fixtures have been executed in a safe development or sandbox environment for `linkedin_id`, `linkedin_username`, `linkedin_url_normalized`, name-only fallback, and ambiguous strong-identity conflict cases.
- Duplicate-validation results confirm the resolver prevents silent duplicate canonical companies across all identity paths.
- The Task 6a QA report captures dry-run findings and duplicate-validation outcomes.
- No pilot write or full backfill is performed until Task 6a is approved.

## [ ] Task 6b: Run 100-Row Pilot Company Backfill And Review Results
- After Task 6a approval, run a controlled 100-row pilot write using the same deterministic script order validated in Task 6a.
- The 100 pilot-written `companies_v2` rows may remain in place and become part of the final canonical dataset.
- Review the resulting `companies_v2` rows directly in the database before permitting the full migration.
- Confirm the pilot preserves the required stored fields:
  - `name`
  - `normalized_name`
  - `data_source`
  - `identity_basis`
  - `source_record_refs`
- Confirm website values are normalized to scheme + host only before storage.
- Confirm canonical uniqueness rules on `linkedin_id`, `linkedin_username`, and `linkedin_url_normalized` are respected without enforcing uniqueness on `normalized_name` alone.
- Confirm ambiguous matches are logged and skipped rather than silently merged.
- Confirm the later full backfill can safely continue from the pilot-written state without creating duplicate canonical company rows or duplicate provenance entries for the same source rows.
- Emit a pilot QA report under `reports/qa/` with actual inserted, updated, skipped, ambiguous, and duplicate-check outcomes for the 100 written rows.

Done when:
- A controlled 100-row pilot write has been completed after Task 6a approval.
- The pilot-written canonical rows are allowed to remain in place for the final migration.
- The pilot rows have been reviewed in the database.
- Pilot QA confirms the expected field mapping, normalization, duplicate handling, and ambiguity handling behavior.
- The later full backfill is proven to continue safely from the pilot-written state without duplicating canonical company rows or provenance entries for the same source rows.
- The full backfill is not run until the pilot-write review is approved.

## [ ] Task 6c: Run Full Company Backfill
- After Task 6b approval, run the full `companies` -> `companies_v2` backfill using the approved script and checkpoint strategy.
- Continue safely from the pilot-written state or another explicitly approved checkpoint state.
- Preserve the same mapping, normalization, precedence, ambiguity handling, and idempotency rules validated in Tasks 6a and 6b.
- Emit a final QA report for the full migration under `reports/qa/`.

Done when:
- The full backfill has been completed only after Task 6a and Task 6b approval.
- `companies_v2` rows written by the backfill populate `name`, `normalized_name`, `data_source`, and `identity_basis`, and preserve raw provenance in `source_record_refs`.
- Website values are normalized to scheme + host only before storage.
- Canonical uniqueness rules on `linkedin_id`, `linkedin_username`, and `linkedin_url_normalized` are respected without enforcing uniqueness on `normalized_name` alone.
- Ambiguous matches are logged and skipped rather than silently merged.
- A QA report captures rows read, inserted, updated, skipped, ambiguous, duplicate-reduction counts, and final migration outcomes.
- Re-running the backfill does not create duplicate `companies_v2` rows or duplicate provenance entries.
- Company resolution precedence is reproducible from the stored data and helper rules.

## [ ] Task 7a: Implement Candidate Profile And Email Backfill Script And Preflight Validation
- Scope for this task is only:
  - legacy `candidates` -> `candidate_profiles_v2`
  - legacy `candidate_emails` -> `candidate_emails_v2`
- Treat legacy `candidate_emails` as the primary email source.
- Legacy `candidates.email` may be used only as a fallback email source when:
  - it normalizes to a non-null email, and
  - the same normalized email is not already present for that candidate from legacy `candidate_emails`.
- Keep candidate profiles and emails as one combined backfill at the task level because email rows depend on the resolved canonical candidate row and should share one checkpoint and approval flow.
- In scope:
  - create missing `candidate_profiles_v2` rows
  - update existing `candidate_profiles_v2` rows only under the canonical match and source-precedence rules
  - create or update related `candidate_emails_v2` rows under deterministic dedupe rules
  - preserve profile-level provenance in `candidate_profiles_v2.source_record_refs`
  - preserve email verification metadata and email-source lineage fields supported by `candidate_emails_v2`
- Out of scope:
  - creating or updating `companies_v2`
  - creating or updating `candidate_experiences_v2`
  - deriving or backfilling `current_title`, `current_company_id`, `current_company_name`, or `experience_years`
  - copying full raw `linkedin_data`, `work_experience`, `resume_text`, `notes`, embeddings, or retrieval artifacts into canonical profile rows
  - mutating or deleting legacy tables
  - running a committed pilot or full backfill in this task beyond dry-run and duplicate-validation fixtures
- This task may:
  - insert new canonical candidate rows
  - update existing canonical candidate rows when `match_existing` resolves unambiguously
  - insert or update related canonical candidate-email rows
- This task must not:
  - create new canonical company rows
  - create experience rows
  - create retrieval rows
- Follow `SCHEMA_CONTRACT.md` exactly for the destination schema.
- `candidate_profiles_v2` columns touched by this backfill are:
  - `id uuid not null` — for new inserts, reuse legacy `candidates.id`
  - `full_name text null`
  - `first_name text null`
  - `last_name text null`
  - `linkedin_username text null`
  - `linkedin_url text null`
  - `linkedin_url_normalized text null`
  - `headline text null`
  - `summary text null`
  - `location text null`
  - `profile_picture_url text null`
  - `phone text null`
  - `education_summary text null`
  - `education_schools text[] null`
  - `education_degrees text[] null`
  - `education_fields text[] null`
  - `skills_text text null`
  - `top_skills text[] null`
  - `source text not null`
  - `source_record_refs jsonb null` — required provenance field for rows written by this task
  - `linkedin_enrichment_status text null`
  - `linkedin_enrichment_date timestamptz null`
- `candidate_profiles_v2` columns explicitly **not** touched in this task are:
  - `current_title`
  - `current_company_id`
  - `current_company_name`
  - `experience_years`
  - `created_at`
  - `updated_at`
- `candidate_emails_v2` columns touched by this backfill are:
  - `candidate_id uuid not null`
  - `email_raw text not null`
  - `email_normalized citext not null`
  - `email_type text null`
  - `email_source text null`
  - `is_primary boolean not null`
  - `quality text null`
  - `result text null`
  - `resultcode text null`
  - `subresult text null`
  - `verification_date timestamptz null`
  - `verification_attempts integer not null`
  - `last_verification_attempt timestamptz null`
  - `raw_response jsonb null`
- `candidate_emails_v2.id` should be DB-generated on insert. Do **not** rely on legacy `candidate_emails.id` as the canonical email PK because the canonical email identity for reruns is the natural key `(candidate_id, email_normalized)`.
- `candidate_emails_v2` does **not** have a dedicated provenance JSONB column. Do **not** invent one in this task. Preserve email traceability through:
  - `email_source`
  - `raw_response`
  - deterministic QA output keyed by legacy email-source row ids and normalized email outcomes
- Map legacy `candidates` fields into `candidate_profiles_v2` as follows unless `SCHEMA_CONTRACT.md` is updated first:
  - `id` <- legacy `candidates.id`
  - `full_name` <- trimmed legacy `candidates.full_name`; blank -> `null`
  - `first_name` <- trimmed legacy `candidates.first_name`; blank -> `null`
  - `last_name` <- trimmed legacy `candidates.last_name`; blank -> `null`
  - `linkedin_username` <- `normalize_linkedin_username(candidates.linkedin_username)`; leave `null` when blank or invalid
  - `linkedin_url_normalized` <- `normalize_candidate_linkedin_url(candidates.linkedin_url)`; leave `null` when blank or not a canonical candidate LinkedIn URL
  - `linkedin_url` <- cleaned canonical LinkedIn URL used for display; when normalization succeeds, store the normalized canonical URL and preserve the raw incoming URL in `source_record_refs`
  - `headline` <- trimmed legacy `candidates.headline`; blank -> `null`
  - `summary` <- trimmed legacy `candidates.profile_summary`; blank -> `null`
  - `location` <- trimmed legacy `candidates.location`; blank -> `null`
  - `profile_picture_url` <- trimmed legacy `candidates.profile_picture_url`; blank -> `null`
  - `phone` <- trimmed legacy `candidates.phone`; blank -> `null`
  - `education_summary` <- trimmed legacy `candidates.education`; blank -> `null`
  - `education_schools` <- normalized legacy `candidates.education_schools`; trim elements, drop blanks, preserve order, dedupe exact repeats, empty array -> `null`
  - `education_degrees` <- normalized legacy `candidates.education_degrees`; trim elements, drop blanks, preserve order, dedupe exact repeats, empty array -> `null`
  - `education_fields` <- normalized legacy `candidates.education_fields`; trim elements, drop blanks, preserve order, dedupe exact repeats, empty array -> `null`
  - `skills_text` <- trimmed legacy `candidates.all_skills_text`; blank -> `null`
  - `top_skills` <- normalized legacy `candidates.top_skills`; trim elements, drop blanks, preserve order, dedupe exact repeats, empty array -> `null`
  - `linkedin_enrichment_status` <- trimmed legacy `candidates.linkedin_enrichment_status`; blank -> `null`
  - `linkedin_enrichment_date` <- legacy `candidates.linkedin_enrichment_date` when present, otherwise `null`
  - `source` <- `'legacy_backfill'` for newly inserted rows; for matched existing rows, do not downgrade a higher-precedence existing source
  - `source_record_refs` <- deduplicated provenance JSONB containing at minimum:
    - source table name `candidates`
    - legacy candidate id
    - legacy source field from `candidates.source` when present
    - raw `linkedin_username`
    - raw `linkedin_url`
    - raw `full_name`
    - legacy `created_at`
    - legacy `updated_at`
    - deterministic `canonical_match_outcome` object containing:
      - `legacy_candidate_id`
      - `resolved_candidate_id`
      - `match_action` in `create_new` or `match_existing`
      - `match_basis` in `legacy_id`, `linkedin_username`, or `linkedin_url_normalized`
- Do **not** map these legacy `candidates` fields into `candidate_profiles_v2` in this task:
  - `current_title`
  - `current_company`
  - `current_company_id`
  - `years_of_experience`
  - `calculated_experience_years`
  - `total_experience_years`
  - `previous_companies`
  - `work_experience`
  - `linkedin_data`
  - `resume_text`
  - `ai_summary`
  - `notes`
  - `matching_embedding`
  - `resume_embedding`
  - `visa_status`
  - `security_clearance`
  - any other retrieval-only or out-of-scope fields
- Map legacy email data into `candidate_emails_v2` as follows:
  - primary email source rows come from legacy `candidate_emails`
  - fallback email source rows may be synthesized from legacy `candidates.email` only when the normalized email is non-null and not already represented for that candidate
  - `candidate_id` <- resolved canonical candidate id from the profile match decision
  - `email_raw` <- trimmed source email value preserving original case; do not lowercase `email_raw`
  - `email_normalized` <- `normalize_email_address(source_email)`; this trims whitespace, strips a leading `mailto:`, lowercases only, and does not apply plus-tag or dot-removal heuristics
  - `email_type` <- normalized legacy type using:
    - `personal` -> `personal`
    - `business` -> `work`
    - `academic` -> `work`
    - `government` -> `work`
    - blank -> `null`
    - unrecognized non-blank value -> `unknown`
  - `email_source` <- trimmed legacy `candidate_emails.email_source` when present, otherwise:
    - `'legacy_candidate_emails'` for rows sourced from `candidate_emails`
    - `'legacy_candidates_email_fallback'` for rows synthesized from `candidates.email`
  - `is_primary` <- determined after per-candidate email dedupe and primary-resolution rules
  - `quality` <- trimmed legacy `candidate_emails.quality`; blank -> `null`
  - `result` <- trimmed legacy `candidate_emails.result`; blank -> `null`
  - `resultcode` <- legacy `candidate_emails.resultcode` cast to text when present, otherwise `null`
  - `subresult` <- trimmed legacy `candidate_emails.subresult`; blank -> `null`
  - `verification_date` <- legacy `candidate_emails.verification_date`
  - `verification_attempts` <- `coalesce(candidate_emails.verification_attempts, 0)`; fallback rows from `candidates.email` use `0`
  - `last_verification_attempt` <- legacy `candidate_emails.last_verification_attempt`
  - `raw_response` <- legacy `candidate_emails.raw_response`; fallback rows from `candidates.email` use `null`
- Profile-level provenance and email-level provenance must remain separate:
  - profile-level raw source references belong in `candidate_profiles_v2.source_record_refs`
  - email-level provenance must not be stuffed into `candidate_profiles_v2.source_record_refs`
  - email-level lineage is limited to `email_source`, preserved verification payload fields, and QA artifacts keyed by legacy email source rows
- Respect the canonical candidate matching rules from Task 5 and the implemented helper contract:
  - use stable legacy candidate UUID first
  - then normalized `linkedin_username`
  - then normalized `linkedin_url_normalized`
  - do **not** auto-merge by name, title, location, education, or skills
- Resolve each incoming candidate profile with `match_existing`, `create_new`, or `skip` behavior:
  - `match_existing` when:
    - legacy candidate id already exists in `candidate_profiles_v2.id` and does not conflict with LinkedIn identity, or
    - strong LinkedIn identity resolves unambiguously to one existing canonical row
  - `create_new` when no existing canonical row matches by stable id or strong LinkedIn identity
  - `skip` when the profile is ambiguous
- Ambiguous candidate-profile cases include:
  - incoming `linkedin_username` conflicts with the username extracted from the incoming `linkedin_url`
  - incoming legacy candidate id conflicts with an existing canonical row found by LinkedIn identity
  - normalized incoming username and normalized incoming LinkedIn URL resolve to different existing canonical rows
- Ambiguous profile cases must be logged to `canonicalization_ambiguities` with:
  - `entity_type = 'candidate_profile'`
  - `source_system = 'legacy_backfill'`
  - `source_record_ref = <legacy candidates.id>`
  - deterministic normalized input payload
  - matched record ids when present
- Repeated runs must reuse the same open ambiguity instead of multiplying identical open ambiguity rows.
- Emit a deterministic candidate-resolution mapping artifact under `reports/qa/` keyed by legacy `candidates.id` with:
  - `match_action` in `create_new`, `match_existing`, or `skip`
  - `match_basis` in `legacy_id`, `linkedin_username`, `linkedin_url_normalized`, or `ambiguous`
  - resolved canonical `candidate_profiles_v2.id` when present
  - skip or ambiguity reason when no canonical row is written
- Later Tasks 8 and 9 must consume this approved mapping artifact or an equivalently materialized lookup rather than re-deriving candidate links ad hoc from free-form JSONB.
- New rows inserted by this backfill must reuse legacy `candidates.id`.
- If a rerun or pre-existing canonical row resolves by strong LinkedIn identity to an existing row, do **not** create a second canonical profile row solely to force another id reuse.
- Field overwrite rules for `candidate_profiles_v2` are:
  - higher-precedence existing values must not be overwritten by this legacy backfill
  - when the incoming legacy-backed value is higher precedence than the stored source for that field under the shared canonical precedence contract, it may replace a weaker existing non-null value
  - lower-precedence incoming values may fill blank nullable profile fields only
  - equal-precedence reruns must preserve existing non-null values so reruns remain idempotent
  - `source_record_refs` updates must be deduplicated so reruns do not append the same legacy provenance twice
- Email uniqueness and dedupe rules are:
  - `candidate_emails_v2` duplicate identity is `email_normalized`
  - DB uniqueness to preserve:
    - unique on `(candidate_id, email_normalized)`
    - partial unique on `email_normalized` where not null
    - partial unique on `(candidate_id)` where `is_primary = true`
  - reruns must upsert by the natural key `(candidate_id, email_normalized)` and must not create duplicate email rows
  - if the same normalized email already exists for the same candidate, `match_existing` and merge fields using this matrix:
    - `email_raw`: fill blank existing values only; equal-precedence reruns keep the existing non-blank display form
    - `email_type`, `quality`, `result`, `resultcode`, `subresult`, `email_source`, and `raw_response`: legacy `candidate_emails` input may replace blanks from a weaker `candidates.email` fallback row; equal-precedence reruns preserve existing non-null values
    - `verification_date` and `last_verification_attempt`: keep the newest non-null value
    - `verification_attempts`: keep the greatest non-null value
    - `is_primary`: recompute after candidate-level dedupe and primary-resolution; do not blindly preserve or overwrite the prior flag before that pass
  - if the same normalized email already exists for a different candidate, treat the incoming email row as ambiguous/conflicting, skip that email write, and log the condition against `entity_type = 'candidate_profile'` in `canonicalization_ambiguities`
- Multiple legacy email rows for one candidate must be handled deterministically:
  - first dedupe within candidate on `email_normalized`
  - when duplicate source rows collapse to one canonical email row, choose one deterministic representative in this order:
    1. legacy `candidate_emails` row over `candidates.email` fallback
    2. `is_primary = true`
    3. newest `verification_date`
    4. highest `verification_attempts`
    5. newest `last_verification_attempt`
    6. oldest `created_at`
    7. lowest legacy source row id
  - the representative row supplies non-conflicting verification metadata and payload fields
  - lower-precedence fallback input from `candidates.email` may fill blanks only and must never overwrite verification metadata from `candidate_emails`
- Primary-email rules are:
  - if exactly one surviving email row for a candidate has `is_primary = true`, preserve it
  - if more than one surviving email row is flagged primary, choose one canonical primary using the same deterministic representative ordering above and demote the rest to `false`; record the condition in the QA report
  - if no surviving email row is flagged primary and exactly one surviving email row exists, set that row `is_primary = true`
  - if no surviving email row is flagged primary and multiple surviving rows exist, use the row matching normalized legacy `candidates.email` as primary when exactly one such row exists; otherwise leave all rows `is_primary = false`
  - the script must never attempt to write more than one primary email row per candidate
- Normalize and clean values before matching or writing:
  - trim surrounding whitespace on all scalar text fields
  - convert blank strings to `null`
  - normalize email only with `normalize_email_address`
  - normalize LinkedIn usernames only with `normalize_linkedin_username`
  - normalize candidate LinkedIn URLs only with `normalize_candidate_linkedin_url`
  - do not use name normalization for identity matching
  - for display text fields, preserve case after trimming; do not lowercase names, headlines, summaries, locations, or education text
  - for arrays, trim elements, drop blank elements, preserve first-seen order, and dedupe exact repeats
  - preserve raw profile identity inputs in `source_record_refs`
- This task requires a dedicated backfill script under `scripts/backfills/`, for example:
  - `scripts/backfills/07_candidate_profiles_emails_backfill.py`
- The script must be:
  - deterministic
  - idempotent
  - resumable
  - checkpoint-aware
  - batch-oriented
  - safe to rerun
- The script must support:
  - a stable batch cursor in real script order using legacy `candidates.id` ascending
  - deterministic email-row ordering within each candidate batch
  - `--dry-run`
  - checkpoint files under `scripts/checkpoints/`
  - one DB transaction per candidate batch covering profile writes, related email writes, and ambiguity logging
  - checkpoint advance only after that transaction has committed durably
  - QA report output under `reports/qa/`
- Performance requirements for large volume are:
  - do not do row-by-row chatty writes when set-based or batched writes are possible
  - fetch candidate batches from `candidates` in stable order
  - bulk fetch related `candidate_emails` rows for the batch candidate ids
  - bulk read existing canonical profile matches using indexed lookups on:
    - `candidate_profiles_v2.id`
    - `candidate_profiles_v2.linkedin_username`
    - `candidate_profiles_v2.linkedin_url_normalized`
  - bulk read existing canonical email matches using indexed lookups on:
    - `candidate_emails_v2.candidate_id`
    - `candidate_emails_v2.email_normalized`
  - batch profiles first, then batch email writes separately for the resolved candidate ids, but keep both write phases inside the same per-batch transaction so reruns never observe a half-written batch
- Required preflight validation in this task:
  - run a deterministic `--dry-run` on the first 100 legacy `candidates` rows in real script order
  - in that dry-run, include all related legacy `candidate_emails` rows and any eligible `candidates.email` fallback rows for those 100 candidates
  - dry-run must not commit destination writes
  - dry-run must not mutate checkpoint state
  - create duplicate-validation fixtures in a safe dev/sandbox path instead of mutating production legacy data
  - validate profile matching cases for:
    - stable legacy candidate id rerun
    - username-only match
    - URL-only match
    - conflicting incoming username vs URL
    - legacy id conflicting with existing LinkedIn identity
    - username and URL resolving to different existing canonical rows
  - validate email cases for:
    - duplicate normalized email within one candidate
    - duplicate normalized email across different candidates
    - duplicate `candidate_emails` row vs `candidates.email` fallback
    - multiple primary emails for one candidate
    - no-primary multi-email candidate
- Emit a QA report for Task 7a under `reports/qa/` with counts for:
  - candidate source rows read
  - profile rows that would be inserted
  - profile rows that would be matched/updated
  - profile rows skipped
  - profile ambiguities logged
  - legacy `candidate_emails` rows read
  - fallback `candidates.email` rows synthesized
  - canonical email rows that would be inserted
  - canonical email rows that would be matched/updated
  - canonical email rows skipped
  - duplicate email rows collapsed within candidate
  - cross-candidate email conflicts
  - candidates with one primary email after normalization
  - candidates with no primary email after normalization
  - candidates with multi-primary source conflicts resolved deterministically
  - candidate-resolution mapping outcomes keyed by legacy `candidates.id`
  - sample outcomes for `match_existing`, `create_new`, `skip`, and email-conflict cases
  - duplicate-validation fixture outcomes
- QA artifacts should be clearly named under `reports/qa/`, for example:
  - `YYYYMMDD__qa_candidate_profiles_emails_preflight.md`
  - `YYYYMMDD__qa_candidate_profiles_emails_preflight.json`
  - `YYYYMMDD__qa_candidate_profiles_emails_candidate_map.json`

Done when:
- A dedicated candidate profile and email backfill script exists under `scripts/backfills/`.
- The script supports deterministic ordering, resumable checkpoints, one-transaction-per-batch profile/email writes, safe reruns, and `--dry-run`.
- The script processes only legacy `candidates` and legacy `candidate_emails`, with `candidates.email` used only as the defined fallback source.
- `candidate_profiles_v2.id` reuse, LinkedIn identity matching, and profile ambiguity handling are explicit and implemented in the script contract.
- Candidate-profile provenance is preserved in deduplicated `source_record_refs`.
- A deterministic candidate-resolution mapping artifact is emitted and approved for downstream consumers.
- Candidate-email uniqueness, cross-candidate email-conflict handling, and one-primary-email rules are explicit and implemented in the script contract.
- A deterministic `--dry-run` on the first 100 legacy candidates in real script order has been completed and reviewed.
- Controlled duplicate-validation fixtures have been executed in a safe development or sandbox environment for profile identity and email uniqueness behaviors.
- The Task 7a QA report captures dry-run findings, duplicate-validation outcomes, profile ambiguity outcomes, and email dedupe outcomes.
- No committed pilot write or full backfill is performed until Task 7a is approved.

## [ ] Task 7b: Run 100-Row Pilot Candidate Profile And Email Backfill And Review Results
- After Task 7a approval, run a committed 100-row pilot write using the same deterministic candidate order validated in Task 7a.
- The profile and email rows inserted or updated for this 100-candidate pilot cohort may remain in place and become part of the final canonical dataset.
- Review the resulting rows directly in the database before permitting the full migration.
- Confirm the pilot preserves the required profile fields:
  - stable canonical candidate id behavior
  - profile identity fields
  - profile source provenance in `source_record_refs`
  - required nullable canonical profile fields populated only when source values are present
- Confirm the pilot preserves the required email fields:
  - `email_raw`
  - `email_normalized`
  - `email_source`
  - `is_primary`
  - verification metadata
- Confirm profile ambiguity cases were logged and skipped rather than silently merged.
- Confirm cross-candidate normalized-email conflicts were skipped rather than silently attached to the wrong candidate.
- Confirm the pilot respects:
  - unique `(candidate_id, email_normalized)`
  - global normalized-email uniqueness
  - one-primary-email-per-candidate
- Confirm the later full backfill can safely continue from the pilot-written state without creating:
  - duplicate canonical candidate rows
  - duplicate candidate email rows
  - duplicate profile provenance entries
- Confirm candidate counts reconcile for the pilot cohort:
  - legacy candidates processed
  - canonical profiles inserted or matched
  - profile skips explained by logged ambiguity or explicit skip rules
- Confirm email counts reconcile for the pilot cohort after applying the approved normalization and dedupe rules.
- Emit a pilot QA report under `reports/qa/` with actual inserted, updated, skipped, ambiguous, deduped, and conflict outcomes for the 100 written candidates and their related emails.

Done when:
- A committed 100-row pilot write has been completed after Task 7a approval.
- The pilot-written profile and email rows are allowed to remain in place for the final migration.
- The pilot rows have been reviewed directly in the database.
- Pilot QA confirms the expected mapping, normalization, dedupe, ambiguity handling, primary-email handling, and provenance behavior.
- The later full backfill is proven to continue safely from the pilot-written state without duplicating canonical candidate rows, canonical email rows, or profile provenance entries.
- The full backfill is not run until the pilot-write review is approved.

## [ ] Task 7c: Run Full Candidate Profile And Email Backfill
- After Task 7b approval, run the full `candidates` + `candidate_emails` -> `candidate_profiles_v2` + `candidate_emails_v2` backfill using the approved script and checkpoint strategy.
- Continue safely from the pilot-written state or another explicitly approved checkpoint state.
- Preserve the same mapping, normalization, precedence, ambiguity handling, email dedupe, primary-email, and idempotency rules validated in Tasks 7a and 7b.
- Preserve the rule that `candidates.email` is fallback-only and must not override canonical email rows sourced from legacy `candidate_emails`.
- Emit a final QA report for the full migration under `reports/qa/`.

Done when:
- The full backfill has been completed only after Task 7a and Task 7b approval.
- Canonical profile rows written by the backfill preserve stable candidate identity rules and populate the approved canonical profile fields only.
- Profile rows preserve deduplicated profile-level provenance in `source_record_refs`.
- Canonical email rows preserve normalized-email uniqueness, one-primary-email constraints, and verification metadata according to the approved mapping rules.
- Candidate-profile ambiguities and cross-candidate email conflicts are logged and skipped rather than silently merged.
- A final QA report captures rows read, profile inserts, profile updates, profile skips, profile ambiguities, email inserts, email updates, email skips, duplicate-collapses, cross-candidate email conflicts, primary-email outcomes, and final reconciliation totals.
- Re-running the backfill does not create duplicate `candidate_profiles_v2` rows, duplicate `candidate_emails_v2` rows, or duplicate profile provenance entries.
- The full run remains blocked until explicit approval of Task 7b.

## [ ] Task 8a: Implement Candidate Experience Backfill Script And Preflight Validation
- Scope for this task is **only** legacy candidate experience payloads -> `candidate_experiences_v2`.
- The authoritative legacy source table for row creation in this task is `public.candidates`.
- Source extraction order for each candidate is:
  - primary source path: `public.candidates.work_experience`
  - candidate-level fallback source path: `public.candidates.linkedin_data -> 'data' -> 'experience'`
- If `work_experience` is present and non-empty for a candidate, use it as the source array for that candidate and do **not** merge it item-by-item with the fallback array.
- Use the fallback `linkedin_data.data.experience` array only when `work_experience` is null or empty for that candidate.
- The script must support the observed legacy experience item shapes:
  - older collector shape with fields such as `companyName`, `title`, `description`, `location`, `start`, `end`, `is_current`
  - newer LinkedIn-enrichment shape with fields such as `company`, `company_id`, `company_linkedin_url`, `title`, `description`, `location`, `start_date`, `end_date`, `is_current`
- `candidate_company_history` is **not** the authoritative source of truth for this task:
  - do **not** use it as the primary source for canonical row creation
  - do **not** use it to create or infer new canonical company rows
  - it may be referenced only for optional manual QA if needed
- `company_staging` is out of scope for this task.
- Destination scope for this task is **only** `public.candidate_experiences_v2`.
- Do **not** create or update `companies_v2` rows in this task.
- Do **not** create or update retrieval-layer rows in this task.
- Do **not** mutate legacy source tables in this task.
- Do **not** directly rebuild or overwrite `candidate_profiles_v2.current_title`, `current_company_id`, `current_company_name`, or `experience_years` in this task:
  - this task establishes `candidate_experiences_v2` as the work-history source of truth
  - later cache/rebuild logic may derive profile-level current-role fields from these rows
- This task must only link to already-canonical `companies_v2` rows produced by the approved company backfill.
- This task depends on:
  - approved company backfill output from `Task 6c`
  - approved candidate profile/email backfill output and candidate-resolution mapping artifact from `Task 7c`
- Task 8a implementation work may begin before later approvals, but the required Task 8a dry-run and validation are not complete until they run against a target DB state that already contains the approved outputs of `Task 6c` and `Task 7c`, including the approved Task 7c candidate-resolution mapping artifact.

- Follow `SCHEMA_CONTRACT.md` exactly for `candidate_experiences_v2`. Columns written by this backfill are:
  - `candidate_id uuid not null`
    - must link to an existing `candidate_profiles_v2.id`
    - must resolve to the approved canonical candidate row produced by `Task 7c`
    - when `Task 7c` inserted a new canonical row by stable legacy id reuse, `candidate_id` will equal `public.candidates.id`
    - when `Task 7c` matched an existing canonical row by strong LinkedIn identity, this task must use that existing canonical candidate id rather than forcing a second profile row
  - `company_id uuid null`
    - may be null when no unambiguous canonical company match exists
    - when non-null, it must reference an existing `companies_v2.id`
  - `experience_index integer not null`
    - stable zero-based order from the chosen source array for that candidate
    - preserve source array order rather than inventing a new sort order
  - `title text null`
  - `description text null`
  - `location text null`
  - `raw_company_name text null`
    - preserve the raw company text even when `company_id` resolves
  - `source_company_linkedin_username text null`
    - preserve or derive the source-side company LinkedIn username when available for audit and matching
  - `start_date date null`
  - `start_date_precision text null`
    - normalized precision must be one of `year`, `month`, `day`, `unknown`
  - `end_date date null`
  - `end_date_precision text null`
    - normalized precision must be one of `year`, `month`, `day`, `present`, `unknown`
  - `is_current boolean not null`
    - current-role semantics must follow the canonical contract:
      - current role => `end_date = null`
      - current role => `end_date_precision = 'present'`
      - current role => `is_current = true`
  - `source_payload jsonb null`
    - required provenance payload for this task
    - must store deterministic traceability including:
      - `source_table`
      - `legacy_candidate_id`
      - `source_path`
      - `source_array_index`
      - `source_family`
      - raw experience fragment
      - raw company identity inputs used for company resolution
      - raw date payload used for normalization
  - `source_hash text not null`
    - required deterministic dedupe/idempotency key
    - must be built with the canonical helper `public.build_candidate_experience_source_hash(...)`
    - uniqueness remains `UNIQUE (candidate_id, source_hash)`
- `id`, `created_at`, and `updated_at` are DB-managed and should not be mapped from legacy source data.

- Map fields from the chosen source experience item to `candidate_experiences_v2` as follows:
  - `public.candidates.id` -> resolved canonical `candidate_id` from the approved `Task 7c` match outcome
  - chosen source item array ordinal -> `experience_index`
  - source `title` -> `title`
    - trim outer whitespace
    - blank string -> `null`
    - preserve human-readable casing in storage
    - use canonical normalized text only for `source_hash`
  - source `description` -> `description`
    - trim outer whitespace
    - blank string -> `null`
    - preserve content for traceability
    - do **not** include `description` in `source_hash`
  - source `location` -> `location`
    - trim outer whitespace
    - blank string -> `null`
    - do **not** include `location` in `source_hash`
  - source `company` or `companyName` -> `raw_company_name`
    - trim outer whitespace
    - blank string -> `null`
    - preserve raw text separately from canonical `company_id`
  - source `company_id` when present -> company resolution input only
    - use as incoming LinkedIn company id for resolver matching
    - do **not** store it directly as a destination column
  - source `company_linkedin_url` or equivalent canonical company/school LinkedIn URL -> company resolution input only
    - normalize with the company LinkedIn URL helper
    - extract normalized company LinkedIn username when possible
    - store extracted username in `source_company_linkedin_username`
    - if the URL is not a canonical `/company/` or `/school/` URL, treat it as non-identity input and leave `source_company_linkedin_username` null
  - source `start_date` / older `start` / parseable legacy date object or string -> `start_date`, `start_date_precision`
  - source `end_date` / older `end` / parseable legacy date object or string -> `end_date`, `end_date_precision`
  - source `is_current` -> `is_current`
    - if true, force current-role storage semantics from the schema contract
  - raw chosen source item + deterministic source reference metadata -> `source_payload`
  - normalized identity fields -> `source_hash`
- Lower-precedence source usage for this task is explicit:
  - `work_experience` is higher precedence than `linkedin_data.data.experience`
  - the fallback array may supply the full source array only when `work_experience` is absent or empty for that candidate
  - do **not** interleave or merge the two arrays item-by-item
  - do **not** let lower-precedence fallback values overwrite non-blank values from the chosen higher-precedence source array

- Candidate linkage, company linkage, duplication, and rerun rules for this task are:
  - one chosen legacy experience item maps to at most one canonical `candidate_experiences_v2` row
  - candidate linkage must follow the same approved candidate-resolution outcome established by `Task 7c`
  - resolve the destination `candidate_id` by deterministic lookup from the legacy `public.candidates.id` to the canonical `candidate_profiles_v2.id` already approved from Task 7c via the Task 7c candidate-resolution mapping artifact:
    - if `Task 7c` inserted a canonical profile row by stable legacy id reuse, use that same id
    - if `Task 7c` matched the legacy candidate onto an existing canonical row by strong LinkedIn identity, use that matched canonical id
    - do **not** perform a new fuzzy candidate match in this task
  - if no approved canonical candidate mapping can be resolved for the legacy candidate, the experience row must be `skip`:
    - do **not** create a candidate profile in this task
    - do **not** silently remap the experience to another candidate outside the approved `Task 7c` match outcome
    - count and sample these rows in QA output
  - company linkage must use the approved canonical company dataset in `companies_v2`
  - company resolution inputs are:
    - incoming LinkedIn company id from source `company_id` when present
    - incoming LinkedIn username from a direct company username field when present, otherwise extracted from canonical `company_linkedin_url` when possible
    - incoming LinkedIn URL from source `company_linkedin_url` when canonical and parseable
    - incoming company name from `raw_company_name`
  - company matching must follow the canonical precedence already locked in the schema helpers:
    1. `linkedin_id`
    2. `linkedin_username`
    3. `linkedin_url_normalized`
    4. `normalized_name` fallback only when strong identity is absent
  - company resolver outcomes are handled as follows:
    - `match_existing` -> set `company_id` to the matched `companies_v2.id`
    - `create_new` -> **do not create** a new `companies_v2` row in this task; treat the experience as unresolved and store `company_id = null`
    - `ambiguous` -> record a `candidate_experience` ambiguity in `canonicalization_ambiguities`, keep `company_id = null`, and continue processing the experience row
  - ambiguous company resolution must never create or merge canonical company rows silently
  - if company resolution is ambiguous, the ambiguity log must include enough normalized input to explain the decision and must reuse the same open ambiguity on rerun instead of multiplying duplicate open ambiguity rows
  - experience row dedupe/idempotency rules are:
    - canonical uniqueness guard = `(candidate_id, source_hash)`
    - `source_hash` must be built from the schema-contract inputs only:
      - `candidate_id`
      - normalized `title`
      - resolved `company_id` when present, otherwise normalized `raw_company_name`
      - normalized `start_date` + `start_date_precision`
      - normalized `end_date` + `end_date_precision`
      - normalized `is_current`
    - `description`, `location`, and raw payload noise must not change `source_hash`
    - `experience_index` is ordering metadata, not the dedupe key
  - rerun/update decision contract for the backfill script is:
    - `match_existing` when the same deterministic source record reference already exists for that candidate in `source_payload`, or when `(candidate_id, source_hash)` already exists
    - `create_new` when candidate linkage exists and no existing canonical row matches by source record reference or source hash
    - `skip` when candidate linkage is missing or the source item is structurally empty after normalization
  - reruns must not create duplicate experience rows
  - reruns must not create duplicate open ambiguity rows
  - reruns must not append duplicate provenance history:
    - `source_payload` should remain one deterministic provenance object per source row, not an ever-growing appended array

- Normalization rules for this task are:
  - trim outer whitespace on all scalar text inputs
  - convert blank strings to `null`
  - preserve the stored display text for `title`, `description`, `location`, and `raw_company_name` after trimming
  - normalize company names with `public.normalize_company_name(...)` only for:
    - fallback company resolution
    - `source_hash` construction when `company_id` is null
  - normalize company LinkedIn usernames and URLs with the canonical helper functions
  - only canonical LinkedIn company/school URLs count as strong company identity
  - search-result URLs or non-company LinkedIn URLs are not strong company identity inputs
  - title normalization for `source_hash` must follow the canonical helper behavior:
    - lowercase/collapse whitespace for identity comparison
    - punctuation remains significant for the hash
  - date normalization must follow the schema contract and helpers:
    - year-only -> store January 1 of that year + precision `year`
    - month-known -> store the first day of that month + precision `month`
    - exact day-known -> preserve day + precision `day`
    - current/present end date -> `end_date = null`, `end_date_precision = 'present'`, `is_current = true`
    - missing or unparsable date -> `null` + precision `unknown`
  - current-role normalization must preserve multiple current roles if the source has them:
    - do **not** force exactly one current role per candidate in this task
  - `source_payload` must be deterministic in structure and contents so reruns stay comparable and traceable

- This task requires a dedicated backfill script under `scripts/backfills/`, for example `scripts/backfills/08_candidate_experiences_backfill.py`.
- The script must be:
  - deterministic
  - idempotent
  - resumable
  - checkpoint-aware
  - batch-oriented
  - safe to rerun
- Script requirements:
  - stable real execution order must be explicit and reproducible
  - real execution order should be the flattened source experience row order:
    - candidate id ascending
    - chosen source array index ascending within the candidate
  - the checkpoint cursor must be stable enough to resume without skipping or duplicating rows inside a candidate’s source array
  - checkpoints may advance only after durable successful batch writes
  - dry-run mode must execute the real matching/upsert logic inside a transaction that is rolled back
  - dry-run mode must not mutate committed target data
  - dry-run mode must not mutate durable checkpoint state
  - the script must emit clear QA output under `reports/qa/`

- Performance requirements for this task are:
  - avoid row-by-row chatty writes where possible
  - flatten source experience arrays in SQL or another deterministic batchable form before writing
  - prefer set-based or batched insert/update logic where possible
  - use the approved Task 7c candidate-resolution mapping artifact keyed by legacy `public.candidates.id` -> canonical `candidate_profiles_v2.id`; materialize or batch that lookup instead of scanning free-form JSONB row-by-row
  - use indexed company lookups on `companies_v2.linkedin_id`, `linkedin_username`, `linkedin_url_normalized`, and `normalized_name`
  - company resolution may be the most expensive path in this task and must be optimized carefully
  - batch strategy must be explicit in the script and QA report so pilot and full runs use the same behavior

- Required preflight validation for Task 8a:
  - run a deterministic `--dry-run` on the first 100 flattened legacy experience rows in the real script order
  - produce a QA report for that 100-row dry-run under `reports/qa/`
  - create duplicate-validation fixtures only in a safe dev/sandbox path where relevant
  - do **not** commit writes during the dry run
  - explicitly validate duplicate-experience prevention behavior for at least:
    - same candidate + same experience with whitespace/case noise in title/company/date precision
    - same candidate + same experience with description-only differences
    - punctuation-significant title differences that should remain distinct
  - explicitly validate missing-candidate-link behavior
  - explicitly validate ambiguous-company-resolution behavior
  - confirm the preflight results show that reruns would not create duplicate experience rows or duplicate open ambiguity rows

- Emit a QA report for Task 8a under `reports/qa/` with counts and samples for:
  - candidates read
  - flattened source experience rows read
  - rows sourced from `work_experience`
  - rows sourced from `linkedin_data.data.experience`
  - rows normalized
  - rows that would be inserted
  - rows that would be matched/updated
  - rows that would be no-op on rerun
  - rows skipped
  - rows with missing candidate links
  - rows with matched canonical companies by match basis
  - rows with unresolved company links (`company_id = null` because no match)
  - rows with ambiguous company resolution
  - date-precision distributions
  - current-role counts
  - sample outcomes by action and company-match basis
  - duplicate-validation fixture outcomes

Done when:
- A dedicated candidate-experience backfill script exists under `scripts/backfills/`.
- The script uses deterministic flattened source ordering, resumable checkpoints, safe reruns, and `--dry-run`.
- The script processes only approved legacy experience payloads from `public.candidates` for this task.
- The task definition explicitly states that this task may **not** create new `companies_v2` rows and may only link to already-canonical company rows.
- A deterministic `--dry-run` on the first 100 flattened source experience rows has been completed and reviewed.
- Duplicate-validation fixtures have been executed in a safe development or sandbox path for duplicate-experience prevention, missing candidate links, and ambiguous company resolution.
- Task 8a QA output shows that duplicate handling, company-link behavior, candidate-link behavior, and current-role/date normalization behavior are working as specified.
- No pilot write or full backfill is performed until Task 8a is approved.

## [ ] Task 8b: Run 100-Row Pilot Candidate Experience Backfill And Review Results
- After Task 8a approval, run a committed 100-row pilot write using the same deterministic flattened source order validated in Task 8a.
- The pilot must run only after the approved outputs of `Task 6c` and `Task 7c` are present in the target DB, including the approved Task 7c candidate-resolution mapping artifact.
- The 100 pilot-written `candidate_experiences_v2` rows may remain in place and become part of the final canonical dataset.
- Review the resulting pilot rows directly in the database before permitting the full migration.
- Confirm the pilot preserves the required stored fields and semantics:
  - `candidate_id`
  - `company_id`
  - `experience_index`
  - `title`
  - `description`
  - `location`
  - `raw_company_name`
  - `source_company_linkedin_username`
  - `start_date`
  - `start_date_precision`
  - `end_date`
  - `end_date_precision`
  - `is_current`
  - `source_payload`
  - `source_hash`
- Confirm candidate linkage is correct for the written pilot rows:
  - every pilot experience row must link to a valid `candidate_profiles_v2.id`
- Confirm company linkage behavior is correct for the written pilot rows:
  - every non-null `company_id` must link to a valid `companies_v2.id`
  - no new `companies_v2` rows may be created by this task
  - unresolved company matches must remain `company_id = null`
  - ambiguous company matches must remain measurable via `canonicalization_ambiguities`
- Confirm date normalization and current-role semantics are correct in the real DB.
- Confirm the later full backfill can safely continue from the pilot-written state without creating duplicate experience rows, duplicate deterministic provenance, or duplicate open ambiguity rows for the same inputs.
- Emit a pilot QA report under `reports/qa/` with actual inserted, updated, no-op, skipped, unresolved-company, ambiguous-company, and duplicate-check outcomes for the 100 written rows.

Done when:
- A committed 100-row pilot write has been completed after Task 8a approval.
- The pilot rows are allowed to remain in place for the final migration.
- The pilot rows have been reviewed directly in the database.
- Pilot QA confirms candidate linkage, company linkage behavior, date/current-role normalization, duplicate handling, and ambiguity handling are correct in the real DB.
- The later full backfill is proven to continue safely from the pilot-written state without duplicating experience rows, provenance, or open ambiguity rows.
- The full backfill is not run until the pilot review is approved.

## [ ] Task 8c: Run Full Candidate Experience Backfill
- After Task 8b approval, run the full `public.candidates` experience-payload -> `candidate_experiences_v2` backfill using the approved script and checkpoint strategy.
- Continue safely from the pilot-written state or another explicitly approved checkpoint state.
- Preserve the same source selection, mapping, normalization, dedupe, candidate-link, company-link, ambiguity, provenance, and idempotency rules validated in Tasks 8a and 8b.
- Do **not** broaden scope during the full run:
  - do not create new `companies_v2` rows
  - do not mutate legacy source tables
  - do not backfill retrieval tables in this task
- Emit a final QA report for the full migration under `reports/qa/`.

Done when:
- The full experience backfill has been completed only after Task 8a and Task 8b approval.
- `candidate_experiences_v2` is populated from the approved legacy experience payload sources with deterministic source ordering and resumable checkpoints.
- Every written experience row links to a valid `candidate_profiles_v2.id`.
- Every non-null `company_id` links to a valid `companies_v2.id`.
- Unresolved company links remain explicit as `company_id = null` rather than broken links or silent company creation.
- Ambiguous company-resolution outcomes remain logged and measurable.
- Required provenance is present in `source_payload`.
- Required idempotency protection is present in `source_hash`.
- Re-running the backfill does not create duplicate `candidate_experiences_v2` rows, duplicate deterministic provenance, or duplicate open ambiguity rows.
- The resulting canonical work-history rows are sufficient for later current-role derivation so current-role state comes from `candidate_experiences_v2`, not legacy profile text fields.
- A final QA report captures rows read, rows written, rows skipped, company-match outcomes, ambiguity outcomes, date/current-role distributions, and rerun/idempotency outcomes.

## [ ] Task 9a: Implement Candidate Source Document Backfill Script And Preflight Validation
- Scope for this task is **only** legacy candidate source artifacts -> `candidate_source_documents`, plus `canonicalization_ambiguities` logging when a source-document decision is ambiguous.
- Do **not** create `candidate_search_chunks`, `candidate_chunk_embeddings`, or `candidate_search_documents` in this task.
- Do **not** modify `candidate_profiles_v2`, `candidate_emails_v2`, `candidate_experiences_v2`, or any legacy table in this task.
- Exact legacy source tables in scope for this task series are:
  - `candidates`
  - `recruiter_candidates`
- Exact legacy source tables out of scope for this task series are:
  - `candidate_communications`
  - any transcript table, transcript file source, or future artifact source not documented in the current repo
- Source families included in this task series are:
  - `linkedin_profile`
  - `resume`
  - `manual_profile_note`
  - `recruiter_note_raw`
- Source families explicitly out of scope for this task series are:
  - `recruiter_note_summary`
  - `transcript_summary`
  - future candidate artifacts
- Inference from the current schema/docs and legacy schema snapshot:
  - `candidates.ai_summary` is **out of scope** for Task 9 because it is not authoritatively classified as an approved recruiter-note summary or a transcript summary
  - no `transcript_summary` rows should be created in Task 9 because no authoritative legacy transcript source is currently documented
  - no `candidate_communications` rows should be read or converted in Task 9 because communication history is not yet contract-defined as candidate retrieval evidence for this backfill

- Build a dedicated checkpoint-aware backfill entrypoint under `scripts/backfills/` for this migration, for example `scripts/backfills/09_candidate_source_documents_backfill.py`.
- This task depends on the approved Task 7c candidate-resolution outcome and mapping artifact.
- `candidate_source_documents.candidate_id` must always point to the resolved canonical `candidate_profiles_v2.id` established by Task 7c for the underlying legacy candidate source row.
- Do **not** assume the raw legacy candidate UUID is always the canonical destination id.
- If a legacy candidate source row cannot be resolved to an existing canonical candidate row from Task 7c, skip it, report it, and do **not** create an orphan source-document row.
- The script must be deterministic, idempotent, resumable, checkpoint-aware, batch-oriented, and safe to rerun.
- The script must support `--dry-run` and must emit QA output under `reports/qa/`.
- The script must use the Task 5 / retrieval helper contract already defined in the database:
  - `build_candidate_source_document_identity_key(...)`
  - `build_candidate_source_document_content_hash(...)`
  - `decide_candidate_source_document_action(...)`
  - `record_canonicalization_ambiguity(...)`

- Follow `SCHEMA_CONTRACT.md` exactly. `candidate_source_documents` columns touched by this backfill are:
  - `candidate_id uuid not null` — existing `candidate_profiles_v2.id`; required for every inserted row
  - `source_type text not null` — one of `linkedin_profile`, `resume`, `manual_profile_note`, `recruiter_note_raw`
  - `source_subtype text null` — set to `null` for all Task 9 backfilled rows; source-table provenance lives in `external_source_ref` and `metadata_json`
  - `title text null` — deterministic display/debug label by source family
  - `source_url text null` — populated only for `linkedin_profile`; otherwise `null`
  - `external_source_ref text null` — deterministic stable upstream reference used for rerun-safe identity/provenance
  - `raw_payload jsonb null` — original structured source payload when available
  - `raw_text text null` — original text artifact or deterministic assembled legacy text
  - `normalized_text text null` — cleaned deterministic text used for source-document comparison and later chunking
  - `metadata_json jsonb null` — deterministic provenance and document-identity metadata
  - `trust_level text not null` — `baseline`, `high`, or `supplemental` depending on source family
  - `document_version integer not null` — initial version `1`; increment only on `supersede`
  - `is_active boolean not null` — `true` for current active version; older superseded versions become `false`
  - `effective_at timestamptz null` — best available source timestamp when the document version became current
  - `superseded_at timestamptz null` — set only when an active version is superseded
  - `ingested_at timestamptz not null` — backfill write timestamp
  - `created_at timestamptz not null` — write-time timestamp/default; do **not** backdate from legacy source timestamps
  - `updated_at timestamptz not null` — write-time timestamp/default
- `candidate_source_documents.id` may use the DB default UUID generator; it is not source-mapped.
- Ambiguous cases must **not** write `candidate_source_documents`; they must be logged to `canonicalization_ambiguities` via the existing helper with:
  - `entity_type = 'candidate_source_document'`
  - deterministic `source_system`
  - deterministic `source_record_ref`
  - deterministic `normalized_input`
  - `recommended_action = 'manual_review'` or `skip` per the helper contract

- Map legacy source fields into `candidate_source_documents` exactly as follows unless `SCHEMA_CONTRACT.md` is updated first:

  - `linkedin_profile` from `candidates`
    - include one baseline logical LinkedIn-family document per candidate row
    - `candidate_id` <- resolved canonical `candidate_profiles_v2.id` for the legacy `candidates` row under the approved Task 7c candidate-resolution outcome
    - `source_type` <- `'linkedin_profile'`
    - `source_subtype` <- `null`
    - `title` <- `'LinkedIn profile'`
    - `source_url` <- normalized `candidates.linkedin_url` using the candidate LinkedIn URL helper; blank -> `null`
    - do **not** synthesize `source_url` from `linkedin_username`
    - `external_source_ref` <- `legacy:candidates:<candidate_id>:linkedin_profile`
    - `raw_payload` <- `candidates.linkedin_data` when present; otherwise a deterministic JSON object containing the raw legacy candidate fields used to assemble the LinkedIn document text
    - `raw_text` <- deterministic sectioned assembly, in fixed order, from:
      - `headline`
      - `profile_summary`
      - `location`
      - `current_title`
      - `current_company`
      - `all_skills_text`
      - `top_skills` only when `all_skills_text` is blank
      - `education`
      - `work_experience`
      - `previous_companies` only when `work_experience` is blank or unusable
    - do **not** deep-parse additional nested text out of `linkedin_data` in Task 9
    - when any assembled input field is array- or JSON-shaped, serialize it deterministically: preserve source order for arrays/lists, use stable key order for object fields, and join repeated items with a fixed newline-separated format used identically in dry-run, pilot, and full runs
    - `normalized_text` <- cleaned deterministic text version of `raw_text`; no summarization or paraphrasing
    - `metadata_json` <- deterministic provenance including:
      - deterministic `document_identity_key`, e.g. `legacy:candidates:<candidate_id>:linkedin_profile`, for provenance consistency even though LinkedIn decision logic still treats the family as one logical document per candidate
      - `source_table = 'candidates'`
      - `legacy_candidate_id`
      - `source_fields_used`
      - raw `linkedin_username`
      - raw `linkedin_url`
      - `legacy_created_at`
      - `legacy_updated_at`
      - `linkedin_enrichment_date`
      - `linkedin_enrichment_status`
      - optional deterministic `content_sha256`
    - `trust_level` <- `'baseline'`
    - `effective_at` <- `linkedin_enrichment_date`, else `updated_at`, else `created_at`, else `null`

  - `resume` from `candidates.resume_text`
    - include only when `resume_text` is nonblank after trimming
    - `candidate_id` <- resolved canonical `candidate_profiles_v2.id` for the legacy `candidates` row under the approved Task 7c candidate-resolution outcome
    - `source_type` <- `'resume'`
    - `source_subtype` <- `null`
    - `title` <- `'Resume text'`
    - `source_url` <- `null`
    - `external_source_ref` <- `legacy:candidates:<candidate_id>:resume_text`
    - `raw_payload` <- `null`
    - `raw_text` <- `candidates.resume_text`
    - `normalized_text` <- cleaned deterministic text version of `resume_text`
    - `metadata_json` <- deterministic provenance including:
      - `document_identity_key = 'legacy:candidates:<candidate_id>:resume_text'`
      - `source_table = 'candidates'`
      - `source_column = 'resume_text'`
      - `legacy_candidate_id`
      - `legacy_created_at`
      - `legacy_updated_at`
      - optional deterministic `content_sha256`
    - `trust_level` <- `'high'`
    - `effective_at` <- `updated_at`, else `created_at`, else `null`

  - `manual_profile_note` from `candidates.notes`
    - include only when `notes` is nonblank after trimming
    - `candidate_id` <- resolved canonical `candidate_profiles_v2.id` for the legacy `candidates` row under the approved Task 7c candidate-resolution outcome
    - `source_type` <- `'manual_profile_note'`
    - `source_subtype` <- `null`
    - `title` <- `'Legacy profile note'`
    - `source_url` <- `null`
    - `external_source_ref` <- `legacy:candidates:<candidate_id>:notes`
    - `raw_payload` <- `null`
    - `raw_text` <- `candidates.notes`
    - `normalized_text` <- cleaned deterministic text version of `notes`
    - `metadata_json` <- deterministic provenance including:
      - `document_identity_key = 'legacy:candidates:<candidate_id>:notes'`
      - `source_table = 'candidates'`
      - `source_column = 'notes'`
      - `legacy_candidate_id`
      - `legacy_created_at`
      - `legacy_updated_at`
      - optional deterministic `content_sha256`
    - `trust_level` <- `'supplemental'`
    - `effective_at` <- `updated_at`, else `created_at`, else `null`

  - `recruiter_note_raw` from `recruiter_candidates.notes`
    - include one optional note document per `recruiter_candidates` row whose `notes` field is nonblank after trimming
    - `candidate_id` <- resolved canonical `candidate_profiles_v2.id` for the underlying legacy candidate referenced by `recruiter_candidates.candidate_id` under the approved Task 7c candidate-resolution outcome
    - `source_type` <- `'recruiter_note_raw'`
    - `source_subtype` <- `null`
    - `title` <- `'Recruiter note'`
    - `source_url` <- `null`
    - `external_source_ref` <- `legacy:recruiter_candidates:<row_id>:notes`
    - `raw_payload` <- `null`
    - `raw_text` <- `recruiter_candidates.notes`
    - `normalized_text` <- cleaned deterministic text version of `notes`
    - `metadata_json` <- deterministic provenance including:
      - `document_identity_key = 'legacy:recruiter_candidates:<row_id>:notes'`
      - `source_table = 'recruiter_candidates'`
      - `legacy_row_id`
      - `legacy_candidate_id`
      - `recruiter_id`
      - `assigned_at`
      - `status`
      - `last_contact_date`
      - `last_contact_type`
      - optional deterministic `content_sha256`
    - `trust_level` <- `'supplemental'`
    - `effective_at` <- `last_contact_date`, else `assigned_at`, else `null`

- Explicit Task 9 field exclusions:
  - do **not** map `candidates.ai_summary`
  - do **not** map any `candidate_communications` field
  - do **not** create `recruiter_note_summary` rows
  - do **not** create `transcript_summary` rows
  - do **not** create future-artifact rows in this backfill
  - do **not** fabricate transcript provenance, summary approval state, or new source families not already defined in `SCHEMA_CONTRACT.md`

- Identity, matching, duplication, and versioning rules for this task are:
  - use `decide_candidate_source_document_action(...)` as the authoritative decision contract
  - `linkedin_profile`
    - exactly one logical LinkedIn document family per candidate
    - same normalized content -> `no_op`
    - changed normalized content -> `supersede`
    - multiple active LinkedIn rows for the same candidate -> `ambiguous`
  - `resume`
    - stable logical identity is the deterministic legacy resume ref stored in `metadata_json.document_identity_key` / `external_source_ref`
    - same identity + same normalized content -> `no_op`
    - same identity + changed normalized content -> `supersede`
    - different identity -> `parallel`
  - `manual_profile_note`
    - stable logical identity is `legacy:candidates:<candidate_id>:notes`
    - same identity + same normalized content -> `no_op`
    - same identity + changed normalized content -> `supersede`
    - different identity -> `parallel`
  - `recruiter_note_raw`
    - stable logical identity is `legacy:recruiter_candidates:<row_id>:notes`
    - same identity + same normalized content -> `no_op`
    - same identity + changed normalized content -> `supersede`
    - different recruiter-note row refs remain `parallel` even if the text is identical
  - content-hash comparison rules:
    - use `build_candidate_source_document_content_hash(...)`
    - normalized content comparison is case-insensitive and whitespace-normalized through the helper
    - title-only changes must remain `no_op`
    - metadata-only changes must remain `no_op` when logical identity and normalized text are unchanged
  - `supersede` behavior:
    - must be applied transactionally
    - old active row becomes `is_active = false`
    - old active row gets `superseded_at`
    - new row inserts with incremented `document_version`
    - only one active row remains current within a logical document family
  - `ambiguous` behavior:
    - do **not** auto-merge
    - do **not** insert a new source document
    - log the ambiguity in `canonicalization_ambiguities`
    - reruns must reuse the same open ambiguity where the normalized input is identical
  - reruns must not create duplicate source documents or duplicate open ambiguity entries

- Normalization rules for this task are:
  - trim leading/trailing whitespace on all incoming text fields
  - convert blank strings to `null`
  - normalize LinkedIn URLs with the canonical LinkedIn URL helper before storing `source_url`
  - preserve raw text in `raw_text`; do not paraphrase or summarize it
  - build `normalized_text` deterministically by:
    - using a fixed section order
    - normalizing line endings
    - trimming section boundaries
    - collapsing repeated internal whitespace
    - collapsing blank sections
    - converting empty final output to `null`
  - `metadata_json` must be deterministic and contain source-derived provenance only; do **not** include run-unique noise
  - store timestamptz values in UTC-compatible format
  - `ingested_at` is the backfill write timestamp
  - `effective_at` is the best available source timestamp and may remain `null` when unknown
  - `created_at` / `updated_at` are destination write timestamps, not legacy source timestamps

- Processing, cursor, checkpoint, and performance requirements:
  - this backfill is candidate-centric
  - the stable primary processing unit is the legacy `candidates` row
  - real script order must be:
    - legacy `candidates` ordered by `id`
    - for each candidate in that order:
      - `linkedin_profile`
      - optional `resume`
      - optional `manual_profile_note`
      - related `recruiter_candidates.notes` rows ordered by `recruiter_candidates.id`
  - the script must batch on candidate rows, not on output document rows
  - the checkpoint must store enough state to resume safely by candidate cursor and batch counters
  - checkpoints must advance only after durable success for the full candidate batch
  - `--dry-run` must not mutate `candidate_source_documents`, `canonicalization_ambiguities`, or checkpoint state
  - avoid row-by-row chatty writes where possible
  - prefer set-based or batched lookups and writes
  - use indexed lookups on:
    - `candidate_profiles_v2(id)`
    - `candidate_source_documents(candidate_id, source_type, is_active)`
  - likely expensive paths are large `resume_text` and note normalization; normalize once per artifact and reuse the result for decisioning, insert payloads, and QA output
  - deep parsing of additional nested text from `linkedin_data` is out of scope for Task 9; if 9a shows it is required for baseline coverage, update docs before implementation

- Required preflight validation for Task 9a:
  - run a deterministic `--dry-run` on the first **100 legacy `candidates` rows** in real script order
  - include all in-scope `recruiter_candidates.notes` rows attached to those same 100 candidates
  - perform duplicate-validation fixtures in a safe development or sandbox environment instead of mutating real legacy data
  - explicitly validate `no_op`, `parallel`, `supersede`, and `ambiguous` outcomes, including at minimum:
    - LinkedIn same-content `no_op`
    - LinkedIn changed-content `supersede`
    - LinkedIn duplicate-active-row `ambiguous`
    - resume same stable key + same content `no_op`
    - resume same stable key + changed content `supersede`
    - manual/recruiter note same stable ref + same content `no_op`
    - manual/recruiter note same stable ref + changed content `supersede`
    - different stable note refs remaining `parallel`
    - duplicate active stable-identity matches resolving to `ambiguous`
  - no committed writes are allowed in the dry run
  - emit a QA report for Task 9a under `reports/qa/` with counts for:
    - legacy candidate rows read
    - related recruiter-note rows read
    - `linkedin_profile` docs that would be inserted
    - `resume` docs that would be inserted
    - `manual_profile_note` docs that would be inserted
    - `recruiter_note_raw` docs that would be inserted
    - `no_op` outcomes
    - `parallel` outcomes
    - `supersede` outcomes
    - ambiguous outcomes
    - blank-source skips by family
    - orphan-source skips where `candidate_profiles_v2` is missing
    - candidates that would end without the required active `linkedin_profile` document; this count must resolve to zero before Task 9a approval
    - sample normalized-text outputs by family
    - duplicate-validation fixture outcomes by decision type

Done when:
- A dedicated `candidate_source_documents` backfill script exists under `scripts/backfills/`.
- The script supports deterministic ordering, candidate-based batching, resumable checkpoints, safe reruns, and `--dry-run`.
- The script reads only the approved legacy source tables for this task series.
- The field mapping, normalization, identity, ambiguity, and versioning rules are explicit and match `SCHEMA_CONTRACT.md` and the existing helper behavior.
- A deterministic dry run of the first 100 legacy candidate rows in real script order has been completed and reviewed.
- Duplicate-validation fixtures have confirmed `no_op`, `parallel`, `supersede`, and `ambiguous` handling without silently creating duplicate source documents.
- The Task 9a QA report captures dry-run findings, source-family counts, blank/orphan skips, candidate baseline coverage, and duplicate-validation outcomes.
- The dry run confirms that every candidate in scope can be resolved to a canonical candidate row and can produce the required active `linkedin_profile` document under the approved Task 9 rules.
- No pilot write or full backfill is performed until Task 9a is approved.

## [ ] Task 9b: Run 100-Row Pilot Candidate Source Document Backfill And Review Results
- After Task 9a approval, run a controlled pilot write using the first **100 legacy `candidates` rows** in the same deterministic script order validated in Task 9a.
- Include all in-scope `recruiter_candidates.notes` rows attached to those same 100 candidates.
- The pilot unit is 100 legacy candidate rows; the pilot may create more than 100 `candidate_source_documents` rows.
- Pilot-written source-document rows may remain in place and become part of the final retrieval dataset.
- Review the resulting `candidate_source_documents` rows directly in the database before permitting the full migration.
- Review the resulting `canonicalization_ambiguities` rows directly in the database before permitting the full migration.
- Confirm the pilot preserves the required stored fields and semantics:
  - `source_type`
  - `title`
  - `source_url`
  - `external_source_ref`
  - `raw_payload`
  - `raw_text`
  - `normalized_text`
  - `metadata_json`
  - `trust_level`
  - `document_version`
  - `is_active`
  - `effective_at`
  - `superseded_at`
  - `ingested_at`
- Confirm the pilot writes only the approved source families:
  - `linkedin_profile`
  - `resume`
  - `manual_profile_note`
  - `recruiter_note_raw`
- Confirm the pilot writes **no** rows from excluded inputs:
  - `candidates.ai_summary`
  - `candidate_communications`
  - `recruiter_note_summary`
  - `transcript_summary`
  - future artifacts
- Confirm every pilot candidate in scope has exactly one active `linkedin_profile` document after the pilot.
- Confirm trust levels are correct:
  - `linkedin_profile` = `baseline`
  - `resume` = `high`
  - note families = `supplemental`
- Confirm active/inactive document version behavior is correct in the real DB:
  - `no_op` creates no duplicate row
  - `supersede` deactivates the old row and inserts the next version
  - `parallel` keeps distinct logical artifacts separate
  - `ambiguous` writes only to `canonicalization_ambiguities`
- Confirm the later full migration can safely continue from the pilot-written state without creating duplicate source documents or duplicate open ambiguity entries.
- Confirm rerunning the pilot from the same starting point is idempotent.
- Emit a pilot QA report under `reports/qa/` with actual inserted, superseded, no-op, parallel, ambiguous, skipped, and coverage outcomes.

Done when:
- A controlled pilot write for the first 100 legacy candidate rows has been completed after Task 9a approval.
- The pilot-created source-document rows are allowed to remain in place for the final migration.
- The pilot rows and ambiguity rows have been reviewed directly in the database.
- Pilot QA confirms the expected field mapping, normalization, source-family inclusion/exclusion, duplicate handling, ambiguity handling, and versioning behavior.
- The later full backfill is proven to continue safely from the pilot-written state without duplicating source documents or open ambiguity entries.
- The full backfill is not run until the pilot-write review is approved.

## [ ] Task 9c: Run Full Candidate Source Document Backfill
- After Task 9b approval, run the full candidate source-document backfill using the approved script and checkpoint strategy.
- Continue safely from the pilot-written state or another explicitly approved checkpoint state.
- Preserve the same mapping, normalization, inclusion/exclusion, dedupe, ambiguity, versioning, and provenance rules validated in Tasks 9a and 9b.
- Do **not** widen scope during the full run:
  - do **not** add `candidate_communications`
  - do **not** add `candidates.ai_summary`
  - do **not** add `recruiter_note_summary`
  - do **not** add `transcript_summary`
  - do **not** create chunks, embeddings, or aggregate search rows
- Emit a final QA report for the full migration under `reports/qa/`.

Done when:
- The full backfill has been completed only after Task 9a and Task 9b approval.
- `candidate_source_documents` contains the approved Task 9 source families with the required mapped fields populated.
- Every candidate in scope has one active `linkedin_profile` document after the full run.
- Resume and note artifacts remain separate source documents and are not merged into LinkedIn rows.
- Ambiguous cases are logged and skipped rather than silently merged.
- A final QA report captures rows read, rows written, decision counts by `no_op` / `parallel` / `supersede` / `ambiguous`, per-family counts, blank/orphan skips, baseline-coverage gaps, and final migration outcomes.
- Re-running the backfill does not create duplicate source documents or duplicate open ambiguity entries.
- The full run preserves the active/inactive version semantics validated in the pilot.

## [ ] Task 10: Build Candidate Search Chunks
- Chunk candidate source documents by semantic section rather than only fixed windows where possible.
- Create chunking rules by source type:
  - LinkedIn: headline/about, current role, experience items, skills blocks
  - Resume: summary, role/project sections, skills blocks, certifications/education where useful
  - Recruiter notes and transcript summaries: raw-note chunks plus cleaned-summary chunks when available
- Record chunk source type, source priority, chunk order, document version, and confidence/trust metadata.

Done when:
- A candidate can produce multiple searchable chunks from LinkedIn, resume, and recruiter evidence.
- Chunk boundaries are reproducible and explainable.

## [ ] Task 11: Add Candidate Chunk Embeddings And Retrieval Indexes
- Generate embeddings for candidate search chunks.
- Store model/version metadata and rebuild status.
- Add ANN or exact-search indexes appropriate for the chosen model and scale.
- Support structured filters alongside vector retrieval.

Done when:
- Candidate retrieval runs against chunk embeddings rather than only one candidate-level vector.
- The system can explain which chunk caused a match.

## [ ] Task 12: Build Aggregate Candidate Search Documents
- Build one aggregate candidate summary row in `candidate_search_documents` from canonical data and selected retrieval artifacts.
- Keep structured filter fields beside the flattened summary text.
- Do not treat this table as the only search surface.

Done when:
- Aggregate candidate summaries exist for debugging, coarse retrieval, and admin views.
- The retrieval loop still depends on chunk-level evidence.

## [ ] Task 13: Add Validation And QA Checks
- Add row-count, orphan, duplicate, and spot-check queries.
- Validate source-document coverage, chunk coverage, and embedding coverage.
- Produce migration QA reports and retrieval-corpus QA reports.

Done when:
- The canonical and retrieval layers are structurally trustworthy.
- Coverage gaps are measurable.

## [ ] Task 14: Add Initial Retrieval Evaluation Harness
- Build an offline evaluation harness for candidate retrieval before job matching is fully built.
- Create a small gold set of representative jobs and known-good candidates.
- Compare candidate-level retrieval and chunk-level retrieval for recall and precision.

Done when:
- Retrieval quality can be measured before the UI depends on it.
- The project has an evidence-based baseline for future tuning.

## Phase 2: Job Intake And Matching Core

## [ ] Task 15: Create Job, Reference-Candidate, And Match-State Tables
- Create:
  - `jobs`
  - `job_source_documents`
  - `job_search_chunks`
  - `job_chunk_embeddings`
  - `job_reference_candidates`
  - `job_candidates`
  - `job_rejection_memory`
- Define status values, provenance fields, and relationship rules.

Done when:
- The database can persist JD uploads, hard-filter settings, attached reference candidates, surfaced candidate pools, and rejection memory.

## [ ] Task 16: Implement Job Creation, JD Storage, And Hard-Filter Config
- Store raw JD text and normalized fields.
- Persist recruiter-selected hard filters and optional preferred filters separately.
- Keep unknown candidate values out of automatic exclusion unless the user explicitly chooses otherwise.

Done when:
- A new job stores both its source document and its filter contract.
- The system can distinguish between fail, pass, and needs-screening outcomes.

## [ ] Task 17: Implement Reference-Candidate Attachment And Import
- Allow one or more ideal/reference candidates to be attached during job creation.
- Accept LinkedIn URLs from the recruiter or hiring manager.
- Check whether the candidate already exists in the database.
- If not, fetch LinkedIn data via the approved ingestion path, create the canonical candidate record, and backfill retrieval artifacts.
- Support optional archetype labels so a job can keep multiple acceptable candidate shapes.

Done when:
- A job can attach existing or newly imported reference candidates.
- Reference candidates become permanent candidate records in the main candidate database.

## [ ] Task 18: Implement JD Chunking And Embeddings
- Chunk the JD into meaningful sections such as summary, responsibilities, required skills, preferred skills, and domain context.
- Generate JD chunk embeddings and store model/version metadata.
- Keep the raw JD and normalized fields linked to the chunk set that was used.

Done when:
- A job has multiple searchable JD chunks instead of only one monolithic embedding.
- JD chunk generation is reproducible and versioned.

## [ ] Task 19: Implement Reference-Candidate Snapshotting And Retrieval Artifacts
- Freeze a job-scoped snapshot of each reference candidate when attached to the job.
- Build reference-candidate text/chunks/embeddings from that snapshot.
- Preserve the link to the permanent `candidate_id` while keeping the job-time snapshot stable for reproducibility.

Done when:
- Every job reference candidate has stable, job-scoped retrieval artifacts.
- Future candidate-profile refreshes do not silently change historical job runs.

## [ ] Task 20: Implement Pass 1 JD Retrieval
- Search candidate chunk embeddings by JD similarity.
- Add configurable thresholds and top-K controls.
- Collapse chunk hits back to candidate-level results with evidence references.

Done when:
- A job can return an initial candidate pool ranked by JD similarity with chunk-level evidence.

## [ ] Task 21: Implement Exact-Match And Keyword Funnel
- Support strict and progressively broader keyword levels.
- Include exact technical matching for high-signal stack terms where semantic similarity alone is too loose.
- Expand the funnel only when the result set is too thin.
- Persist the level each candidate passed and the evidence that satisfied it.

Done when:
- Candidate results carry a reproducible keyword-level badge.
- Technical exact-match requirements can reinforce weak LinkedIn profiles without replacing semantic retrieval.

## [ ] Task 22: Implement Reference-Candidate Similarity Retrieval
- Run one retrieval pass for each attached reference candidate.
- Keep the passes separate so multiple archetypes do not collapse into a muddy average too early.
- Persist which reference candidate and which evidence produced each match.

Done when:
- A job can surface candidates based on similarity to one or more attached reference candidates.
- The system can explain which reference candidate drove each result.

## [ ] Task 23: Implement Recruiter Feedback Capture And Durable Job Memory
- Support `strong`, `rejected`, `skipped`, and `needs_screening` actions.
- Persist rejection memory for the job.
- Allow a recruiter to promote a surfaced strong candidate into `job_reference_candidates` when that improves the search.
- Preserve action provenance and timestamps.

Done when:
- Recruiter actions are durable and can influence later ranking.
- Strong candidates can become additional job reference candidates without losing the original attached examples.

## [ ] Task 24: Implement Ensemble Re-Ranking And Evidence Persistence
- Calculate JD similarity, reference-candidate similarity, exact-match/keyword evidence, and source-aware evidence weights.
- Weight candidate evidence by source type:
  - LinkedIn as baseline
  - resume chunks stronger when present
  - raw recruiter-note chunks lighter than approved structured summaries
- Keep JD influence non-zero.
- Persist the evidence rows needed to explain the final score.

Done when:
- Ranking is explainable and follows the weighting rules.
- Resume-rich candidates can benefit from stronger evidence without suppressing LinkedIn-only candidates unfairly.

## [ ] Task 25: Implement Iteration Limits, Stop Conditions, And Needs-Screening Buckets
- Cap iteration count.
- Stop when no new candidates clear the threshold.
- Route candidates with unknown hard-filter values into a needs-screening bucket.
- Preserve iteration history for debugging.

Done when:
- The matching loop is bounded and auditable.
- Unknown values no longer behave like silent exclusions.

## [ ] Task 26: Add Real-Job Retrieval Evaluation And Tuning Controls
- Measure retrieval quality on real job runs using recruiter-reviewed outcomes.
- Compare JD-only retrieval, JD plus reference-candidate retrieval, and different source-weighting strategies.
- Track tuning changes and evaluation results over time.

Done when:
- Matching improvements are measured rather than guessed.
- The team can tune retrieval policies with evidence.

## Phase 3: Backend Application Layer

## [ ] Task 27: Build FastAPI Match And Ingestion Endpoints
- Add endpoints for job creation, JD upload, reference-candidate attachment, run kickoff, result retrieval, recruiter actions, and candidate-evidence views.
- Expose scores, badges, evidence references, screening state, and candidate state cleanly.

Done when:
- The UI can run the system without direct database access.
- The API returns enough information to explain why candidates ranked.

## [ ] Task 28: Build Celery Workflows For Ingestion, Chunking, Embeddings, And Reruns
- Trigger candidate retrieval-artifact generation on insert or document upload.
- Trigger JD chunking and embedding generation on upload.
- Trigger reference-candidate import/snapshotting when attached to a job.
- Trigger reruns or incremental expansion after recruiter confirmation where appropriate.

Done when:
- Async workflows are automatic and idempotent.
- Adding resumes, notes, or new reference candidates updates retrieval artifacts safely.

## [ ] Task 29: Add Observability For Matching Jobs And Retrieval Pipelines
- Log ingestion, chunking, embedding, retrieval, reranking, and worker outcomes.
- Track job status transitions, failures, coverage gaps, and index rebuilds.
- Record which retrieval strategy version was used for each run.

Done when:
- Matching runs can be monitored and debugged without database forensics.
- Retrieval regressions are discoverable.

## Phase 4: Recruiter UI

## [ ] Task 30: Build The Three-Panel Recruiter Layout
- Left panel for job review, hard-filter settings, and run controls.
- Center feed for ranked candidates.
- Right panel for candidate details, source evidence, and recruiter notes.

Done when:
- A recruiter can run and review a job from a single screen.

## [ ] Task 31: Build Candidate Cards, Evidence Views, And Actions
- Show name, title, score indicators, keyword badge, and screening state.
- Show why a candidate surfaced, including matched chunks and matched reference candidates.
- Support strong, reject, skip, and needs-screening directly from the feed.

Done when:
- Recruiter actions update backend state immediately and clearly.
- Recruiters can see the evidence behind each surfaced candidate.

## [ ] Task 32: Add Hard-Filter, Threshold, Funnel, And Reference-Candidate Controls
- Add semantic-threshold controls.
- Show the active keyword level and exact-match rules.
- Allow the recruiter to widen the funnel deliberately.
- Allow recruiters to manage attached reference candidates and archetype labels from the job view.

Done when:
- Search breadth can be tuned from the UI.
- Reference-candidate management does not require direct database work.

## [ ] Task 33: Add Needs-Screening And Match-Explanation Views
- Separate clear matches from needs-screening candidates.
- Show which missing or unknown fields triggered screening.
- Show score composition and source-evidence weighting in an understandable way.

Done when:
- Recruiters can act differently on clear matches and incomplete-but-promising candidates.
- The UI explains match quality without hiding uncertainty.

## Phase 5: Recruiter Intelligence And Automations

## [ ] Task 34: Add Candidate Enrichment
- Enrich candidate and company records on insert.
- Store enrichment provenance and freshness metadata.
- Feed approved enrichment into the retrieval corpus where it materially improves search quality.

Done when:
- Recruiters see usable enrichment data on surfaced candidates.
- Enrichment improves retrieval without silently replacing canonical source data.

## [ ] Task 35: Add A Structured Recruiter Assessment Framework
- Create a standard cross-role recruiter checklist.
- Add a job-specific checklist extension generated from the JD and attached reference candidates.
- Store job-scoped answers separately from reusable candidate-level signals.
- Represent answers with strength, recency, confidence, and unknown states rather than only yes/no where possible.

Done when:
- Recruiters can capture structured candidate-fit evidence during a search.
- The system can reuse durable facts later without overwriting canonical profile fields blindly.

## [ ] Task 36: Add Recruiter Notes, Transcript Processing, And Approved Signal Promotion
- Store raw recruiter notes as source documents immediately.
- Convert raw notes and transcripts into structured summaries asynchronously.
- Let raw notes influence retrieval lightly.
- Promote approved structured summaries into stronger search signals and reusable recruiter-signal records.

Done when:
- Fresh recruiter insight can help discovery quickly.
- Approved summaries become higher-trust ranking signals than raw notes.

## [ ] Task 37: Add Outreach Draft Generation
- Generate first-touch outreach drafts from candidate plus job context.
- Use match evidence and recruiter signals to personalize drafts.
- Support optional follow-up scheduling with a stop-on-response rule.

Done when:
- Recruiters can review high-quality outreach drafts instead of writing from scratch.

## [ ] Task 38: Add Candidate Brief Generation
- Produce a client-ready brief from the stored candidate profile, retrieval evidence, and match context.
- Make sure the brief distinguishes between confirmed facts, inferred fit, and recruiter observations.

Done when:
- The system can generate a reusable external-facing candidate summary.
- The generated brief remains faithful to stored evidence.

## [ ] Task 39: Add Alerts, Hygiene, And Digest Jobs
- Track job changes for relevant candidates.
- Detect stale pipelines, duplicate candidates, and outdated retrieval artifacts.
- Generate weekly recruiter digests.

Done when:
- The system supports ongoing recruiter follow-through, not just one-off search.

## Phase 6: Hardening And Rollout

## [ ] Task 40: Add Cost, Quality, And Prompt-Version Controls
- Track embedding, model, and worker costs.
- Track retrieval quality, ranking quality, and structured-note quality.
- Add prompt/version controls for generated outputs and note-structuring workflows.

Done when:
- Quality and spend can be measured rather than guessed.
- The team can tie retrieval outcomes back to model and prompt versions.

## [ ] Task 41: Switch Read Paths To The Canonical And Retrieval Model
- Move app and scripts onto the new canonical and retrieval tables.
- Compare old and new outputs for representative recruiting flows.
- Ensure all production retrieval paths use chunk-level evidence and the new ranking stack.

Done when:
- Normal reads no longer depend on legacy candidate and company structure.
- Production matching uses the new retrieval model.

## [ ] Task 42: Freeze Or Archive Legacy Structures
- Mark old tables as deprecated.
- Remove or freeze remaining legacy-targeted writes.
- Keep a rollback plan and migration audit trail.

Done when:
- The system operates on the canonical recruiting model with a safe fallback story.
