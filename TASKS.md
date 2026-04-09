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

## Phase 1: Canonical Candidate Data

## [ ] Task 1: Lock The Canonical Data Model
- Finalize the exact `v2` tables:
  - `candidate_profiles_v2`
  - `candidate_emails_v2`
  - `companies_v2`
  - `candidate_experiences_v2`
  - `candidate_search_documents`
- Map legacy fields into the new model.
- Decide which fields stay canonical versus derived or cached.

Done when:
- The schema contract is documented.
- Unique constraints, foreign keys, and source-of-truth rules are agreed.

## [ ] Task 2: Create Migration And Validation Scaffolding
- Add migration, backfill, checkpoint, and validation structure.
- Define how long-running backfills resume safely.
- Define where QA reports live.

Done when:
- The repo has a clear place for SQL, scripts, checkpoints, and validation queries.
- A backfill can resume without starting over.

## [ ] Task 3: Create Canonical Tables And Constraints
- Create all `v2` tables.
- Add keys, timestamps, uniqueness rules, and retrieval-oriented indexes.
- Keep legacy tables untouched.

Done when:
- The tables compile successfully.
- Test inserts work.

## [ ] Task 4: Implement Canonicalization Rules
- Normalize emails.
- Resolve canonical companies by strong identity first and name fallback last.
- Normalize experience dates, current-role flags, and raw company names.

Done when:
- Canonicalization behavior is deterministic.
- Helpers exist in reusable SQL or application code.

## [ ] Task 5: Backfill Companies
- Seed `companies_v2` from the existing company data.
- Deduplicate against the canonical identity rules.
- Preserve traceability and source-quality markers.

Done when:
- Canonical companies exist with measurable duplicate reduction.

## [ ] Task 6: Backfill Candidate Profiles And Emails
- Copy one stable profile row per candidate.
- Backfill normalized candidate emails.
- Preserve source references and verification metadata.

Done when:
- Candidate counts reconcile.
- Candidate-email duplicates are prevented.

## [ ] Task 7: Backfill Candidate Experiences
- Parse work history from legacy experience sources.
- Resolve each experience to a canonical company.
- Store one row per experience item with raw payload traceability.

Done when:
- Experience rows link to valid candidates and companies.
- Current role state comes from experience rows, not legacy text fields.

## [ ] Task 8: Build Search Documents
- Create one search document per candidate from normalized profile, experience, education, and skills data.
- Keep structured filter fields beside the flattened text.

Done when:
- Search documents are generated from canonical tables instead of legacy blobs.

## [ ] Task 9: Add Candidate Embeddings And Retrieval Indexes
- Add vector storage and similarity indexes.
- Generate candidate embeddings automatically.
- Support structured filters alongside semantic retrieval.

Done when:
- Candidate retrieval runs from the canonical model with vectors stored in the database.

## [ ] Task 10: Add Validation And QA Checks
- Add row-count, orphan, duplicate, and spot-check queries.
- Produce a migration QA report.

Done when:
- The canonical data layer is structurally trustworthy.

## Phase 2: Job Matching Core

## [ ] Task 11: Create Job And Match-State Tables
- Create:
  - `jobs`
  - `job_candidates`
  - `seeds`
  - `rejected_embeddings`
- Define status values and relationship rules.

Done when:
- The database can persist JD uploads, candidate pools, seeds, and rejection memory.

## [ ] Task 12: Implement JD Upload And Embedding Generation
- Store raw JD text and normalized fields.
- Generate the JD embedding immediately on upload.
- Mark jobs ready for retrieval.

Done when:
- Every searchable job has a stored embedding.

## [ ] Task 13: Implement Pass 1 JD Semantic Sweep
- Search candidate embeddings by JD similarity.
- Add a configurable similarity threshold.
- Pull the top candidate set for the first pass.

Done when:
- A job can return an initial candidate pool ranked by JD similarity.

## [ ] Task 14: Implement The Keyword Funnel
- Support strict and progressively broader keyword levels.
- Expand the funnel only when the result set is too thin.
- Persist the level each candidate passed.

Done when:
- Candidate results carry a keyword-level badge and the funnel logic is reproducible.

## [ ] Task 15: Implement Recruiter Feedback Capture
- Support `strong`, `rejected`, and `skipped` actions.
- Persist rejection memory for the job.
- Persist seeds when a candidate is marked strong.

Done when:
- Recruiter actions are durable and can influence later ranking.

## [ ] Task 16: Implement Pass 3 Seed-Based Candidate Search
- Search the full pool using seed-candidate similarity.
- Apply the keyword funnel and rejection filter again.
- Merge and deduplicate against the existing pool.

Done when:
- A confirmed strong candidate can expand the pool with seed-similar candidates.

## [ ] Task 17: Implement Ensemble Re-Ranking
- Calculate JD similarity, seed similarity, and composite score.
- Shift weighting dynamically based on seed count.
- Keep JD influence non-zero.

Done when:
- Ranking is explainable and follows the weighting rules.

## [ ] Task 18: Implement Iteration Limits And Stop Conditions
- Cap iteration count.
- Stop when no new candidates clear the threshold.
- Preserve iteration history for debugging.

Done when:
- The matching loop is bounded and auditable.

## Phase 3: Backend Application Layer

## [ ] Task 19: Build FastAPI Match Endpoints
- Add endpoints for JD creation, run kickoff, result retrieval, and recruiter actions.
- Expose scores, badges, and candidate state cleanly.

Done when:
- The UI can run the system without direct database access.

## [ ] Task 20: Build Celery Workflows
- Trigger candidate embedding generation on insert.
- Trigger JD embedding generation on upload.
- Trigger seed expansion after recruiter confirmation.

Done when:
- Async workflows are automatic and idempotent.

## [ ] Task 21: Add Observability For Matching Jobs
- Log retrieval, ranking, and worker outcomes.
- Track job status transitions and failures.

Done when:
- Matching runs can be monitored and debugged without database forensics.

## Phase 4: Recruiter UI

## [ ] Task 22: Build The Three-Panel Recruiter Layout
- Left panel for JD review and run controls.
- Center feed for ranked candidates.
- Right panel for candidate details.

Done when:
- A recruiter can run and review a job from a single screen.

## [ ] Task 23: Build Candidate Cards And Actions
- Show name, title, score indicators, and keyword badge.
- Support strong, reject, and skip directly from the feed.

Done when:
- Recruiter actions update the backend state immediately and clearly.

## [ ] Task 24: Add Threshold And Funnel Controls
- Add semantic-threshold controls.
- Show the active keyword level.
- Allow the recruiter to widen the funnel deliberately.

Done when:
- Search breadth can be tuned from the UI.

## Phase 5: Recruiter Automations

## [ ] Task 25: Add Candidate Enrichment
- Enrich candidate and company records on insert.
- Store enrichment provenance and freshness metadata.

Done when:
- Recruiters see usable enrichment data on surfaced candidates.

## [ ] Task 26: Add Outreach Draft Generation
- Generate first-touch outreach drafts from candidate plus JD context.
- Support optional follow-up scheduling with a stop-on-response rule.

Done when:
- Recruiters can review high-quality outreach drafts instead of writing from scratch.

## [ ] Task 27: Add Candidate Brief Generation
- Produce a client-ready brief from the stored candidate profile and match context.

Done when:
- The system can generate a reusable external-facing candidate summary.

## [ ] Task 28: Add Transcript Processing And Record Updates
- Turn call or interview transcripts into structured notes.
- Write approved summaries back to the candidate record.

Done when:
- Candidate records can be updated from recruiter conversations without manual re-entry.

## [ ] Task 29: Add Alerts, Hygiene, And Digest Jobs
- Track job changes for relevant candidates.
- Detect stale pipelines and duplicate candidates.
- Generate weekly recruiter digests.

Done when:
- The system supports ongoing recruiter follow-through, not just one-off search.

## Phase 6: Hardening And Rollout

## [ ] Task 30: Add Evaluation, Cost, And Quality Controls
- Build offline evaluation datasets for retrieval quality.
- Track embedding, model, and worker costs.
- Add prompt/version controls for generated outputs.

Done when:
- Quality and spend can be measured rather than guessed.

## [ ] Task 31: Switch Read Paths To The Canonical Model
- Move app and scripts onto the new tables.
- Compare old and new outputs for representative recruiting flows.

Done when:
- Normal reads no longer depend on legacy candidate and company structure.

## [ ] Task 32: Freeze Or Archive Legacy Structures
- Mark old tables as deprecated.
- Remove or freeze remaining legacy-targeted writes.
- Keep a rollback plan.

Done when:
- The system operates on the canonical recruiting model with a safe fallback story.
