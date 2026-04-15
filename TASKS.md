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

## [ ] Task 4: Create Candidate Retrieval Corpus Tables, Constraints, And Retrieval Validation
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

## [ ] Task 5: Implement Canonicalization Rules
- Normalize emails.
- Resolve canonical companies by strong identity first and name fallback last.
- Normalize experience dates, current-role flags, raw company names, and LinkedIn identity fields.

Done when:
- Canonicalization behavior is deterministic.
- Helpers exist in reusable SQL or application code.
- Canonicalization is independent from chunking and embedding generation.

## [ ] Task 6: Backfill Companies
- Seed `companies_v2` from the existing company data.
- Deduplicate against canonical identity rules.
- Preserve traceability and source-quality markers.

Done when:
- Canonical companies exist with measurable duplicate reduction.
- Company resolution precedence is reproducible.

## [ ] Task 7: Backfill Candidate Profiles And Emails
- Copy one stable profile row per candidate.
- Backfill normalized candidate emails.
- Preserve source references and verification metadata.

Done when:
- Candidate counts reconcile.
- Candidate-email duplicates are prevented.

## [ ] Task 8: Backfill Candidate Experiences
- Parse work history from legacy experience sources.
- Resolve each experience to a canonical company.
- Store one row per experience item with raw payload traceability.

Done when:
- Experience rows link to valid candidates and companies.
- Current role state comes from experience rows, not legacy text fields.

## [ ] Task 9: Backfill Candidate Source Documents
- Create retrieval-source rows for every candidate from available LinkedIn data.
- Import resume text and metadata when present.
- Store recruiter notes, transcript summaries, and future candidate artifacts as separate source documents with provenance and trust metadata.
- Preserve document versions and timestamps so newer evidence can be tracked without losing history.

Done when:
- Every candidate has at least one retrieval source document from LinkedIn data.
- Candidates with resumes and notes have additional searchable source documents instead of merged opaque blobs.

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
