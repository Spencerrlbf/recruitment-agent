# Recruitment AI Platform Agent Guide

## Mission
Build an automated recruiting intelligence system that:
- ingests and normalizes candidate data
- preserves a canonical candidate and company model
- builds chunk-level semantic retrieval over candidate and job evidence
- runs a multi-pass matching loop for every job
- improves ranking quality using recruiter feedback and reference candidates
- supports downstream recruiter workflows like outreach, briefs, alerts, and hygiene

## Authoritative Documents
Use the repo docs in this order:

1. `SCHEMA_CONTRACT.md`
   - authoritative source for table design, relationships, constraints, retrieval boundaries, and source-of-truth rules

2. `TASKS.md`
   - authoritative source for implementation order and phased execution

3. `README.md`
   - high-level repo overview only

If any schema or table-design language conflicts across docs, `SCHEMA_CONTRACT.md` wins.
If any sequencing language conflicts across docs, `TASKS.md` wins.

Do not implement tables or flows that contradict `SCHEMA_CONTRACT.md`.

## Product Scope
This system is not just a candidate database cleanup project.

The target product includes:
- canonical candidate and company data
- a retrieval corpus over 100,000+ candidates
- chunk-level embeddings for candidates and jobs
- JD-driven retrieval
- reference-candidate similarity retrieval
- recruiter feedback loops using strong, rejected, skipped, and needs-screening states
- async enrichment and content-generation workflows
- a recruiter-facing UI for reviewing and iterating on matches
- explainable ranking with evidence preserved in storage

## Default Stack
- FastAPI for the backend API
- Supabase Postgres for application data
- pgvector for similarity search
- Celery + Redis for async workflows
- `text-embedding-3-small` as the initial retrieval embedding model
- `gpt-4o-mini` for text generation, summarization, and note-structuring tasks

If a future implementation changes any of the above, update the docs first.

## Terminology Rules
Use these terms consistently:
- `job_reference_candidates` instead of `seeds`
- `job_rejection_memory` instead of `rejected_embeddings`
- `candidate_source_documents`, `candidate_search_chunks`, and `candidate_chunk_embeddings` for candidate retrieval storage
- `job_source_documents`, `job_search_chunks`, and `job_chunk_embeddings` for job retrieval storage

Do not reintroduce old `seed` terminology in new migrations, code, or docs.

## Core Data Layers
There are four data layers in this repo.

### 1. Canonical candidate/company data
These are the source-of-truth entity tables:
- `candidate_profiles_v2`
- `candidate_emails_v2`
- `companies_v2`
- `candidate_experiences_v2`

Rules:
- `candidate_experiences_v2` is the source of truth for work history
- profile-level `current_*` fields and `experience_years` are derived caches
- canonical tables must remain valid without retrieval artifacts

### 2. Candidate retrieval corpus
These tables store searchable candidate evidence:
- `candidate_source_documents`
- `candidate_search_chunks`
- `candidate_chunk_embeddings`
- `candidate_search_documents`

Rules:
- `candidate_search_documents` is an aggregate summary/cache only
- it is not the only retrieval surface
- embeddings do not live on canonical candidate tables
- retrieval runs against chunk embeddings, not only one candidate-level vector

### 3. Job retrieval corpus and matching memory
These tables store job search inputs, job-scoped snapshots, surfaced candidates, and durable job memory:
- `jobs`
- `job_source_documents`
- `job_search_chunks`
- `job_chunk_embeddings`
- `job_reference_candidates`
- `job_candidates`
- `job_rejection_memory`

Rules:
- a reference candidate always points to a permanent `candidate_id`
- each attached reference candidate also gets a job-scoped snapshot for reproducibility
- `job_candidates` must preserve enough evidence to explain why a candidate ranked

### 4. Later structured recruiter assessment layer
These can be deferred until the recruiter checklist task:
- `job_candidate_assessments`
- `candidate_recruiter_signals`

Rules:
- job-scoped recruiter answers should not blindly overwrite canonical candidate fields
- only durable confirmed signals should be promoted into reusable candidate-level recruiter signals

## Non-Negotiable Constraints
- Build inside the current Supabase project first.
- Do not mutate or delete legacy tables during migration.
- Keep candidate IDs stable where possible.
- Reuse the stable legacy candidate UUID for `candidate_profiles_v2.id` during backfill.
- Prefer deterministic, idempotent, resumable backfills and workers.
- Store enough traceability to explain how a candidate, company, chunk, embedding, score, or generated artifact was produced.
- Treat LinkedIn as the baseline searchable source for every candidate.
- Treat resumes as higher-signal evidence when present, but do not penalize candidates who only have LinkedIn data.
- Treat recruiter notes and transcript summaries as supplemental evidence.
- Treat raw recruiter notes as lower-trust than approved structured summaries.
- Keep `candidate_search_documents` as an aggregate summary/cache, not the only search surface.
- Generate embeddings at the chunk level, not directly on canonical entity tables.
- Preserve the JD signal in ranking at all times.
- Reference-candidate similarity can rebalance ranking, but it must not replace role fit entirely.
- Do not collapse multiple reference candidates into one averaged embedding before retrieval.
- Route unknown hard-filter values into a `needs_screening` path by default instead of excluding candidates automatically.
- Keep legacy tables untouched until the migration and cutover phases explicitly allow otherwise.

## Retrieval Design Rules
### Candidate-side retrieval
Every candidate should be searchable through one or more source documents.

Expected source families:
- LinkedIn profile snapshot
- resume
- recruiter note raw
- recruiter note summary
- transcript summary
- future approved candidate artifacts

Minimum baseline:
- every candidate must have at least one active LinkedIn source document

Expected chunking behavior:
- LinkedIn: headline/about, current role, one chunk per experience item, skills block
- Resume: summary, one chunk per role/project section, skills block, optionally certifications and education
- Recruiter note raw: one chunk if short, multiple section chunks if long
- Approved summaries: concise topic-based chunks

### Job-side retrieval
Every job should be searchable through one or more source documents.

Expected source families:
- JD
- reference-candidate snapshot

Expected chunking behavior:
- JD summary
- JD must-have requirements
- JD preferred requirements
- JD responsibilities
- JD domain or company context
- reference-candidate current role
- reference-candidate strongest experience items
- reference-candidate skills or specialization summary

### Embedding rules
- Generate embeddings at the chunk level.
- Multiple embeddings per candidate are expected.
- Multiple embeddings per job are expected.
- Store model name, model version, and embedding dimensions with every embedding row.
- Add ANN indexes only for the active production model and dimension combination.
- Keep older embeddings only when they are useful for reproducibility, evaluation, or controlled migration.

## Matching Rules
Matching must support:
- JD chunk retrieval
- reference-candidate snapshot retrieval
- exact-match and keyword reinforcement for high-signal technical terms
- structured filters
- configurable thresholds and top-K controls
- evidence-aware reranking

Ranking must preserve:
- JD score
- reference-candidate score
- exact-match or keyword score
- source-aware evidence weighting
- screening outcome
- enough evidence to explain which job chunk and candidate chunk matched

Evidence-weighting families are:
- LinkedIn = baseline
- resume = stronger when present
- recruiter note raw = lighter-weight supplemental evidence
- recruiter note summary / transcript summary = higher-trust supplemental evidence

Unknown filter values must support:
- `pass`
- `fail`
- `needs_screening`

## Delivery Order
Implement in this order unless a documented dependency forces a change:

1. Lock schema and migration rules.
2. Add migration, checkpoint, and validation scaffolding.
3. Create canonical candidate and company tables.
4. Create candidate retrieval corpus tables.
5. Implement canonicalization rules.
6. Backfill canonical companies, candidate profiles, emails, and experiences.
7. Backfill candidate source documents, chunks, embeddings, and aggregate candidate summaries.
8. Add structural validation and an initial retrieval evaluation harness.
9. Create job, job-retrieval, reference-candidate, and match-state tables.
10. Implement JD ingestion, reference-candidate attachment/import, JD chunking, and reference-candidate snapshotting.
11. Implement JD retrieval, exact-match funnel, reference-candidate retrieval, reranking, and bounded iteration.
12. Build FastAPI endpoints, Celery workflows, and observability.
13. Build the recruiter UI.
14. Add enrichment, structured recruiter assessments, note processing, and recruiter automations.
15. Add cost, quality, prompt-version, and rollout controls.
16. Switch read paths to the new canonical and retrieval model.
17. Freeze or archive legacy structures only after the new model is proven.

Do not jump to UI polish or outreach flows before the retrieval loop is measurable.

## Repo Conventions
- `db/migrations/`: schema changes only
- `db/validation/`: row-count, orphan, duplicate, QA, and retrieval-coverage checks
- `scripts/`: backfills, maintenance, checkpoint-aware jobs, and migration helpers
- `docs/`: product, schema, architecture, and decision records
- `app/`: API, worker, and UI code when implementation begins

If a dedicated checkpoint/state directory is introduced during Task 2, document it in both `TASKS.md` and `README.md`.

## Definition Of Done
### For schema work
- `SCHEMA_CONTRACT.md` and the implementation are aligned
- migrations run cleanly
- indexes and constraints exist
- validation queries are added
- canonical and retrieval boundaries are preserved
- no table or field contradicts the schema contract

### For backfills
- safe to resume
- deterministic and idempotent
- row-count parity and duplicate checks exist
- traceability fields are preserved
- source-document, chunk, and embedding coverage can be measured

### For retrieval and ranking
- thresholds and weights are configurable
- candidate inclusion, screening, and rejection behavior is persisted
- ranking decisions are explainable from stored data
- the system can identify which chunk caused a match
- evaluation exists before major UI dependence

### For async AI workflows
- tasks are idempotent
- prompt inputs and outputs are explicit
- failures can be retried without corrupting state
- versioned outputs can be traced back to the prompt/model that produced them

### For UI work
- recruiter actions write back to job-scoped match-state tables
- score badges and action states reflect backend truth
- surfaced evidence is consistent with stored retrieval evidence
- reference-candidate and needs-screening behavior is visible and auditable

## Working Rules
- When a task is ambiguous, prefer documenting the contract before writing the implementation.
- When schema language is ambiguous, stop and update `SCHEMA_CONTRACT.md` before writing migrations.
- When sequencing is ambiguous, follow `TASKS.md`.
- Do not invent new retrieval tables, scoring concepts, or recruiter states without first updating the docs.
- Do not replace chunk-level retrieval with a simpler one-vector-per-candidate shortcut.
- Do not let raw recruiter notes behave like hard filters.
- Keep the system explainable at every stage.