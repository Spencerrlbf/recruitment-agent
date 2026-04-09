# Recruitment AI Platform Agent Guide

## Mission
Build an automated recruiting intelligence system that:
- ingests and normalizes candidate data
- stores embeddings for candidates and job descriptions
- runs a multi-pass matching loop for every job
- improves ranking quality using recruiter feedback
- supports downstream recruiter workflows like outreach, briefs, alerts, and hygiene

## Product Scope
The system is not just a candidate database cleanup project.

The target product includes:
- canonical candidate and company data
- semantic retrieval over 100,000+ candidates
- recruiter feedback loops using strong and rejected candidates
- async enrichment and content generation workflows
- a recruiter-facing UI for reviewing and iterating on matches

## Default Stack
- FastAPI for the backend API
- Supabase Postgres for application data
- pgvector for similarity search
- Celery + Redis for async workflows
- `text-embedding-3-small` for embeddings
- `gpt-4o-mini` for text generation and summarization tasks

If a future implementation changes any of the above, update the docs first.

## Core Data Boundaries
There are two data layers in this repo:

1. Canonical candidate data
- `candidate_profiles_v2`
- `candidate_emails_v2`
- `companies_v2`
- `candidate_experiences_v2`
- `candidate_search_documents`

2. Matching and recruiter-feedback data
- `jobs`
- `job_candidates`
- `seeds`
- `rejected_embeddings`

The canonical layer is the source of truth for search documents and matching inputs.

## Non-Negotiable Constraints
- Build inside the current Supabase project first.
- Do not mutate or delete legacy tables during migration.
- Keep candidate IDs stable where possible.
- Prefer deterministic, idempotent, resumable backfills and workers.
- Store enough traceability to explain how a candidate, company, score, or generated artifact was produced.
- Keep the JD signal in ranking at all times. Seed-based retrieval can rebalance weighting, but it must not replace role fit entirely.

## Delivery Order
Implement in this order unless a documented dependency forces a change:

1. Lock schema and migration rules.
2. Build canonical candidate/company/search tables.
3. Backfill and validate the normalized data.
4. Add embeddings and retrieval indexes.
5. Build job ingestion and match-state tables.
6. Implement Pass 1 JD search.
7. Implement recruiter feedback capture.
8. Implement seed-based expansion and ensemble reranking.
9. Build the recruiter UI.
10. Add enrichment and communication automations.
11. Add evaluation, observability, and rollout controls.

Do not jump to UI polish or outreach flows before the retrieval loop is measurable.

## Repo Conventions
- `db/migrations/`: schema changes only
- `db/validation/`: row-count, orphan, duplicate, and QA checks
- `scripts/`: backfills, maintenance, and migration helpers
- `docs/`: product, schema, and decision records
- `app/`: API, worker, and UI code when implementation begins

## Definition Of Done
For schema work:
- migrations run cleanly
- indexes and constraints exist
- validation queries are added

For backfills:
- safe to resume
- row-count parity and duplicate checks exist
- traceability fields are preserved

For retrieval and ranking:
- thresholds and weights are configurable
- candidate inclusion and rejection behavior is persisted
- ranking decisions are explainable from stored data

For async AI workflows:
- tasks are idempotent
- prompt inputs and outputs are explicit
- failures can be retried without corrupting state

For UI work:
- recruiter actions write back to canonical match-state tables
- score badges and action states reflect backend truth

## Working Rule
When a task is ambiguous, prefer documenting the contract before writing the implementation.
