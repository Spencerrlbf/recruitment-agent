# Recruitment AI Platform

This repo is for building an automated recruiting intelligence system: candidate ingestion, canonical profile storage, chunk-level semantic retrieval, recruiter feedback loops, and downstream recruiter automations.

This workspace is intentionally separate from the legacy source workspace:
- Legacy source workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/Recruitment-Matching`
- New product workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/recruitment-ai-platform`

## Product Goal
Build a system that:
- stores clean canonical candidate and company data
- builds a retrieval corpus from LinkedIn, resumes, recruiter notes, and future candidate artifacts
- embeds candidate and job chunks for semantic search
- matches JDs to candidates with multi-pass retrieval and reranking
- learns from recruiter actions like strong, rejected, skipped, and needs-screening decisions
- supports recruiter workflows like outreach drafts, briefs, alerts, and hygiene

## Core Product Shape
This is not just a database cleanup project.

The target product includes:
- canonical candidate and company data
- a candidate retrieval corpus with source documents, chunks, and embeddings
- job ingestion with JD chunking and embeddings
- attached reference candidates for similarity search
- a multi-pass matching loop with exact-match reinforcement and explainable reranking
- recruiter feedback capture and durable job memory
- a recruiter-facing UI for reviewing and iterating on results
- async enrichment and recruiter automation workflows

## Target Stack
- FastAPI backend
- Supabase Postgres
- pgvector for embeddings and similarity search
- Celery + Redis for async jobs
- OpenAI embeddings and lightweight generation models

## Authoritative Documents
Use the repo docs in this order:
1. `SCHEMA_CONTRACT.md` — source of truth for table design, relationships, constraints, and retrieval boundaries
2. `TASKS.md` — source of truth for phased implementation order
3. `AGENT.md` — implementation rules, guardrails, and working conventions

If any schema language conflicts across docs, `SCHEMA_CONTRACT.md` wins.  
If any sequencing language conflicts across docs, `TASKS.md` wins.

## Delivery Strategy
1. Lock the canonical and retrieval data model.
2. Create migration, checkpoint, and validation scaffolding.
3. Build canonical candidate and company tables.
4. Build the candidate retrieval corpus:
   - source documents
   - search chunks
   - chunk embeddings
   - aggregate candidate search documents
5. Backfill canonical data and candidate retrieval artifacts from legacy data.
6. Add validation checks and an initial retrieval evaluation harness.
7. Build job, reference-candidate, and match-state tables.
8. Implement JD ingestion, reference-candidate attachment, chunking, embeddings, and multi-pass retrieval.
9. Implement ensemble reranking, screening buckets, and durable recruiter feedback.
10. Build the FastAPI, worker, and recruiter UI layers.
11. Add enrichment, structured recruiter assessments, outreach, alerts, observability, and hardening.
12. Switch read paths to the new model and later freeze/archive legacy structures.

## Retrieval Principles
- Treat LinkedIn as the baseline searchable source for every candidate.
- Treat resumes as higher-signal evidence when present.
- Treat recruiter notes and transcript summaries as supplemental evidence.
- Keep `candidate_search_documents` as an aggregate summary/cache, not the only retrieval surface.
- Run retrieval at the chunk level rather than only one vector per candidate.
- Preserve the JD signal in ranking at all times.
- Support reference-candidate similarity without replacing JD fit entirely.
- Route candidates with unknown hard-filter values into a needs-screening path by default.

## Repo Structure
- `README.md`: repo overview and entry point
- `AGENT.md`: implementation rules and delivery constraints
- `TASKS.md`: phased execution checklist
- `SCHEMA_CONTRACT.md`: table, relationship, and retrieval-storage contract
- `app/`: backend, worker, and UI code
- `db/migrations/`: forward-only schema migrations
- `db/validation/canonical/`: canonical row-count, orphan, duplicate, and coverage SQL
- `db/validation/retrieval/`: retrieval row-count, orphan, duplicate, and coverage SQL
- `docs/`: product, schema, architecture, and migration notes
- `scripts/backfills/`: checkpoint-aware backfill and rebuild entrypoints
- `scripts/lib/`: shared Python helpers for migration and backfill support
- `scripts/checkpoints/`: generated JSON checkpoint state for resumable jobs
- `reports/qa/`: generated QA outputs from migration and validation runs
- `reports/retrieval_eval/`: generated retrieval-evaluation outputs

Generated checkpoint JSON and generated report artifacts are intentionally ignored by git while the scaffold directories and their README files remain tracked.

## Starting Documents
- [README.md](./README.md)
- [AGENT.md](./AGENT.md)
- [TASKS.md](./TASKS.md)
- [SCHEMA_CONTRACT.md](./SCHEMA_CONTRACT.md)
- [docs/recruiting-intelligence-system.md](./docs/recruiting-intelligence-system.md)
- [docs/legacy-data-model.md](./docs/legacy-data-model.md)
- [docs/target-data-model.md](./docs/target-data-model.md)
