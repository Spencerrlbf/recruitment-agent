# Recruitment AI Platform

This repo is for building an automated recruiting intelligence system: candidate ingestion, canonical profile storage, semantic search, recruiter feedback loops, and downstream recruiter automations.

This workspace is intentionally separate from the legacy source workspace:
- Legacy source workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/Recruitment-Matching`
- New product workspace: `/Users/spencerbarton-fisher/Mac-Mini-Projects/recruitment-ai-platform`

## Product Goal
Build a system that:
- stores clean candidate and company data
- embeds candidates and job descriptions
- matches JDs to candidates with semantic search
- learns from recruiter actions like strong and rejected decisions
- supports recruiter workflows like outreach drafts, briefs, and alerts

## Target Stack
- FastAPI backend
- Supabase Postgres
- pgvector for embeddings and similarity search
- Celery + Redis for async jobs
- OpenAI embeddings and lightweight generation models

## Delivery Strategy
1. Normalize the candidate and company data model.
2. Backfill canonical data from the legacy system without mutating legacy tables.
3. Build search documents and embeddings.
4. Add job, match-state, seed, and rejection-memory tables.
5. Implement the multi-pass matching loop.
6. Build the recruiter UI.
7. Add enrichment and recruiter automations.

## Repo Structure
- `AGENT.md`: implementation rules and delivery constraints
- `TASKS.md`: phased execution checklist
- `app/`: backend, worker, and UI code
- `db/migrations/`: schema changes
- `db/validation/`: QA and validation SQL
- `docs/`: product, schema, and migration notes
- `scripts/`: backfills and maintenance jobs

## Starting Documents
- [AGENT.md](./AGENT.md)
- [TASKS.md](./TASKS.md)
- [docs/recruiting-intelligence-system.md](./docs/recruiting-intelligence-system.md)
- [docs/legacy-data-model.md](./docs/legacy-data-model.md)
- [docs/target-data-model.md](./docs/target-data-model.md)
