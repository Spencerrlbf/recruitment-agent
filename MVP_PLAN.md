# MVP_PLAN.md

This file does **not** change the wording, numbering, or scope of `TASKS.md`.

Its purpose is only to:
- group the existing tasks into **MVP Core** and **V2 Expansion**
- preserve the current task bodies exactly in `TASKS.md`
- show the current completion state for early tasks

## Current Completed Status
- [x] Task 1: Lock The Canonical And Retrieval Data Model
- [x] Task 2: Create Migration, Checkpoint, And Validation Scaffolding
- [x] Task 3: Create Canonical Tables And Constraints
- [x] Task 4: Create Retrieval Corpus Tables And Constraints

## MVP Goal
The MVP should prove the core product works end to end.

For this repo, MVP means the system can:
- make the current candidate corpus searchable
- create jobs from JD text
- attach one or more reference candidates to a job
- run hybrid matching using JD chunks plus reference-candidate similarity
- return ranked candidates with explainable evidence
- support candidate ingestion/upload through the application layer

## Important Note On Candidate Upload
The current `TASKS.md` does not contain a standalone task named "candidate upload".

For MVP planning purposes, candidate upload should be delivered through the existing ingestion/application tasks already in the task list:
- Task 27: Build FastAPI Match And Ingestion Endpoints
- Task 28: Build Celery Workflows For Ingestion, Chunking, Embeddings, And Reruns

If a safe synchronous path is available, MVP can ship before full async hardening. If async ingestion is required from day one, pull Task 28 into the MVP implementation track.

---

# MVP Core

These are the existing tasks that should be treated as the shortest path to a usable internal MVP.

## Foundation Already Complete
- [x] Task 1: Lock The Canonical And Retrieval Data Model
- [x] Task 2: Create Migration, Checkpoint, And Validation Scaffolding
- [x] Task 3: Create Canonical Tables And Constraints

## Candidate Searchability
- [x] Task 4: Create Retrieval Corpus Tables And Constraints
- [ ] Task 5: Implement Canonicalization Rules
- [ ] Task 6a: Implement Company Backfill Script And Preflight Validation
- [ ] Task 6b: Run 100-Row Pilot Company Backfill And Review Results
- [ ] Task 6c: Run Full Company Backfill
- [ ] Task 7: Backfill Candidate Profiles And Emails
- [ ] Task 8: Backfill Candidate Experiences
- [ ] Task 9: Backfill Candidate Source Documents
- [ ] Task 10: Build Candidate Search Chunks
- [ ] Task 11: Add Candidate Chunk Embeddings And Retrieval Indexes
- [ ] Task 13: Add Validation And QA Checks

## Job Creation And Matching
- [ ] Task 15: Create Job, Reference-Candidate, And Match-State Tables
- [ ] Task 16: Implement Job Creation, JD Storage, And Hard-Filter Config
- [ ] Task 17: Implement Reference-Candidate Attachment And Import
- [ ] Task 18: Implement JD Chunking And Embeddings
- [ ] Task 19: Implement Reference-Candidate Snapshotting And Retrieval Artifacts
- [ ] Task 20: Implement Pass 1 JD Retrieval
- [ ] Task 22: Implement Reference-Candidate Similarity Retrieval
- [ ] Task 24: Implement Ensemble Re-Ranking And Evidence Persistence

## Application Layer Needed For MVP
- [ ] Task 27: Build FastAPI Match And Ingestion Endpoints

## Pull Forward Only If Needed For MVP Operations
- [ ] Task 28: Build Celery Workflows For Ingestion, Chunking, Embeddings, And Reruns

Use Task 28 in the MVP only if:
- candidate upload must be asynchronous from day one
- JD/reference-candidate processing is too slow for safe synchronous execution
- the MVP would otherwise require manual operational steps to stay usable

## MVP Acceptance Gate
The MVP should be considered complete when the system can:
- search the current candidate corpus
- create jobs from JD text
- attach one or more reference candidates
- run JD retrieval plus reference-candidate similarity retrieval
- rerank results with explainable evidence
- expose the workflow through API endpoints
- ingest new candidates through the application path without requiring direct database work

---

# V2 Expansion

These tasks are important, but should not block the first usable internal MVP.

## Candidate Retrieval Quality And Evaluation
- [ ] Task 12: Build Aggregate Candidate Search Documents
- [ ] Task 14: Add Initial Retrieval Evaluation Harness

## Matching Depth And Feedback Loops
- [ ] Task 21: Implement Exact-Match And Keyword Funnel
- [ ] Task 23: Implement Recruiter Feedback Capture And Durable Job Memory
- [ ] Task 25: Implement Iteration Limits, Stop Conditions, And Needs-Screening Buckets
- [ ] Task 26: Add Real-Job Retrieval Evaluation And Tuning Controls

## Backend Hardening And Observability
- [ ] Task 29: Add Observability For Matching Jobs And Retrieval Pipelines

## Recruiter UI
- [ ] Task 30: Build The Three-Panel Recruiter Layout
- [ ] Task 31: Build Candidate Cards, Evidence Views, And Actions
- [ ] Task 32: Add Hard-Filter, Threshold, Funnel, And Reference-Candidate Controls
- [ ] Task 33: Add Needs-Screening And Match-Explanation Views

## Recruiter Intelligence And Automations
- [ ] Task 34: Add Candidate Enrichment
- [ ] Task 35: Add A Structured Recruiter Assessment Framework
- [ ] Task 36: Add Recruiter Notes, Transcript Processing, And Approved Signal Promotion
- [ ] Task 37: Add Outreach Draft Generation
- [ ] Task 38: Add Candidate Brief Generation
- [ ] Task 39: Add Alerts, Hygiene, And Digest Jobs

## Hardening And Rollout
- [ ] Task 40: Add Cost, Quality, And Prompt-Version Controls
- [ ] Task 41: Switch Read Paths To The Canonical And Retrieval Model
- [ ] Task 42: Freeze Or Archive Legacy Structures

---

## Practical Reading Of This Plan
If you want the shortest path to value, think about the work in this order:

1. Finish the candidate retrieval corpus so the current pool is searchable.
2. Add job creation and reference-candidate attachment.
3. Add JD retrieval, reference-candidate retrieval, and reranking.
4. Expose the flow through API endpoints.
5. Only then deepen feedback loops, UI, automation, and hardening.

This keeps the implementation focused on proving the core matching hypothesis first.
