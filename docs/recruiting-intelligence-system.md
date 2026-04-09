# Recruiting Intelligence System Spec

## Goal
Build an AI-powered recruiting system that matches job descriptions to the best candidates in a large candidate database and improves with recruiter feedback over time.

The system should support:
- semantic retrieval from a 100,000+ candidate pool
- recruiter-guided reranking through confirmed strong candidates
- persistent rejection memory per job
- async enrichment, drafting, and hygiene workflows

## Core Stack
- FastAPI backend
- Supabase Postgres
- pgvector for vector similarity search
- Celery + Redis for async processing
- `text-embedding-3-small` for embeddings
- `gpt-4o-mini` for summaries, drafts, briefs, and transcript processing

## Primary Data Model

### Canonical candidate data
Used to normalize legacy recruiting data and support retrieval.

- `candidate_profiles_v2`
- `candidate_emails_v2`
- `companies_v2`
- `candidate_experiences_v2`
- `candidate_search_documents`

### Matching system data
Used to run and persist JD matching loops.

#### `jobs`
Stores uploaded job descriptions and their embeddings.

Suggested fields:
- `id`
- `title`
- `description`
- `raw_text`
- `embedding`
- `status`
- `created_at`

#### `job_candidates`
Stores the candidate pool and state for each job run.

Suggested fields:
- `id`
- `job_id`
- `candidate_id`
- `jd_similarity_score`
- `candidate_similarity_score`
- `composite_score`
- `keyword_level_passed`
- `status`
- `created_at`

#### `seeds`
Stores recruiter-confirmed strong candidates for a job.

Suggested fields:
- `id`
- `job_id`
- `candidate_id`
- `created_at`

#### `rejected_embeddings`
Stores rejection memory for each job.

Suggested fields:
- `id`
- `job_id`
- `candidate_id`
- `embedding`
- `created_at`

## Embedding Behavior

### Candidate insert
When a new candidate is inserted:
- create or update the candidate search document
- generate an embedding automatically
- store the vector without requiring a manual trigger

### JD upload
When a recruiter uploads a job description:
- generate an embedding immediately
- store it on the `jobs` row
- mark the job ready for matching

## Matching Loop

### Pass 1: JD semantic sweep
Triggered when the recruiter starts a search run.

Flow:
1. Take the JD embedding.
2. Search the candidate embedding space with cosine similarity.
3. Pull the top candidate set above a configurable threshold.
4. Apply a staged keyword funnel.
5. Filter or deprioritize candidates that are too close to rejected candidates for the same job.
6. Surface the top ranked candidates with a keyword-level badge.

Keyword funnel behavior:
- Level 1: all required keywords must match
- Level 2: any of the main keywords may match
- Later levels: progressively broader rules
- The system opens the funnel only when the result set is too thin

### Pass 2: recruiter signal
The recruiter can mark surfaced candidates as:
- `strong`
- `rejected`
- `skipped`

Effects:
- `strong` creates a seed and triggers seed-based expansion
- `rejected` stores rejection memory for the job
- `skipped` preserves the candidate without changing the model state

### Pass 3: candidate-seed search
Triggered automatically after a recruiter marks a candidate as strong.

Flow:
1. Take the embedding of the strong candidate.
2. Search the full candidate pool again using candidate similarity, not JD similarity.
3. Apply the same keyword funnel.
4. Apply the same rejection filter.
5. Merge and deduplicate with the existing pool.

### Pass 4: ensemble reranking
Every candidate in the merged pool gets:
- JD similarity score
- seed similarity score
- composite score

Dynamic weighting:
- 0 seeds: 100 percent JD
- 1 seed: 60 percent JD / 40 percent seed
- 2 to 3 seeds: 40 percent JD / 60 percent seed
- 4 or more seeds: 25 percent JD / 75 percent seed

The JD signal must remain present to prevent drift.

### Pass 5: bounded iteration
Each new strong candidate becomes another seed.

The loop continues until:
- 3 iterations have completed, or
- no new candidates clear the threshold

## Additional Async Automations

### Candidate enrichment
On candidate insert:
- enrich company data
- enrich public profile data
- verify contact data where applicable

### Job change alerts
Periodically check tracked candidates for title or company changes and notify recruiters.

### Outreach drafting
Generate personalized outreach drafts using candidate context plus JD context. Support scheduled follow-up with stop conditions.

### Candidate briefs
Generate a clean client-ready candidate summary on demand.

### Transcript processing
Convert interview or call transcripts into structured candidate updates.

### Pipeline hygiene
Detect stale records, inactivity, and duplicates. Trigger recruiter nudges when needed.

### Weekly digest
Summarize:
- candidate momentum
- thin pipelines
- stale opportunities

## Recruiter UI

### Left panel
JD viewer and run controls.

### Center panel
Ranked candidate feed with:
- composite score
- keyword level
- action buttons for strong, reject, and skip

### Right panel
Candidate detail view with:
- full profile
- resume summary
- JD similarity
- seed similarity
- enrichment data

### Top control strip
Controls for:
- semantic threshold
- current keyword level
- manual funnel widening

## Delivery Guidance
This repo should deliver the system in phases:
1. canonical data and search documents
2. embeddings and retrieval primitives
3. job and match-state schema
4. multi-pass matching engine
5. recruiter UI
6. automations and operational tooling

## Success Criteria
- Candidate search quality improves after recruiter feedback.
- Every surfaced candidate can be explained by stored scores and actions.
- Async jobs are safe to retry.
- Search and ranking work off normalized candidate data rather than legacy blobs.
