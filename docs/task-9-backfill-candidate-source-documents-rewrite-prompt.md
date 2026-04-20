# Task 9 Rewrite Prompt

**Task to improve:** `Task 9: Backfill Candidate Source Documents`

I want you to do for this task what we did for Task 6.

## Objective
Review the current task definition in `TASKS.md` and rewrite it so it is implementation-ready before any code is written or run.

## Rules
- Treat `SCHEMA_CONTRACT.md` as authoritative for schema.
- Treat `TASKS.md` as authoritative for sequencing.
- Do **not** implement the task yet.
- Do **not** run any migration/backfill yet.
- This step is only to improve the task definition and split it into execution phases.

## What I want added
Split the task into three separate subtasks so they can be requested and approved independently:

- `Task 9a: Implement Candidate Source Document Backfill Script And Preflight Validation`
- `Task 9b: Run 100-Row Pilot Candidate Source Document Backfill And Review Results`
- `Task 9c: Run Full Candidate Source Document Backfill`

## For the rewritten task, include all of the following

### 1. Scope
Make the scope explicit:
- exact legacy source table(s)
- exact destination table(s)
- what is **in scope**
- what is **out of scope**
- which source families are included in this task (LinkedIn, resume, recruiter notes, transcript summaries, future artifacts)
- whether this task creates only source documents or also any related aggregate/search rows

### 2. Destination schema contract
List the destination columns actually touched by this backfill, including:
- `candidate_source_documents` columns touched
- column name
- type
- nullability expectation where relevant
- provenance / traceability fields
- document identity/version fields
- source type, subtype, trust/confidence, timestamps, and active-version semantics

### 3. Field mapping
Add explicit field-by-field mapping from legacy source columns to destination columns:
- source field -> destination field
- transformation/normalization rules
- when to leave a field null
- how LinkedIn, resume, recruiter-note, and transcript inputs differ
- how raw text, normalized text, metadata JSON, source URLs, titles, and external references are mapped
- how document versions are handled

### 4. Identity / matching / duplication rules
Make the dedupe and matching contract explicit:
- exact document identity-key rules
- content-hash rules
- when an incoming document is `no_op`, `parallel`, `supersede`, or `ambiguous`
- how active vs inactive document versions are handled
- how reruns avoid creating duplicate source documents
- what counts as ambiguous
- how ambiguity must be logged
- rule that reruns must not create duplicate source documents or duplicate provenance entries

### 5. Normalization rules
Spell out the normalization rules relevant to the task, for example:
- trim whitespace
- blank string -> `null`
- URL cleanup
- normalized text cleanup
- metadata normalization
- source-type normalization
- timestamp/version normalization
- provenance normalization

### 6. Script requirements
Make it explicit that this task requires a dedicated backfill script under `scripts/backfills/` and that the script must be:
- deterministic
- idempotent
- resumable
- checkpoint-aware
- batch-oriented
- safe to rerun

Also include:
- stable cursor/order
- checkpoint advances only after durable batch success
- `--dry-run`
- clear QA report output under `reports/qa/`

### 7. Performance requirements
Add performance guidance appropriate for large data volume:
- avoid row-by-row chatty writes where possible
- prefer set-based or batched writes
- use indexed lookups for document identity checks
- state the intended batch strategy
- explicitly call out any likely expensive text or metadata normalization path

### 8. Required preflight validation for `Task 9a`
Require:
- deterministic `--dry-run` on first 100 rows in real script order
- duplicate-validation fixtures in a safe dev/sandbox path where relevant
- QA report with counts and sample outcomes
- no committed writes in the dry run
- explicit checks for `no_op`, `parallel`, `supersede`, and ambiguous document cases

### 9. Required pilot validation for `Task 9b`
Require:
- committed 100-row pilot write
- pilot rows may remain in place for final migration
- direct DB review before full migration
- confirmation that full migration can continue safely from pilot state without duplication
- confirmation that active/inactive document version behavior is correct in the real DB

### 10. Required final run rules for `Task 9c`
Require:
- full migration only after approval of `Task 9b`
- continue from pilot/checkpoint state
- preserve all mapping, normalization, dedupe, ambiguity, versioning, and provenance rules validated earlier
- emit final QA report

### 11. Acceptance criteria
Replace vague “done when” language with concrete pass criteria for each subtask:
- script exists
- dry run completed and reviewed
- pilot completed and reviewed
- duplicate handling confirmed
- required fields populated
- ambiguity behavior confirmed
- rerun/idempotency behavior confirmed
- versioning behavior confirmed
- full run blocked until approval

## Output format
Give me:

1. **Short review of what is missing in the current task**
2. **Recommended subtask split**
3. **Exact replacement markdown for `TASKS.md`**
4. **Any matching update needed in `MVP_PLAN.md`**
5. **Do not implement yet**

## Important
Use the same rigor we used for Task 6:
- explicit mapping
- explicit dedupe rules
- explicit dry-run requirement
- explicit pilot-write requirement
- explicit approval gate
- explicit QA outputs
- explicit resumability/idempotency expectations

Derive the task-specific source-document identity, versioning, and duplication rules from the current schema/docs instead of leaving them vague.
