# Task 7 Rewrite Prompt

**Task to improve:** `Task 7: Backfill Candidate Profiles And Emails`

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

- `Task 7a: Implement Candidate Profile And Email Backfill Script And Preflight Validation`
- `Task 7b: Run 100-Row Pilot Candidate Profile And Email Backfill And Review Results`
- `Task 7c: Run Full Candidate Profile And Email Backfill`

## For the rewritten task, include all of the following

### 1. Scope
Make the scope explicit:
- exact legacy source table(s)
- exact destination table(s)
- what is **in scope**
- what is **out of scope**
- whether this task is allowed to create new canonical rows, update existing rows, or only attach related rows
- whether candidate profiles and emails should remain one combined backfill or be split further into separate profile/email subtasks

### 2. Destination schema contract
List the destination columns actually touched by this backfill, including:
- `candidate_profiles_v2` columns touched
- `candidate_emails_v2` columns touched
- column name
- type
- nullability expectation where relevant
- required provenance / traceability fields
- any verification / primary-email semantics that must be preserved

### 3. Field mapping
Add explicit field-by-field mapping from legacy source columns to destination columns:
- source field -> destination field
- transformation/normalization rules
- when to leave a field null
- when to preserve existing value
- when lower-precedence values may fill blanks only
- how to map profile-level raw source references vs email-level raw source references

### 4. Identity / matching / duplication rules
Make the dedupe and matching contract explicit:
- whether `candidate_profiles_v2.id` must reuse the stable legacy candidate UUID
- email uniqueness rules and partial unique expectations
- exact email normalization rules
- how to prevent duplicate candidate email rows on rerun
- how to handle multiple legacy emails for one candidate
- how to determine primary vs secondary email if the schema supports it
- what counts as ambiguous
- when to `match_existing`
- when to `create_new`
- when to `skip`
- how ambiguity must be logged
- rule that reruns must not create duplicate canonical rows or duplicate provenance entries

### 5. Normalization rules
Spell out the normalization rules relevant to the task, for example:
- trim whitespace
- blank string -> `null`
- email normalization
- LinkedIn normalization
- candidate-name normalization if relevant
- URL cleanup if relevant
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
- use indexed lookups for matching
- state the intended batch strategy
- call out whether emails should be batched separately from profile writes

### 8. Required preflight validation for `Task 7a`
Require:
- deterministic `--dry-run` on first 100 rows in real script order
- duplicate-validation fixtures in a safe dev/sandbox path where relevant
- QA report with counts and sample outcomes
- no committed writes in the dry run
- explicit duplicate checks for profile identity and email uniqueness behavior

### 9. Required pilot validation for `Task 7b`
Require:
- committed 100-row pilot write
- pilot rows may remain in place for final migration
- direct DB review before full migration
- confirmation that full migration can continue safely from pilot state without duplication
- confirmation that candidate counts reconcile and email dedupe behavior is correct in the real DB

### 10. Required final run rules for `Task 7c`
Require:
- full migration only after approval of `Task 7b`
- continue from pilot/checkpoint state
- preserve all mapping, normalization, dedupe, ambiguity, and provenance rules validated earlier
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

Derive the task-specific profile and email rules from the current schema/docs instead of leaving them vague.
