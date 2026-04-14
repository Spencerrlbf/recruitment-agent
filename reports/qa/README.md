# `reports/qa/`

Use this directory for generated QA artifacts from migration and validation runs.

Typical contents:
- row-count reconciliation summaries
- duplicate and orphan findings
- spot-check outputs
- markdown, JSON, or CSV summaries for handoff and review

Naming convention:
- `YYYYMMDD__qa_<scope>.<ext>`
- examples: `20260414__qa_candidate_profiles.md`, `20260414__qa_retrieval_counts.csv`

Generated files in this directory are ignored by git. Keep only the scaffold and documentation tracked.
