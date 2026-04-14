# `db/validation/retrieval/`

Use this directory for SQL that validates the retrieval layer after retrieval tables and rebuild jobs land.

Expected check types:
- row-count reconciliation
- orphan detection
- duplicate detection
- coverage checks for source documents, chunks, and embeddings

Naming convention:
- `NN_check_name.sql`
- keep numeric prefixes in execution order
- keep filenames short and specific, for example `04_coverage_checks.sql`

Outputs from these checks typically belong in `reports/qa/` or `reports/retrieval_eval/`, depending on whether the run is structural QA or retrieval evaluation.
