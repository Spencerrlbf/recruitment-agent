# `db/validation/retrieval/`

Use this directory for SQL that validates the retrieval layer after candidate retrieval tables land.

Current checks:
- schema object existence, including required vector support
- PK/FK and trigger checks
- required uniqueness checks
- required non-vector and ANN index checks
- rollback smoke test coverage for multi-document, multi-chunk, and multi-embedding candidate evidence

Later data-quality checks can add:
- row-count reconciliation
- orphan detection
- duplicate detection
- coverage checks for source documents, chunks, and embeddings

Naming convention:
- `NN_check_name.sql`
- keep numeric prefixes in execution order
- keep filenames short and specific, for example `04_index_checks.sql`

Outputs from these checks typically belong in `reports/qa/` or `reports/retrieval_eval/`, depending on whether the run is structural QA or retrieval evaluation.
