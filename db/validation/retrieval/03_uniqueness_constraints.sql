-- Candidate retrieval Layer B uniqueness checks.
-- Zero rows returned from each query means the check passed.

with expected_unique_constraints(constraint_name, table_name, constraint_definition) as (
    values
        (
            'candidate_search_chunks_source_document_chunk_index_key',
            'candidate_search_chunks',
            'UNIQUE (source_document_id, chunk_index)'
        ),
        (
            'candidate_chunk_embeddings_chunk_model_version_key',
            'candidate_chunk_embeddings',
            'UNIQUE (chunk_id, model_name, model_version)'
        )
),
actual_unique_constraints as (
    select
        pg_constraint.conname as constraint_name,
        pg_class.relname as table_name,
        pg_get_constraintdef(pg_constraint.oid) as constraint_definition
    from pg_constraint
    join pg_class on pg_class.oid = pg_constraint.conrelid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where pg_namespace.nspname = 'public'
      and pg_constraint.contype = 'u'
)
select
    expected_unique_constraints.constraint_name,
    expected_unique_constraints.table_name,
    expected_unique_constraints.constraint_definition,
    actual_unique_constraints.constraint_definition as actual_constraint_definition
from expected_unique_constraints
left join actual_unique_constraints using (constraint_name)
where actual_unique_constraints.constraint_name is null
   or actual_unique_constraints.table_name <> expected_unique_constraints.table_name
   or actual_unique_constraints.constraint_definition <> expected_unique_constraints.constraint_definition
order by expected_unique_constraints.table_name, expected_unique_constraints.constraint_name;
