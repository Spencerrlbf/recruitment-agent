-- Candidate retrieval Layer B object existence checks.
-- Zero rows returned from each query means the check passed.

with expected_tables(table_name) as (
    values
        ('candidate_source_documents'),
        ('candidate_search_chunks'),
        ('candidate_chunk_embeddings'),
        ('candidate_search_documents')
)
select
    expected_tables.table_name as missing_table
from expected_tables
left join pg_class on pg_class.relname = expected_tables.table_name
left join pg_namespace on pg_namespace.oid = pg_class.relnamespace
where pg_class.relkind <> 'r'
   or pg_namespace.nspname <> 'public'
   or pg_class.oid is null
order by expected_tables.table_name;

with expected_extensions(extension_name) as (
    values
        ('vector')
)
select
    expected_extensions.extension_name as missing_extension
from expected_extensions
left join pg_extension on pg_extension.extname = expected_extensions.extension_name
where pg_extension.oid is null
order by expected_extensions.extension_name;

with expected_vector_columns(table_name, column_name, udt_name) as (
    values
        ('candidate_chunk_embeddings', 'embedding', 'vector')
)
select
    expected_vector_columns.table_name,
    expected_vector_columns.column_name,
    expected_vector_columns.udt_name,
    information_schema.columns.udt_name as actual_udt_name
from expected_vector_columns
left join information_schema.columns
    on information_schema.columns.table_schema = 'public'
   and information_schema.columns.table_name = expected_vector_columns.table_name
   and information_schema.columns.column_name = expected_vector_columns.column_name
where information_schema.columns.column_name is null
   or information_schema.columns.udt_name <> expected_vector_columns.udt_name;

select
    information_schema.columns.table_name,
    information_schema.columns.column_name,
    information_schema.columns.udt_name as unexpected_vector_type
from information_schema.columns
where information_schema.columns.table_schema = 'public'
  and information_schema.columns.udt_name = 'vector'
  and information_schema.columns.table_name <> 'candidate_chunk_embeddings'
  and information_schema.columns.table_name in (
        'candidate_profiles_v2',
        'candidate_emails_v2',
        'companies_v2',
        'candidate_experiences_v2',
        'candidate_source_documents',
        'candidate_search_chunks',
        'candidate_search_documents'
  )
order by information_schema.columns.table_name, information_schema.columns.column_name;
