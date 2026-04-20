-- Candidate source-document decision helper checks.
-- Zero rows returned means the check passed.

with expected_functions(function_name, identity_args) as (
    values
        ('build_candidate_source_document_identity_key', 'source_type text, source_subtype text, source_url text, external_source_ref text, metadata_json jsonb'),
        ('build_candidate_source_document_content_hash', 'source_type text, title text, normalized_text text, metadata_json jsonb'),
        ('decide_candidate_source_document_action', 'incoming_candidate_id uuid, incoming_source_type text, incoming_source_subtype text, incoming_title text, incoming_source_url text, incoming_external_source_ref text, incoming_normalized_text text, incoming_metadata_json jsonb')
),
actual_functions as (
    select
        pg_proc.proname as function_name,
        pg_get_function_identity_arguments(pg_proc.oid) as identity_args
    from pg_proc
    join pg_namespace on pg_namespace.oid = pg_proc.pronamespace
    where pg_namespace.nspname = 'public'
)
select
    expected_functions.function_name,
    expected_functions.identity_args,
    actual_functions.identity_args as actual_identity_args
from expected_functions
left join actual_functions
    on actual_functions.function_name = expected_functions.function_name
   and actual_functions.identity_args = expected_functions.identity_args
where actual_functions.function_name is null
order by expected_functions.function_name;
