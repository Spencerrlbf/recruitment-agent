-- Canonicalization support-object and helper checks.
-- Zero rows returned means the check passed.

with expected_columns(table_name, column_name, data_type, is_not_null) as (
    values
        ('companies_v2', 'source_record_refs', 'jsonb', false),
        ('canonicalization_ambiguities', 'id', 'uuid', true),
        ('canonicalization_ambiguities', 'entity_type', 'text', true),
        ('canonicalization_ambiguities', 'ambiguity_type', 'text', true),
        ('canonicalization_ambiguities', 'source_system', 'text', false),
        ('canonicalization_ambiguities', 'source_record_ref', 'text', false),
        ('canonicalization_ambiguities', 'normalized_input', 'jsonb', true),
        ('canonicalization_ambiguities', 'matched_record_ids', 'uuid[]', false),
        ('canonicalization_ambiguities', 'recommended_action', 'text', true),
        ('canonicalization_ambiguities', 'status', 'text', true),
        ('canonicalization_ambiguities', 'resolution_notes', 'text', false),
        ('canonicalization_ambiguities', 'created_at', 'timestamp with time zone', true),
        ('canonicalization_ambiguities', 'updated_at', 'timestamp with time zone', true),
        ('canonicalization_ambiguities', 'resolved_at', 'timestamp with time zone', false)
),
actual_columns as (
    select
        pg_class.relname as table_name,
        pg_attribute.attname as column_name,
        format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as data_type,
        pg_attribute.attnotnull as is_not_null
    from pg_attribute
    join pg_class on pg_class.oid = pg_attribute.attrelid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where pg_namespace.nspname = 'public'
      and pg_attribute.attnum > 0
      and not pg_attribute.attisdropped
)
select
    expected_columns.table_name,
    expected_columns.column_name,
    expected_columns.data_type,
    expected_columns.is_not_null,
    actual_columns.data_type as actual_data_type,
    actual_columns.is_not_null as actual_is_not_null
from expected_columns
left join actual_columns
    on actual_columns.table_name = expected_columns.table_name
   and actual_columns.column_name = expected_columns.column_name
where actual_columns.column_name is null
   or actual_columns.data_type <> expected_columns.data_type
   or actual_columns.is_not_null <> expected_columns.is_not_null
order by expected_columns.table_name, expected_columns.column_name;

with expected_checks(constraint_name, table_name) as (
    values
        ('canonicalization_ambiguities_entity_type_ck', 'canonicalization_ambiguities'),
        ('canonicalization_ambiguities_recommended_action_ck', 'canonicalization_ambiguities'),
        ('canonicalization_ambiguities_status_ck', 'canonicalization_ambiguities')
),
actual_checks as (
    select
        pg_constraint.conname as constraint_name,
        pg_class.relname as table_name
    from pg_constraint
    join pg_class on pg_class.oid = pg_constraint.conrelid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where pg_namespace.nspname = 'public'
      and pg_constraint.contype = 'c'
)
select
    expected_checks.constraint_name,
    expected_checks.table_name,
    actual_checks.table_name as actual_table_name
from expected_checks
left join actual_checks using (constraint_name)
where actual_checks.constraint_name is null
   or actual_checks.table_name <> expected_checks.table_name
order by expected_checks.table_name, expected_checks.constraint_name;

with expected_triggers(trigger_name, table_name) as (
    values
        ('set_canonicalization_ambiguities_updated_at', 'canonicalization_ambiguities')
),
actual_triggers as (
    select
        pg_trigger.tgname as trigger_name,
        pg_class.relname as table_name
    from pg_trigger
    join pg_class on pg_class.oid = pg_trigger.tgrelid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    where pg_namespace.nspname = 'public'
      and not pg_trigger.tgisinternal
)
select
    expected_triggers.trigger_name,
    expected_triggers.table_name,
    actual_triggers.table_name as actual_table_name
from expected_triggers
left join actual_triggers using (trigger_name)
where actual_triggers.trigger_name is null
   or actual_triggers.table_name <> expected_triggers.table_name;

with expected_indexes(index_name, table_name, column_names) as (
    values
        (
            'canonicalization_ambiguities_status_entity_type_idx',
            'canonicalization_ambiguities',
            array['status', 'entity_type']::text[]
        ),
        (
            'canonicalization_ambiguities_entity_ambiguity_type_idx',
            'canonicalization_ambiguities',
            array['entity_type', 'ambiguity_type']::text[]
        ),
        (
            'canonicalization_ambiguities_source_ref_idx',
            'canonicalization_ambiguities',
            array['source_system', 'source_record_ref']::text[]
        )
),
actual_indexes as (
    select
        idx.relname as index_name,
        tbl.relname as table_name,
        array_agg(att.attname::text order by ordinality.ordinality) as column_names,
        pg_index.indisunique as is_unique,
        pg_get_expr(pg_index.indpred, pg_index.indrelid) as predicate_sql
    from pg_index
    join pg_class as idx on idx.oid = pg_index.indexrelid
    join pg_class as tbl on tbl.oid = pg_index.indrelid
    join pg_namespace on pg_namespace.oid = tbl.relnamespace
    join unnest(pg_index.indkey) with ordinality as ordinality(attnum, ordinality) on true
    join pg_attribute as att
        on att.attrelid = tbl.oid
       and att.attnum = ordinality.attnum
    where pg_namespace.nspname = 'public'
    group by idx.relname, tbl.relname, pg_index.indisunique, pg_index.indpred, pg_index.indrelid
)
select
    expected_indexes.index_name,
    expected_indexes.table_name,
    expected_indexes.column_names,
    actual_indexes.table_name as actual_table_name,
    actual_indexes.column_names as actual_column_names,
    actual_indexes.is_unique as actual_is_unique,
    actual_indexes.predicate_sql as actual_predicate_sql
from expected_indexes
left join actual_indexes using (index_name)
where actual_indexes.index_name is null
   or actual_indexes.table_name <> expected_indexes.table_name
   or actual_indexes.is_unique
   or actual_indexes.predicate_sql is not null
   or actual_indexes.column_names <> expected_indexes.column_names
order by expected_indexes.table_name, expected_indexes.index_name;

with expected_partial_unique_indexes(index_name, table_name, predicate_sql) as (
    values
        (
            'canonicalization_ambiguities_open_identity_uq',
            'canonicalization_ambiguities',
            'status = ''open'''
        )
),
actual_partial_unique_indexes as (
    select
        idx.relname as index_name,
        tbl.relname as table_name,
        pg_index.indisunique as is_unique,
        pg_get_expr(pg_index.indpred, pg_index.indrelid) as predicate_sql
    from pg_index
    join pg_class as idx on idx.oid = pg_index.indexrelid
    join pg_class as tbl on tbl.oid = pg_index.indrelid
    join pg_namespace on pg_namespace.oid = tbl.relnamespace
    where pg_namespace.nspname = 'public'
      and idx.relname = 'canonicalization_ambiguities_open_identity_uq'
)
select
    expected_partial_unique_indexes.index_name,
    expected_partial_unique_indexes.table_name,
    expected_partial_unique_indexes.predicate_sql,
    actual_partial_unique_indexes.table_name as actual_table_name,
    actual_partial_unique_indexes.is_unique as actual_is_unique,
    actual_partial_unique_indexes.predicate_sql as actual_predicate_sql
from expected_partial_unique_indexes
left join actual_partial_unique_indexes using (index_name)
where actual_partial_unique_indexes.index_name is null
   or actual_partial_unique_indexes.table_name <> expected_partial_unique_indexes.table_name
   or actual_partial_unique_indexes.is_unique is distinct from true
   or regexp_replace(
        regexp_replace(lower(coalesce(actual_partial_unique_indexes.predicate_sql, '')), '::text', '', 'g'),
        '[\\s()]',
        '',
        'g'
      ) <> regexp_replace(lower(expected_partial_unique_indexes.predicate_sql), '[\\s()]', '', 'g');

with expected_functions(function_name, identity_args) as (
    values
        ('normalize_identity_text', 'raw_text text'),
        ('normalize_search_text', 'raw_text text'),
        ('normalize_identity_url', 'raw_url text'),
        ('normalize_linkedin_username', 'raw_username text'),
        ('normalize_candidate_linkedin_url', 'raw_url text'),
        ('normalize_company_linkedin_url', 'raw_url text'),
        ('extract_candidate_linkedin_username_from_url', 'raw_url text'),
        ('extract_company_linkedin_username_from_url', 'raw_url text'),
        ('normalize_email_address', 'raw_email text'),
        ('normalize_company_name', 'raw_name text'),
        ('canonical_source_precedence_rank', 'source_name text'),
        ('should_replace_canonical_value', 'existing_source text, incoming_source text, existing_value text, incoming_value text'),
        ('normalize_experience_date_precision', 'raw_precision text, is_end_date boolean, raw_is_current boolean'),
        ('normalize_experience_date', 'raw_date date, raw_precision text, is_end_date boolean, raw_is_current boolean'),
        ('normalize_experience_is_current', 'raw_is_current boolean, raw_end_date_precision text'),
        ('build_candidate_experience_source_hash', 'candidate_id uuid, title text, company_id uuid, raw_company_name text, start_date date, start_date_precision text, end_date date, end_date_precision text, is_current boolean'),
        ('resolve_candidate_profile_match', 'incoming_candidate_id uuid, incoming_linkedin_username text, incoming_linkedin_url text'),
        ('resolve_company_match', 'incoming_linkedin_id text, incoming_linkedin_username text, incoming_linkedin_url text, incoming_name text'),
        ('record_canonicalization_ambiguity', 'p_entity_type text, p_ambiguity_type text, p_source_system text, p_source_record_ref text, p_normalized_input jsonb, p_matched_record_ids uuid[], p_recommended_action text')
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
