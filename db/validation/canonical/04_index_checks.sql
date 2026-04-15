-- Canonical Layer A non-unique index checks.
-- Zero rows returned means the check passed.

with expected_indexes(index_name, table_name, column_names) as (
    values
        (
            'candidate_profiles_v2_current_company_id_idx',
            'candidate_profiles_v2',
            array['current_company_id']::text[]
        ),
        (
            'candidate_profiles_v2_updated_at_idx',
            'candidate_profiles_v2',
            array['updated_at']::text[]
        ),
        (
            'candidate_emails_v2_candidate_id_idx',
            'candidate_emails_v2',
            array['candidate_id']::text[]
        ),
        (
            'companies_v2_normalized_name_idx',
            'companies_v2',
            array['normalized_name']::text[]
        ),
        (
            'candidate_experiences_v2_candidate_id_experience_index_idx',
            'candidate_experiences_v2',
            array['candidate_id', 'experience_index']::text[]
        ),
        (
            'candidate_experiences_v2_company_id_idx',
            'candidate_experiences_v2',
            array['company_id']::text[]
        ),
        (
            'candidate_experiences_v2_candidate_id_is_current_idx',
            'candidate_experiences_v2',
            array['candidate_id', 'is_current']::text[]
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
