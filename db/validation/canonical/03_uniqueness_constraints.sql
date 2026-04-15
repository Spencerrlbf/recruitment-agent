-- Canonical Layer A uniqueness checks.
-- Zero rows returned from each query means the check passed.

with expected_unique_constraints(constraint_name, table_name, constraint_definition) as (
    values
        (
            'candidate_emails_v2_candidate_id_email_normalized_key',
            'candidate_emails_v2',
            'UNIQUE (candidate_id, email_normalized)'
        ),
        (
            'candidate_experiences_v2_candidate_id_source_hash_key',
            'candidate_experiences_v2',
            'UNIQUE (candidate_id, source_hash)'
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

with expected_partial_unique_indexes(index_name, table_name, column_names, predicate_sql) as (
    values
        (
            'candidate_profiles_v2_linkedin_username_uq',
            'candidate_profiles_v2',
            array['linkedin_username']::text[],
            'linkedin_username is not null'
        ),
        (
            'candidate_profiles_v2_linkedin_url_normalized_uq',
            'candidate_profiles_v2',
            array['linkedin_url_normalized']::text[],
            'linkedin_url_normalized is not null'
        ),
        (
            'candidate_emails_v2_email_normalized_uq',
            'candidate_emails_v2',
            array['email_normalized']::text[],
            'email_normalized is not null'
        ),
        (
            'candidate_emails_v2_one_primary_email_uq',
            'candidate_emails_v2',
            array['candidate_id']::text[],
            'is_primary'
        ),
        (
            'companies_v2_linkedin_id_uq',
            'companies_v2',
            array['linkedin_id']::text[],
            'linkedin_id is not null'
        ),
        (
            'companies_v2_linkedin_username_uq',
            'companies_v2',
            array['linkedin_username']::text[],
            'linkedin_username is not null'
        ),
        (
            'companies_v2_linkedin_url_normalized_uq',
            'companies_v2',
            array['linkedin_url_normalized']::text[],
            'linkedin_url_normalized is not null'
        )
),
actual_partial_unique_indexes as (
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
      and pg_index.indpred is not null
    group by idx.relname, tbl.relname, pg_index.indisunique, pg_index.indpred, pg_index.indrelid
)
select
    expected_partial_unique_indexes.index_name,
    expected_partial_unique_indexes.table_name,
    expected_partial_unique_indexes.column_names,
    expected_partial_unique_indexes.predicate_sql,
    actual_partial_unique_indexes.table_name as actual_table_name,
    actual_partial_unique_indexes.column_names as actual_column_names,
    actual_partial_unique_indexes.predicate_sql as actual_predicate_sql
from expected_partial_unique_indexes
left join actual_partial_unique_indexes using (index_name)
where actual_partial_unique_indexes.index_name is null
   or actual_partial_unique_indexes.table_name <> expected_partial_unique_indexes.table_name
   or actual_partial_unique_indexes.is_unique is distinct from true
   or actual_partial_unique_indexes.column_names <> expected_partial_unique_indexes.column_names
   or regexp_replace(lower(actual_partial_unique_indexes.predicate_sql), '[\s()]', '', 'g')
        <> regexp_replace(lower(expected_partial_unique_indexes.predicate_sql), '[\s()]', '', 'g')
order by expected_partial_unique_indexes.table_name, expected_partial_unique_indexes.index_name;
