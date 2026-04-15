-- Canonical Layer A object existence checks.
-- Zero rows returned from each query means the check passed.

with expected_tables(table_name) as (
    values
        ('candidate_profiles_v2'),
        ('candidate_emails_v2'),
        ('companies_v2'),
        ('candidate_experiences_v2')
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
        ('citext'),
        ('uuid-ossp')
)
select
    expected_extensions.extension_name as missing_extension
from expected_extensions
left join pg_extension on pg_extension.extname = expected_extensions.extension_name
where pg_extension.oid is null
order by expected_extensions.extension_name;

select
    'public.set_updated_at()' as missing_helper
where not exists (
    select 1
    from pg_proc
    join pg_namespace on pg_namespace.oid = pg_proc.pronamespace
    where pg_namespace.nspname = 'public'
      and pg_proc.proname = 'set_updated_at'
      and pg_get_function_identity_arguments(pg_proc.oid) = ''
);
