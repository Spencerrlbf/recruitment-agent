-- Candidate retrieval Layer B PK/FK and trigger checks.
-- Zero rows returned from each query means the check passed.

with expected_constraints(constraint_name, table_name, constraint_type, referenced_table, delete_rule) as (
    values
        ('candidate_source_documents_pkey', 'candidate_source_documents', 'PRIMARY KEY', null, null),
        ('candidate_source_documents_candidate_id_fkey', 'candidate_source_documents', 'FOREIGN KEY', 'candidate_profiles_v2', 'CASCADE'),
        ('candidate_search_chunks_pkey', 'candidate_search_chunks', 'PRIMARY KEY', null, null),
        ('candidate_search_chunks_candidate_id_fkey', 'candidate_search_chunks', 'FOREIGN KEY', 'candidate_profiles_v2', 'CASCADE'),
        ('candidate_search_chunks_source_document_id_fkey', 'candidate_search_chunks', 'FOREIGN KEY', 'candidate_source_documents', 'CASCADE'),
        ('candidate_chunk_embeddings_pkey', 'candidate_chunk_embeddings', 'PRIMARY KEY', null, null),
        ('candidate_chunk_embeddings_candidate_id_fkey', 'candidate_chunk_embeddings', 'FOREIGN KEY', 'candidate_profiles_v2', 'CASCADE'),
        ('candidate_chunk_embeddings_chunk_id_fkey', 'candidate_chunk_embeddings', 'FOREIGN KEY', 'candidate_search_chunks', 'CASCADE'),
        ('candidate_search_documents_pkey', 'candidate_search_documents', 'PRIMARY KEY', null, null),
        ('candidate_search_documents_candidate_id_fkey', 'candidate_search_documents', 'FOREIGN KEY', 'candidate_profiles_v2', 'CASCADE'),
        ('candidate_search_documents_current_company_id_fkey', 'candidate_search_documents', 'FOREIGN KEY', 'companies_v2', 'SET NULL')
),
actual_constraints as (
    select
        pg_constraint.conname as constraint_name,
        source_table.relname as table_name,
        case pg_constraint.contype
            when 'p' then 'PRIMARY KEY'
            when 'f' then 'FOREIGN KEY'
            when 'u' then 'UNIQUE'
        end as constraint_type,
        target_table.relname as referenced_table,
        case pg_constraint.confdeltype
            when 'a' then 'NO ACTION'
            when 'c' then 'CASCADE'
            when 'd' then 'SET DEFAULT'
            when 'n' then 'SET NULL'
            when 'r' then 'RESTRICT'
        end as delete_rule
    from pg_constraint
    join pg_class as source_table on source_table.oid = pg_constraint.conrelid
    join pg_namespace on pg_namespace.oid = source_table.relnamespace
    left join pg_class as target_table on target_table.oid = pg_constraint.confrelid
    where pg_namespace.nspname = 'public'
)
select
    expected_constraints.constraint_name,
    expected_constraints.table_name,
    expected_constraints.constraint_type,
    expected_constraints.referenced_table,
    expected_constraints.delete_rule,
    actual_constraints.table_name as actual_table_name,
    actual_constraints.constraint_type as actual_constraint_type,
    actual_constraints.referenced_table as actual_referenced_table,
    actual_constraints.delete_rule as actual_delete_rule
from expected_constraints
left join actual_constraints using (constraint_name)
where actual_constraints.constraint_name is null
   or actual_constraints.table_name <> expected_constraints.table_name
   or actual_constraints.constraint_type <> expected_constraints.constraint_type
   or coalesce(actual_constraints.referenced_table, '') <> coalesce(expected_constraints.referenced_table, '')
   or (
        expected_constraints.delete_rule is not null
        and actual_constraints.delete_rule <> expected_constraints.delete_rule
   )
order by expected_constraints.table_name, expected_constraints.constraint_name;

with expected_triggers(trigger_name, table_name) as (
    values
        ('set_candidate_source_documents_updated_at', 'candidate_source_documents'),
        ('set_candidate_search_chunks_updated_at', 'candidate_search_chunks'),
        ('set_candidate_search_documents_updated_at', 'candidate_search_documents')
)
select
    expected_triggers.trigger_name as missing_trigger,
    expected_triggers.table_name
from expected_triggers
left join pg_trigger on pg_trigger.tgname = expected_triggers.trigger_name
left join pg_class on pg_class.oid = pg_trigger.tgrelid
left join pg_namespace on pg_namespace.oid = pg_class.relnamespace
where pg_trigger.oid is null
   or pg_trigger.tgisinternal
   or pg_class.relname <> expected_triggers.table_name
   or pg_namespace.nspname <> 'public'
order by expected_triggers.table_name;
