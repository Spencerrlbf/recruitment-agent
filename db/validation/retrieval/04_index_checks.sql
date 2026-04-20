-- Candidate retrieval Layer B non-unique and ANN index checks.
-- Zero rows returned means the check passed.

with expected_indexes(index_name, table_name, column_names) as (
    values
        (
            'candidate_source_documents_candidate_source_active_idx',
            'candidate_source_documents',
            array['candidate_id', 'source_type', 'is_active']::text[]
        ),
        (
            'candidate_source_documents_document_version_idx',
            'candidate_source_documents',
            array['document_version']::text[]
        ),
        (
            'candidate_source_documents_trust_level_idx',
            'candidate_source_documents',
            array['trust_level']::text[]
        ),
        (
            'candidate_search_chunks_candidate_source_type_idx',
            'candidate_search_chunks',
            array['candidate_id', 'source_type']::text[]
        ),
        (
            'candidate_search_chunks_candidate_chunk_type_idx',
            'candidate_search_chunks',
            array['candidate_id', 'chunk_type']::text[]
        ),
        (
            'candidate_search_chunks_source_document_searchable_idx',
            'candidate_search_chunks',
            array['source_document_id', 'is_searchable']::text[]
        ),
        (
            'candidate_chunk_embeddings_candidate_model_active_idx',
            'candidate_chunk_embeddings',
            array['candidate_id', 'model_name', 'is_active']::text[]
        ),
        (
            'candidate_chunk_embeddings_chunk_active_idx',
            'candidate_chunk_embeddings',
            array['chunk_id', 'is_active']::text[]
        ),
        (
            'candidate_search_documents_current_company_idx',
            'candidate_search_documents',
            array['current_company_id']::text[]
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
      and idx.relname <> 'candidate_chunk_embeddings_active_embedding_hnsw_idx'
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

with ann_support as (
    select
        exists (
            select 1
            from pg_extension
            where extname = 'vector'
        ) as should_exist
),
expected_ann_index as (
    select
        'candidate_chunk_embeddings_active_embedding_hnsw_idx'::text as index_name,
        'candidate_chunk_embeddings'::text as table_name,
        'hnsw'::text as access_method,
        'is_active and model_name = ''text-embedding-3-small'' and embedding_dimensions = 1536'::text as predicate_sql,
        'embedding::vector(1536)'::text as expression_sql
),
actual_ann_index as (
    select
        idx.relname as index_name,
        tbl.relname as table_name,
        pg_am.amname as access_method,
        pg_get_expr(pg_index.indpred, pg_index.indrelid) as predicate_sql,
        pg_get_expr(pg_index.indexprs, pg_index.indrelid) as expression_sql
    from pg_index
    join pg_class as idx on idx.oid = pg_index.indexrelid
    join pg_class as tbl on tbl.oid = pg_index.indrelid
    join pg_namespace on pg_namespace.oid = tbl.relnamespace
    join pg_am on pg_am.oid = idx.relam
    where pg_namespace.nspname = 'public'
      and idx.relname = 'candidate_chunk_embeddings_active_embedding_hnsw_idx'
)
select
    expected_ann_index.index_name,
    expected_ann_index.table_name,
    expected_ann_index.access_method,
    expected_ann_index.predicate_sql,
    expected_ann_index.expression_sql,
    actual_ann_index.table_name as actual_table_name,
    actual_ann_index.access_method as actual_access_method,
    actual_ann_index.predicate_sql as actual_predicate_sql,
    actual_ann_index.expression_sql as actual_expression_sql
from expected_ann_index
cross join ann_support
left join actual_ann_index using (index_name)
where (
        ann_support.should_exist
        and (
            actual_ann_index.index_name is null
            or actual_ann_index.table_name <> expected_ann_index.table_name
            or actual_ann_index.access_method <> expected_ann_index.access_method
            or regexp_replace(lower(coalesce(actual_ann_index.expression_sql, '')), '[\\s()]', '', 'g')
                <> regexp_replace(lower(expected_ann_index.expression_sql), '[\\s()]', '', 'g')
            or regexp_replace(
                    regexp_replace(lower(coalesce(actual_ann_index.predicate_sql, '')), '::text', '', 'g'),
                    '[\\s()]',
                    '',
                    'g'
               ) <> regexp_replace(lower(expected_ann_index.predicate_sql), '[\\s()]', '', 'g')
        )
    )
   or (
        not ann_support.should_exist
        and actual_ann_index.index_name is not null
   );

with expected_ann_indexes(index_name) as (
    values
        ('candidate_chunk_embeddings_active_embedding_hnsw_idx')
),
actual_ann_indexes as (
    select
        idx.relname as index_name,
        tbl.relname as table_name,
        pg_am.amname as access_method,
        pg_get_indexdef(idx.oid) as index_definition
    from pg_index
    join pg_class as idx on idx.oid = pg_index.indexrelid
    join pg_class as tbl on tbl.oid = pg_index.indrelid
    join pg_namespace on pg_namespace.oid = tbl.relnamespace
    join pg_am on pg_am.oid = idx.relam
    where pg_namespace.nspname = 'public'
      and tbl.relname in (
            'candidate_source_documents',
            'candidate_search_chunks',
            'candidate_chunk_embeddings',
            'candidate_search_documents'
      )
      and pg_am.amname in ('hnsw', 'ivfflat')
)
select
    actual_ann_indexes.index_name as unexpected_ann_index,
    actual_ann_indexes.table_name,
    actual_ann_indexes.access_method,
    actual_ann_indexes.index_definition
from actual_ann_indexes
left join expected_ann_indexes using (index_name)
where expected_ann_indexes.index_name is null
order by actual_ann_indexes.table_name, actual_ann_indexes.index_name;
