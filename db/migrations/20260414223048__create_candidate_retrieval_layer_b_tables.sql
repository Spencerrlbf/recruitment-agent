begin;

set local search_path = public, extensions;

create schema if not exists extensions;

create extension if not exists vector with schema extensions;

create table public.candidate_source_documents (
    id uuid not null default uuid_generate_v4(),
    candidate_id uuid not null,
    source_type text not null,
    source_subtype text,
    title text,
    source_url text,
    external_source_ref text,
    raw_payload jsonb,
    raw_text text,
    normalized_text text,
    metadata_json jsonb,
    trust_level text not null,
    document_version integer not null default 1,
    is_active boolean not null default true,
    effective_at timestamptz,
    superseded_at timestamptz,
    ingested_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_source_documents_pkey primary key (id),
    constraint candidate_source_documents_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade
);

create table public.candidate_search_chunks (
    id uuid not null default uuid_generate_v4(),
    candidate_id uuid not null,
    source_document_id uuid not null,
    source_type text not null,
    chunk_type text not null,
    section_key text,
    chunk_index integer not null,
    chunk_text text not null,
    token_count_estimate integer,
    char_count integer,
    source_priority integer not null,
    trust_level text not null,
    document_version integer not null,
    is_searchable boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_search_chunks_pkey primary key (id),
    constraint candidate_search_chunks_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade,
    constraint candidate_search_chunks_source_document_id_fkey
        foreign key (source_document_id)
        references public.candidate_source_documents (id)
        on delete cascade,
    constraint candidate_search_chunks_source_document_chunk_index_key
        unique (source_document_id, chunk_index)
);

create table public.candidate_chunk_embeddings (
    id uuid not null default uuid_generate_v4(),
    candidate_id uuid not null,
    chunk_id uuid not null,
    model_name text not null,
    model_version text,
    embedding_dimensions smallint not null,
    embedding vector not null,
    is_active boolean not null default true,
    generated_at timestamptz not null,
    created_at timestamptz not null default now(),
    constraint candidate_chunk_embeddings_pkey primary key (id),
    constraint candidate_chunk_embeddings_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade,
    constraint candidate_chunk_embeddings_chunk_id_fkey
        foreign key (chunk_id)
        references public.candidate_search_chunks (id)
        on delete cascade,
    constraint candidate_chunk_embeddings_chunk_model_version_key
        unique (chunk_id, model_name, model_version)
);

create table public.candidate_search_documents (
    candidate_id uuid not null,
    search_text text not null,
    current_title text,
    current_company_id uuid,
    current_company_name text,
    location text,
    experience_years numeric(5, 2),
    education_schools text[],
    education_degrees text[],
    skills text[],
    prior_company_ids uuid[],
    prior_company_names text[],
    summary_source_types text[],
    document_version integer not null default 1,
    rebuilt_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_search_documents_pkey primary key (candidate_id),
    constraint candidate_search_documents_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade,
    constraint candidate_search_documents_current_company_id_fkey
        foreign key (current_company_id)
        references public.companies_v2 (id)
        on delete set null
);

create index candidate_source_documents_candidate_source_active_idx
    on public.candidate_source_documents (candidate_id, source_type, is_active);

create index candidate_source_documents_document_version_idx
    on public.candidate_source_documents (document_version);

create index candidate_source_documents_trust_level_idx
    on public.candidate_source_documents (trust_level);

create index candidate_search_chunks_candidate_source_type_idx
    on public.candidate_search_chunks (candidate_id, source_type);

create index candidate_search_chunks_candidate_chunk_type_idx
    on public.candidate_search_chunks (candidate_id, chunk_type);

create index candidate_search_chunks_source_document_searchable_idx
    on public.candidate_search_chunks (source_document_id, is_searchable);

create index candidate_chunk_embeddings_candidate_model_active_idx
    on public.candidate_chunk_embeddings (candidate_id, model_name, is_active);

create index candidate_chunk_embeddings_chunk_active_idx
    on public.candidate_chunk_embeddings (chunk_id, is_active);

create index candidate_chunk_embeddings_active_embedding_hnsw_idx
    on public.candidate_chunk_embeddings
    using hnsw ((embedding::vector(1536)) vector_cosine_ops)
    where is_active
      and model_name = 'text-embedding-3-small'
      and embedding_dimensions = 1536;

create index candidate_search_documents_current_company_idx
    on public.candidate_search_documents (current_company_id);

create trigger set_candidate_source_documents_updated_at
before update on public.candidate_source_documents
for each row
execute function public.set_updated_at();

create trigger set_candidate_search_chunks_updated_at
before update on public.candidate_search_chunks
for each row
execute function public.set_updated_at();

create trigger set_candidate_search_documents_updated_at
before update on public.candidate_search_documents
for each row
execute function public.set_updated_at();

commit;
