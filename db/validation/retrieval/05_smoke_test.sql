-- Candidate retrieval Layer B smoke test.
-- This script intentionally rolls back so it can be run repeatedly.

begin;

set local search_path = public, extensions;

insert into public.candidate_profiles_v2 (
    id,
    full_name,
    source
)
values
    (
        '00000000-0000-0000-0000-000000000401',
        'Retrieval Smoke Candidate One',
        'schema_smoke_test'
    ),
    (
        '00000000-0000-0000-0000-000000000402',
        'Retrieval Smoke Candidate Two',
        'schema_smoke_test'
    );

insert into public.candidate_source_documents (
    id,
    candidate_id,
    source_type,
    title,
    raw_text,
    normalized_text,
    trust_level
)
values
    (
        '00000000-0000-0000-0000-000000000411',
        '00000000-0000-0000-0000-000000000401',
        'linkedin_profile',
        'Candidate One LinkedIn',
        'Candidate one raw linkedin text',
        'candidate one normalized linkedin text',
        'baseline'
    ),
    (
        '00000000-0000-0000-0000-000000000412',
        '00000000-0000-0000-0000-000000000402',
        'linkedin_profile',
        'Candidate Two LinkedIn',
        'Candidate two raw linkedin text',
        'candidate two normalized linkedin text',
        'baseline'
    ),
    (
        '00000000-0000-0000-0000-000000000413',
        '00000000-0000-0000-0000-000000000402',
        'resume',
        'Candidate Two Resume',
        'Candidate two raw resume text',
        'candidate two normalized resume text',
        'high'
    ),
    (
        '00000000-0000-0000-0000-000000000414',
        '00000000-0000-0000-0000-000000000402',
        'recruiter_note_raw',
        'Candidate Two Recruiter Note',
        'Candidate two recruiter note text',
        'candidate two recruiter note text',
        'supplemental'
    );

insert into public.candidate_search_chunks (
    id,
    candidate_id,
    source_document_id,
    source_type,
    chunk_type,
    chunk_index,
    chunk_text,
    source_priority,
    trust_level,
    document_version
)
values
    (
        '00000000-0000-0000-0000-000000000421',
        '00000000-0000-0000-0000-000000000401',
        '00000000-0000-0000-0000-000000000411',
        'linkedin_profile',
        'headline_about',
        0,
        'Candidate one headline and about chunk',
        10,
        'baseline',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000422',
        '00000000-0000-0000-0000-000000000401',
        '00000000-0000-0000-0000-000000000411',
        'linkedin_profile',
        'skills_block',
        1,
        'Candidate one skills chunk',
        10,
        'baseline',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000423',
        '00000000-0000-0000-0000-000000000402',
        '00000000-0000-0000-0000-000000000412',
        'linkedin_profile',
        'headline_about',
        0,
        'Candidate two headline chunk',
        10,
        'baseline',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000424',
        '00000000-0000-0000-0000-000000000402',
        '00000000-0000-0000-0000-000000000412',
        'linkedin_profile',
        'current_role',
        1,
        'Candidate two current role chunk',
        10,
        'baseline',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000425',
        '00000000-0000-0000-0000-000000000402',
        '00000000-0000-0000-0000-000000000413',
        'resume',
        'resume_summary',
        0,
        'Candidate two resume summary chunk',
        20,
        'high',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000426',
        '00000000-0000-0000-0000-000000000402',
        '00000000-0000-0000-0000-000000000413',
        'resume',
        'project_section',
        1,
        'Candidate two resume project chunk',
        20,
        'high',
        1
    ),
    (
        '00000000-0000-0000-0000-000000000427',
        '00000000-0000-0000-0000-000000000402',
        '00000000-0000-0000-0000-000000000414',
        'recruiter_note_raw',
        'note_summary',
        0,
        'Candidate two recruiter note chunk',
        5,
        'supplemental',
        1
    );

with active_embedding as (
    select ('[' || array_to_string(array_fill('0'::text, array[1536]), ',') || ']')::vector as embedding
)
insert into public.candidate_chunk_embeddings (
    id,
    candidate_id,
    chunk_id,
    model_name,
    model_version,
    embedding_dimensions,
    embedding,
    generated_at
)
select
    embedding_rows.id,
    embedding_rows.candidate_id,
    embedding_rows.chunk_id,
    'text-embedding-3-small',
    null,
    1536,
    active_embedding.embedding,
    now()
from active_embedding
cross join (
    values
        (
            '00000000-0000-0000-0000-000000000431'::uuid,
            '00000000-0000-0000-0000-000000000401'::uuid,
            '00000000-0000-0000-0000-000000000421'::uuid
        ),
        (
            '00000000-0000-0000-0000-000000000432'::uuid,
            '00000000-0000-0000-0000-000000000401'::uuid,
            '00000000-0000-0000-0000-000000000422'::uuid
        ),
        (
            '00000000-0000-0000-0000-000000000433'::uuid,
            '00000000-0000-0000-0000-000000000402'::uuid,
            '00000000-0000-0000-0000-000000000423'::uuid
        ),
        (
            '00000000-0000-0000-0000-000000000434'::uuid,
            '00000000-0000-0000-0000-000000000402'::uuid,
            '00000000-0000-0000-0000-000000000425'::uuid
        ),
        (
            '00000000-0000-0000-0000-000000000435'::uuid,
            '00000000-0000-0000-0000-000000000402'::uuid,
            '00000000-0000-0000-0000-000000000427'::uuid
        )
) as embedding_rows(id, candidate_id, chunk_id);

insert into public.candidate_search_documents (
    candidate_id,
    search_text,
    summary_source_types,
    rebuilt_at
)
values
    (
        '00000000-0000-0000-0000-000000000401',
        'Candidate one flattened retrieval summary',
        array['linkedin_profile']::text[],
        now()
    ),
    (
        '00000000-0000-0000-0000-000000000402',
        'Candidate two flattened retrieval summary',
        array['linkedin_profile', 'resume', 'recruiter_note_raw']::text[],
        now()
    );

do $$
begin
    if (
        select count(*)
        from public.candidate_source_documents
        where candidate_id = '00000000-0000-0000-0000-000000000402'
    ) <> 3 then
        raise exception 'expected one candidate to support multiple source documents';
    end if;
end;
$$;

do $$
begin
    if (
        select count(*)
        from public.candidate_search_chunks
        where source_document_id = '00000000-0000-0000-0000-000000000412'
    ) <> 2 then
        raise exception 'expected one source document to support multiple chunks';
    end if;
end;
$$;

do $$
begin
    if (
        select count(*)
        from public.candidate_chunk_embeddings
        where candidate_id = '00000000-0000-0000-0000-000000000402'
    ) < 2 then
        raise exception 'expected one candidate to support multiple chunk embeddings';
    end if;
end;
$$;

do $$
begin
    if exists (
        select 1
        from public.candidate_source_documents
        where candidate_id = '00000000-0000-0000-0000-000000000401'
          and source_type <> 'linkedin_profile'
    ) then
        raise exception 'expected linkedin-only evidence to be valid without resume or note rows';
    end if;
end;
$$;

do $$
begin
    if not exists (
        select 1
        from public.candidate_source_documents
        where candidate_id = '00000000-0000-0000-0000-000000000402'
          and source_type = 'linkedin_profile'
    ) or not exists (
        select 1
        from public.candidate_source_documents
        where candidate_id = '00000000-0000-0000-0000-000000000402'
          and source_type = 'resume'
    ) or not exists (
        select 1
        from public.candidate_source_documents
        where candidate_id = '00000000-0000-0000-0000-000000000402'
          and source_type = 'recruiter_note_raw'
    ) then
        raise exception 'expected linkedin, resume, and recruiter-note evidence to coexist without schema changes';
    end if;
end;
$$;

do $$
begin
    if not exists (
        select 1
        from public.candidate_search_documents
        where candidate_id = '00000000-0000-0000-0000-000000000401'
    ) or not exists (
        select 1
        from public.candidate_chunk_embeddings
        where candidate_id = '00000000-0000-0000-0000-000000000401'
    ) then
        raise exception 'expected candidate_search_documents to coexist with chunk embeddings instead of replacing them';
    end if;
end;
$$;

rollback;
