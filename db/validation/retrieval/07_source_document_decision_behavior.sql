-- Candidate source-document decision behavior checks.
-- This script intentionally rolls back so it can be run repeatedly.

begin;

set local search_path = public, extensions;

insert into public.candidate_profiles_v2 (
    id,
    full_name,
    source
)
values (
    '00000000-0000-0000-0000-000000000601',
    'Source Document Decision Candidate',
    'schema_source_document_test'
);

insert into public.candidate_source_documents (
    id,
    candidate_id,
    source_type,
    title,
    source_url,
    normalized_text,
    metadata_json,
    trust_level,
    document_version,
    is_active
)
values
    (
        '00000000-0000-0000-0000-000000000611',
        '00000000-0000-0000-0000-000000000601',
        'linkedin_profile',
        'Candidate LinkedIn',
        'https://www.linkedin.com/in/source-document-test',
        'linkedin content v1',
        '{}'::jsonb,
        'baseline',
        1,
        true
    ),
    (
        '00000000-0000-0000-0000-000000000612',
        '00000000-0000-0000-0000-000000000601',
        'resume',
        'Resume A',
        null,
        'resume content v1',
        '{"document_identity_key":"resume-a"}'::jsonb,
        'high',
        1,
        true
    ),
    (
        '00000000-0000-0000-0000-000000000613',
        '00000000-0000-0000-0000-000000000601',
        'resume',
        'Anonymous Resume',
        null,
        'anonymous resume v1',
        '{}'::jsonb,
        'high',
        1,
        true
    ),
    (
        '00000000-0000-0000-0000-000000000614',
        '00000000-0000-0000-0000-000000000601',
        'recruiter_note_raw',
        'Recruiter Note',
        null,
        'note same content',
        '{}'::jsonb,
        'supplemental',
        1,
        true
    );

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'linkedin_profile',
        null,
        'Candidate LinkedIn',
        'https://linkedin.com/in/source-document-test?trk=public',
        null,
        'linkedin content v1',
        '{}'::jsonb
    );

    if decision_row.decision <> 'no_op'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000611'::uuid then
        raise exception 'expected same linkedin profile content to be a no_op';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'linkedin_profile',
        null,
        'Candidate LinkedIn',
        'https://www.linkedin.com/in/source-document-test',
        null,
        'linkedin content v2',
        '{}'::jsonb
    );

    if decision_row.decision <> 'supersede'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000611'::uuid
       or decision_row.next_document_version <> 2 then
        raise exception 'expected changed linkedin profile content to supersede the active row';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Resume A',
        null,
        null,
        'resume content v1',
        '{"document_identity_key":"resume-a"}'::jsonb
    );

    if decision_row.decision <> 'no_op'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000612'::uuid then
        raise exception 'expected same logical resume + same content to be a no_op';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Resume A Renamed',
        null,
        null,
        'resume content v1',
        '{"document_identity_key":"resume-a"}'::jsonb
    );

    if decision_row.decision <> 'no_op'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000612'::uuid then
        raise exception 'expected title-only resume changes not to force a supersede';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Resume A',
        null,
        null,
        'resume content v1',
        '{"document_identity_key":"resume-a","parser_version":"v2"}'::jsonb
    );

    if decision_row.decision <> 'no_op'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000612'::uuid then
        raise exception 'expected metadata-only resume changes not to force a supersede';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Resume A',
        null,
        null,
        'resume content v2',
        '{"document_identity_key":"resume-a"}'::jsonb
    );

    if decision_row.decision <> 'supersede'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000612'::uuid
       or decision_row.next_document_version <> 2 then
        raise exception 'expected same logical resume + changed content to supersede';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Resume B',
        null,
        null,
        'resume content v1',
        '{"document_identity_key":"resume-b"}'::jsonb
    );

    if decision_row.decision <> 'parallel'
       or decision_row.next_document_version <> 1 then
        raise exception 'expected different logical resume identity to remain parallel';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'resume',
        null,
        'Anonymous Resume',
        null,
        null,
        'anonymous resume v2',
        '{}'::jsonb
    );

    if decision_row.decision <> 'ambiguous'
       or decision_row.ambiguity_type <> 'anonymous_resume_identity_conflict' then
        raise exception 'expected conflicting anonymous resume content to be ambiguous';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'recruiter_note_raw',
        null,
        'Recruiter Note',
        null,
        null,
        'note same content',
        '{}'::jsonb
    );

    if decision_row.decision <> 'no_op'
       or decision_row.matched_document_id <> '00000000-0000-0000-0000-000000000614'::uuid then
        raise exception 'expected exact duplicate recruiter note content to be a no_op';
    end if;
end;
$$;

do $$
declare
    decision_row record;
begin
    select *
    into decision_row
    from public.decide_candidate_source_document_action(
        '00000000-0000-0000-0000-000000000601',
        'recruiter_note_raw',
        null,
        'Recruiter Note',
        null,
        null,
        'note different content',
        '{}'::jsonb
    );

    if decision_row.decision <> 'parallel'
       or decision_row.next_document_version <> 1 then
        raise exception 'expected different recruiter note content without stable identity to remain parallel';
    end if;
end;
$$;

rollback;
