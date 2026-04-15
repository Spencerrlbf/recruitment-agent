-- Canonical Layer A smoke test.
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
        '00000000-0000-0000-0000-000000000301',
        'Smoke Candidate One',
        'schema_smoke_test'
    ),
    (
        '00000000-0000-0000-0000-000000000302',
        'Smoke Candidate Two',
        'schema_smoke_test'
    );

do $$
begin
    if not exists (
        select 1
        from public.candidate_profiles_v2
        where id = '00000000-0000-0000-0000-000000000301'
          and source = 'schema_smoke_test'
    ) then
        raise exception 'minimal candidate_profiles_v2 insert failed';
    end if;
end;
$$;

insert into public.candidate_emails_v2 (
    id,
    candidate_id,
    email_raw,
    email_normalized,
    is_primary
)
values
    (
        '00000000-0000-0000-0000-000000000311',
        '00000000-0000-0000-0000-000000000301',
        'Person@example.com',
        'person@example.com',
        true
    ),
    (
        '00000000-0000-0000-0000-000000000312',
        '00000000-0000-0000-0000-000000000301',
        'second@example.com',
        'second@example.com',
        false
    );

do $$
begin
    begin
        insert into public.candidate_emails_v2 (
            id,
            candidate_id,
            email_raw,
            email_normalized,
            is_primary
        )
        values (
            '00000000-0000-0000-0000-000000000313',
            '00000000-0000-0000-0000-000000000302',
            'PERSON@example.com',
            'person@example.com',
            false
        );

        raise exception 'expected email_normalized uniqueness to reject duplicate normalized email';
    exception
        when unique_violation then
            null;
    end;
end;
$$;

do $$
begin
    begin
        insert into public.candidate_emails_v2 (
            id,
            candidate_id,
            email_raw,
            email_normalized,
            is_primary
        )
        values (
            '00000000-0000-0000-0000-000000000314',
            '00000000-0000-0000-0000-000000000301',
            'primary-two@example.com',
            'primary-two@example.com',
            true
        );

        raise exception 'expected one-primary-email rule to reject a second primary email';
    exception
        when unique_violation then
            null;
    end;
end;
$$;

insert into public.companies_v2 (
    id,
    name,
    normalized_name,
    data_source,
    identity_basis
)
values
    (
        '00000000-0000-0000-0000-000000000321',
        'Acme Incorporated',
        'acme',
        'schema_smoke_test',
        'name'
    ),
    (
        '00000000-0000-0000-0000-000000000322',
        'ACME Inc.',
        'acme',
        'schema_smoke_test',
        'name'
    );

do $$
begin
    if (
        select count(*)
        from public.companies_v2
        where normalized_name = 'acme'
    ) <> 2 then
        raise exception 'expected companies_v2 to allow duplicate normalized_name fallback rows';
    end if;
end;
$$;

insert into public.candidate_experiences_v2 (
    id,
    candidate_id,
    company_id,
    experience_index,
    title,
    raw_company_name,
    start_date,
    start_date_precision,
    end_date_precision,
    is_current,
    source_hash
)
values (
    '00000000-0000-0000-0000-000000000331',
    '00000000-0000-0000-0000-000000000301',
    null,
    0,
    'Founder',
    'Independent Consulting',
    date '2022-01-01',
    'month',
    'present',
    true,
    'schema-smoke-experience-1'
);

do $$
begin
    if not exists (
        select 1
        from public.candidate_experiences_v2
        where id = '00000000-0000-0000-0000-000000000331'
          and candidate_id = '00000000-0000-0000-0000-000000000301'
          and company_id is null
    ) then
        raise exception 'expected candidate_experiences_v2 to accept a null company_id';
    end if;
end;
$$;

rollback;
