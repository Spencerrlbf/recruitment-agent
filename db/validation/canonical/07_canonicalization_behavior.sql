-- Canonicalization behavior checks.
-- This script intentionally rolls back so it can be run repeatedly.

begin;

set local search_path = public, extensions;

insert into public.candidate_profiles_v2 (
    id,
    full_name,
    linkedin_username,
    linkedin_url,
    linkedin_url_normalized,
    source
)
values
    (
        '00000000-0000-0000-0000-000000000501',
        'Alpha Candidate',
        'alpha-user',
        'https://www.linkedin.com/in/alpha-user',
        'https://www.linkedin.com/in/alpha-user',
        'schema_canonicalization_test'
    ),
    (
        '00000000-0000-0000-0000-000000000502',
        'Beta Candidate',
        'beta-user',
        null,
        null,
        'schema_canonicalization_test'
    ),
    (
        '00000000-0000-0000-0000-000000000503',
        'Gamma Candidate',
        null,
        'https://www.linkedin.com/in/gamma-user',
        'https://www.linkedin.com/in/gamma-user',
        'schema_canonicalization_test'
    ),
    (
        '00000000-0000-0000-0000-000000000504',
        'Split Username Candidate',
        'split-identity',
        null,
        null,
        'schema_canonicalization_test'
    ),
    (
        '00000000-0000-0000-0000-000000000505',
        'Split Url Candidate',
        null,
        'https://www.linkedin.com/in/split-identity',
        'https://www.linkedin.com/in/split-identity',
        'schema_canonicalization_test'
    );

insert into public.companies_v2 (
    id,
    name,
    normalized_name,
    linkedin_id,
    linkedin_username,
    linkedin_url,
    linkedin_url_normalized,
    data_source,
    identity_basis,
    source_record_refs
)
values
    (
        '00000000-0000-0000-0000-000000000521',
        'Acme Incorporated',
        public.normalize_company_name('Acme Incorporated'),
        'acme-001',
        'acme',
        'https://www.linkedin.com/company/acme',
        'https://www.linkedin.com/company/acme',
        'schema_canonicalization_test',
        'linkedin_id',
        '[{"source":"schema_canonicalization_test","record_id":"acme-legacy","raw_name":"Acme Incorporated"}]'::jsonb
    ),
    (
        '00000000-0000-0000-0000-000000000522',
        'Beta Labs LLC',
        public.normalize_company_name('Beta Labs LLC'),
        null,
        null,
        null,
        null,
        'schema_canonicalization_test',
        'name',
        '[{"source":"schema_canonicalization_test","record_id":"beta-legacy","raw_name":"Beta Labs LLC"}]'::jsonb
    ),
    (
        '00000000-0000-0000-0000-000000000523',
        'Contoso Holdings',
        public.normalize_company_name('Contoso Holdings'),
        null,
        'contoso-identity',
        null,
        null,
        'schema_canonicalization_test',
        'linkedin_username',
        '[{"source":"schema_canonicalization_test","record_id":"contoso-user","raw_name":"Contoso Holdings"}]'::jsonb
    ),
    (
        '00000000-0000-0000-0000-000000000524',
        'Contoso Group',
        public.normalize_company_name('Contoso Group'),
        null,
        null,
        'https://www.linkedin.com/company/contoso-identity',
        'https://www.linkedin.com/company/contoso-identity',
        'schema_canonicalization_test',
        'linkedin_url',
        '[{"source":"schema_canonicalization_test","record_id":"contoso-url","raw_name":"Contoso Group"}]'::jsonb
    ),
    (
        '00000000-0000-0000-0000-000000000525',
        'DupCo LLC',
        public.normalize_company_name('DupCo LLC'),
        null,
        null,
        null,
        null,
        'schema_canonicalization_test',
        'name',
        '[{"source":"schema_canonicalization_test","record_id":"dupco-one","raw_name":"DupCo LLC"}]'::jsonb
    ),
    (
        '00000000-0000-0000-0000-000000000526',
        'DupCo Inc.',
        public.normalize_company_name('DupCo Inc.'),
        null,
        null,
        null,
        null,
        'schema_canonicalization_test',
        'name',
        '[{"source":"schema_canonicalization_test","record_id":"dupco-two","raw_name":"DupCo Inc."}]'::jsonb
    );

do $$
begin
    if public.normalize_email_address(' MAILTO:Person+Alias@Example.com ') <> 'person+alias@example.com'::citext then
        raise exception 'expected normalize_email_address to trim, strip mailto, and lowercase only';
    end if;

    if public.normalize_email_address('A B@example.com') <> 'a b@example.com'::citext then
        raise exception 'expected normalize_email_address not to remove interior whitespace heuristically';
    end if;

    if public.normalize_company_name('ACME, Inc.') <> 'acme' then
        raise exception 'expected normalize_company_name to produce fallback key without legal suffix noise';
    end if;
end;
$$;

do $$
begin
    if public.canonical_source_precedence_rank('linkedin_import')
        <= public.canonical_source_precedence_rank('resume_upload') then
        raise exception 'expected linkedin_import to outrank resume_upload';
    end if;

    if not public.should_replace_canonical_value('resume_upload', 'linkedin_import', 'old', 'new') then
        raise exception 'expected higher-precedence linkedin_import to replace lower-precedence value';
    end if;

    if public.should_replace_canonical_value('linkedin_import', 'resume_upload', 'old', 'new') then
        raise exception 'expected lower-precedence resume_upload not to replace linkedin_import value';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_candidate_profile_match(
        '00000000-0000-0000-0000-000000000501',
        null,
        null
    );

    if match_row.decision <> 'match_existing'
       or match_row.matched_candidate_id <> '00000000-0000-0000-0000-000000000501'::uuid
       or match_row.match_basis <> 'legacy_candidate_id' then
        raise exception 'expected stable legacy candidate UUID to match existing candidate';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_candidate_profile_match(
        null,
        'alpha-user',
        'https://linkedin.com/in/alpha-user/?trk=public_profile'
    );

    if match_row.decision <> 'match_existing'
       or match_row.matched_candidate_id <> '00000000-0000-0000-0000-000000000501'::uuid then
        raise exception 'expected strong LinkedIn identity to resolve existing candidate';
    end if;

    if match_row.normalized_linkedin_url <> 'https://www.linkedin.com/in/alpha-user' then
        raise exception 'expected candidate LinkedIn URL to normalize deterministically';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_candidate_profile_match(
        null,
        null,
        null
    );

    if match_row.decision <> 'create_new' then
        raise exception 'expected create_new when no stable legacy UUID or strong LinkedIn identity is present';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_candidate_profile_match(
        null,
        'split-identity',
        'https://www.linkedin.com/in/split-identity'
    );

    if match_row.decision <> 'ambiguous'
       or match_row.ambiguity_type <> 'conflicting_existing_linkedin_identity' then
        raise exception 'expected conflicting candidate LinkedIn keys to be ambiguous';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_company_match(
        'acme-001',
        null,
        null,
        'Acme Inc.'
    );

    if match_row.decision <> 'match_existing'
       or match_row.matched_company_id <> '00000000-0000-0000-0000-000000000521'::uuid
       or match_row.match_basis <> 'linkedin_id' then
        raise exception 'expected linkedin_id to take precedence for company resolution';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_company_match(
        null,
        null,
        null,
        'Beta Labs, LLC'
    );

    if match_row.decision <> 'match_existing'
       or match_row.matched_company_id <> '00000000-0000-0000-0000-000000000522'::uuid
       or match_row.match_basis <> 'normalized_name' then
        raise exception 'expected single normalized-name fallback company match to resolve';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_company_match(
        null,
        'contoso-identity',
        'https://www.linkedin.com/company/contoso-identity',
        'Contoso'
    );

    if match_row.decision <> 'ambiguous'
       or match_row.ambiguity_type <> 'conflicting_existing_strong_identity' then
        raise exception 'expected conflicting company LinkedIn keys to be ambiguous';
    end if;
end;
$$;

do $$
declare
    match_row record;
begin
    select *
    into match_row
    from public.resolve_company_match(
        null,
        null,
        null,
        'DupCo LLC'
    );

    if match_row.decision <> 'ambiguous'
       or match_row.ambiguity_type <> 'multiple_normalized_name_matches' then
        raise exception 'expected multiple normalized-name fallback matches to be ambiguous';
    end if;
end;
$$;

do $$
declare
    hash_one text;
    hash_two text;
    punctuation_hash_one text;
    punctuation_hash_two text;
begin
    hash_one := public.build_candidate_experience_source_hash(
        '00000000-0000-0000-0000-000000000501',
        'Senior Engineer',
        null,
        'Acme, Inc.',
        date '2020-05-21',
        'month',
        null,
        'present',
        true
    );

    hash_two := public.build_candidate_experience_source_hash(
        '00000000-0000-0000-0000-000000000501',
        ' senior   engineer ',
        null,
        'ACME INC',
        date '2020-05-01',
        'month',
        null,
        'present',
        true
    );

    if hash_one <> hash_two then
        raise exception 'expected experience source hash to be stable across case and whitespace noise';
    end if;

    punctuation_hash_one := public.build_candidate_experience_source_hash(
        '00000000-0000-0000-0000-000000000501',
        'C++ Engineer',
        null,
        'Acme, Inc.',
        date '2020-05-01',
        'month',
        null,
        'present',
        true
    );

    punctuation_hash_two := public.build_candidate_experience_source_hash(
        '00000000-0000-0000-0000-000000000501',
        'C# Engineer',
        null,
        'Acme, Inc.',
        date '2020-05-01',
        'month',
        null,
        'present',
        true
    );

    if punctuation_hash_one = punctuation_hash_two then
        raise exception 'expected punctuation-significant titles to produce different experience hashes';
    end if;

    if public.normalize_experience_date(date '2018-07-19', 'year', false, false) <> date '2018-01-01' then
        raise exception 'expected year-precision dates to normalize to the first day of the year';
    end if;

    if public.normalize_experience_date(date '2018-07-19', 'month', false, false) <> date '2018-07-01' then
        raise exception 'expected month-precision dates to normalize to the first day of the month';
    end if;

    if public.normalize_experience_date(date '2018-07-19', 'present', true, true) is not null then
        raise exception 'expected present/current end dates to normalize to null';
    end if;
end;
$$;

do $$
declare
    ambiguity_id_one uuid;
    ambiguity_id_two uuid;
begin
    ambiguity_id_one := public.record_canonicalization_ambiguity(
        'company',
        'multiple_normalized_name_matches',
        'schema_canonicalization_test',
        'dupco',
        '{"normalized_name":"dupco"}'::jsonb,
        array[
            '00000000-0000-0000-0000-000000000525'::uuid,
            '00000000-0000-0000-0000-000000000526'::uuid
        ],
        'manual_review'
    );

    ambiguity_id_two := public.record_canonicalization_ambiguity(
        'company',
        'multiple_normalized_name_matches',
        'schema_canonicalization_test',
        'dupco',
        '{"normalized_name":"dupco"}'::jsonb,
        array[
            '00000000-0000-0000-0000-000000000525'::uuid,
            '00000000-0000-0000-0000-000000000526'::uuid
        ],
        'manual_review'
    );

    if ambiguity_id_one <> ambiguity_id_two then
        raise exception 'expected ambiguity logging to reuse an identical open ambiguity';
    end if;

    if (
        select count(*)
        from public.canonicalization_ambiguities
        where source_system = 'schema_canonicalization_test'
          and source_record_ref = 'dupco'
    ) <> 1 then
        raise exception 'expected only one open ambiguity row for identical normalized input';
    end if;
end;
$$;

rollback;
