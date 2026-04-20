begin;

set local search_path = public, extensions;

create schema if not exists extensions;

create extension if not exists citext with schema extensions;
create extension if not exists "uuid-ossp" with schema extensions;

alter table public.companies_v2
    add column source_record_refs jsonb;

create table public.canonicalization_ambiguities (
    id uuid not null default uuid_generate_v4(),
    entity_type text not null,
    ambiguity_type text not null,
    source_system text,
    source_record_ref text,
    normalized_input jsonb not null default '{}'::jsonb,
    matched_record_ids uuid[],
    recommended_action text not null default 'manual_review',
    status text not null default 'open',
    resolution_notes text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    resolved_at timestamptz,
    constraint canonicalization_ambiguities_pkey primary key (id),
    constraint canonicalization_ambiguities_entity_type_ck
        check (entity_type in ('candidate_profile', 'company', 'candidate_experience', 'candidate_source_document')),
    constraint canonicalization_ambiguities_recommended_action_ck
        check (recommended_action in ('skip', 'manual_review')),
    constraint canonicalization_ambiguities_status_ck
        check (status in ('open', 'resolved', 'ignored'))
);

create index canonicalization_ambiguities_status_entity_type_idx
    on public.canonicalization_ambiguities (status, entity_type);

create index canonicalization_ambiguities_entity_ambiguity_type_idx
    on public.canonicalization_ambiguities (entity_type, ambiguity_type);

create index canonicalization_ambiguities_source_ref_idx
    on public.canonicalization_ambiguities (source_system, source_record_ref);

create unique index canonicalization_ambiguities_open_identity_uq
    on public.canonicalization_ambiguities (
        entity_type,
        ambiguity_type,
        coalesce(source_system, ''),
        coalesce(source_record_ref, ''),
        md5(normalized_input::text)
    )
    where status = 'open';

create trigger set_canonicalization_ambiguities_updated_at
before update on public.canonicalization_ambiguities
for each row
execute function public.set_updated_at();

create or replace function public.normalize_identity_text(raw_text text)
returns text
language sql
immutable
strict
as $$
    select nullif(
        btrim(
            regexp_replace(
                regexp_replace(lower(btrim(raw_text)), '[[:punct:]]+', ' ', 'g'),
                '\s+',
                ' ',
                'g'
            )
        ),
        ''
    );
$$;

create or replace function public.normalize_search_text(raw_text text)
returns text
language sql
immutable
strict
as $$
    select nullif(
        btrim(regexp_replace(lower(btrim(raw_text)), '\s+', ' ', 'g')),
        ''
    );
$$;

create or replace function public.normalize_identity_url(raw_url text)
returns text
language plpgsql
immutable
as $$
declare
    v_url text;
begin
    if raw_url is null then
        return null;
    end if;

    v_url := lower(btrim(raw_url));

    if v_url = '' then
        return null;
    end if;

    if v_url like '//%' then
        v_url := 'https:' || v_url;
    elsif v_url !~ '^[a-z][a-z0-9+.-]*://' then
        v_url := 'https://' || regexp_replace(v_url, '^/+', '');
    end if;

    v_url := regexp_replace(v_url, '#.*$', '');
    v_url := regexp_replace(v_url, '\?.*$', '');
    v_url := regexp_replace(v_url, '^http://', 'https://');
    v_url := regexp_replace(v_url, '/+$', '');

    return nullif(v_url, '');
end;
$$;

create or replace function public.normalize_linkedin_username(raw_username text)
returns text
language plpgsql
immutable
as $$
declare
    v_username text;
begin
    if raw_username is null then
        return null;
    end if;

    v_username := lower(btrim(raw_username));

    if v_username = '' or v_username like '%linkedin.com/%' then
        return null;
    end if;

    v_username := regexp_replace(v_username, '^@+', '');
    v_username := regexp_replace(v_username, '^/+', '');
    v_username := regexp_replace(v_username, '^(in|company|school)/', '');
    v_username := regexp_replace(v_username, '/+$', '');

    return nullif(v_username, '');
end;
$$;

create or replace function public.normalize_candidate_linkedin_url(raw_url text)
returns text
language plpgsql
immutable
as $$
declare
    v_url text;
    v_slug text;
    v_pub_path text;
begin
    v_url := public.normalize_identity_url(raw_url);

    if v_url is null then
        return null;
    end if;

    v_url := regexp_replace(v_url, '^https://([a-z]{2,3}\.)?linkedin\.com', 'https://www.linkedin.com');
    v_url := regexp_replace(v_url, '^https://m\.linkedin\.com', 'https://www.linkedin.com');

    if v_url ~ '^https://www\.linkedin\.com/in/' then
        v_slug := regexp_replace(v_url, '^https://www\.linkedin\.com/in/([^/?#]+)/?.*$', '\1');

        if v_slug is null or v_slug = '' then
            return null;
        end if;

        return 'https://www.linkedin.com/in/' || v_slug;
    end if;

    if v_url ~ '^https://www\.linkedin\.com/pub/' then
        v_pub_path := regexp_replace(v_url, '^https://www\.linkedin\.com/pub/(.+)$', '\1');
        v_pub_path := regexp_replace(v_pub_path, '/+$', '');

        if v_pub_path is null or v_pub_path = '' then
            return null;
        end if;

        return 'https://www.linkedin.com/pub/' || v_pub_path;
    end if;

    return null;
end;
$$;

create or replace function public.normalize_company_linkedin_url(raw_url text)
returns text
language plpgsql
immutable
as $$
declare
    v_url text;
    v_type text;
    v_slug text;
begin
    v_url := public.normalize_identity_url(raw_url);

    if v_url is null then
        return null;
    end if;

    v_url := regexp_replace(v_url, '^https://([a-z]{2,3}\.)?linkedin\.com', 'https://www.linkedin.com');
    v_url := regexp_replace(v_url, '^https://m\.linkedin\.com', 'https://www.linkedin.com');

    if v_url ~ '^https://www\.linkedin\.com/(company|school)/' then
        v_type := regexp_replace(v_url, '^https://www\.linkedin\.com/(company|school)/.*$', '\1');
        v_slug := regexp_replace(v_url, '^https://www\.linkedin\.com/(company|school)/([^/?#]+)/?.*$', '\2');

        if v_slug is null or v_slug = '' then
            return null;
        end if;

        return 'https://www.linkedin.com/' || v_type || '/' || v_slug;
    end if;

    return null;
end;
$$;

create or replace function public.extract_candidate_linkedin_username_from_url(raw_url text)
returns text
language plpgsql
immutable
as $$
declare
    v_url text;
begin
    v_url := public.normalize_candidate_linkedin_url(raw_url);

    if v_url is null or v_url !~ '^https://www\.linkedin\.com/in/' then
        return null;
    end if;

    return nullif(regexp_replace(v_url, '^https://www\.linkedin\.com/in/([^/]+)$', '\1'), '');
end;
$$;

create or replace function public.extract_company_linkedin_username_from_url(raw_url text)
returns text
language plpgsql
immutable
as $$
declare
    v_url text;
begin
    v_url := public.normalize_company_linkedin_url(raw_url);

    if v_url is null or v_url !~ '^https://www\.linkedin\.com/(company|school)/' then
        return null;
    end if;

    return nullif(regexp_replace(v_url, '^https://www\.linkedin\.com/(company|school)/([^/]+)$', '\2'), '');
end;
$$;

create or replace function public.normalize_email_address(raw_email text)
returns extensions.citext
language plpgsql
immutable
as $$
declare
    v_email text;
begin
    if raw_email is null then
        return null;
    end if;

    v_email := lower(btrim(raw_email));
    v_email := regexp_replace(v_email, '^mailto:', '', 'i');
    v_email := btrim(v_email);

    if v_email = '' then
        return null;
    end if;

    return v_email::extensions.citext;
end;
$$;

create or replace function public.normalize_company_name(raw_name text)
returns text
language plpgsql
immutable
as $$
declare
    v_name text;
begin
    v_name := public.normalize_identity_text(raw_name);

    if v_name is null then
        return null;
    end if;

    loop
        exit when v_name !~ ' (incorporated|inc|corporation|corp|company|co|llc|limited|ltd|plc|llp|lp|gmbh|ag|sa|bv|nv|pte|pty|sarl|oy|ab)$';
        v_name := regexp_replace(
            v_name,
            ' (incorporated|inc|corporation|corp|company|co|llc|limited|ltd|plc|llp|lp|gmbh|ag|sa|bv|nv|pte|pty|sarl|oy|ab)$',
            '',
            ''
        );
        v_name := btrim(regexp_replace(v_name, '\s+', ' ', 'g'));
    end loop;

    return nullif(v_name, '');
end;
$$;

create or replace function public.canonical_source_precedence_rank(source_name text)
returns integer
language sql
immutable
as $$
    select case lower(btrim(coalesce(source_name, '')))
        when 'linkedin_import' then 500
        when 'linkedin_profile' then 500
        when 'legacy_backfill' then 400
        when 'resume_upload' then 300
        when 'resume' then 300
        when 'recruiter_note_summary' then 200
        when 'transcript_summary' then 200
        when 'recruiter_note_raw' then 100
        when 'manual_profile_note' then 100
        else 0
    end;
$$;

create or replace function public.should_replace_canonical_value(
    existing_source text,
    incoming_source text,
    existing_value text,
    incoming_value text
)
returns boolean
language plpgsql
immutable
as $$
begin
    if incoming_value is null or btrim(incoming_value) = '' then
        return false;
    end if;

    if existing_value is null or btrim(existing_value) = '' then
        return true;
    end if;

    if public.canonical_source_precedence_rank(incoming_source)
        > public.canonical_source_precedence_rank(existing_source) then
        return true;
    end if;

    return false;
end;
$$;

create or replace function public.normalize_experience_date_precision(
    raw_precision text,
    is_end_date boolean default false,
    raw_is_current boolean default false
)
returns text
language plpgsql
immutable
as $$
declare
    v_precision text;
begin
    if coalesce(raw_is_current, false) and coalesce(is_end_date, false) then
        return 'present';
    end if;

    v_precision := lower(btrim(coalesce(raw_precision, '')));

    if v_precision = '' then
        return 'unknown';
    end if;

    if v_precision in ('year', 'month', 'day', 'unknown') then
        return v_precision;
    end if;

    if coalesce(is_end_date, false) and v_precision = 'present' then
        return 'present';
    end if;

    return 'unknown';
end;
$$;

create or replace function public.normalize_experience_date(
    raw_date date,
    raw_precision text,
    is_end_date boolean default false,
    raw_is_current boolean default false
)
returns date
language plpgsql
immutable
as $$
declare
    v_precision text;
begin
    v_precision := public.normalize_experience_date_precision(raw_precision, is_end_date, raw_is_current);

    if v_precision = 'present' then
        return null;
    end if;

    if raw_date is null then
        return null;
    end if;

    if v_precision = 'year' then
        return make_date(extract(year from raw_date)::integer, 1, 1);
    end if;

    if v_precision = 'month' then
        return date_trunc('month', raw_date)::date;
    end if;

    return raw_date;
end;
$$;

create or replace function public.normalize_experience_is_current(
    raw_is_current boolean,
    raw_end_date_precision text
)
returns boolean
language sql
immutable
as $$
    select coalesce(raw_is_current, false)
        or lower(btrim(coalesce(raw_end_date_precision, ''))) = 'present';
$$;

create or replace function public.build_candidate_experience_source_hash(
    candidate_id uuid,
    title text,
    company_id uuid,
    raw_company_name text,
    start_date date,
    start_date_precision text,
    end_date date,
    end_date_precision text,
    is_current boolean
)
returns text
language plpgsql
immutable
as $$
declare
    v_title text;
    v_company_identity text;
    v_start_date date;
    v_start_precision text;
    v_end_date date;
    v_end_precision text;
    v_is_current boolean;
begin
    v_title := public.normalize_search_text(title);
    v_is_current := public.normalize_experience_is_current(is_current, end_date_precision);
    v_start_precision := public.normalize_experience_date_precision(start_date_precision, false, false);
    v_start_date := public.normalize_experience_date(start_date, start_date_precision, false, false);
    v_end_precision := public.normalize_experience_date_precision(end_date_precision, true, v_is_current);
    v_end_date := public.normalize_experience_date(end_date, end_date_precision, true, v_is_current);

    if company_id is not null then
        v_company_identity := 'company_id:' || company_id::text;
    else
        v_company_identity := 'company_name:' || coalesce(public.normalize_company_name(raw_company_name), '');
    end if;

    return md5(
        concat_ws(
            '|',
            coalesce(candidate_id::text, ''),
            coalesce(v_title, ''),
            v_company_identity,
            coalesce(v_start_date::text, ''),
            coalesce(v_start_precision, ''),
            coalesce(v_end_date::text, ''),
            coalesce(v_end_precision, ''),
            case when v_is_current then '1' else '0' end
        )
    );
end;
$$;

create or replace function public.build_candidate_source_document_identity_key(
    source_type text,
    source_subtype text,
    source_url text,
    external_source_ref text,
    metadata_json jsonb
)
returns text
language plpgsql
immutable
as $$
declare
    v_source_type text;
    v_source_subtype text;
    v_identity_key text;
begin
    v_source_type := lower(btrim(coalesce(source_type, '')));
    v_source_subtype := nullif(lower(btrim(coalesce(source_subtype, ''))), '');

    if v_source_type = '' then
        return null;
    end if;

    if v_source_type = 'linkedin_profile' then
        return 'linkedin_profile';
    end if;

    v_identity_key := nullif(btrim(coalesce(metadata_json ->> 'document_identity_key', '')), '');

    if v_identity_key is null then
        v_identity_key := nullif(btrim(coalesce(external_source_ref, '')), '');
    end if;

    if v_identity_key is null then
        v_identity_key := public.normalize_identity_url(source_url);
    end if;

    if v_identity_key is null then
        return null;
    end if;

    if v_source_subtype is not null then
        return v_source_type || ':' || v_source_subtype || ':' || v_identity_key;
    end if;

    return v_source_type || ':' || v_identity_key;
end;
$$;

create or replace function public.build_candidate_source_document_content_hash(
    source_type text,
    title text,
    normalized_text text,
    metadata_json jsonb
)
returns text
language plpgsql
immutable
as $$
declare
    v_source_type text;
    v_text text;
    v_supplied_hash text;
begin
    v_source_type := lower(btrim(coalesce(source_type, '')));
    v_text := public.normalize_search_text(normalized_text);
    v_supplied_hash := nullif(
        lower(
            btrim(
                coalesce(
                    metadata_json ->> 'content_sha256',
                    metadata_json ->> 'content_hash',
                    metadata_json ->> 'text_hash',
                    ''
                )
            )
        ),
        ''
    );

    return md5(
        concat_ws(
            '|',
            coalesce(v_source_type, ''),
            coalesce(v_text, v_supplied_hash, '')
        )
    );
end;
$$;

create or replace function public.resolve_candidate_profile_match(
    incoming_candidate_id uuid,
    incoming_linkedin_username text,
    incoming_linkedin_url text
)
returns table (
    decision text,
    matched_candidate_id uuid,
    match_basis text,
    normalized_linkedin_username text,
    normalized_linkedin_url text,
    ambiguity_type text
)
language plpgsql
stable
as $$
declare
    v_existing_candidate_id uuid;
    v_candidate_by_username uuid;
    v_candidate_by_url uuid;
    v_input_username text;
    v_url_username text;
begin
    v_input_username := public.normalize_linkedin_username(incoming_linkedin_username);
    normalized_linkedin_url := public.normalize_candidate_linkedin_url(incoming_linkedin_url);
    v_url_username := public.extract_candidate_linkedin_username_from_url(incoming_linkedin_url);

    if v_input_username is not null
       and v_url_username is not null
       and v_input_username <> v_url_username then
        decision := 'ambiguous';
        matched_candidate_id := null;
        match_basis := null;
        normalized_linkedin_username := v_input_username;
        ambiguity_type := 'conflicting_incoming_linkedin_identity';
        return next;
        return;
    end if;

    normalized_linkedin_username := coalesce(v_input_username, v_url_username);

    if incoming_candidate_id is not null then
        select id
        into v_existing_candidate_id
        from public.candidate_profiles_v2
        where id = incoming_candidate_id;
    end if;

    if normalized_linkedin_username is not null then
        select id
        into v_candidate_by_username
        from public.candidate_profiles_v2
        where linkedin_username = normalized_linkedin_username;
    end if;

    if normalized_linkedin_url is not null then
        select id
        into v_candidate_by_url
        from public.candidate_profiles_v2
        where linkedin_url_normalized = normalized_linkedin_url;
    end if;

    if v_existing_candidate_id is not null then
        if (v_candidate_by_username is not null and v_candidate_by_username <> v_existing_candidate_id)
           or (v_candidate_by_url is not null and v_candidate_by_url <> v_existing_candidate_id) then
            decision := 'ambiguous';
            matched_candidate_id := null;
            match_basis := null;
            ambiguity_type := 'legacy_id_conflicts_with_linkedin_identity';
            return next;
            return;
        end if;

        decision := 'match_existing';
        matched_candidate_id := v_existing_candidate_id;
        match_basis := 'legacy_candidate_id';
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_candidate_by_username is not null
       and v_candidate_by_url is not null
       and v_candidate_by_username <> v_candidate_by_url then
        decision := 'ambiguous';
        matched_candidate_id := null;
        match_basis := null;
        ambiguity_type := 'conflicting_existing_linkedin_identity';
        return next;
        return;
    end if;

    if v_candidate_by_username is not null then
        decision := 'match_existing';
        matched_candidate_id := v_candidate_by_username;
        match_basis := case
            when v_candidate_by_url = v_candidate_by_username and normalized_linkedin_url is not null
                then 'linkedin_username_and_url'
            else 'linkedin_username'
        end;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_candidate_by_url is not null then
        decision := 'match_existing';
        matched_candidate_id := v_candidate_by_url;
        match_basis := 'linkedin_url_normalized';
        ambiguity_type := null;
        return next;
        return;
    end if;

    decision := 'create_new';
    matched_candidate_id := null;
    match_basis := null;
    ambiguity_type := null;
    return next;
end;
$$;

create or replace function public.resolve_company_match(
    incoming_linkedin_id text,
    incoming_linkedin_username text,
    incoming_linkedin_url text,
    incoming_name text
)
returns table (
    decision text,
    matched_company_id uuid,
    match_basis text,
    normalized_name text,
    normalized_linkedin_username text,
    normalized_linkedin_url text,
    ambiguity_type text
)
language plpgsql
stable
as $$
declare
    v_linkedin_id text;
    v_input_username text;
    v_url_username text;
    v_normalized_name text;
    v_company_by_id uuid;
    v_company_by_username uuid;
    v_company_by_username_linkedin_id text;
    v_company_by_url uuid;
    v_company_by_url_linkedin_id text;
    v_fallback_company_id uuid;
    v_fallback_count integer;
begin
    v_linkedin_id := nullif(lower(btrim(coalesce(incoming_linkedin_id, ''))), '');
    v_input_username := public.normalize_linkedin_username(incoming_linkedin_username);
    normalized_linkedin_url := public.normalize_company_linkedin_url(incoming_linkedin_url);
    v_url_username := public.extract_company_linkedin_username_from_url(incoming_linkedin_url);
    v_normalized_name := public.normalize_company_name(incoming_name);
    normalized_name := v_normalized_name;

    if v_input_username is not null
       and v_url_username is not null
       and v_input_username <> v_url_username then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        normalized_linkedin_username := v_input_username;
        ambiguity_type := 'conflicting_incoming_linkedin_identity';
        return next;
        return;
    end if;

    normalized_linkedin_username := coalesce(v_input_username, v_url_username);

    if v_linkedin_id is not null then
        select id
        into v_company_by_id
        from public.companies_v2
        where linkedin_id = v_linkedin_id;
    end if;

    if normalized_linkedin_username is not null then
        select id, linkedin_id
        into v_company_by_username, v_company_by_username_linkedin_id
        from public.companies_v2
        where linkedin_username = normalized_linkedin_username;
    end if;

    if normalized_linkedin_url is not null then
        select id, linkedin_id
        into v_company_by_url, v_company_by_url_linkedin_id
        from public.companies_v2
        where linkedin_url_normalized = normalized_linkedin_url;
    end if;

    if v_company_by_id is not null
       and v_company_by_username is not null
       and v_company_by_id <> v_company_by_username then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'conflicting_existing_strong_identity';
        return next;
        return;
    end if;

    if v_company_by_id is not null
       and v_company_by_url is not null
       and v_company_by_id <> v_company_by_url then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'conflicting_existing_strong_identity';
        return next;
        return;
    end if;

    if v_company_by_username is not null
       and v_company_by_url is not null
       and v_company_by_username <> v_company_by_url then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'conflicting_existing_strong_identity';
        return next;
        return;
    end if;

    if v_linkedin_id is not null
       and v_company_by_id is null
       and v_company_by_username is not null
       and v_company_by_username_linkedin_id is not null
       and v_company_by_username_linkedin_id <> v_linkedin_id then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'linkedin_id_conflicts_with_username_match';
        return next;
        return;
    end if;

    if v_linkedin_id is not null
       and v_company_by_id is null
       and v_company_by_url is not null
       and v_company_by_url_linkedin_id is not null
       and v_company_by_url_linkedin_id <> v_linkedin_id then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'linkedin_id_conflicts_with_url_match';
        return next;
        return;
    end if;

    if v_company_by_id is not null then
        decision := 'match_existing';
        matched_company_id := v_company_by_id;
        match_basis := 'linkedin_id';
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_company_by_username is not null then
        decision := 'match_existing';
        matched_company_id := v_company_by_username;
        match_basis := case
            when v_company_by_url = v_company_by_username and normalized_linkedin_url is not null
                then 'linkedin_username_and_url'
            else 'linkedin_username'
        end;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_company_by_url is not null then
        decision := 'match_existing';
        matched_company_id := v_company_by_url;
        match_basis := 'linkedin_url_normalized';
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_linkedin_id is not null
       or normalized_linkedin_username is not null
       or normalized_linkedin_url is not null then
        decision := 'create_new';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_normalized_name is null then
        decision := 'create_new';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := null;
        return next;
        return;
    end if;

    select min(c.id::text)::uuid, count(*)
    into v_fallback_company_id, v_fallback_count
    from public.companies_v2 as c
    where c.normalized_name = v_normalized_name;

    if v_fallback_count > 1 then
        decision := 'ambiguous';
        matched_company_id := null;
        match_basis := null;
        ambiguity_type := 'multiple_normalized_name_matches';
        return next;
        return;
    end if;

    if v_fallback_count = 1 then
        decision := 'match_existing';
        matched_company_id := v_fallback_company_id;
        match_basis := 'normalized_name';
        ambiguity_type := null;
        return next;
        return;
    end if;

    decision := 'create_new';
    matched_company_id := null;
    match_basis := null;
    ambiguity_type := null;
    return next;
end;
$$;

create or replace function public.decide_candidate_source_document_action(
    incoming_candidate_id uuid,
    incoming_source_type text,
    incoming_source_subtype text,
    incoming_title text,
    incoming_source_url text,
    incoming_external_source_ref text,
    incoming_normalized_text text,
    incoming_metadata_json jsonb
)
returns table (
    decision text,
    matched_document_id uuid,
    next_document_version integer,
    document_identity_key text,
    content_hash text,
    ambiguity_type text
)
language plpgsql
stable
as $$
declare
    v_source_type text;
    v_existing_id uuid;
    v_existing_version integer;
    v_match_count integer;
    v_anonymous_count integer;
    v_same_content_count integer;
begin
    if incoming_candidate_id is null then
        raise exception 'incoming_candidate_id is required';
    end if;

    v_source_type := lower(btrim(coalesce(incoming_source_type, '')));

    if v_source_type = '' then
        raise exception 'incoming_source_type is required';
    end if;

    document_identity_key := public.build_candidate_source_document_identity_key(
        incoming_source_type,
        incoming_source_subtype,
        incoming_source_url,
        incoming_external_source_ref,
        coalesce(incoming_metadata_json, '{}'::jsonb)
    );

    content_hash := public.build_candidate_source_document_content_hash(
        incoming_source_type,
        incoming_title,
        incoming_normalized_text,
        coalesce(incoming_metadata_json, '{}'::jsonb)
    );

    if v_source_type = 'linkedin_profile' then
        select count(*), min(id::text)::uuid, max(document_version)
        into v_match_count, v_existing_id, v_existing_version
        from public.candidate_source_documents
        where candidate_id = incoming_candidate_id
          and source_type = 'linkedin_profile'
          and is_active;

        if v_match_count > 1 then
            decision := 'ambiguous';
            matched_document_id := null;
            next_document_version := null;
            ambiguity_type := 'multiple_active_linkedin_profiles';
            return next;
            return;
        end if;

        if v_match_count = 1 then
            if public.build_candidate_source_document_content_hash(
                incoming_source_type,
                (select title from public.candidate_source_documents where id = v_existing_id),
                (select normalized_text from public.candidate_source_documents where id = v_existing_id),
                coalesce((select metadata_json from public.candidate_source_documents where id = v_existing_id), '{}'::jsonb)
            ) = content_hash then
                decision := 'no_op';
                matched_document_id := v_existing_id;
                next_document_version := v_existing_version;
                ambiguity_type := null;
                return next;
                return;
            end if;

            decision := 'supersede';
            matched_document_id := v_existing_id;
            next_document_version := v_existing_version + 1;
            ambiguity_type := null;
            return next;
            return;
        end if;

        decision := 'parallel';
        matched_document_id := null;
        next_document_version := 1;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if document_identity_key is not null then
        select count(*), min(id::text)::uuid, max(document_version)
        into v_match_count, v_existing_id, v_existing_version
        from public.candidate_source_documents
        where candidate_id = incoming_candidate_id
          and source_type = v_source_type
          and is_active
          and public.build_candidate_source_document_identity_key(
                source_type,
                source_subtype,
                source_url,
                external_source_ref,
                coalesce(metadata_json, '{}'::jsonb)
              ) = document_identity_key;

        if v_match_count > 1 then
            decision := 'ambiguous';
            matched_document_id := null;
            next_document_version := null;
            ambiguity_type := 'multiple_active_document_identity_matches';
            return next;
            return;
        end if;

        if v_match_count = 1 then
            if public.build_candidate_source_document_content_hash(
                incoming_source_type,
                (select title from public.candidate_source_documents where id = v_existing_id),
                (select normalized_text from public.candidate_source_documents where id = v_existing_id),
                coalesce((select metadata_json from public.candidate_source_documents where id = v_existing_id), '{}'::jsonb)
            ) = content_hash then
                decision := 'no_op';
                matched_document_id := v_existing_id;
                next_document_version := v_existing_version;
                ambiguity_type := null;
                return next;
                return;
            end if;

            decision := 'supersede';
            matched_document_id := v_existing_id;
            next_document_version := v_existing_version + 1;
            ambiguity_type := null;
            return next;
            return;
        end if;

        decision := 'parallel';
        matched_document_id := null;
        next_document_version := 1;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_source_type = 'resume' then
        select count(*)
        into v_anonymous_count
        from public.candidate_source_documents
        where candidate_id = incoming_candidate_id
          and source_type = 'resume'
          and is_active
          and public.build_candidate_source_document_identity_key(
                source_type,
                source_subtype,
                source_url,
                external_source_ref,
                coalesce(metadata_json, '{}'::jsonb)
              ) is null;

        select count(*), min(id::text)::uuid, max(document_version)
        into v_same_content_count, v_existing_id, v_existing_version
        from public.candidate_source_documents
        where candidate_id = incoming_candidate_id
          and source_type = 'resume'
          and is_active
          and public.build_candidate_source_document_identity_key(
                source_type,
                source_subtype,
                source_url,
                external_source_ref,
                coalesce(metadata_json, '{}'::jsonb)
              ) is null
          and public.build_candidate_source_document_content_hash(
                source_type,
                title,
                normalized_text,
                coalesce(metadata_json, '{}'::jsonb)
              ) = content_hash;

        if v_same_content_count = 1 then
            decision := 'no_op';
            matched_document_id := v_existing_id;
            next_document_version := v_existing_version;
            ambiguity_type := null;
            return next;
            return;
        end if;

        if v_same_content_count > 1 then
            decision := 'ambiguous';
            matched_document_id := null;
            next_document_version := null;
            ambiguity_type := 'multiple_anonymous_resume_content_matches';
            return next;
            return;
        end if;

        if v_anonymous_count = 0 then
            decision := 'parallel';
            matched_document_id := null;
            next_document_version := 1;
            ambiguity_type := null;
            return next;
            return;
        end if;

        decision := 'ambiguous';
        matched_document_id := null;
        next_document_version := null;
        ambiguity_type := 'anonymous_resume_identity_conflict';
        return next;
        return;
    end if;

    select count(*), min(id::text)::uuid, max(document_version)
    into v_same_content_count, v_existing_id, v_existing_version
    from public.candidate_source_documents
    where candidate_id = incoming_candidate_id
      and source_type = v_source_type
      and is_active
      and public.build_candidate_source_document_content_hash(
            source_type,
            title,
            normalized_text,
            coalesce(metadata_json, '{}'::jsonb)
          ) = content_hash;

    if v_same_content_count = 1 then
        decision := 'no_op';
        matched_document_id := v_existing_id;
        next_document_version := v_existing_version;
        ambiguity_type := null;
        return next;
        return;
    end if;

    if v_same_content_count > 1 then
        decision := 'ambiguous';
        matched_document_id := null;
        next_document_version := null;
        ambiguity_type := 'multiple_same_content_matches';
        return next;
        return;
    end if;

    decision := 'parallel';
    matched_document_id := null;
    next_document_version := 1;
    ambiguity_type := null;
    return next;
end;
$$;

create or replace function public.record_canonicalization_ambiguity(
    p_entity_type text,
    p_ambiguity_type text,
    p_source_system text,
    p_source_record_ref text,
    p_normalized_input jsonb,
    p_matched_record_ids uuid[],
    p_recommended_action text default 'manual_review'
)
returns uuid
language plpgsql
volatile
as $$
declare
    v_new_id uuid;
begin
    insert into public.canonicalization_ambiguities (
        entity_type,
        ambiguity_type,
        source_system,
        source_record_ref,
        normalized_input,
        matched_record_ids,
        recommended_action
    )
    values (
        p_entity_type,
        p_ambiguity_type,
        p_source_system,
        p_source_record_ref,
        coalesce(p_normalized_input, '{}'::jsonb),
        p_matched_record_ids,
        coalesce(p_recommended_action, 'manual_review')
    )
    on conflict (
        entity_type,
        ambiguity_type,
        (coalesce(source_system, '')),
        (coalesce(source_record_ref, '')),
        (md5(normalized_input::text))
    )
    where status = 'open'
    do update
        set updated_at = public.canonicalization_ambiguities.updated_at
    returning id into v_new_id;

    return v_new_id;
end;
$$;

commit;
