begin;

set local search_path = public, extensions;

create schema if not exists extensions;

create extension if not exists citext with schema extensions;
create extension if not exists "uuid-ossp" with schema extensions;

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

create table public.companies_v2 (
    id uuid not null default uuid_generate_v4(),
    name text not null,
    normalized_name text not null,
    linkedin_id text,
    linkedin_username text,
    linkedin_url text,
    linkedin_url_normalized text,
    website text,
    description text,
    industries text[],
    specialties text[],
    company_type text,
    staff_count integer,
    staff_count_range text,
    headquarters_city text,
    headquarters_country text,
    logo_url text,
    enrichment_status text,
    last_enrichment_sync timestamptz,
    data_source text not null,
    identity_basis text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint companies_v2_pkey primary key (id)
);

create table public.candidate_profiles_v2 (
    id uuid not null,
    full_name text,
    first_name text,
    last_name text,
    linkedin_username text,
    linkedin_url text,
    linkedin_url_normalized text,
    headline text,
    summary text,
    location text,
    profile_picture_url text,
    phone text,
    education_summary text,
    education_schools text[],
    education_degrees text[],
    education_fields text[],
    skills_text text,
    top_skills text[],
    current_title text,
    current_company_id uuid,
    current_company_name text,
    experience_years numeric(5, 2),
    source text not null,
    source_record_refs jsonb,
    linkedin_enrichment_status text,
    linkedin_enrichment_date timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_profiles_v2_pkey primary key (id),
    constraint candidate_profiles_v2_current_company_id_fkey
        foreign key (current_company_id)
        references public.companies_v2 (id)
);

create table public.candidate_emails_v2 (
    id uuid not null default uuid_generate_v4(),
    candidate_id uuid not null,
    email_raw text not null,
    email_normalized citext not null,
    email_type text,
    email_source text,
    is_primary boolean not null default false,
    quality text,
    result text,
    resultcode text,
    subresult text,
    verification_date timestamptz,
    verification_attempts integer not null default 0,
    last_verification_attempt timestamptz,
    raw_response jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_emails_v2_pkey primary key (id),
    constraint candidate_emails_v2_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade,
    constraint candidate_emails_v2_candidate_id_email_normalized_key
        unique (candidate_id, email_normalized)
);

create table public.candidate_experiences_v2 (
    id uuid not null default uuid_generate_v4(),
    candidate_id uuid not null,
    company_id uuid,
    experience_index integer not null,
    title text,
    description text,
    location text,
    raw_company_name text,
    source_company_linkedin_username text,
    start_date date,
    start_date_precision text,
    end_date date,
    end_date_precision text,
    is_current boolean not null default false,
    source_payload jsonb,
    source_hash text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint candidate_experiences_v2_pkey primary key (id),
    constraint candidate_experiences_v2_candidate_id_fkey
        foreign key (candidate_id)
        references public.candidate_profiles_v2 (id)
        on delete cascade,
    constraint candidate_experiences_v2_company_id_fkey
        foreign key (company_id)
        references public.companies_v2 (id)
        on delete set null,
    constraint candidate_experiences_v2_candidate_id_source_hash_key
        unique (candidate_id, source_hash)
);

create unique index candidate_profiles_v2_linkedin_username_uq
    on public.candidate_profiles_v2 (linkedin_username)
    where linkedin_username is not null;

create unique index candidate_profiles_v2_linkedin_url_normalized_uq
    on public.candidate_profiles_v2 (linkedin_url_normalized)
    where linkedin_url_normalized is not null;

create index candidate_profiles_v2_current_company_id_idx
    on public.candidate_profiles_v2 (current_company_id);

create index candidate_profiles_v2_updated_at_idx
    on public.candidate_profiles_v2 (updated_at);

create unique index candidate_emails_v2_email_normalized_uq
    on public.candidate_emails_v2 (email_normalized)
    where email_normalized is not null;

create unique index candidate_emails_v2_one_primary_email_uq
    on public.candidate_emails_v2 (candidate_id)
    where is_primary;

create index candidate_emails_v2_candidate_id_idx
    on public.candidate_emails_v2 (candidate_id);

create unique index companies_v2_linkedin_id_uq
    on public.companies_v2 (linkedin_id)
    where linkedin_id is not null;

create unique index companies_v2_linkedin_username_uq
    on public.companies_v2 (linkedin_username)
    where linkedin_username is not null;

create unique index companies_v2_linkedin_url_normalized_uq
    on public.companies_v2 (linkedin_url_normalized)
    where linkedin_url_normalized is not null;

create index companies_v2_normalized_name_idx
    on public.companies_v2 (normalized_name);

create index candidate_experiences_v2_candidate_id_experience_index_idx
    on public.candidate_experiences_v2 (candidate_id, experience_index);

create index candidate_experiences_v2_company_id_idx
    on public.candidate_experiences_v2 (company_id);

create index candidate_experiences_v2_candidate_id_is_current_idx
    on public.candidate_experiences_v2 (candidate_id, is_current);

create trigger set_companies_v2_updated_at
before update on public.companies_v2
for each row
execute function public.set_updated_at();

create trigger set_candidate_profiles_v2_updated_at
before update on public.candidate_profiles_v2
for each row
execute function public.set_updated_at();

create trigger set_candidate_emails_v2_updated_at
before update on public.candidate_emails_v2
for each row
execute function public.set_updated_at();

create trigger set_candidate_experiences_v2_updated_at
before update on public.candidate_experiences_v2
for each row
execute function public.set_updated_at();

commit;
