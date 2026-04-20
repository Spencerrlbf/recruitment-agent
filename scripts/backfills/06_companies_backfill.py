#!/usr/bin/env python3
"""Checkpoint-aware canonical company backfill script.

Task coverage:
- Task 6a: implement the script, run preflight dry-run on the first 100 rows,
  and run duplicate-validation fixtures in a rolled-back transaction
- Task 6b: later reuse this script in apply mode for a 100-row pilot write
- Task 6c: later reuse this script in apply mode for the full migration

Design notes:
- source reads are deterministic and ordered by strong-identity rows first,
  then name-only fallback rows, then source id text
- dry-run uses the real insert/update logic inside a transaction that is rolled
  back, so duplicate handling within the inspected batch behaves like a real run
- duplicate validation uses temporary fixture writes inside a rolled-back
  transaction and never mutates the legacy source table
- the script avoids Python DB dependencies and talks to Postgres via `psql`
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.checkpoint import JsonCheckpoint
from scripts.lib.psql import PsqlClient, PsqlError, SupabaseLinkedClient, load_dsn

LOGGER = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_NAME = "06_companies_backfill"
DEFAULT_BATCH_SIZE = 500
DEFAULT_PREFLIGHT_LIMIT = 100
DEFAULT_SOURCE_ENV = "LEGACY_DATABASE_URL"
DEFAULT_TARGET_ENV = "DATABASE_URL"
LEGACY_SOURCE_TABLE = "public.companies"
LEGACY_SOURCE_SYSTEM = "legacy_companies"
INCOMING_DATA_SOURCE = "legacy_backfill"
REPORT_DIR = REPO_ROOT / "reports" / "qa"
STRONG_MATCH_BASES = {
    "linkedin_id",
    "linkedin_username",
    "linkedin_username_and_url",
    "linkedin_url_normalized",
}
TASK5_MIGRATION_PATH = (
    REPO_ROOT
    / "db"
    / "migrations"
    / "20260420120000__add_canonicalization_helpers_and_ambiguity_logging.sql"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill canonical companies from the legacy companies table."
    )
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help=(
            "Execution mode. `dry-run` performs the real per-row logic inside a "
            "rolled-back transaction. `apply` commits writes to companies_v2."
        ),
    )
    parser.add_argument(
        "--checkpoint-name",
        default=DEFAULT_CHECKPOINT_NAME,
        help="Checkpoint file stem stored under scripts/checkpoints/.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Deterministic source fetch batch size.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional max source row count for this run. Defaults to 100 for "
            "dry-run mode and unlimited for apply mode."
        ),
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Reset a completed checkpoint and start from the beginning.",
    )
    parser.add_argument(
        "--confirm-apply",
        action="store_true",
        help="Required when --mode=apply because it performs committed writes.",
    )
    parser.add_argument(
        "--run-duplicate-validation",
        action="store_true",
        help=(
            "Run rolled-back duplicate-validation fixtures against companies_v2 "
            "after the main pass."
        ),
    )
    parser.add_argument(
        "--skip-duplicate-validation",
        action="store_true",
        help=(
            "Skip duplicate-validation fixtures. Dry-run mode runs them by "
            "default because Task 6a requires them."
        ),
    )
    parser.add_argument(
        "--confirm-duplicate-fixture-writes",
        action="store_true",
        help=(
            "Required with --run-duplicate-validation. Fixtures write temporary "
            "rows inside a transaction that is rolled back."
        ),
    )
    parser.add_argument(
        "--source-dsn-env",
        default=DEFAULT_SOURCE_ENV,
        help="Environment variable containing the legacy/source database DSN.",
    )
    parser.add_argument(
        "--target-dsn-env",
        default=DEFAULT_TARGET_ENV,
        help="Environment variable containing the target database DSN.",
    )
    parser.add_argument(
        "--report-dir",
        default=str(REPORT_DIR),
        help="Directory for generated QA reports.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=8,
        help="Max sample rows to keep per result category in the QA report.",
    )
    parser.add_argument(
        "--linked-workdir",
        default=None,
        help=(
            "Use `supabase db query --linked` from the given Supabase project "
            "directory instead of raw Postgres DSNs."
        ),
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size must be > 0.")

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be > 0 when provided.")

    if args.mode == "apply" and not args.confirm_apply:
        parser.error("--confirm-apply is required with --mode=apply.")

    if args.run_duplicate_validation and args.skip_duplicate_validation:
        parser.error(
            "--run-duplicate-validation and --skip-duplicate-validation cannot "
            "be used together."
        )

    if should_run_duplicate_validation(args) and not args.confirm_duplicate_fixture_writes:
        parser.error(
            "--confirm-duplicate-fixture-writes is required when duplicate "
            "validation is enabled."
        )

    return args


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def should_run_duplicate_validation(args: argparse.Namespace) -> bool:
    if args.skip_duplicate_validation:
        return False
    if args.run_duplicate_validation:
        return True
    return args.mode == "dry-run"


def blank_to_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return value


def normalize_linkedin_id(value: Any) -> str | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    return str(cleaned).lower()


def clean_text_list(values: Any) -> list[str] | None:
    if not values:
        return None

    cleaned_values: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = blank_to_none(value)
        if cleaned is None:
            continue
        text = str(cleaned)
        if text not in seen:
            seen.add(text)
            cleaned_values.append(text)

    return cleaned_values or None


def clean_website(raw_value: Any) -> str | None:
    cleaned = blank_to_none(raw_value)
    if cleaned is None:
        return None

    raw_text = str(cleaned)
    if "://" not in raw_text:
        raw_text = f"https://{raw_text}"

    parsed = urlparse(raw_text)
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or ""

    if not netloc and path:
        netloc = path.lower()
        path = ""

    if not netloc:
        return None

    return f"{scheme}://{netloc}"


def derive_staff_count_range(
    staff_count: int | None,
    fallback_range: str | None,
) -> str | None:
    if staff_count is None:
        return blank_to_none(fallback_range)

    if staff_count <= 0:
        return None
    if staff_count <= 10:
        return "1-10"
    if staff_count <= 50:
        return "11-50"
    if staff_count <= 200:
        return "51-200"
    if staff_count <= 500:
        return "201-500"
    if staff_count <= 1000:
        return "501-1000"
    if staff_count <= 5000:
        return "1001-5000"
    if staff_count <= 10000:
        return "5001-10000"
    return "10001+"


def clean_optional_timestamptz(value: Any) -> str | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None

    text = str(cleaned)
    normalized = text.replace("Z", "+00:00")

    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        return None

    return text


def identity_phase(raw_record: Mapping[str, Any]) -> int:
    if any(
        blank_to_none(raw_record.get(field)) is not None
        for field in ("linkedin_id", "linkedin_username", "linkedin_url")
    ):
        return 0
    return 1


def prepare_source_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    source_id = str(raw_record["source_id"])
    raw_name = raw_record.get("name")
    raw_website = raw_record.get("website")
    raw_linkedin_id = raw_record.get("linkedin_id")
    raw_linkedin_username = raw_record.get("linkedin_username")
    raw_linkedin_url = raw_record.get("linkedin_url")
    raw_legacy_data_source = raw_record.get("legacy_data_source")

    staff_count = raw_record.get("staff_count")
    if staff_count is not None:
        staff_count = int(staff_count)

    prepared = {
        "source_id": source_id,
        "identity_phase": identity_phase(raw_record),
        "name": blank_to_none(raw_name),
        "linkedin_id": normalize_linkedin_id(raw_linkedin_id),
        "linkedin_username": blank_to_none(raw_linkedin_username),
        "linkedin_url": blank_to_none(raw_linkedin_url),
        "website": clean_website(raw_website),
        "description": blank_to_none(raw_record.get("description")),
        "industries": clean_text_list(raw_record.get("industries")),
        "specialties": clean_text_list(raw_record.get("specialties")),
        "company_type": blank_to_none(raw_record.get("company_type")),
        "staff_count": staff_count,
        "staff_count_range": derive_staff_count_range(
            staff_count,
            blank_to_none(raw_record.get("staff_count_range")),
        ),
        "headquarters_city": blank_to_none(raw_record.get("headquarters_city")),
        "headquarters_country": blank_to_none(raw_record.get("headquarters_country")),
        "logo_url": blank_to_none(raw_record.get("logo_url")),
        "enrichment_status": blank_to_none(raw_record.get("enrichment_status")),
        "last_enrichment_sync": clean_optional_timestamptz(
            raw_record.get("last_enrichment_sync")
        ),
        "incoming_source": INCOMING_DATA_SOURCE,
        "source_record_refs": [
            {
                "source_table": LEGACY_SOURCE_TABLE,
                "source_id": source_id,
                "raw_name": raw_name,
                "raw_website": raw_website,
                "raw_linkedin_id": raw_linkedin_id,
                "raw_linkedin_username": raw_linkedin_username,
                "raw_linkedin_url": raw_linkedin_url,
                "legacy_data_source": raw_legacy_data_source,
            }
        ],
    }

    return prepared


def sql_text_literal(value: str | None) -> str:
    if value is None:
        return "null"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_jsonb_literal(value: Any) -> str:
    return f"{sql_text_literal(json.dumps(value, sort_keys=True))}::jsonb"


def fetch_source_columns(source_db: PsqlClient) -> set[str]:
    sql = """
select row_to_json(t)::text
from (
    select column_name
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'companies'
    order by ordinal_position
) as t;
"""
    rows = source_db.query_json_rows(sql)
    return {str(row["column_name"]) for row in rows}


def fetch_target_prerequisites(target_db: PsqlClient | SupabaseLinkedClient) -> dict[str, Any]:
    sql = """
select json_build_object(
  'companies_v2_exists', to_regclass('public.companies_v2') is not null,
  'canonicalization_ambiguities_exists', to_regclass('public.canonicalization_ambiguities') is not null,
  'resolve_company_match_exists', to_regprocedure('public.resolve_company_match(text,text,text,text)') is not null,
  'record_canonicalization_ambiguity_exists', to_regprocedure('public.record_canonicalization_ambiguity(text,text,text,text,jsonb,uuid[],text)') is not null,
  'normalize_company_name_exists', to_regprocedure('public.normalize_company_name(text)') is not null,
  'should_replace_canonical_value_exists', to_regprocedure('public.should_replace_canonical_value(text,text,text,text)') is not null,
  'source_record_refs_exists', exists (
      select 1
      from information_schema.columns
      where table_schema='public'
        and table_name='companies_v2'
        and column_name='source_record_refs'
  )
) as status;
"""
    row = target_db.query_json_row(sql)
    if row is None:
        raise RuntimeError("Could not determine target prerequisite status.")
    status = row.get("status", row)
    if not isinstance(status, dict):
        raise RuntimeError(f"Unexpected prerequisite status payload: {row!r}")
    return status


def build_dry_run_bootstrap_sql(
    target_db: PsqlClient | SupabaseLinkedClient,
    *,
    enabled: bool,
) -> str | None:
    if not enabled:
        return None

    status = fetch_target_prerequisites(target_db)
    required_truthy = [
        "companies_v2_exists",
        "canonicalization_ambiguities_exists",
        "resolve_company_match_exists",
        "record_canonicalization_ambiguity_exists",
        "normalize_company_name_exists",
        "should_replace_canonical_value_exists",
        "source_record_refs_exists",
    ]

    if all(bool(status.get(key)) for key in required_truthy):
        return None

    migration_sql = TASK5_MIGRATION_PATH.read_text(encoding="utf-8")
    stripped_lines: list[str] = []
    for raw_line in migration_sql.splitlines():
        line = raw_line.strip()
        if line.lower() in {"begin;", "commit;"}:
            continue
        stripped_lines.append(raw_line)

    bootstrap_sql = "\n".join(stripped_lines)
    bootstrap_sql = bootstrap_sql.replace(
        "alter table public.companies_v2\n    add column source_record_refs jsonb;",
        "alter table public.companies_v2\n    add column if not exists source_record_refs jsonb;",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create table public.canonicalization_ambiguities (",
        "create table if not exists public.canonicalization_ambiguities (",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create index canonicalization_ambiguities_status_entity_type_idx",
        "create index if not exists canonicalization_ambiguities_status_entity_type_idx",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create index canonicalization_ambiguities_entity_ambiguity_type_idx",
        "create index if not exists canonicalization_ambiguities_entity_ambiguity_type_idx",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create index canonicalization_ambiguities_source_ref_idx",
        "create index if not exists canonicalization_ambiguities_source_ref_idx",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create unique index canonicalization_ambiguities_open_identity_uq",
        "create unique index if not exists canonicalization_ambiguities_open_identity_uq",
    )
    bootstrap_sql = bootstrap_sql.replace(
        "create trigger set_canonicalization_ambiguities_updated_at",
        "create or replace trigger set_canonicalization_ambiguities_updated_at",
    )

    return bootstrap_sql


def source_select_expression(column_name: str, available_columns: set[str]) -> str:
    if column_name in available_columns:
        return f"c.{column_name}"

    null_by_type = {
        "industries": "null::text[]",
        "specialties": "null::text[]",
        "staff_count": "null::integer",
        "last_enrichment_sync": "null::timestamptz",
    }
    return null_by_type.get(column_name, "null::text")


def build_source_batch_sql(
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    *,
    available_columns: set[str],
) -> str:
    if cursor is None:
        cursor_clause = "true"
    else:
        cursor_phase = int(cursor["phase"])
        cursor_source_id = str(cursor["source_id"])
        cursor_clause = f"""(
            ranked.identity_phase > {cursor_phase}
            or (
                ranked.identity_phase = {cursor_phase}
                and ranked.source_id > {sql_text_literal(cursor_source_id)}
            )
        )"""

    return f"""
with ranked as (
    select
        c.id::text as source_id,
        {source_select_expression('name', available_columns)} as name,
        {source_select_expression('linkedin_id', available_columns)} as linkedin_id,
        {source_select_expression('linkedin_username', available_columns)} as linkedin_username,
        {source_select_expression('linkedin_url', available_columns)} as linkedin_url,
        {source_select_expression('website', available_columns)} as website,
        {source_select_expression('description', available_columns)} as description,
        {source_select_expression('industries', available_columns)} as industries,
        {source_select_expression('specialties', available_columns)} as specialties,
        {source_select_expression('company_type', available_columns)} as company_type,
        {source_select_expression('staff_count', available_columns)} as staff_count,
        {source_select_expression('staff_count_range', available_columns)} as staff_count_range,
        {source_select_expression('headquarters_city', available_columns)} as headquarters_city,
        {source_select_expression('headquarters_country', available_columns)} as headquarters_country,
        {source_select_expression('logo_url', available_columns)} as logo_url,
        {source_select_expression('enrichment_status', available_columns)} as enrichment_status,
        {source_select_expression('last_enrichment_sync', available_columns)} as last_enrichment_sync,
        {source_select_expression('data_source', available_columns)} as legacy_data_source,
        case
            when nullif(btrim(coalesce({source_select_expression('linkedin_id', available_columns)}, '')), '') is not null
              or nullif(btrim(coalesce({source_select_expression('linkedin_username', available_columns)}, '')), '') is not null
              or nullif(btrim(coalesce({source_select_expression('linkedin_url', available_columns)}, '')), '') is not null
                then 0
            else 1
        end as identity_phase
    from {LEGACY_SOURCE_TABLE} as c
)
select row_to_json(t)::text
from (
    select *
    from ranked
    where {cursor_clause}
    order by identity_phase asc, source_id asc
    limit {batch_size}
) as t;
"""


def fetch_source_batch(
    source_db: PsqlClient,
    *,
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    available_columns: set[str],
) -> list[dict[str, Any]]:
    sql = build_source_batch_sql(
        cursor=cursor,
        batch_size=batch_size,
        available_columns=available_columns,
    )
    rows = source_db.query_json_rows(sql)
    return [prepare_source_record(row) for row in rows]


def next_cursor(
    prepared_batch: list[dict[str, Any]],
    current_cursor: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not prepared_batch:
        return current_cursor

    last = prepared_batch[-1]
    return {
        "phase": int(last["identity_phase"]),
        "source_id": str(last["source_id"]),
    }


def build_batch_sql(
    prepared_batch: list[dict[str, Any]],
    *,
    commit_writes: bool,
    prelude_sql: str | None = None,
) -> str:
    payload_literal = sql_jsonb_literal(prepared_batch)
    final_statement = "commit;" if commit_writes else "rollback;"
    prelude_block = f"{prelude_sql.strip()}\n" if prelude_sql else ""

    return f"""
begin;
set local search_path = public, extensions;

{prelude_block}create temp table tmp_company_input (
    source_id text not null,
    identity_phase integer not null,
    name text,
    linkedin_id text,
    linkedin_username text,
    linkedin_url text,
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
    incoming_source text not null,
    source_record_refs jsonb
) on commit drop;

insert into tmp_company_input (
    source_id,
    identity_phase,
    name,
    linkedin_id,
    linkedin_username,
    linkedin_url,
    website,
    description,
    industries,
    specialties,
    company_type,
    staff_count,
    staff_count_range,
    headquarters_city,
    headquarters_country,
    logo_url,
    enrichment_status,
    last_enrichment_sync,
    incoming_source,
    source_record_refs
)
select
    source_id,
    identity_phase,
    name,
    linkedin_id,
    linkedin_username,
    linkedin_url,
    website,
    description,
    industries,
    specialties,
    company_type,
    staff_count,
    staff_count_range,
    headquarters_city,
    headquarters_country,
    logo_url,
    enrichment_status,
    nullif(last_enrichment_sync, '')::timestamptz,
    incoming_source,
    source_record_refs
from jsonb_to_recordset({payload_literal}) as x(
    source_id text,
    identity_phase integer,
    name text,
    linkedin_id text,
    linkedin_username text,
    linkedin_url text,
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
    last_enrichment_sync text,
    incoming_source text,
    source_record_refs jsonb
)
order by identity_phase asc, source_id asc;

create temp table tmp_company_results (
    source_id text not null,
    identity_phase integer not null,
    effective_action text not null,
    original_decision text not null,
    company_id uuid,
    match_basis text,
    ambiguity_type text,
    skip_reason text,
    normalized_name text,
    normalized_linkedin_username text,
    normalized_linkedin_url text,
    identity_basis text,
    website text,
    source_record_refs jsonb
) on commit drop;

do $plpgsql$
declare
    rec tmp_company_input%rowtype;
    match_rec record;
    existing_row public.companies_v2%rowtype;
    v_company_id uuid;
    v_existing_rank integer;
    v_incoming_rank integer;
    v_name text;
    v_normalized_name text;
    v_linkedin_id text;
    v_linkedin_username text;
    v_linkedin_url text;
    v_linkedin_url_normalized text;
    v_website text;
    v_description text;
    v_industries text[];
    v_specialties text[];
    v_company_type text;
    v_staff_count integer;
    v_staff_count_range text;
    v_headquarters_city text;
    v_headquarters_country text;
    v_logo_url text;
    v_enrichment_status text;
    v_last_enrichment_sync timestamptz;
    v_data_source text;
    v_identity_basis text;
    v_source_record_refs jsonb;
    v_ambiguity_match_ids uuid[];
begin
    for rec in
        select *
        from tmp_company_input
        order by identity_phase asc, source_id asc
    loop
        select *
        into match_rec
        from public.resolve_company_match(
            rec.linkedin_id,
            rec.linkedin_username,
            rec.linkedin_url,
            rec.name
        );

        if match_rec.decision is null then
            raise exception 'resolve_company_match returned no decision for source_id=%', rec.source_id;
        end if;

        if match_rec.decision = 'ambiguous' then
            select array_agg(distinct c.id order by c.id)
            into v_ambiguity_match_ids
            from public.companies_v2 as c
            where (
                rec.linkedin_id is not null
                and c.linkedin_id = rec.linkedin_id
            )
            or (
                match_rec.normalized_linkedin_username is not null
                and c.linkedin_username = match_rec.normalized_linkedin_username
            )
            or (
                match_rec.normalized_linkedin_url is not null
                and c.linkedin_url_normalized = match_rec.normalized_linkedin_url
            )
            or (
                match_rec.ambiguity_type = 'multiple_normalized_name_matches'
                and match_rec.normalized_name is not null
                and c.normalized_name = match_rec.normalized_name
            );

            perform public.record_canonicalization_ambiguity(
                'company',
                match_rec.ambiguity_type,
                '{LEGACY_SOURCE_SYSTEM}',
                rec.source_id,
                jsonb_build_object(
                    'name', rec.name,
                    'normalized_name', match_rec.normalized_name,
                    'linkedin_id', rec.linkedin_id,
                    'linkedin_username', match_rec.normalized_linkedin_username,
                    'linkedin_url', rec.linkedin_url,
                    'linkedin_url_normalized', match_rec.normalized_linkedin_url,
                    'website', rec.website
                ),
                v_ambiguity_match_ids,
                'manual_review'
            );

            insert into tmp_company_results (
                source_id,
                identity_phase,
                effective_action,
                original_decision,
                company_id,
                match_basis,
                ambiguity_type,
                skip_reason,
                normalized_name,
                normalized_linkedin_username,
                normalized_linkedin_url,
                identity_basis,
                website,
                source_record_refs
            )
            values (
                rec.source_id,
                rec.identity_phase,
                'ambiguous',
                match_rec.decision,
                null,
                match_rec.match_basis,
                match_rec.ambiguity_type,
                null,
                match_rec.normalized_name,
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                case
                    when rec.linkedin_id is not null then 'linkedin_id'
                    when match_rec.normalized_linkedin_username is not null then 'linkedin_username'
                    when match_rec.normalized_linkedin_url is not null then 'linkedin_url'
                    else 'name'
                end,
                rec.website,
                rec.source_record_refs
            );
            continue;
        end if;

        if match_rec.decision = 'create_new'
           and (rec.name is null or match_rec.normalized_name is null) then
            insert into tmp_company_results (
                source_id,
                identity_phase,
                effective_action,
                original_decision,
                company_id,
                match_basis,
                ambiguity_type,
                skip_reason,
                normalized_name,
                normalized_linkedin_username,
                normalized_linkedin_url,
                identity_basis,
                website,
                source_record_refs
            )
            values (
                rec.source_id,
                rec.identity_phase,
                'skip',
                match_rec.decision,
                null,
                match_rec.match_basis,
                null,
                'missing_company_name',
                match_rec.normalized_name,
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                'name',
                rec.website,
                rec.source_record_refs
            );
            continue;
        end if;

        if match_rec.decision = 'create_new' then
            v_identity_basis := case
                when rec.linkedin_id is not null then 'linkedin_id'
                when match_rec.normalized_linkedin_username is not null then 'linkedin_username'
                when match_rec.normalized_linkedin_url is not null then 'linkedin_url'
                else 'name'
            end;

            insert into public.companies_v2 (
                name,
                normalized_name,
                linkedin_id,
                linkedin_username,
                linkedin_url,
                linkedin_url_normalized,
                website,
                description,
                industries,
                specialties,
                company_type,
                staff_count,
                staff_count_range,
                headquarters_city,
                headquarters_country,
                logo_url,
                enrichment_status,
                last_enrichment_sync,
                data_source,
                identity_basis,
                source_record_refs
            )
            values (
                rec.name,
                match_rec.normalized_name,
                rec.linkedin_id,
                match_rec.normalized_linkedin_username,
                rec.linkedin_url,
                match_rec.normalized_linkedin_url,
                rec.website,
                rec.description,
                rec.industries,
                rec.specialties,
                rec.company_type,
                rec.staff_count,
                rec.staff_count_range,
                rec.headquarters_city,
                rec.headquarters_country,
                rec.logo_url,
                rec.enrichment_status,
                rec.last_enrichment_sync,
                rec.incoming_source,
                v_identity_basis,
                coalesce(rec.source_record_refs, '[]'::jsonb)
            )
            returning id into v_company_id;

            insert into tmp_company_results (
                source_id,
                identity_phase,
                effective_action,
                original_decision,
                company_id,
                match_basis,
                ambiguity_type,
                skip_reason,
                normalized_name,
                normalized_linkedin_username,
                normalized_linkedin_url,
                identity_basis,
                website,
                source_record_refs
            )
            values (
                rec.source_id,
                rec.identity_phase,
                'create_new',
                match_rec.decision,
                v_company_id,
                match_rec.match_basis,
                null,
                null,
                match_rec.normalized_name,
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                v_identity_basis,
                rec.website,
                rec.source_record_refs
            );
            continue;
        end if;

        select *
        into existing_row
        from public.companies_v2
        where id = match_rec.matched_company_id
        for update;

        if not found then
            raise exception 'Matched company % not found for source_id=%', match_rec.matched_company_id, rec.source_id;
        end if;

        v_existing_rank := public.canonical_source_precedence_rank(existing_row.data_source);
        v_incoming_rank := public.canonical_source_precedence_rank(rec.incoming_source);

        v_name := existing_row.name;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.name, rec.name) then
            v_name := rec.name;
        end if;

        v_normalized_name := existing_row.normalized_name;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.normalized_name, match_rec.normalized_name) then
            v_normalized_name := match_rec.normalized_name;
        end if;

        v_linkedin_id := existing_row.linkedin_id;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.linkedin_id, rec.linkedin_id) then
            v_linkedin_id := rec.linkedin_id;
        end if;

        v_linkedin_username := existing_row.linkedin_username;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.linkedin_username, match_rec.normalized_linkedin_username) then
            v_linkedin_username := match_rec.normalized_linkedin_username;
        end if;

        v_linkedin_url := existing_row.linkedin_url;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.linkedin_url, rec.linkedin_url) then
            v_linkedin_url := rec.linkedin_url;
        end if;

        v_linkedin_url_normalized := existing_row.linkedin_url_normalized;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.linkedin_url_normalized, match_rec.normalized_linkedin_url) then
            v_linkedin_url_normalized := match_rec.normalized_linkedin_url;
        end if;

        v_website := existing_row.website;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.website, rec.website) then
            v_website := rec.website;
        end if;

        v_description := existing_row.description;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.description, rec.description) then
            v_description := rec.description;
        end if;

        v_company_type := existing_row.company_type;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.company_type, rec.company_type) then
            v_company_type := rec.company_type;
        end if;

        v_staff_count_range := existing_row.staff_count_range;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.staff_count_range, rec.staff_count_range) then
            v_staff_count_range := rec.staff_count_range;
        end if;

        v_headquarters_city := existing_row.headquarters_city;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.headquarters_city, rec.headquarters_city) then
            v_headquarters_city := rec.headquarters_city;
        end if;

        v_headquarters_country := existing_row.headquarters_country;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.headquarters_country, rec.headquarters_country) then
            v_headquarters_country := rec.headquarters_country;
        end if;

        v_logo_url := existing_row.logo_url;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.logo_url, rec.logo_url) then
            v_logo_url := rec.logo_url;
        end if;

        v_enrichment_status := existing_row.enrichment_status;
        if public.should_replace_canonical_value(existing_row.data_source, rec.incoming_source, existing_row.enrichment_status, rec.enrichment_status) then
            v_enrichment_status := rec.enrichment_status;
        end if;

        v_industries := existing_row.industries;
        if rec.industries is not null and cardinality(rec.industries) > 0 then
            if existing_row.industries is null or cardinality(existing_row.industries) = 0 or v_incoming_rank > v_existing_rank then
                v_industries := rec.industries;
            end if;
        end if;

        v_specialties := existing_row.specialties;
        if rec.specialties is not null and cardinality(rec.specialties) > 0 then
            if existing_row.specialties is null or cardinality(existing_row.specialties) = 0 or v_incoming_rank > v_existing_rank then
                v_specialties := rec.specialties;
            end if;
        end if;

        v_staff_count := existing_row.staff_count;
        if rec.staff_count is not null then
            if existing_row.staff_count is null or v_incoming_rank > v_existing_rank then
                v_staff_count := rec.staff_count;
            end if;
        end if;

        v_last_enrichment_sync := existing_row.last_enrichment_sync;
        if rec.last_enrichment_sync is not null then
            if existing_row.last_enrichment_sync is null or v_incoming_rank > v_existing_rank then
                v_last_enrichment_sync := rec.last_enrichment_sync;
            end if;
        end if;

        v_data_source := existing_row.data_source;
        if v_data_source is null or btrim(v_data_source) = '' or v_incoming_rank > v_existing_rank then
            v_data_source := rec.incoming_source;
        end if;

        select coalesce(jsonb_agg(elem order by elem::text), '[]'::jsonb)
        into v_source_record_refs
        from (
            select distinct elem
            from jsonb_array_elements(
                (case jsonb_typeof(coalesce(existing_row.source_record_refs, '[]'::jsonb))
                    when 'array' then coalesce(existing_row.source_record_refs, '[]'::jsonb)
                    when 'object' then jsonb_build_array(existing_row.source_record_refs)
                    else '[]'::jsonb
                 end)
                ||
                (case jsonb_typeof(coalesce(rec.source_record_refs, '[]'::jsonb))
                    when 'array' then coalesce(rec.source_record_refs, '[]'::jsonb)
                    when 'object' then jsonb_build_array(rec.source_record_refs)
                    else '[]'::jsonb
                 end)
            ) as elem
        ) as deduped;

        v_identity_basis := case
            when nullif(lower(btrim(coalesce(v_linkedin_id, ''))), '') is not null then 'linkedin_id'
            when v_linkedin_username is not null then 'linkedin_username'
            when v_linkedin_url_normalized is not null then 'linkedin_url'
            else 'name'
        end;

        if v_name is not distinct from existing_row.name
           and v_normalized_name is not distinct from existing_row.normalized_name
           and v_linkedin_id is not distinct from existing_row.linkedin_id
           and v_linkedin_username is not distinct from existing_row.linkedin_username
           and v_linkedin_url is not distinct from existing_row.linkedin_url
           and v_linkedin_url_normalized is not distinct from existing_row.linkedin_url_normalized
           and v_website is not distinct from existing_row.website
           and v_description is not distinct from existing_row.description
           and v_industries is not distinct from existing_row.industries
           and v_specialties is not distinct from existing_row.specialties
           and v_company_type is not distinct from existing_row.company_type
           and v_staff_count is not distinct from existing_row.staff_count
           and v_staff_count_range is not distinct from existing_row.staff_count_range
           and v_headquarters_city is not distinct from existing_row.headquarters_city
           and v_headquarters_country is not distinct from existing_row.headquarters_country
           and v_logo_url is not distinct from existing_row.logo_url
           and v_enrichment_status is not distinct from existing_row.enrichment_status
           and v_last_enrichment_sync is not distinct from existing_row.last_enrichment_sync
           and v_data_source is not distinct from existing_row.data_source
           and v_identity_basis is not distinct from existing_row.identity_basis
           and v_source_record_refs is not distinct from existing_row.source_record_refs then
            insert into tmp_company_results (
                source_id,
                identity_phase,
                effective_action,
                original_decision,
                company_id,
                match_basis,
                ambiguity_type,
                skip_reason,
                normalized_name,
                normalized_linkedin_username,
                normalized_linkedin_url,
                identity_basis,
                website,
                source_record_refs
            )
            values (
                rec.source_id,
                rec.identity_phase,
                'no_op',
                match_rec.decision,
                existing_row.id,
                match_rec.match_basis,
                null,
                null,
                match_rec.normalized_name,
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                v_identity_basis,
                v_website,
                v_source_record_refs
            );
            continue;
        end if;

        update public.companies_v2
        set
            name = v_name,
            normalized_name = v_normalized_name,
            linkedin_id = v_linkedin_id,
            linkedin_username = v_linkedin_username,
            linkedin_url = v_linkedin_url,
            linkedin_url_normalized = v_linkedin_url_normalized,
            website = v_website,
            description = v_description,
            industries = v_industries,
            specialties = v_specialties,
            company_type = v_company_type,
            staff_count = v_staff_count,
            staff_count_range = v_staff_count_range,
            headquarters_city = v_headquarters_city,
            headquarters_country = v_headquarters_country,
            logo_url = v_logo_url,
            enrichment_status = v_enrichment_status,
            last_enrichment_sync = v_last_enrichment_sync,
            data_source = v_data_source,
            identity_basis = v_identity_basis,
            source_record_refs = v_source_record_refs
        where id = existing_row.id;

        insert into tmp_company_results (
            source_id,
            identity_phase,
            effective_action,
            original_decision,
            company_id,
            match_basis,
            ambiguity_type,
            skip_reason,
            normalized_name,
            normalized_linkedin_username,
            normalized_linkedin_url,
            identity_basis,
            website,
            source_record_refs
        )
        values (
            rec.source_id,
            rec.identity_phase,
            'match_existing',
            match_rec.decision,
            existing_row.id,
            match_rec.match_basis,
            null,
            null,
            match_rec.normalized_name,
            match_rec.normalized_linkedin_username,
            match_rec.normalized_linkedin_url,
            v_identity_basis,
            v_website,
            v_source_record_refs
        );
    end loop;
end
$plpgsql$;

select row_to_json(t)::text
from (
    select
        source_id,
        identity_phase,
        effective_action,
        original_decision,
        company_id::text as company_id,
        match_basis,
        ambiguity_type,
        skip_reason,
        normalized_name,
        normalized_linkedin_username,
        normalized_linkedin_url,
        identity_basis,
        website,
        source_record_refs
    from tmp_company_results
    order by identity_phase asc, source_id asc
) as t;

{final_statement}
"""


def run_batch(
    target_db: PsqlClient,
    prepared_batch: list[dict[str, Any]],
    *,
    commit_writes: bool,
    prelude_sql: str | None = None,
) -> list[dict[str, Any]]:
    sql = build_batch_sql(
        prepared_batch,
        commit_writes=commit_writes,
        prelude_sql=prelude_sql,
    )
    return target_db.query_json_rows(sql)


def summarize_results(
    rows: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    action_counts: Counter[str] = Counter()
    match_basis_counts: Counter[str] = Counter()
    ambiguity_type_counts: Counter[str] = Counter()
    samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    rows_list = list(rows)
    for row in rows_list:
        action = str(row.get("effective_action"))
        action_counts[action] += 1

        match_basis = blank_to_none(row.get("match_basis"))
        if match_basis is not None:
            match_basis_counts[str(match_basis)] += 1

        ambiguity_type = blank_to_none(row.get("ambiguity_type"))
        if ambiguity_type is not None:
            ambiguity_type_counts[str(ambiguity_type)] += 1

        if len(samples[action]) < sample_limit:
            samples[action].append(
                {
                    "source_id": row.get("source_id"),
                    "company_id": row.get("company_id"),
                    "match_basis": row.get("match_basis"),
                    "ambiguity_type": row.get("ambiguity_type"),
                    "skip_reason": row.get("skip_reason"),
                    "normalized_name": row.get("normalized_name"),
                    "normalized_linkedin_username": row.get(
                        "normalized_linkedin_username"
                    ),
                    "normalized_linkedin_url": row.get("normalized_linkedin_url"),
                    "identity_basis": row.get("identity_basis"),
                    "website": row.get("website"),
                }
            )

    strong_identity_matches = sum(
        count for basis, count in match_basis_counts.items() if basis in STRONG_MATCH_BASES
    )

    return {
        "rows_processed": len(rows_list),
        "action_counts": dict(action_counts),
        "match_basis_counts": dict(match_basis_counts),
        "ambiguity_type_counts": dict(ambiguity_type_counts),
        "duplicate_reduction": {
            "strong_identity_matches": strong_identity_matches,
            "name_fallback_matches": match_basis_counts.get("normalized_name", 0),
        },
        "samples": dict(samples),
    }


def merge_summary(base: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    base["rows_processed"] += int(incoming["rows_processed"])

    for key in ("action_counts", "match_basis_counts", "ambiguity_type_counts"):
        counter = Counter(base[key])
        counter.update(incoming[key])
        base[key] = dict(counter)

    duplicate_reduction = dict(base["duplicate_reduction"])
    duplicate_reduction["strong_identity_matches"] += int(
        incoming["duplicate_reduction"]["strong_identity_matches"]
    )
    duplicate_reduction["name_fallback_matches"] += int(
        incoming["duplicate_reduction"]["name_fallback_matches"]
    )
    base["duplicate_reduction"] = duplicate_reduction

    for action, action_samples in incoming["samples"].items():
        existing = base["samples"].setdefault(action, [])
        remaining = max(0, base["sample_limit"] - len(existing))
        if remaining:
            existing.extend(action_samples[:remaining])

    return base


def base_summary(*, sample_limit: int) -> dict[str, Any]:
    return {
        "rows_processed": 0,
        "action_counts": {},
        "match_basis_counts": {},
        "ambiguity_type_counts": {},
        "duplicate_reduction": {
            "strong_identity_matches": 0,
            "name_fallback_matches": 0,
        },
        "samples": {},
        "sample_limit": sample_limit,
    }


def build_report_path(
    report_dir: Path,
    *,
    scope: str,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    base = report_dir / f"{stamp}__qa_{scope}.json"

    if not base.exists():
        return base

    counter = 2
    while True:
        candidate = report_dir / f"{stamp}__qa_{scope}_{counter}.json"
        if not candidate.exists():
            return candidate
        counter += 1


def write_report(report_dir: Path, report: Mapping[str, Any], *, scope: str) -> Path:
    path = build_report_path(report_dir, scope=scope)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def build_duplicate_fixture_prelude(prefix: str) -> str:
    fixtures = [
        {
            "name": f"{prefix} Existing Id Co",
            "normalized_name": f"{prefix.lower()} existing id",
            "linkedin_id": f"{prefix.lower()}-id-001",
            "linkedin_username": None,
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "name": f"{prefix} Existing Username Co",
            "normalized_name": f"{prefix.lower()} existing username",
            "linkedin_id": None,
            "linkedin_username": f"{prefix.lower()}-username-001",
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "name": f"{prefix} Existing Url Co",
            "normalized_name": f"{prefix.lower()} existing url",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": f"https://www.linkedin.com/company/{prefix.lower()}-url-001",
            "linkedin_url_normalized": f"https://www.linkedin.com/company/{prefix.lower()}-url-001",
        },
        {
            "name": f"{prefix} Name Only LLC",
            "normalized_name": f"{prefix.lower()} name only",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "name": f"{prefix} Conflict Username Co",
            "normalized_name": f"{prefix.lower()} conflict username",
            "linkedin_id": f"{prefix.lower()}-existing-conflict-id",
            "linkedin_username": f"{prefix.lower()}-conflict-username",
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
    ]

    values_sql: list[str] = []
    for fixture in fixtures:
        name_expr = sql_text_literal(fixture["name"])
        normalized_name_expr = f"public.normalize_company_name({name_expr})"
        linkedin_username_expr = (
            f"public.normalize_linkedin_username({sql_text_literal(fixture['linkedin_username'])})"
            if fixture["linkedin_username"] is not None
            else "null"
        )
        linkedin_url_expr = sql_text_literal(fixture["linkedin_url"])
        linkedin_url_normalized_expr = (
            f"public.normalize_company_linkedin_url({linkedin_url_expr})"
            if fixture["linkedin_url"] is not None
            else "null"
        )

        values_sql.append(
            "("
            + ", ".join(
                [
                    name_expr,
                    normalized_name_expr,
                    sql_text_literal(fixture["linkedin_id"]),
                    linkedin_username_expr,
                    linkedin_url_expr,
                    linkedin_url_normalized_expr,
                    sql_text_literal(INCOMING_DATA_SOURCE),
                    sql_text_literal(
                        "linkedin_id"
                        if fixture["linkedin_id"]
                        else "linkedin_username"
                        if fixture["linkedin_username"]
                        else "linkedin_url"
                        if fixture["linkedin_url_normalized"]
                        else "name"
                    ),
                ]
            )
            + ", '[]'::jsonb)"
        )

    return f"""
insert into public.companies_v2 (
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
{",\n".join(values_sql)};
"""


def build_duplicate_fixture_records(prefix: str) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    fixture_records = [
        {
            "source_id": f"{prefix}-case-duplicate-linkedin-id",
            "identity_phase": 0,
            "name": f"{prefix} Incoming Duplicate Linkedin Id",
            "linkedin_id": f"{prefix.lower()}-id-001",
            "linkedin_username": None,
            "linkedin_url": None,
            "website": "https://duplicate-id.example.com/jobs",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "duplicate_linkedin_id"}],
        },
        {
            "source_id": f"{prefix}-case-duplicate-linkedin-username",
            "identity_phase": 0,
            "name": f"{prefix} Incoming Duplicate Linkedin Username",
            "linkedin_id": None,
            "linkedin_username": f"{prefix.lower()}-username-001",
            "linkedin_url": None,
            "website": "https://duplicate-username.example.com/jobs",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "duplicate_linkedin_username"}],
        },
        {
            "source_id": f"{prefix}-case-duplicate-linkedin-url",
            "identity_phase": 0,
            "name": f"{prefix} Incoming Duplicate Linkedin Url",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": f"https://www.linkedin.com/company/{prefix.lower()}-url-001/jobs",
            "website": "https://duplicate-url.example.com/jobs",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "duplicate_linkedin_url_normalized"}],
        },
        {
            "source_id": f"{prefix}-case-name-only-fallback",
            "identity_phase": 1,
            "name": f"{prefix} Name Only LLC",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": None,
            "website": "https://name-only.example.com/roles",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "name_only_fallback_duplicate"}],
        },
        {
            "source_id": f"{prefix}-case-conflicting-incoming-identity",
            "identity_phase": 0,
            "name": f"{prefix} Incoming Conflicting Identity",
            "linkedin_id": None,
            "linkedin_username": f"{prefix.lower()}-username-a",
            "linkedin_url": f"https://www.linkedin.com/company/{prefix.lower()}-username-b/",
            "website": "https://conflicting-incoming.example.com/openings",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "conflicting_incoming_linkedin_identity"}],
        },
        {
            "source_id": f"{prefix}-case-linkedin-id-vs-username-conflict",
            "identity_phase": 0,
            "name": f"{prefix} Incoming Username Conflict",
            "linkedin_id": f"{prefix.lower()}-incoming-conflict-id",
            "linkedin_username": f"{prefix.lower()}-conflict-username",
            "linkedin_url": None,
            "website": "https://id-vs-username.example.com/openings",
            "description": None,
            "industries": None,
            "specialties": None,
            "company_type": None,
            "staff_count": None,
            "staff_count_range": None,
            "headquarters_city": None,
            "headquarters_country": None,
            "logo_url": None,
            "enrichment_status": None,
            "last_enrichment_sync": None,
            "incoming_source": INCOMING_DATA_SOURCE,
            "source_record_refs": [{"fixture_case": "linkedin_id_conflicts_with_username_match"}],
        },
    ]

    expectations = {
        f"{prefix}-case-duplicate-linkedin-id": {
            "effective_action": "match_existing",
            "match_basis": "linkedin_id",
        },
        f"{prefix}-case-duplicate-linkedin-username": {
            "effective_action": "match_existing",
            "match_basis": "linkedin_username",
        },
        f"{prefix}-case-duplicate-linkedin-url": {
            "effective_action": "match_existing",
            "match_basis": "linkedin_url_normalized",
        },
        f"{prefix}-case-name-only-fallback": {
            "effective_action": "match_existing",
            "match_basis": "normalized_name",
        },
        f"{prefix}-case-conflicting-incoming-identity": {
            "effective_action": "ambiguous",
            "ambiguity_type": "conflicting_incoming_linkedin_identity",
        },
        f"{prefix}-case-linkedin-id-vs-username-conflict": {
            "effective_action": "ambiguous",
            "ambiguity_type": "linkedin_id_conflicts_with_username_match",
        },
    }

    return fixture_records, expectations


def run_duplicate_validation(
    target_db: PsqlClient | SupabaseLinkedClient,
    *,
    bootstrap_sql: str | None = None,
) -> dict[str, Any]:
    prefix = f"task6a_{uuid4().hex[:10]}"
    prelude = build_duplicate_fixture_prelude(prefix)
    incoming_records, expectations = build_duplicate_fixture_records(prefix)
    result_rows = run_batch(
        target_db,
        incoming_records,
        commit_writes=False,
        prelude_sql=f"{bootstrap_sql}\n{prelude}" if bootstrap_sql else prelude,
    )

    by_source_id = {str(row["source_id"]): row for row in result_rows}
    case_results: list[dict[str, Any]] = []
    passed_count = 0

    for source_id, expectation in expectations.items():
        actual = by_source_id.get(source_id, {})
        passed = True
        mismatches: list[str] = []

        for key, expected_value in expectation.items():
            actual_value = actual.get(key)
            if actual_value != expected_value:
                passed = False
                mismatches.append(
                    f"{key}: expected {expected_value!r}, received {actual_value!r}"
                )

        if passed:
            passed_count += 1

        case_results.append(
            {
                "source_id": source_id,
                "passed": passed,
                "expected": expectation,
                "actual": actual,
                "mismatches": mismatches,
            }
        )

    return {
        "fixture_prefix": prefix,
        "cases_run": len(expectations),
        "cases_passed": passed_count,
        "cases_failed": len(expectations) - passed_count,
        "all_passed": passed_count == len(expectations),
        "results": case_results,
    }


def effective_limit(args: argparse.Namespace) -> int | None:
    if args.limit is not None:
        return args.limit
    if args.mode == "dry-run":
        return DEFAULT_PREFLIGHT_LIMIT
    return None


def run_main_pass(
    args: argparse.Namespace,
    *,
    source_db: PsqlClient | SupabaseLinkedClient,
    target_db: PsqlClient | SupabaseLinkedClient,
    checkpoint: JsonCheckpoint,
    bootstrap_sql: str | None = None,
) -> dict[str, Any]:
    dry_run = args.mode == "dry-run"
    limit = effective_limit(args)
    available_source_columns = fetch_source_columns(source_db)

    if dry_run:
        state, should_run = checkpoint.begin(
            batch_size=min(args.batch_size, limit or args.batch_size),
            dry_run=True,
            force_rerun=args.force_rerun,
            metadata={"script_name": Path(__file__).name, "mode": args.mode},
        )
    else:
        state, should_run = checkpoint.begin(
            batch_size=args.batch_size,
            dry_run=False,
            force_rerun=args.force_rerun,
            metadata={"script_name": Path(__file__).name, "mode": args.mode},
        )

    if not should_run:
        LOGGER.info(
            "checkpoint %s already completed at %s; use --force-rerun to start over",
            checkpoint.path.name,
            state.get("completed_at"),
        )
        return {
            "checkpoint_short_circuit": True,
            "checkpoint_state": state,
            "summary": base_summary(sample_limit=args.sample_limit),
            "batch_summaries": [],
            "rows_seen": 0,
            "rows_written": 0,
            "cursor": state["progress"]["cursor"],
            "mode": args.mode,
            "limit": limit,
        }

    summary = base_summary(sample_limit=args.sample_limit)
    batch_summaries: list[dict[str, Any]] = []
    total_rows_seen = 0
    total_rows_written = 0
    batch_number = int(state["progress"]["batch_number"])
    cursor = state["progress"]["cursor"]
    source_exhausted = False

    while True:
        remaining = None if limit is None else max(0, limit - total_rows_seen)
        if remaining == 0:
            break

        current_batch_size = args.batch_size
        if dry_run and remaining is not None:
            current_batch_size = remaining
        elif remaining is not None:
            current_batch_size = min(current_batch_size, remaining)

        prepared_batch = fetch_source_batch(
            source_db,
            cursor=cursor,
            batch_size=current_batch_size,
            available_columns=available_source_columns,
        )

        if not prepared_batch:
            source_exhausted = True
            break

        batch_number += 1
        batch_results = run_batch(
            target_db,
            prepared_batch,
            commit_writes=not dry_run,
            prelude_sql=bootstrap_sql,
        )
        batch_summary = summarize_results(batch_results, sample_limit=args.sample_limit)
        batch_summary["batch_number"] = batch_number
        batch_summary["rows_seen"] = len(prepared_batch)
        batch_summaries.append(batch_summary)
        merge_summary(summary, batch_summary)

        total_rows_seen += len(prepared_batch)
        rows_written = batch_summary["action_counts"].get("create_new", 0) + batch_summary[
            "action_counts"
        ].get("match_existing", 0)
        total_rows_written += rows_written

        cursor = next_cursor(prepared_batch, cursor)

        if dry_run:
            LOGGER.info(
                "dry-run batch=%s rows_seen=%s action_counts=%s",
                batch_number,
                len(prepared_batch),
                json.dumps(batch_summary["action_counts"], sort_keys=True),
            )
        else:
            checkpoint.record_batch(
                batch_number=batch_number,
                batch_size=args.batch_size,
                cursor=cursor,
                last_seen_key=prepared_batch[-1]["source_id"],
                rows_seen=len(prepared_batch),
                rows_processed=batch_summary["rows_processed"],
                rows_written=rows_written,
                rows_skipped=batch_summary["action_counts"].get("skip", 0)
                + batch_summary["action_counts"].get("ambiguous", 0)
                + batch_summary["action_counts"].get("no_op", 0),
                has_more=(remaining is None or remaining > len(prepared_batch)),
            )

        if len(prepared_batch) < current_batch_size:
            source_exhausted = True
            break

    if not dry_run:
        if source_exhausted:
            checkpoint.mark_completed(
                summary={
                    "mode": args.mode,
                    "rows_seen": total_rows_seen,
                    "rows_written": total_rows_written,
                }
            )
        else:
            checkpoint.update(
                {
                    "status": "running",
                    "progress": {
                        "has_more": True,
                    },
                    "summary": {
                        "mode": args.mode,
                        "rows_seen": total_rows_seen,
                        "rows_written": total_rows_written,
                        "stopped_due_to_limit": True,
                    },
                }
            )

    return {
        "checkpoint_short_circuit": False,
        "summary": summary,
        "batch_summaries": batch_summaries,
        "rows_seen": total_rows_seen,
        "rows_written": total_rows_written,
        "cursor": cursor,
        "mode": args.mode,
        "limit": limit,
    }


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.linked_workdir:
        linked_workdir = Path(args.linked_workdir).resolve()
        source_env_used = "SUPABASE_LINKED_WORKDIR"
        target_env_used = "SUPABASE_LINKED_WORKDIR"
        source_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="companies-backfill-source",
        )
        target_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="companies-backfill-target",
        )
    else:
        source_dsn, source_env_used = load_dsn(
            args.source_dsn_env,
            fallback_env=args.target_dsn_env,
        )
        target_dsn, target_env_used = load_dsn(args.target_dsn_env)
        source_db = PsqlClient(source_dsn, app_name="companies-backfill-source")
        target_db = PsqlClient(target_dsn, app_name="companies-backfill-target")

    checkpoint = JsonCheckpoint(args.checkpoint_name)
    report_dir = Path(args.report_dir).resolve()

    try:
        bootstrap_sql = build_dry_run_bootstrap_sql(
            target_db,
            enabled=args.mode == "dry-run",
        )

        main_pass = run_main_pass(
            args,
            source_db=source_db,
            target_db=target_db,
            checkpoint=checkpoint,
            bootstrap_sql=bootstrap_sql,
        )
        duplicate_validation = (
            run_duplicate_validation(target_db, bootstrap_sql=bootstrap_sql)
            if should_run_duplicate_validation(args)
            else None
        )
    except PsqlError as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - defensive runtime reporting
        LOGGER.exception("companies backfill failed: %s", exc)
        return 1

    report = {
        "generated_at": utc_now(),
        "script_name": Path(__file__).name,
        "task_scope": "6a" if args.mode == "dry-run" else "6b_or_6c_apply",
        "mode": args.mode,
        "limit": main_pass["limit"],
        "batch_size": args.batch_size,
        "checkpoint_name": args.checkpoint_name,
        "source_dsn_env_used": source_env_used,
        "target_dsn_env_used": target_env_used,
        "legacy_source_table": LEGACY_SOURCE_TABLE,
        "main_pass": {
            "checkpoint_short_circuit": main_pass["checkpoint_short_circuit"],
            "rows_seen": main_pass["rows_seen"],
            "rows_written": main_pass["rows_written"],
            "cursor": main_pass["cursor"],
            "summary": {
                key: value
                for key, value in main_pass["summary"].items()
                if key != "sample_limit"
            },
            "batch_summaries": [
                {
                    key: value
                    for key, value in batch_summary.items()
                    if key != "sample_limit"
                }
                for batch_summary in main_pass["batch_summaries"]
            ],
        },
        "duplicate_validation": duplicate_validation,
    }

    report_scope = (
        "companies_6a_preflight"
        if args.mode == "dry-run"
        else "companies_backfill_apply"
    )
    report_path = write_report(report_dir, report, scope=report_scope)
    LOGGER.info("wrote QA report to %s", report_path)

    if duplicate_validation is not None and not duplicate_validation["all_passed"]:
        LOGGER.error("duplicate-validation fixtures failed; inspect %s", report_path)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
