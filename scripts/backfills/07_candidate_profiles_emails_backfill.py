#!/usr/bin/env python3
"""Checkpoint-aware candidate profile and email backfill script.

Task coverage:
- Task 7a: implement the script, run preflight dry-run on the first 100 rows,
  and run duplicate-validation fixtures in a rolled-back transaction
- Task 7b: later reuse this script in apply mode for a 100-row pilot write
- Task 7c: later reuse this script in apply mode for the full migration

Design notes:
- source reads are deterministic and ordered by legacy `candidates.id` ascending
- dry-run uses the real insert/update logic inside a transaction that is rolled
  back, so profile matching, email dedupe, and primary-email behavior match a
  real run for the inspected batch
- duplicate validation uses temporary fixture writes inside a rolled-back
  transaction and never mutates the legacy source tables
- the script avoids Python DB dependencies and talks to Postgres via `psql`
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.checkpoint import JsonCheckpoint
from scripts.lib.psql import PsqlClient, PsqlError, SupabaseLinkedClient, load_dsn

LOGGER = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_NAME = "07_candidate_profiles_emails_backfill"
DEFAULT_BATCH_SIZE = 250
DEFAULT_PREFLIGHT_LIMIT = 100
DEFAULT_SOURCE_ENV = "LEGACY_DATABASE_URL"
DEFAULT_TARGET_ENV = "DATABASE_URL"
LEGACY_CANDIDATES_TABLE = "public.candidates"
LEGACY_CANDIDATE_EMAILS_TABLE = "public.candidate_emails"
PROFILE_SOURCE_VALUE = "legacy_backfill"
AMBIGUITY_SOURCE_SYSTEM = "legacy_backfill"
REPORT_DIR = REPO_ROOT / "reports" / "qa"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill canonical candidate profiles and emails from legacy tables."
    )
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help=(
            "Execution mode. `dry-run` performs the real per-batch logic inside a "
            "rolled-back transaction. `apply` commits writes to candidate_profiles_v2 "
            "and candidate_emails_v2."
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
            "Run rolled-back duplicate-validation fixtures against candidate_profiles_v2 "
            "and candidate_emails_v2 after the main pass."
        ),
    )
    parser.add_argument(
        "--skip-duplicate-validation",
        action="store_true",
        help=(
            "Skip duplicate-validation fixtures. Dry-run mode runs them by default "
            "because Task 7a requires them."
        ),
    )
    parser.add_argument(
        "--confirm-duplicate-fixture-writes",
        action="store_true",
        help=(
            "Required with duplicate-validation because fixtures write temporary rows "
            "inside a transaction that is rolled back."
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
            "--run-duplicate-validation and --skip-duplicate-validation cannot be "
            "used together."
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


def preserve_raw_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def clean_text_list(values: Any) -> list[str] | None:
    if values is None:
        return None

    iterable: Sequence[Any]
    if isinstance(values, (list, tuple)):
        iterable = values
    else:
        iterable = [values]

    cleaned_values: list[str] = []
    seen: set[str] = set()
    for value in iterable:
        cleaned = blank_to_none(value)
        if cleaned is None:
            continue
        text = str(cleaned)
        if text not in seen:
            seen.add(text)
            cleaned_values.append(text)

    return cleaned_values or None


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


def coerce_int(value: Any) -> int | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    return int(cleaned)


def sql_text_literal(value: str | None) -> str:
    if value is None:
        return "null"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_jsonb_literal(value: Any) -> str:
    return f"{sql_text_literal(json.dumps(value, sort_keys=True))}::jsonb"


def sql_uuid_array_literal(values: Sequence[str]) -> str:
    if not values:
        return "array[]::uuid[]"
    inner = ", ".join(f"{sql_text_literal(value)}::uuid" for value in values)
    return f"array[{inner}]"


def source_select_expression(
    column_name: str,
    available_columns: set[str],
    *,
    table_alias: str,
    null_expr: str = "null::text",
) -> str:
    if column_name in available_columns:
        return f"{table_alias}.{column_name}"
    return null_expr


def source_select_expression_any(
    column_names: Sequence[str],
    available_columns: set[str],
    *,
    table_alias: str,
    null_expr: str = "null::text",
) -> str:
    for column_name in column_names:
        if column_name in available_columns:
            return f"{table_alias}.{column_name}"
    return null_expr


def fetch_source_table_columns(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    schema_name: str,
    table_name: str,
) -> set[str]:
    sql = f"""
select row_to_json(t)::text
from (
    select column_name
    from information_schema.columns
    where table_schema = {sql_text_literal(schema_name)}
      and table_name = {sql_text_literal(table_name)}
    order by ordinal_position
) as t;
"""
    rows = source_db.query_json_rows(sql)
    return {str(row["column_name"]) for row in rows}


def build_source_candidate_batch_sql(
    cursor: str | None,
    batch_size: int,
    *,
    available_columns: set[str],
) -> str:
    cursor_clause = "true"
    if cursor is not None:
        cursor_clause = f"c.id > {sql_text_literal(cursor)}::uuid"

    return f"""
select row_to_json(t)::text
from (
    select
        c.id::text as source_candidate_id,
        {source_select_expression('full_name', available_columns, table_alias='c')} as full_name,
        {source_select_expression('first_name', available_columns, table_alias='c')} as first_name,
        {source_select_expression('last_name', available_columns, table_alias='c')} as last_name,
        {source_select_expression('linkedin_username', available_columns, table_alias='c')} as raw_linkedin_username,
        {source_select_expression('linkedin_url', available_columns, table_alias='c')} as raw_linkedin_url,
        {source_select_expression('headline', available_columns, table_alias='c')} as headline,
        {source_select_expression('profile_summary', available_columns, table_alias='c')} as summary,
        {source_select_expression('location', available_columns, table_alias='c')} as location,
        {source_select_expression('profile_picture_url', available_columns, table_alias='c')} as profile_picture_url,
        {source_select_expression('phone', available_columns, table_alias='c')} as phone,
        {source_select_expression('education', available_columns, table_alias='c')} as education_summary,
        {source_select_expression('education_schools', available_columns, table_alias='c', null_expr='null::text[]')} as education_schools,
        {source_select_expression('education_degrees', available_columns, table_alias='c', null_expr='null::text[]')} as education_degrees,
        {source_select_expression('education_fields', available_columns, table_alias='c', null_expr='null::text[]')} as education_fields,
        {source_select_expression('all_skills_text', available_columns, table_alias='c')} as skills_text,
        {source_select_expression('top_skills', available_columns, table_alias='c', null_expr='null::text[]')} as top_skills,
        {source_select_expression('linkedin_enrichment_status', available_columns, table_alias='c')} as linkedin_enrichment_status,
        {source_select_expression('linkedin_enrichment_date', available_columns, table_alias='c', null_expr='null::timestamptz')} as linkedin_enrichment_date,
        {source_select_expression('source', available_columns, table_alias='c')} as legacy_source,
        {source_select_expression('created_at', available_columns, table_alias='c', null_expr='null::timestamptz')} as legacy_created_at,
        {source_select_expression('updated_at', available_columns, table_alias='c', null_expr='null::timestamptz')} as legacy_updated_at,
        {source_select_expression('email', available_columns, table_alias='c')} as fallback_email_raw
    from {LEGACY_CANDIDATES_TABLE} as c
    where {cursor_clause}
    order by c.id asc
    limit {batch_size}
) as t;
"""


def fetch_source_candidates(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    cursor: str | None,
    batch_size: int,
    available_columns: set[str],
) -> list[dict[str, Any]]:
    sql = build_source_candidate_batch_sql(
        cursor=cursor,
        batch_size=batch_size,
        available_columns=available_columns,
    )
    rows = source_db.query_json_rows(sql)
    return [prepare_candidate_record(row) for row in rows]


def build_source_email_batch_sql(
    candidate_ids: Sequence[str],
    *,
    available_columns: set[str],
) -> str:
    candidate_id_array = sql_uuid_array_literal(candidate_ids)
    email_expr = source_select_expression_any(
        ["email", "email_address"],
        available_columns,
        table_alias="ce",
    )
    email_type_expr = source_select_expression_any(
        ["email_type", "type"],
        available_columns,
        table_alias="ce",
    )
    email_source_expr = source_select_expression(
        "email_source",
        available_columns,
        table_alias="ce",
    )
    is_primary_expr = source_select_expression(
        "is_primary",
        available_columns,
        table_alias="ce",
        null_expr="false",
    )
    quality_expr = source_select_expression(
        "quality",
        available_columns,
        table_alias="ce",
    )
    result_expr = source_select_expression(
        "result",
        available_columns,
        table_alias="ce",
    )
    resultcode_expr = source_select_expression(
        "resultcode",
        available_columns,
        table_alias="ce",
    )
    subresult_expr = source_select_expression(
        "subresult",
        available_columns,
        table_alias="ce",
    )
    verification_date_expr = source_select_expression(
        "verification_date",
        available_columns,
        table_alias="ce",
        null_expr="null::timestamptz",
    )
    verification_attempts_expr = source_select_expression(
        "verification_attempts",
        available_columns,
        table_alias="ce",
        null_expr="null::integer",
    )
    last_attempt_expr = source_select_expression(
        "last_verification_attempt",
        available_columns,
        table_alias="ce",
        null_expr="null::timestamptz",
    )
    raw_response_expr = source_select_expression(
        "raw_response",
        available_columns,
        table_alias="ce",
        null_expr="null::jsonb",
    )
    created_at_expr = source_select_expression(
        "created_at",
        available_columns,
        table_alias="ce",
        null_expr="null::timestamptz",
    )

    return f"""
with batch_ids as (
    select unnest({candidate_id_array}) as candidate_id
)
select row_to_json(t)::text
from (
    select
        ce.id::text as source_row_id,
        ce.candidate_id::text as source_candidate_id,
        {email_expr} as email_raw,
        {email_type_expr} as raw_email_type,
        {email_source_expr} as raw_email_source,
        {is_primary_expr} as raw_is_primary,
        {quality_expr} as quality,
        {result_expr} as result,
        {resultcode_expr} as resultcode,
        {subresult_expr} as subresult,
        {verification_date_expr} as verification_date,
        {verification_attempts_expr} as verification_attempts,
        {last_attempt_expr} as last_verification_attempt,
        {raw_response_expr} as raw_response,
        {created_at_expr} as source_created_at
    from {LEGACY_CANDIDATE_EMAILS_TABLE} as ce
    inner join batch_ids on batch_ids.candidate_id = ce.candidate_id
    order by
        ce.candidate_id asc,
        coalesce({is_primary_expr}, false) desc,
        {verification_date_expr} desc nulls last,
        {last_attempt_expr} desc nulls last,
        {created_at_expr} asc nulls last,
        ce.id asc
) as t;
"""


def fetch_source_emails_for_candidates(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    candidate_ids: Sequence[str],
    available_columns: set[str],
) -> list[dict[str, Any]]:
    if not candidate_ids or not available_columns:
        return []

    sql = build_source_email_batch_sql(
        candidate_ids=candidate_ids,
        available_columns=available_columns,
    )
    rows = source_db.query_json_rows(sql)
    return [prepare_email_record(row) for row in rows]


def prepare_candidate_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    source_candidate_id = str(raw_record["source_candidate_id"])
    return {
        "source_candidate_id": source_candidate_id,
        "full_name": blank_to_none(raw_record.get("full_name")),
        "first_name": blank_to_none(raw_record.get("first_name")),
        "last_name": blank_to_none(raw_record.get("last_name")),
        "raw_linkedin_username": preserve_raw_text(raw_record.get("raw_linkedin_username")),
        "raw_linkedin_url": preserve_raw_text(raw_record.get("raw_linkedin_url")),
        "headline": blank_to_none(raw_record.get("headline")),
        "summary": blank_to_none(raw_record.get("summary")),
        "location": blank_to_none(raw_record.get("location")),
        "profile_picture_url": blank_to_none(raw_record.get("profile_picture_url")),
        "phone": blank_to_none(raw_record.get("phone")),
        "education_summary": blank_to_none(raw_record.get("education_summary")),
        "education_schools": clean_text_list(raw_record.get("education_schools")),
        "education_degrees": clean_text_list(raw_record.get("education_degrees")),
        "education_fields": clean_text_list(raw_record.get("education_fields")),
        "skills_text": blank_to_none(raw_record.get("skills_text")),
        "top_skills": clean_text_list(raw_record.get("top_skills")),
        "linkedin_enrichment_status": blank_to_none(
            raw_record.get("linkedin_enrichment_status")
        ),
        "linkedin_enrichment_date": clean_optional_timestamptz(
            raw_record.get("linkedin_enrichment_date")
        ),
        "legacy_source": blank_to_none(raw_record.get("legacy_source")),
        "raw_full_name": preserve_raw_text(raw_record.get("full_name")),
        "legacy_created_at": clean_optional_timestamptz(
            raw_record.get("legacy_created_at")
        ),
        "legacy_updated_at": clean_optional_timestamptz(
            raw_record.get("legacy_updated_at")
        ),
        "fallback_email_raw": preserve_raw_text(raw_record.get("fallback_email_raw")),
    }


def prepare_email_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    resultcode = raw_record.get("resultcode")
    return {
        "source_candidate_id": str(raw_record["source_candidate_id"]),
        "source_row_kind": "candidate_email",
        "source_row_id": str(raw_record["source_row_id"]),
        "email_raw": preserve_raw_text(raw_record.get("email_raw")),
        "raw_email_type": preserve_raw_text(raw_record.get("raw_email_type")),
        "raw_email_source": preserve_raw_text(raw_record.get("raw_email_source")),
        "raw_is_primary": raw_record.get("raw_is_primary"),
        "quality": blank_to_none(raw_record.get("quality")),
        "result": blank_to_none(raw_record.get("result")),
        "resultcode": None if resultcode is None else str(resultcode),
        "subresult": blank_to_none(raw_record.get("subresult")),
        "verification_date": clean_optional_timestamptz(raw_record.get("verification_date")),
        "verification_attempts": coerce_int(raw_record.get("verification_attempts")),
        "last_verification_attempt": clean_optional_timestamptz(
            raw_record.get("last_verification_attempt")
        ),
        "raw_response": raw_record.get("raw_response"),
        "source_created_at": clean_optional_timestamptz(raw_record.get("source_created_at")),
    }


def next_cursor(
    prepared_batch: list[dict[str, Any]],
    current_cursor: str | None,
) -> str | None:
    if not prepared_batch:
        return current_cursor
    return str(prepared_batch[-1]["source_candidate_id"])


def fetch_target_prerequisites(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> dict[str, Any]:
    sql = """
select json_build_object(
  'candidate_profiles_v2_exists', to_regclass('public.candidate_profiles_v2') is not null,
  'candidate_emails_v2_exists', to_regclass('public.candidate_emails_v2') is not null,
  'canonicalization_ambiguities_exists', to_regclass('public.canonicalization_ambiguities') is not null,
  'resolve_candidate_profile_match_exists', to_regprocedure('public.resolve_candidate_profile_match(uuid,text,text)') is not null,
  'record_canonicalization_ambiguity_exists', to_regprocedure('public.record_canonicalization_ambiguity(text,text,text,text,jsonb,uuid[],text)') is not null,
  'normalize_linkedin_username_exists', to_regprocedure('public.normalize_linkedin_username(text)') is not null,
  'normalize_candidate_linkedin_url_exists', to_regprocedure('public.normalize_candidate_linkedin_url(text)') is not null,
  'normalize_email_address_exists', to_regprocedure('public.normalize_email_address(text)') is not null,
  'should_replace_canonical_value_exists', to_regprocedure('public.should_replace_canonical_value(text,text,text,text)') is not null,
  'candidate_profiles_source_record_refs_exists', exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'candidate_profiles_v2'
        and column_name = 'source_record_refs'
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


def validate_target_prerequisites(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> None:
    status = fetch_target_prerequisites(target_db)
    missing = [key for key, value in status.items() if not bool(value)]
    if missing:
        joined = ", ".join(sorted(missing))
        raise RuntimeError(
            "Target database is missing required Task 7 prerequisites: "
            f"{joined}."
        )


def build_batch_sql(
    prepared_candidates: list[dict[str, Any]],
    prepared_emails: list[dict[str, Any]],
    *,
    commit_writes: bool,
    prelude_sql: str | None = None,
) -> str:
    candidate_payload_literal = sql_jsonb_literal(prepared_candidates)
    email_payload_literal = sql_jsonb_literal(prepared_emails)
    final_statement = "commit;" if commit_writes else "rollback;"
    prelude_block = f"{prelude_sql.strip()}\n" if prelude_sql else ""

    return f"""
begin;
set local search_path = public, extensions;

{prelude_block}create temp table tmp_candidate_input (
    source_candidate_id uuid not null,
    full_name text,
    first_name text,
    last_name text,
    raw_linkedin_username text,
    raw_linkedin_url text,
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
    linkedin_enrichment_status text,
    linkedin_enrichment_date timestamptz,
    legacy_source text,
    raw_full_name text,
    legacy_created_at timestamptz,
    legacy_updated_at timestamptz,
    fallback_email_raw text
) on commit drop;

insert into tmp_candidate_input (
    source_candidate_id,
    full_name,
    first_name,
    last_name,
    raw_linkedin_username,
    raw_linkedin_url,
    headline,
    summary,
    location,
    profile_picture_url,
    phone,
    education_summary,
    education_schools,
    education_degrees,
    education_fields,
    skills_text,
    top_skills,
    linkedin_enrichment_status,
    linkedin_enrichment_date,
    legacy_source,
    raw_full_name,
    legacy_created_at,
    legacy_updated_at,
    fallback_email_raw
)
select
    source_candidate_id::uuid,
    full_name,
    first_name,
    last_name,
    raw_linkedin_username,
    raw_linkedin_url,
    headline,
    summary,
    location,
    profile_picture_url,
    phone,
    education_summary,
    education_schools,
    education_degrees,
    education_fields,
    skills_text,
    top_skills,
    linkedin_enrichment_status,
    nullif(linkedin_enrichment_date, '')::timestamptz,
    legacy_source,
    raw_full_name,
    nullif(legacy_created_at, '')::timestamptz,
    nullif(legacy_updated_at, '')::timestamptz,
    fallback_email_raw
from jsonb_to_recordset({candidate_payload_literal}) as x(
    source_candidate_id text,
    full_name text,
    first_name text,
    last_name text,
    raw_linkedin_username text,
    raw_linkedin_url text,
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
    linkedin_enrichment_status text,
    linkedin_enrichment_date text,
    legacy_source text,
    raw_full_name text,
    legacy_created_at text,
    legacy_updated_at text,
    fallback_email_raw text
)
order by source_candidate_id asc;

create temp table tmp_candidate_results (
    source_candidate_id uuid not null,
    match_action text not null,
    resolved_candidate_id uuid,
    match_basis text,
    original_decision text not null,
    ambiguity_type text,
    skip_reason text,
    profile_write_effect text not null,
    normalized_linkedin_username text,
    normalized_linkedin_url text,
    provenance_ref jsonb,
    ambiguity_logged boolean not null default false
) on commit drop;

do $plpgsql$
declare
    rec tmp_candidate_input%rowtype;
    match_rec record;
    existing_row public.candidate_profiles_v2%rowtype;
    v_incoming_source text := '{PROFILE_SOURCE_VALUE}';
    v_incoming_rank integer;
    v_existing_rank integer;
    v_match_basis text;
    v_ambiguity_match_ids uuid[];
    v_full_name text;
    v_first_name text;
    v_last_name text;
    v_linkedin_username text;
    v_linkedin_url text;
    v_linkedin_url_normalized text;
    v_headline text;
    v_summary text;
    v_location text;
    v_profile_picture_url text;
    v_phone text;
    v_education_summary text;
    v_education_schools text[];
    v_education_degrees text[];
    v_education_fields text[];
    v_skills_text text;
    v_top_skills text[];
    v_source text;
    v_source_record_refs jsonb;
    v_linkedin_enrichment_status text;
    v_linkedin_enrichment_date timestamptz;
    v_new_source_ref jsonb;
begin
    v_incoming_rank := public.canonical_source_precedence_rank(v_incoming_source);

    for rec in
        select *
        from tmp_candidate_input
        order by source_candidate_id asc
    loop
        select *
        into match_rec
        from public.resolve_candidate_profile_match(
            rec.source_candidate_id,
            rec.raw_linkedin_username,
            rec.raw_linkedin_url
        );

        if match_rec.decision is null then
            raise exception 'resolve_candidate_profile_match returned no decision for source_candidate_id=%', rec.source_candidate_id;
        end if;

        v_match_basis := case match_rec.match_basis
            when 'legacy_candidate_id' then 'legacy_id'
            when 'linkedin_username_and_url' then 'linkedin_username'
            else match_rec.match_basis
        end;

        if match_rec.decision = 'ambiguous' then
            select array_agg(distinct c.id order by c.id)
            into v_ambiguity_match_ids
            from public.candidate_profiles_v2 as c
            where (
                rec.source_candidate_id is not null
                and c.id = rec.source_candidate_id
            )
            or (
                match_rec.normalized_linkedin_username is not null
                and c.linkedin_username = match_rec.normalized_linkedin_username
            )
            or (
                match_rec.normalized_linkedin_url is not null
                and c.linkedin_url_normalized = match_rec.normalized_linkedin_url
            );

            perform public.record_canonicalization_ambiguity(
                'candidate_profile',
                match_rec.ambiguity_type,
                '{AMBIGUITY_SOURCE_SYSTEM}',
                rec.source_candidate_id::text,
                jsonb_build_object(
                    'legacy_candidate_id', rec.source_candidate_id::text,
                    'raw_linkedin_username', rec.raw_linkedin_username,
                    'raw_linkedin_url', rec.raw_linkedin_url,
                    'normalized_linkedin_username', match_rec.normalized_linkedin_username,
                    'normalized_linkedin_url', match_rec.normalized_linkedin_url
                ),
                v_ambiguity_match_ids,
                'manual_review'
            );

            insert into tmp_candidate_results (
                source_candidate_id,
                match_action,
                resolved_candidate_id,
                match_basis,
                original_decision,
                ambiguity_type,
                skip_reason,
                profile_write_effect,
                normalized_linkedin_username,
                normalized_linkedin_url,
                provenance_ref,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                'skip',
                null,
                'ambiguous',
                match_rec.decision,
                match_rec.ambiguity_type,
                match_rec.ambiguity_type,
                'skipped',
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                null,
                true
            );
            continue;
        end if;

        if match_rec.decision = 'create_new' then
            v_match_basis := 'legacy_id';
            v_new_source_ref := jsonb_build_object(
                'source_table', 'candidates',
                'legacy_candidate_id', rec.source_candidate_id::text,
                'legacy_source', rec.legacy_source,
                'raw_linkedin_username', rec.raw_linkedin_username,
                'raw_linkedin_url', rec.raw_linkedin_url,
                'raw_full_name', rec.raw_full_name,
                'legacy_created_at', rec.legacy_created_at,
                'legacy_updated_at', rec.legacy_updated_at,
                'canonical_match_outcome', jsonb_build_object(
                    'legacy_candidate_id', rec.source_candidate_id::text,
                    'resolved_candidate_id', rec.source_candidate_id::text,
                    'match_action', 'create_new',
                    'match_basis', v_match_basis
                )
            );

            insert into public.candidate_profiles_v2 (
                id,
                full_name,
                first_name,
                last_name,
                linkedin_username,
                linkedin_url,
                linkedin_url_normalized,
                headline,
                summary,
                location,
                profile_picture_url,
                phone,
                education_summary,
                education_schools,
                education_degrees,
                education_fields,
                skills_text,
                top_skills,
                source,
                source_record_refs,
                linkedin_enrichment_status,
                linkedin_enrichment_date
            )
            values (
                rec.source_candidate_id,
                rec.full_name,
                rec.first_name,
                rec.last_name,
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                match_rec.normalized_linkedin_url,
                rec.headline,
                rec.summary,
                rec.location,
                rec.profile_picture_url,
                rec.phone,
                rec.education_summary,
                rec.education_schools,
                rec.education_degrees,
                rec.education_fields,
                rec.skills_text,
                rec.top_skills,
                v_incoming_source,
                jsonb_build_array(v_new_source_ref),
                rec.linkedin_enrichment_status,
                rec.linkedin_enrichment_date
            );

            insert into tmp_candidate_results (
                source_candidate_id,
                match_action,
                resolved_candidate_id,
                match_basis,
                original_decision,
                ambiguity_type,
                skip_reason,
                profile_write_effect,
                normalized_linkedin_username,
                normalized_linkedin_url,
                provenance_ref,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                'create_new',
                rec.source_candidate_id,
                v_match_basis,
                match_rec.decision,
                null,
                null,
                'inserted',
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                v_new_source_ref,
                false
            );
            continue;
        end if;

        select *
        into existing_row
        from public.candidate_profiles_v2
        where id = match_rec.matched_candidate_id
        for update;

        if not found then
            raise exception 'Matched candidate % not found for source_candidate_id=%', match_rec.matched_candidate_id, rec.source_candidate_id;
        end if;

        v_existing_rank := public.canonical_source_precedence_rank(existing_row.source);
        v_new_source_ref := jsonb_build_object(
            'source_table', 'candidates',
            'legacy_candidate_id', rec.source_candidate_id::text,
            'legacy_source', rec.legacy_source,
            'raw_linkedin_username', rec.raw_linkedin_username,
            'raw_linkedin_url', rec.raw_linkedin_url,
            'raw_full_name', rec.raw_full_name,
            'legacy_created_at', rec.legacy_created_at,
            'legacy_updated_at', rec.legacy_updated_at,
            'canonical_match_outcome', jsonb_build_object(
                'legacy_candidate_id', rec.source_candidate_id::text,
                'resolved_candidate_id', existing_row.id::text,
                'match_action', 'match_existing',
                'match_basis', coalesce(v_match_basis, 'legacy_id')
            )
        );

        v_full_name := existing_row.full_name;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.full_name, rec.full_name) then
            v_full_name := rec.full_name;
        end if;

        v_first_name := existing_row.first_name;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.first_name, rec.first_name) then
            v_first_name := rec.first_name;
        end if;

        v_last_name := existing_row.last_name;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.last_name, rec.last_name) then
            v_last_name := rec.last_name;
        end if;

        v_linkedin_username := existing_row.linkedin_username;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.linkedin_username, match_rec.normalized_linkedin_username) then
            v_linkedin_username := match_rec.normalized_linkedin_username;
        end if;

        v_linkedin_url_normalized := existing_row.linkedin_url_normalized;
        v_linkedin_url := existing_row.linkedin_url;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.linkedin_url_normalized, match_rec.normalized_linkedin_url) then
            v_linkedin_url_normalized := match_rec.normalized_linkedin_url;
            v_linkedin_url := match_rec.normalized_linkedin_url;
        elsif (existing_row.linkedin_url is null or btrim(existing_row.linkedin_url) = '')
              and match_rec.normalized_linkedin_url is not null then
            v_linkedin_url := match_rec.normalized_linkedin_url;
        end if;

        v_headline := existing_row.headline;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.headline, rec.headline) then
            v_headline := rec.headline;
        end if;

        v_summary := existing_row.summary;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.summary, rec.summary) then
            v_summary := rec.summary;
        end if;

        v_location := existing_row.location;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.location, rec.location) then
            v_location := rec.location;
        end if;

        v_profile_picture_url := existing_row.profile_picture_url;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.profile_picture_url, rec.profile_picture_url) then
            v_profile_picture_url := rec.profile_picture_url;
        end if;

        v_phone := existing_row.phone;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.phone, rec.phone) then
            v_phone := rec.phone;
        end if;

        v_education_summary := existing_row.education_summary;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.education_summary, rec.education_summary) then
            v_education_summary := rec.education_summary;
        end if;

        v_skills_text := existing_row.skills_text;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.skills_text, rec.skills_text) then
            v_skills_text := rec.skills_text;
        end if;

        v_linkedin_enrichment_status := existing_row.linkedin_enrichment_status;
        if public.should_replace_canonical_value(existing_row.source, v_incoming_source, existing_row.linkedin_enrichment_status, rec.linkedin_enrichment_status) then
            v_linkedin_enrichment_status := rec.linkedin_enrichment_status;
        end if;

        v_education_schools := existing_row.education_schools;
        if rec.education_schools is not null and cardinality(rec.education_schools) > 0 then
            if existing_row.education_schools is null
               or cardinality(existing_row.education_schools) = 0
               or v_incoming_rank > v_existing_rank then
                v_education_schools := rec.education_schools;
            end if;
        end if;

        v_education_degrees := existing_row.education_degrees;
        if rec.education_degrees is not null and cardinality(rec.education_degrees) > 0 then
            if existing_row.education_degrees is null
               or cardinality(existing_row.education_degrees) = 0
               or v_incoming_rank > v_existing_rank then
                v_education_degrees := rec.education_degrees;
            end if;
        end if;

        v_education_fields := existing_row.education_fields;
        if rec.education_fields is not null and cardinality(rec.education_fields) > 0 then
            if existing_row.education_fields is null
               or cardinality(existing_row.education_fields) = 0
               or v_incoming_rank > v_existing_rank then
                v_education_fields := rec.education_fields;
            end if;
        end if;

        v_top_skills := existing_row.top_skills;
        if rec.top_skills is not null and cardinality(rec.top_skills) > 0 then
            if existing_row.top_skills is null
               or cardinality(existing_row.top_skills) = 0
               or v_incoming_rank > v_existing_rank then
                v_top_skills := rec.top_skills;
            end if;
        end if;

        v_linkedin_enrichment_date := existing_row.linkedin_enrichment_date;
        if rec.linkedin_enrichment_date is not null then
            if existing_row.linkedin_enrichment_date is null or v_incoming_rank > v_existing_rank then
                v_linkedin_enrichment_date := rec.linkedin_enrichment_date;
            end if;
        end if;

        v_source := existing_row.source;
        if v_source is null or btrim(v_source) = '' or v_incoming_rank > v_existing_rank then
            v_source := v_incoming_source;
        end if;

        select coalesce(jsonb_agg(elem order by elem::text), '[]'::jsonb)
        into v_source_record_refs
        from (
            select distinct elem
            from (
                select elem
                from jsonb_array_elements(
                    case jsonb_typeof(coalesce(existing_row.source_record_refs, '[]'::jsonb))
                        when 'array' then coalesce(existing_row.source_record_refs, '[]'::jsonb)
                        when 'object' then jsonb_build_array(existing_row.source_record_refs)
                        else '[]'::jsonb
                    end
                ) as elem
                where coalesce(elem->>'source_table', '') <> 'candidates'
                   or coalesce(elem->>'legacy_candidate_id', '') <> rec.source_candidate_id::text
                union all
                select v_new_source_ref as elem
            ) as merged
        ) as deduped;

        if v_full_name is not distinct from existing_row.full_name
           and v_first_name is not distinct from existing_row.first_name
           and v_last_name is not distinct from existing_row.last_name
           and v_linkedin_username is not distinct from existing_row.linkedin_username
           and v_linkedin_url is not distinct from existing_row.linkedin_url
           and v_linkedin_url_normalized is not distinct from existing_row.linkedin_url_normalized
           and v_headline is not distinct from existing_row.headline
           and v_summary is not distinct from existing_row.summary
           and v_location is not distinct from existing_row.location
           and v_profile_picture_url is not distinct from existing_row.profile_picture_url
           and v_phone is not distinct from existing_row.phone
           and v_education_summary is not distinct from existing_row.education_summary
           and v_education_schools is not distinct from existing_row.education_schools
           and v_education_degrees is not distinct from existing_row.education_degrees
           and v_education_fields is not distinct from existing_row.education_fields
           and v_skills_text is not distinct from existing_row.skills_text
           and v_top_skills is not distinct from existing_row.top_skills
           and v_source is not distinct from existing_row.source
           and v_source_record_refs is not distinct from existing_row.source_record_refs
           and v_linkedin_enrichment_status is not distinct from existing_row.linkedin_enrichment_status
           and v_linkedin_enrichment_date is not distinct from existing_row.linkedin_enrichment_date then
            insert into tmp_candidate_results (
                source_candidate_id,
                match_action,
                resolved_candidate_id,
                match_basis,
                original_decision,
                ambiguity_type,
                skip_reason,
                profile_write_effect,
                normalized_linkedin_username,
                normalized_linkedin_url,
                provenance_ref,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                'match_existing',
                existing_row.id,
                coalesce(v_match_basis, 'legacy_id'),
                match_rec.decision,
                null,
                null,
                'no_op',
                match_rec.normalized_linkedin_username,
                match_rec.normalized_linkedin_url,
                v_new_source_ref,
                false
            );
            continue;
        end if;

        update public.candidate_profiles_v2
        set
            full_name = v_full_name,
            first_name = v_first_name,
            last_name = v_last_name,
            linkedin_username = v_linkedin_username,
            linkedin_url = v_linkedin_url,
            linkedin_url_normalized = v_linkedin_url_normalized,
            headline = v_headline,
            summary = v_summary,
            location = v_location,
            profile_picture_url = v_profile_picture_url,
            phone = v_phone,
            education_summary = v_education_summary,
            education_schools = v_education_schools,
            education_degrees = v_education_degrees,
            education_fields = v_education_fields,
            skills_text = v_skills_text,
            top_skills = v_top_skills,
            source = v_source,
            source_record_refs = v_source_record_refs,
            linkedin_enrichment_status = v_linkedin_enrichment_status,
            linkedin_enrichment_date = v_linkedin_enrichment_date
        where id = existing_row.id;

        insert into tmp_candidate_results (
            source_candidate_id,
            match_action,
            resolved_candidate_id,
            match_basis,
            original_decision,
            ambiguity_type,
            skip_reason,
            profile_write_effect,
            normalized_linkedin_username,
            normalized_linkedin_url,
            provenance_ref,
            ambiguity_logged
        )
        values (
            rec.source_candidate_id,
            'match_existing',
            existing_row.id,
            coalesce(v_match_basis, 'legacy_id'),
            match_rec.decision,
            null,
            null,
            'updated',
            match_rec.normalized_linkedin_username,
            match_rec.normalized_linkedin_url,
            v_new_source_ref,
            false
        );
    end loop;
end
$plpgsql$;

create temp table tmp_email_input (
    source_candidate_id uuid not null,
    source_row_kind text not null,
    source_row_id text not null,
    email_raw text,
    raw_email_type text,
    raw_email_source text,
    raw_is_primary boolean,
    quality text,
    result text,
    resultcode text,
    subresult text,
    verification_date timestamptz,
    verification_attempts integer,
    last_verification_attempt timestamptz,
    raw_response jsonb,
    source_created_at timestamptz
) on commit drop;

insert into tmp_email_input (
    source_candidate_id,
    source_row_kind,
    source_row_id,
    email_raw,
    raw_email_type,
    raw_email_source,
    raw_is_primary,
    quality,
    result,
    resultcode,
    subresult,
    verification_date,
    verification_attempts,
    last_verification_attempt,
    raw_response,
    source_created_at
)
select
    source_candidate_id::uuid,
    source_row_kind,
    source_row_id,
    email_raw,
    raw_email_type,
    raw_email_source,
    raw_is_primary,
    quality,
    result,
    resultcode,
    subresult,
    nullif(verification_date, '')::timestamptz,
    verification_attempts,
    nullif(last_verification_attempt, '')::timestamptz,
    raw_response,
    nullif(source_created_at, '')::timestamptz
from jsonb_to_recordset({email_payload_literal}) as x(
    source_candidate_id text,
    source_row_kind text,
    source_row_id text,
    email_raw text,
    raw_email_type text,
    raw_email_source text,
    raw_is_primary boolean,
    quality text,
    result text,
    resultcode text,
    subresult text,
    verification_date text,
    verification_attempts integer,
    last_verification_attempt text,
    raw_response jsonb,
    source_created_at text
);

insert into tmp_email_input (
    source_candidate_id,
    source_row_kind,
    source_row_id,
    email_raw,
    raw_email_type,
    raw_email_source,
    raw_is_primary,
    quality,
    result,
    resultcode,
    subresult,
    verification_date,
    verification_attempts,
    last_verification_attempt,
    raw_response,
    source_created_at
)
select
    source_candidate_id,
    'candidate_fallback',
    'candidate:' || source_candidate_id::text || ':fallback_email',
    fallback_email_raw,
    null,
    null,
    false,
    null,
    null,
    null,
    null,
    null,
    0,
    null,
    null,
    legacy_created_at
from tmp_candidate_input
where fallback_email_raw is not null;

create temp table tmp_email_candidate_input on commit drop as
with normalized as (
    select
        e.source_candidate_id,
        cr.resolved_candidate_id,
        e.source_row_kind,
        e.source_row_id,
        e.email_raw,
        public.normalize_email_address(e.email_raw) as email_normalized,
        case lower(btrim(coalesce(e.raw_email_type, '')))
            when '' then null
            when 'personal' then 'personal'
            when 'business' then 'work'
            when 'academic' then 'work'
            when 'government' then 'work'
            else 'unknown'
        end as email_type,
        case
            when nullif(btrim(coalesce(e.raw_email_source, '')), '') is not null then btrim(e.raw_email_source)
            when e.source_row_kind = 'candidate_email' then 'legacy_candidate_emails'
            else 'legacy_candidates_email_fallback'
        end as email_source,
        coalesce(e.raw_is_primary, false) as source_is_primary,
        nullif(btrim(coalesce(e.quality, '')), '') as quality,
        nullif(btrim(coalesce(e.result, '')), '') as result,
        nullif(btrim(coalesce(e.resultcode, '')), '') as resultcode,
        nullif(btrim(coalesce(e.subresult, '')), '') as subresult,
        e.verification_date,
        coalesce(e.verification_attempts, 0) as verification_attempts,
        e.last_verification_attempt,
        e.raw_response,
        e.source_created_at
    from tmp_email_input as e
    inner join tmp_candidate_results as cr
        on cr.source_candidate_id = e.source_candidate_id
    where cr.match_action in ('create_new', 'match_existing')
)
select *
from normalized as n
where n.email_normalized is not null
  and not (
      n.source_row_kind = 'candidate_fallback'
      and exists (
          select 1
          from normalized as n2
          where n2.source_candidate_id = n.source_candidate_id
            and n2.source_row_kind = 'candidate_email'
            and n2.email_normalized = n.email_normalized
      )
  );

create temp table tmp_candidate_fallback_hints on commit drop as
select
    cr.resolved_candidate_id,
    public.normalize_email_address(ci.fallback_email_raw) as hint_email
from tmp_candidate_input as ci
inner join tmp_candidate_results as cr
    on cr.source_candidate_id = ci.source_candidate_id
where cr.match_action in ('create_new', 'match_existing')
  and public.normalize_email_address(ci.fallback_email_raw) is not null
group by cr.resolved_candidate_id, public.normalize_email_address(ci.fallback_email_raw);

create temp table tmp_email_contributors on commit drop as
select
    resolved_candidate_id,
    email_normalized,
    jsonb_agg(
        jsonb_build_object(
            'source_candidate_id', source_candidate_id::text,
            'source_row_kind', source_row_kind,
            'source_row_id', source_row_id
        )
        order by
            case when source_row_kind = 'candidate_email' then 0 else 1 end asc,
            source_is_primary desc,
            verification_date desc nulls last,
            verification_attempts desc,
            last_verification_attempt desc nulls last,
            source_created_at asc nulls last,
            source_row_id asc
    ) as contributors
from tmp_email_candidate_input
group by resolved_candidate_id, email_normalized;

create temp table tmp_email_survivors on commit drop as
with ranked as (
    select
        e.*,
        case when e.source_row_kind = 'candidate_email' then 0 else 1 end as source_priority,
        count(*) over (
            partition by e.resolved_candidate_id, e.email_normalized
        ) as source_row_count,
        count(*) filter (
            where e.source_is_primary
        ) over (
            partition by e.resolved_candidate_id, e.email_normalized
        ) as source_primary_count,
        row_number() over (
            partition by e.resolved_candidate_id, e.email_normalized
            order by
                case when e.source_row_kind = 'candidate_email' then 0 else 1 end asc,
                e.source_is_primary desc,
                e.verification_date desc nulls last,
                e.verification_attempts desc,
                e.last_verification_attempt desc nulls last,
                e.source_created_at asc nulls last,
                e.source_row_id asc
        ) as rn
    from tmp_email_candidate_input as e
)
select
    r.resolved_candidate_id,
    r.source_candidate_id,
    r.source_row_kind,
    r.source_row_id,
    r.email_raw,
    r.email_normalized,
    r.email_type,
    r.email_source,
    r.source_is_primary,
    r.quality,
    r.result,
    r.resultcode,
    r.subresult,
    r.verification_date,
    r.verification_attempts,
    r.last_verification_attempt,
    r.raw_response,
    r.source_created_at,
    r.source_priority,
    r.source_row_count,
    r.source_primary_count,
    c.contributors
from ranked as r
inner join tmp_email_contributors as c
    using (resolved_candidate_id, email_normalized)
where r.rn = 1;

create temp table tmp_existing_email_claims on commit drop as
select
    email_normalized,
    candidate_id
from public.candidate_emails_v2
where email_normalized in (
    select distinct email_normalized
    from tmp_email_survivors
);

create temp table tmp_batch_email_claims on commit drop as
select
    email_normalized,
    count(distinct resolved_candidate_id) as batch_candidate_count,
    array_agg(distinct resolved_candidate_id order by resolved_candidate_id) as batch_candidate_ids
from tmp_email_survivors
group by email_normalized;

create temp table tmp_email_conflicts on commit drop as
select
    s.resolved_candidate_id,
    s.source_candidate_id,
    s.email_normalized,
    s.email_raw,
    s.email_source,
    s.source_row_kind,
    s.source_row_id,
    s.source_row_count,
    s.source_primary_count,
    s.contributors,
    s.source_priority,
    s.verification_date,
    s.verification_attempts,
    s.last_verification_attempt,
    s.source_created_at,
    ec.candidate_id as existing_claim_candidate_id,
    bc.batch_candidate_ids
from tmp_email_survivors as s
left join tmp_existing_email_claims as ec
    using (email_normalized)
left join tmp_batch_email_claims as bc
    using (email_normalized)
where (
    ec.candidate_id is null
    and coalesce(bc.batch_candidate_count, 0) > 1
) or (
    ec.candidate_id is not null
    and ec.candidate_id <> s.resolved_candidate_id
);

create temp table tmp_email_writable_survivors on commit drop as
select s.*
from tmp_email_survivors as s
where not exists (
    select 1
    from tmp_email_conflicts as c
    where c.resolved_candidate_id = s.resolved_candidate_id
      and c.email_normalized = s.email_normalized
);

create temp table tmp_candidate_primary_plan (
    resolved_candidate_id uuid primary key,
    surviving_email_count integer not null,
    source_primary_count integer not null,
    fallback_hint_count integer not null,
    chosen_primary_email extensions.citext,
    final_primary_count integer not null
) on commit drop;

create temp table tmp_email_results (
    resolved_candidate_id uuid not null,
    source_candidate_id uuid,
    email_normalized extensions.citext,
    email_action text not null,
    conflict_reason text,
    email_raw text,
    email_source text,
    is_primary boolean not null,
    source_row_kind text,
    source_row_id text,
    source_row_count integer not null,
    source_primary_count integer not null,
    duplicate_collapsed_count integer not null,
    conflict_logged boolean not null default false,
    contributors jsonb
) on commit drop;

do $plpgsql$
declare
    candidate_rec record;
    email_rec record;
    existing_email public.candidate_emails_v2%rowtype;
    v_surviving_count integer;
    v_source_primary_count integer;
    v_fallback_hint_count integer;
    v_fallback_hint_email extensions.citext;
    v_primary_choice extensions.citext;
    v_final_primary_count integer;
    v_conflicting_candidate_ids uuid[];
    v_email_raw text;
    v_email_type text;
    v_email_source text;
    v_quality text;
    v_result text;
    v_resultcode text;
    v_subresult text;
    v_verification_date timestamptz;
    v_verification_attempts integer;
    v_last_verification_attempt timestamptz;
    v_raw_response jsonb;
    v_is_primary boolean;
begin
    for candidate_rec in
        select distinct resolved_candidate_id
        from tmp_candidate_results
        where match_action in ('create_new', 'match_existing')
        order by resolved_candidate_id
    loop
        select
            count(*),
            count(*) filter (where source_is_primary)
        into
            v_surviving_count,
            v_source_primary_count
        from tmp_email_writable_survivors
        where resolved_candidate_id = candidate_rec.resolved_candidate_id;

        select
            count(*),
            min(hint_email)
        into
            v_fallback_hint_count,
            v_fallback_hint_email
        from tmp_candidate_fallback_hints
        where resolved_candidate_id = candidate_rec.resolved_candidate_id;

        v_primary_choice := null;

        if v_source_primary_count = 1 then
            select email_normalized
            into v_primary_choice
            from tmp_email_writable_survivors
            where resolved_candidate_id = candidate_rec.resolved_candidate_id
              and source_is_primary
            limit 1;
        elsif v_source_primary_count > 1 then
            select email_normalized
            into v_primary_choice
            from tmp_email_writable_survivors
            where resolved_candidate_id = candidate_rec.resolved_candidate_id
              and source_is_primary
            order by
                source_priority asc,
                verification_date desc nulls last,
                verification_attempts desc,
                last_verification_attempt desc nulls last,
                source_created_at asc nulls last,
                source_row_id asc
            limit 1;
        elsif v_surviving_count = 1 then
            select email_normalized
            into v_primary_choice
            from tmp_email_writable_survivors
            where resolved_candidate_id = candidate_rec.resolved_candidate_id
            limit 1;
        elsif v_fallback_hint_count = 1
              and exists (
                  select 1
                  from tmp_email_writable_survivors
                  where resolved_candidate_id = candidate_rec.resolved_candidate_id
                    and email_normalized = v_fallback_hint_email
              ) then
            v_primary_choice := v_fallback_hint_email;
        end if;

        if v_primary_choice is not null then
            update public.candidate_emails_v2
            set is_primary = false
            where candidate_id = candidate_rec.resolved_candidate_id
              and email_normalized <> v_primary_choice
              and is_primary;
        elsif coalesce(v_surviving_count, 0) > 0 then
            update public.candidate_emails_v2
            set is_primary = false
            where candidate_id = candidate_rec.resolved_candidate_id
              and email_source in ('legacy_candidate_emails', 'legacy_candidates_email_fallback')
              and is_primary;
        end if;

        for email_rec in
            select *
            from tmp_email_conflicts
            where resolved_candidate_id = candidate_rec.resolved_candidate_id
            order by
                source_priority asc,
                verification_date desc nulls last,
                verification_attempts desc,
                last_verification_attempt desc nulls last,
                source_created_at asc nulls last,
                source_row_id asc
        loop
            v_conflicting_candidate_ids := array_remove(
                array_append(
                    coalesce(email_rec.batch_candidate_ids, array[]::uuid[]),
                    email_rec.existing_claim_candidate_id
                ),
                candidate_rec.resolved_candidate_id
            );

            perform public.record_canonicalization_ambiguity(
                'candidate_profile',
                'cross_candidate_email_conflict',
                '{AMBIGUITY_SOURCE_SYSTEM}',
                coalesce(email_rec.source_candidate_id::text, candidate_rec.resolved_candidate_id::text),
                jsonb_build_object(
                    'resolved_candidate_id', candidate_rec.resolved_candidate_id::text,
                    'email_normalized', email_rec.email_normalized::text,
                    'existing_claim_candidate_id', email_rec.existing_claim_candidate_id::text,
                    'batch_candidate_ids', to_jsonb(coalesce(email_rec.batch_candidate_ids, array[]::uuid[])),
                    'contributors', email_rec.contributors
                ),
                v_conflicting_candidate_ids,
                'manual_review'
            );

            insert into tmp_email_results (
                resolved_candidate_id,
                source_candidate_id,
                email_normalized,
                email_action,
                conflict_reason,
                email_raw,
                email_source,
                is_primary,
                source_row_kind,
                source_row_id,
                source_row_count,
                source_primary_count,
                duplicate_collapsed_count,
                conflict_logged,
                contributors
            )
            values (
                candidate_rec.resolved_candidate_id,
                email_rec.source_candidate_id,
                email_rec.email_normalized,
                'skip_conflict',
                'cross_candidate_email_conflict',
                email_rec.email_raw,
                email_rec.email_source,
                false,
                email_rec.source_row_kind,
                email_rec.source_row_id,
                email_rec.source_row_count,
                email_rec.source_primary_count,
                greatest(email_rec.source_row_count - 1, 0),
                true,
                email_rec.contributors
            );
        end loop;

        for email_rec in
            select *
            from tmp_email_writable_survivors
            where resolved_candidate_id = candidate_rec.resolved_candidate_id
            order by
                source_priority asc,
                verification_date desc nulls last,
                verification_attempts desc,
                last_verification_attempt desc nulls last,
                source_created_at asc nulls last,
                source_row_id asc
        loop
            select *
            into existing_email
            from public.candidate_emails_v2
            where candidate_id = candidate_rec.resolved_candidate_id
              and email_normalized = email_rec.email_normalized
            for update;

            v_is_primary := (v_primary_choice is not null and email_rec.email_normalized = v_primary_choice);

            if found then
                v_email_raw := existing_email.email_raw;
                if (v_email_raw is null or btrim(v_email_raw) = '')
                   and email_rec.email_raw is not null
                   and btrim(email_rec.email_raw) <> '' then
                    v_email_raw := email_rec.email_raw;
                end if;

                v_email_type := existing_email.email_type;
                if (v_email_type is null or btrim(v_email_type) = '')
                   and email_rec.email_type is not null
                   and btrim(email_rec.email_type) <> '' then
                    v_email_type := email_rec.email_type;
                end if;

                v_email_source := existing_email.email_source;
                if (v_email_source is null or btrim(v_email_source) = '')
                   and email_rec.email_source is not null
                   and btrim(email_rec.email_source) <> '' then
                    v_email_source := email_rec.email_source;
                end if;

                v_quality := existing_email.quality;
                if (v_quality is null or btrim(v_quality) = '')
                   and email_rec.quality is not null
                   and btrim(email_rec.quality) <> '' then
                    v_quality := email_rec.quality;
                end if;

                v_result := existing_email.result;
                if (v_result is null or btrim(v_result) = '')
                   and email_rec.result is not null
                   and btrim(email_rec.result) <> '' then
                    v_result := email_rec.result;
                end if;

                v_resultcode := existing_email.resultcode;
                if (v_resultcode is null or btrim(v_resultcode) = '')
                   and email_rec.resultcode is not null
                   and btrim(email_rec.resultcode) <> '' then
                    v_resultcode := email_rec.resultcode;
                end if;

                v_subresult := existing_email.subresult;
                if (v_subresult is null or btrim(v_subresult) = '')
                   and email_rec.subresult is not null
                   and btrim(email_rec.subresult) <> '' then
                    v_subresult := email_rec.subresult;
                end if;

                v_verification_date := existing_email.verification_date;
                if email_rec.verification_date is not null
                   and (
                       existing_email.verification_date is null
                       or email_rec.verification_date > existing_email.verification_date
                   ) then
                    v_verification_date := email_rec.verification_date;
                end if;

                v_verification_attempts := greatest(
                    coalesce(existing_email.verification_attempts, 0),
                    coalesce(email_rec.verification_attempts, 0)
                );

                v_last_verification_attempt := existing_email.last_verification_attempt;
                if email_rec.last_verification_attempt is not null
                   and (
                       existing_email.last_verification_attempt is null
                       or email_rec.last_verification_attempt > existing_email.last_verification_attempt
                   ) then
                    v_last_verification_attempt := email_rec.last_verification_attempt;
                end if;

                v_raw_response := existing_email.raw_response;
                if v_raw_response is null and email_rec.raw_response is not null then
                    v_raw_response := email_rec.raw_response;
                end if;

                if v_email_raw is not distinct from existing_email.email_raw
                   and v_email_type is not distinct from existing_email.email_type
                   and v_email_source is not distinct from existing_email.email_source
                   and v_is_primary is not distinct from existing_email.is_primary
                   and v_quality is not distinct from existing_email.quality
                   and v_result is not distinct from existing_email.result
                   and v_resultcode is not distinct from existing_email.resultcode
                   and v_subresult is not distinct from existing_email.subresult
                   and v_verification_date is not distinct from existing_email.verification_date
                   and v_verification_attempts is not distinct from existing_email.verification_attempts
                   and v_last_verification_attempt is not distinct from existing_email.last_verification_attempt
                   and v_raw_response is not distinct from existing_email.raw_response then
                    insert into tmp_email_results (
                        resolved_candidate_id,
                        source_candidate_id,
                        email_normalized,
                        email_action,
                        conflict_reason,
                        email_raw,
                        email_source,
                        is_primary,
                        source_row_kind,
                        source_row_id,
                        source_row_count,
                        source_primary_count,
                        duplicate_collapsed_count,
                        conflict_logged,
                        contributors
                    )
                    values (
                        candidate_rec.resolved_candidate_id,
                        email_rec.source_candidate_id,
                        email_rec.email_normalized,
                        'no_op',
                        null,
                        coalesce(v_email_raw, existing_email.email_raw),
                        v_email_source,
                        v_is_primary,
                        email_rec.source_row_kind,
                        email_rec.source_row_id,
                        email_rec.source_row_count,
                        email_rec.source_primary_count,
                        greatest(email_rec.source_row_count - 1, 0),
                        false,
                        email_rec.contributors
                    );
                    continue;
                end if;

                update public.candidate_emails_v2
                set
                    email_raw = coalesce(v_email_raw, existing_email.email_raw),
                    email_type = v_email_type,
                    email_source = v_email_source,
                    is_primary = v_is_primary,
                    quality = v_quality,
                    result = v_result,
                    resultcode = v_resultcode,
                    subresult = v_subresult,
                    verification_date = v_verification_date,
                    verification_attempts = v_verification_attempts,
                    last_verification_attempt = v_last_verification_attempt,
                    raw_response = v_raw_response
                where id = existing_email.id;

                insert into tmp_email_results (
                    resolved_candidate_id,
                    source_candidate_id,
                    email_normalized,
                    email_action,
                    conflict_reason,
                    email_raw,
                    email_source,
                    is_primary,
                    source_row_kind,
                    source_row_id,
                    source_row_count,
                    source_primary_count,
                    duplicate_collapsed_count,
                    conflict_logged,
                    contributors
                )
                values (
                    candidate_rec.resolved_candidate_id,
                    email_rec.source_candidate_id,
                    email_rec.email_normalized,
                    'match_existing',
                    null,
                    coalesce(v_email_raw, existing_email.email_raw),
                    v_email_source,
                    v_is_primary,
                    email_rec.source_row_kind,
                    email_rec.source_row_id,
                    email_rec.source_row_count,
                    email_rec.source_primary_count,
                    greatest(email_rec.source_row_count - 1, 0),
                    false,
                    email_rec.contributors
                );
            else
                insert into public.candidate_emails_v2 (
                    candidate_id,
                    email_raw,
                    email_normalized,
                    email_type,
                    email_source,
                    is_primary,
                    quality,
                    result,
                    resultcode,
                    subresult,
                    verification_date,
                    verification_attempts,
                    last_verification_attempt,
                    raw_response
                )
                values (
                    candidate_rec.resolved_candidate_id,
                    email_rec.email_raw,
                    email_rec.email_normalized,
                    email_rec.email_type,
                    email_rec.email_source,
                    v_is_primary,
                    email_rec.quality,
                    email_rec.result,
                    email_rec.resultcode,
                    email_rec.subresult,
                    email_rec.verification_date,
                    coalesce(email_rec.verification_attempts, 0),
                    email_rec.last_verification_attempt,
                    email_rec.raw_response
                );

                insert into tmp_email_results (
                    resolved_candidate_id,
                    source_candidate_id,
                    email_normalized,
                    email_action,
                    conflict_reason,
                    email_raw,
                    email_source,
                    is_primary,
                    source_row_kind,
                    source_row_id,
                    source_row_count,
                    source_primary_count,
                    duplicate_collapsed_count,
                    conflict_logged,
                    contributors
                )
                values (
                    candidate_rec.resolved_candidate_id,
                    email_rec.source_candidate_id,
                    email_rec.email_normalized,
                    'inserted',
                    null,
                    email_rec.email_raw,
                    email_rec.email_source,
                    v_is_primary,
                    email_rec.source_row_kind,
                    email_rec.source_row_id,
                    email_rec.source_row_count,
                    email_rec.source_primary_count,
                    greatest(email_rec.source_row_count - 1, 0),
                    false,
                    email_rec.contributors
                );
            end if;
        end loop;

        select count(*)
        into v_final_primary_count
        from public.candidate_emails_v2
        where candidate_id = candidate_rec.resolved_candidate_id
          and is_primary;

        insert into tmp_candidate_primary_plan (
            resolved_candidate_id,
            surviving_email_count,
            source_primary_count,
            fallback_hint_count,
            chosen_primary_email,
            final_primary_count
        )
        values (
            candidate_rec.resolved_candidate_id,
            coalesce(v_surviving_count, 0),
            coalesce(v_source_primary_count, 0),
            coalesce(v_fallback_hint_count, 0),
            v_primary_choice,
            coalesce(v_final_primary_count, 0)
        );
    end loop;
end
$plpgsql$;

with emitted_results as (
    select
        1 as result_order,
        source_candidate_id::text as sort_key_a,
        ''::text as sort_key_b,
        json_build_object(
            'result_kind', 'candidate_result',
            'payload', row_to_json(t)
        )::text as result_line
    from (
        select
            source_candidate_id::text as source_candidate_id,
            match_action,
            resolved_candidate_id::text as resolved_candidate_id,
            match_basis,
            original_decision,
            ambiguity_type,
            skip_reason,
            profile_write_effect,
            normalized_linkedin_username,
            normalized_linkedin_url,
            provenance_ref,
            ambiguity_logged
        from tmp_candidate_results
    ) as t

    union all

    select
        2 as result_order,
        resolved_candidate_id::text as sort_key_a,
        coalesce(email_normalized::text, '') as sort_key_b,
        json_build_object(
            'result_kind', 'email_result',
            'payload', row_to_json(t)
        )::text as result_line
    from (
        select
            resolved_candidate_id::text as resolved_candidate_id,
            source_candidate_id::text as source_candidate_id,
            email_normalized::text as email_normalized,
            email_action,
            conflict_reason,
            email_raw,
            email_source,
            is_primary,
            source_row_kind,
            source_row_id,
            source_row_count,
            source_primary_count,
            duplicate_collapsed_count,
            conflict_logged,
            contributors
        from tmp_email_results
    ) as t

    union all

    select
        3 as result_order,
        resolved_candidate_id::text as sort_key_a,
        ''::text as sort_key_b,
        json_build_object(
            'result_kind', 'primary_plan',
            'payload', row_to_json(t)
        )::text as result_line
    from (
        select
            resolved_candidate_id::text as resolved_candidate_id,
            surviving_email_count,
            source_primary_count,
            fallback_hint_count,
            chosen_primary_email::text as chosen_primary_email,
            final_primary_count
        from tmp_candidate_primary_plan
    ) as t

    union all

    select
        4 as result_order,
        ''::text as sort_key_a,
        ''::text as sort_key_b,
        json_build_object(
            'result_kind', 'summary',
            'payload', json_build_object(
                'legacy_candidate_email_rows_read', (
                    select count(*)
                    from tmp_email_input
                    where source_row_kind = 'candidate_email'
                ),
                'fallback_rows_synthesized_raw', (
                    select count(*)
                    from tmp_email_input
                    where source_row_kind = 'candidate_fallback'
                      and public.normalize_email_address(email_raw) is not null
                ),
                'fallback_rows_surviving_after_normalization_and_dedupe', (
                    select count(*)
                    from tmp_email_candidate_input
                    where source_row_kind = 'candidate_fallback'
                ),
                'duplicate_email_rows_collapsed', (
                    select coalesce(sum(greatest(source_row_count - 1, 0)), 0)
                    from tmp_email_survivors
                ),
                'candidates_with_one_primary', (
                    select count(*)
                    from tmp_candidate_primary_plan
                    where final_primary_count = 1
                ),
                'candidates_with_no_primary', (
                    select count(*)
                    from tmp_candidate_primary_plan
                    where surviving_email_count > 0
                      and final_primary_count = 0
                ),
                'candidates_with_multi_primary_source_conflicts', (
                    select count(*)
                    from tmp_candidate_primary_plan
                    where source_primary_count > 1
                )
            )
        )::text as result_line
)
select result_line
from emitted_results
order by result_order asc, sort_key_a asc, sort_key_b asc;

{final_statement}
"""


def parse_batch_output(rows: list[dict[str, Any]]) -> dict[str, Any]:
    parsed = {
        "candidate_results": [],
        "email_results": [],
        "primary_plans": [],
        "summary": {},
    }

    for row in rows:
        result_kind = row.get("result_kind")
        payload = row.get("payload", row)
        if result_kind == "candidate_result":
            parsed["candidate_results"].append(payload)
        elif result_kind == "email_result":
            parsed["email_results"].append(payload)
        elif result_kind == "primary_plan":
            parsed["primary_plans"].append(payload)
        elif result_kind == "summary":
            if isinstance(payload, dict):
                parsed["summary"] = payload

    return parsed


def run_batch(
    target_db: PsqlClient | SupabaseLinkedClient,
    prepared_candidates: list[dict[str, Any]],
    prepared_emails: list[dict[str, Any]],
    *,
    commit_writes: bool,
    prelude_sql: str | None = None,
) -> dict[str, Any]:
    sql = build_batch_sql(
        prepared_candidates,
        prepared_emails,
        commit_writes=commit_writes,
        prelude_sql=prelude_sql,
    )
    rows = target_db.query_json_rows(sql)
    return parse_batch_output(rows)


def summarize_batch_results(
    batch_output: Mapping[str, Any],
    *,
    candidate_source_rows_read: int,
    legacy_email_rows_read: int,
    sample_limit: int,
) -> dict[str, Any]:
    candidate_results = list(batch_output["candidate_results"])
    email_results = list(batch_output["email_results"])
    primary_plans = list(batch_output["primary_plans"])
    sql_summary = dict(batch_output.get("summary", {}))

    profile_action_counts: Counter[str] = Counter()
    profile_write_effect_counts: Counter[str] = Counter()
    email_action_counts: Counter[str] = Counter()
    profile_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    email_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in candidate_results:
        action = str(row.get("match_action"))
        effect = str(row.get("profile_write_effect"))
        profile_action_counts[action] += 1
        profile_write_effect_counts[effect] += 1
        if len(profile_samples[action]) < sample_limit:
            profile_samples[action].append(
                {
                    "source_candidate_id": row.get("source_candidate_id"),
                    "resolved_candidate_id": row.get("resolved_candidate_id"),
                    "match_basis": row.get("match_basis"),
                    "ambiguity_type": row.get("ambiguity_type"),
                    "profile_write_effect": row.get("profile_write_effect"),
                }
            )

    for row in email_results:
        action = str(row.get("email_action"))
        email_action_counts[action] += 1
        if len(email_samples[action]) < sample_limit:
            email_samples[action].append(
                {
                    "resolved_candidate_id": row.get("resolved_candidate_id"),
                    "source_candidate_id": row.get("source_candidate_id"),
                    "email_normalized": row.get("email_normalized"),
                    "email_source": row.get("email_source"),
                    "is_primary": row.get("is_primary"),
                    "duplicate_collapsed_count": row.get("duplicate_collapsed_count"),
                    "conflict_reason": row.get("conflict_reason"),
                }
            )

    mapping_artifact = {
        str(row["source_candidate_id"]): {
            "match_action": row.get("match_action"),
            "match_basis": row.get("match_basis"),
            "resolved_candidate_id": row.get("resolved_candidate_id"),
            "skip_reason": row.get("skip_reason") or row.get("ambiguity_type"),
        }
        for row in candidate_results
    }

    return {
        "candidate_source_rows_read": candidate_source_rows_read,
        "legacy_candidate_email_rows_read": legacy_email_rows_read,
        "fallback_rows_synthesized": int(sql_summary.get("fallback_rows_synthesized_raw", 0)),
        "fallback_rows_surviving_after_normalization_and_dedupe": int(
            sql_summary.get(
                "fallback_rows_surviving_after_normalization_and_dedupe",
                0,
            )
        ),
        "profile_action_counts": dict(profile_action_counts),
        "profile_write_effect_counts": dict(profile_write_effect_counts),
        "profile_ambiguities_logged": sum(
            1 for row in candidate_results if row.get("ambiguity_logged")
        ),
        "email_action_counts": dict(email_action_counts),
        "duplicate_email_rows_collapsed": int(
            sql_summary.get("duplicate_email_rows_collapsed", 0)
        ),
        "candidates_with_one_primary_after_normalization": int(
            sql_summary.get("candidates_with_one_primary", 0)
        ),
        "candidates_with_no_primary_after_normalization": int(
            sql_summary.get("candidates_with_no_primary", 0)
        ),
        "candidates_with_multi_primary_source_conflicts_resolved": int(
            sql_summary.get("candidates_with_multi_primary_source_conflicts", 0)
        ),
        "profile_samples": dict(profile_samples),
        "email_samples": dict(email_samples),
        "candidate_results": candidate_results,
        "email_results": email_results,
        "primary_plans": primary_plans,
        "mapping_artifact": mapping_artifact,
    }


def base_summary(*, sample_limit: int) -> dict[str, Any]:
    return {
        "candidate_source_rows_read": 0,
        "legacy_candidate_email_rows_read": 0,
        "fallback_rows_synthesized": 0,
        "fallback_rows_surviving_after_normalization_and_dedupe": 0,
        "profile_action_counts": {},
        "profile_write_effect_counts": {},
        "profile_ambiguities_logged": 0,
        "email_action_counts": {},
        "duplicate_email_rows_collapsed": 0,
        "candidates_with_one_primary_after_normalization": 0,
        "candidates_with_no_primary_after_normalization": 0,
        "candidates_with_multi_primary_source_conflicts_resolved": 0,
        "profile_samples": {},
        "email_samples": {},
        "sample_limit": sample_limit,
    }


def merge_summary(base: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    base["candidate_source_rows_read"] += int(incoming["candidate_source_rows_read"])
    base["legacy_candidate_email_rows_read"] += int(
        incoming["legacy_candidate_email_rows_read"]
    )
    base["fallback_rows_synthesized"] += int(incoming["fallback_rows_synthesized"])
    base["fallback_rows_surviving_after_normalization_and_dedupe"] += int(
        incoming["fallback_rows_surviving_after_normalization_and_dedupe"]
    )
    base["profile_ambiguities_logged"] += int(incoming["profile_ambiguities_logged"])
    base["duplicate_email_rows_collapsed"] += int(
        incoming["duplicate_email_rows_collapsed"]
    )
    base["candidates_with_one_primary_after_normalization"] += int(
        incoming["candidates_with_one_primary_after_normalization"]
    )
    base["candidates_with_no_primary_after_normalization"] += int(
        incoming["candidates_with_no_primary_after_normalization"]
    )
    base["candidates_with_multi_primary_source_conflicts_resolved"] += int(
        incoming["candidates_with_multi_primary_source_conflicts_resolved"]
    )

    for key in ("profile_action_counts", "profile_write_effect_counts", "email_action_counts"):
        counter = Counter(base[key])
        counter.update(incoming[key])
        base[key] = dict(counter)

    for key in ("profile_samples", "email_samples"):
        for action, samples in incoming[key].items():
            existing = base[key].setdefault(action, [])
            remaining = max(0, base["sample_limit"] - len(existing))
            if remaining:
                existing.extend(samples[:remaining])

    return base


def build_report_path(
    report_dir: Path,
    *,
    scope: str,
    extension: str,
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    base = report_dir / f"{stamp}__qa_{scope}.{extension}"

    if not base.exists():
        return base

    counter = 2
    while True:
        candidate = report_dir / f"{stamp}__qa_{scope}_{counter}.{extension}"
        if not candidate.exists():
            return candidate
        counter += 1


def write_json_report(report_dir: Path, payload: Mapping[str, Any], *, scope: str) -> Path:
    path = build_report_path(report_dir, scope=scope, extension="json")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_text_report(report_dir: Path, text: str, *, scope: str, extension: str) -> Path:
    path = build_report_path(report_dir, scope=scope, extension=extension)
    path.write_text(text, encoding="utf-8")
    return path


def render_markdown_report(report: Mapping[str, Any]) -> str:
    overall = report["main_pass"]["summary"]
    lines = [
        "# Candidate Profiles + Emails Backfill QA Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Script: `{report['script_name']}`",
        f"- Mode: `{report['mode']}`",
        f"- Limit: `{report['limit']}`",
        f"- Batch size: `{report['batch_size']}`",
        "",
        "## Overall Counts",
        "",
        f"- Candidate source rows read: `{overall['candidate_source_rows_read']}`",
        f"- Legacy candidate_emails rows read: `{overall['legacy_candidate_email_rows_read']}`",
        f"- Fallback candidate emails synthesized: `{overall['fallback_rows_synthesized']}`",
        f"- Fallback candidate emails surviving normalization/dedupe: `{overall['fallback_rows_surviving_after_normalization_and_dedupe']}`",
        f"- Profile action counts: `{json.dumps(overall['profile_action_counts'], sort_keys=True)}`",
        f"- Profile write effect counts: `{json.dumps(overall['profile_write_effect_counts'], sort_keys=True)}`",
        f"- Profile ambiguities logged: `{overall['profile_ambiguities_logged']}`",
        f"- Email action counts: `{json.dumps(overall['email_action_counts'], sort_keys=True)}`",
        f"- Duplicate email rows collapsed: `{overall['duplicate_email_rows_collapsed']}`",
        f"- Candidates with one primary after normalization: `{overall['candidates_with_one_primary_after_normalization']}`",
        f"- Candidates with no primary after normalization: `{overall['candidates_with_no_primary_after_normalization']}`",
        f"- Candidates with multi-primary source conflicts resolved: `{overall['candidates_with_multi_primary_source_conflicts_resolved']}`",
    ]

    duplicate_validation = report.get("duplicate_validation")
    if duplicate_validation is not None:
        lines.extend(
            [
                "",
                "## Duplicate Validation",
                "",
                f"- Cases run: `{duplicate_validation['cases_run']}`",
                f"- Cases passed: `{duplicate_validation['cases_passed']}`",
                f"- Cases failed: `{duplicate_validation['cases_failed']}`",
                f"- All passed: `{duplicate_validation['all_passed']}`",
            ]
        )

    return "\n".join(lines) + "\n"


def load_state_store(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"batches": {}, "progress": {}}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Progress sidecar {path} contains invalid JSON. "
            "Inspect or remove it before retrying."
        ) from exc
    if not isinstance(data, dict):
        raise ValueError(f"Progress sidecar {path} must contain a JSON object.")

    batches = data.get("batches")
    if not isinstance(batches, dict):
        batches = {}

    progress = data.get("progress")
    if not isinstance(progress, dict):
        progress = {}

    return {"batches": batches, "progress": progress}


def save_state_store(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "batches": state.get("batches", {})
        if isinstance(state.get("batches"), dict)
        else {},
        "progress": state.get("progress", {})
        if isinstance(state.get("progress"), dict)
        else {},
    }
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f"{path.stem}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temp_name = handle.name

    os.replace(temp_name, path)


def strip_batch_summary_for_state(batch_summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in batch_summary.items()
        if key not in {"candidate_results", "email_results", "primary_plans"}
    }


def restore_persisted_progress(
    state_store: Mapping[str, Any],
    *,
    sample_limit: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    summary = base_summary(sample_limit=sample_limit)
    mapping_artifact: dict[str, Any] = {}
    raw_batches = state_store.get("batches", {})
    if not isinstance(raw_batches, dict):
        raw_batches = {}

    batch_summaries = sorted(
        (
            batch
            for batch in raw_batches.values()
            if isinstance(batch, dict)
        ),
        key=lambda batch: (int(batch.get("batch_number", 0)), str(batch.get("rows_seen", ""))),
    )

    for batch_summary in batch_summaries:
        merge_summary(summary, batch_summary)
        mapping_artifact.update(batch_summary.get("mapping_artifact", {}))

    return summary, batch_summaries, mapping_artifact


def count_rows_written_from_summary(summary: Mapping[str, Any]) -> int:
    return int(summary.get("profile_write_effect_counts", {}).get("inserted", 0)) + int(
        summary.get("profile_write_effect_counts", {}).get("updated", 0)
    ) + int(summary.get("email_action_counts", {}).get("inserted", 0)) + int(
        summary.get("email_action_counts", {}).get("match_existing", 0)
    )


def count_rows_skipped_from_summary(summary: Mapping[str, Any]) -> int:
    return int(summary.get("profile_action_counts", {}).get("skip", 0)) + int(
        summary.get("email_action_counts", {}).get("skip_conflict", 0)
    )


def progress_sort_key(progress: Mapping[str, Any]) -> tuple[int, int, str]:
    return (
        int(progress.get("batch_number", 0) or 0),
        int(progress.get("rows_seen", 0) or 0),
        str(progress.get("cursor") or ""),
    )


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
) -> dict[str, Any]:
    dry_run = args.mode == "dry-run"
    limit = effective_limit(args)

    available_candidate_columns = fetch_source_table_columns(
        source_db,
        schema_name="public",
        table_name="candidates",
    )
    available_email_columns = fetch_source_table_columns(
        source_db,
        schema_name="public",
        table_name="candidate_emails",
    )

    validate_target_prerequisites(target_db)

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

    state_path = checkpoint.directory / f"{checkpoint.path.stem}__progress_state.json"
    if dry_run:
        state_store = {"batches": {}}
    else:
        if args.force_rerun and state_path.exists():
            state_path.unlink()
        state_store = load_state_store(state_path)

    if not should_run:
        LOGGER.info(
            "checkpoint %s already completed at %s; use --force-rerun to start over",
            checkpoint.path.name,
            state.get("completed_at"),
        )
        persisted_summary, persisted_batches, persisted_mapping = (
            restore_persisted_progress(state_store, sample_limit=args.sample_limit)
            if not dry_run
            else (base_summary(sample_limit=args.sample_limit), [], {})
        )
        return {
            "checkpoint_short_circuit": True,
            "checkpoint_state": state,
            "summary": persisted_summary,
            "batch_summaries": persisted_batches,
            "rows_seen": persisted_summary["candidate_source_rows_read"],
            "rows_written": count_rows_written_from_summary(persisted_summary),
            "cursor": state["progress"]["cursor"],
            "mode": args.mode,
            "limit": limit,
            "mapping_artifact": persisted_mapping,
        }

    if dry_run:
        summary = base_summary(sample_limit=args.sample_limit)
        batch_summaries: list[dict[str, Any]] = []
        mapping_artifact: dict[str, Any] = {}
    else:
        summary, batch_summaries, mapping_artifact = restore_persisted_progress(
            state_store,
            sample_limit=args.sample_limit,
        )
        persisted_progress = state_store.get("progress", {})
        if isinstance(persisted_progress, dict) and progress_sort_key(
            persisted_progress
        ) > progress_sort_key(state["progress"]):
            LOGGER.warning(
                "progress sidecar %s is ahead of checkpoint %s; reconciling to batch=%s cursor=%s",
                state_path.name,
                checkpoint.path.name,
                persisted_progress.get("batch_number"),
                persisted_progress.get("cursor"),
            )
            restored_rows_seen = int(
                persisted_progress.get(
                    "rows_seen",
                    summary["candidate_source_rows_read"],
                )
                or 0
            )
            restored_rows_written = int(
                persisted_progress.get(
                    "rows_written",
                    count_rows_written_from_summary(summary),
                )
                or 0
            )
            restored_rows_skipped = int(
                persisted_progress.get(
                    "rows_skipped",
                    count_rows_skipped_from_summary(summary),
                )
                or 0
            )
            state = checkpoint.update(
                {
                    "progress": {
                        "batch_number": int(
                            persisted_progress.get(
                                "batch_number",
                                state["progress"]["batch_number"],
                            )
                            or 0
                        ),
                        "batch_size": persisted_progress.get("batch_size")
                        or args.batch_size,
                        "cursor": persisted_progress.get("cursor"),
                        "last_seen_key": persisted_progress.get("last_seen_key"),
                        "rows_seen": restored_rows_seen,
                        "rows_processed": int(
                            persisted_progress.get("rows_processed", restored_rows_seen)
                            or 0
                        ),
                        "rows_written": restored_rows_written,
                        "rows_skipped": restored_rows_skipped,
                        "has_more": bool(
                            persisted_progress.get(
                                "has_more",
                                state["progress"].get("has_more", True),
                            )
                        ),
                    },
                    "summary": {
                        "mode": args.mode,
                        "rows_seen": restored_rows_seen,
                        "rows_written": restored_rows_written,
                    },
                }
            )
    total_rows_seen = int(summary["candidate_source_rows_read"])
    total_rows_written = count_rows_written_from_summary(summary)
    total_rows_skipped = count_rows_skipped_from_summary(summary)
    batch_number = int(state["progress"]["batch_number"])
    cursor = state["progress"]["cursor"]
    source_exhausted = False

    while True:
        remaining = None if limit is None else max(0, limit - total_rows_seen)
        if remaining == 0:
            break

        current_batch_size = args.batch_size
        if remaining is not None:
            current_batch_size = min(current_batch_size, remaining)

        candidate_batch = fetch_source_candidates(
            source_db,
            cursor=cursor,
            batch_size=current_batch_size,
            available_columns=available_candidate_columns,
        )

        if not candidate_batch:
            source_exhausted = True
            break

        candidate_ids = [row["source_candidate_id"] for row in candidate_batch]
        email_batch = fetch_source_emails_for_candidates(
            source_db,
            candidate_ids=candidate_ids,
            available_columns=available_email_columns,
        )

        batch_number += 1
        batch_output = run_batch(
            target_db,
            candidate_batch,
            email_batch,
            commit_writes=not dry_run,
        )
        batch_summary = summarize_batch_results(
            batch_output,
            candidate_source_rows_read=len(candidate_batch),
            legacy_email_rows_read=len(email_batch),
            sample_limit=args.sample_limit,
        )
        batch_summary["batch_number"] = batch_number
        batch_summary["rows_seen"] = len(candidate_batch)

        rows_written = (
            batch_summary["profile_write_effect_counts"].get("inserted", 0)
            + batch_summary["profile_write_effect_counts"].get("updated", 0)
            + batch_summary["email_action_counts"].get("inserted", 0)
            + batch_summary["email_action_counts"].get("match_existing", 0)
        )
        rows_skipped = batch_summary["profile_action_counts"].get(
            "skip", 0
        ) + batch_summary["email_action_counts"].get("skip_conflict", 0)
        batch_cursor = next_cursor(candidate_batch, cursor)

        if dry_run:
            batch_summaries.append(batch_summary)
            merge_summary(summary, batch_summary)
            mapping_artifact.update(batch_summary["mapping_artifact"])
        else:
            batch_key = candidate_batch[-1]["source_candidate_id"]
            state_store.setdefault("batches", {})[batch_key] = strip_batch_summary_for_state(
                batch_summary
            )
            state_store["progress"] = {
                "batch_number": batch_number,
                "batch_size": args.batch_size,
                "cursor": batch_cursor,
                "last_seen_key": candidate_batch[-1]["source_candidate_id"],
                "rows_seen": total_rows_seen + len(candidate_batch),
                "rows_processed": total_rows_seen + len(candidate_batch),
                "rows_written": total_rows_written + rows_written,
                "rows_skipped": total_rows_skipped + rows_skipped,
                "has_more": (remaining is None or remaining > len(candidate_batch)),
            }
            save_state_store(state_path, state_store)
            summary, batch_summaries, mapping_artifact = restore_persisted_progress(
                state_store,
                sample_limit=args.sample_limit,
            )

        total_rows_seen += len(candidate_batch)
        total_rows_written += rows_written
        total_rows_skipped += rows_skipped
        cursor = batch_cursor

        if dry_run:
            LOGGER.info(
                "dry-run batch=%s candidates=%s profile_actions=%s email_actions=%s",
                batch_number,
                len(candidate_batch),
                json.dumps(batch_summary["profile_action_counts"], sort_keys=True),
                json.dumps(batch_summary["email_action_counts"], sort_keys=True),
            )
        else:
            checkpoint.record_batch(
                batch_number=batch_number,
                batch_size=args.batch_size,
                cursor=cursor,
                last_seen_key=candidate_batch[-1]["source_candidate_id"],
                rows_seen=len(candidate_batch),
                rows_processed=len(candidate_batch),
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                has_more=(remaining is None or remaining > len(candidate_batch)),
            )

        if len(candidate_batch) < current_batch_size:
            source_exhausted = True
            break

    if not dry_run:
        if source_exhausted:
            checkpoint.mark_completed(
                summary={
                    "mode": args.mode,
                    "rows_seen": summary["candidate_source_rows_read"],
                    "rows_written": count_rows_written_from_summary(summary),
                }
            )
        else:
            checkpoint.update(
                {
                    "status": "running",
                    "progress": {"has_more": True},
                    "summary": {
                        "mode": args.mode,
                        "rows_seen": summary["candidate_source_rows_read"],
                        "rows_written": count_rows_written_from_summary(summary),
                        "stopped_due_to_limit": True,
                    },
                }
            )

    return {
        "checkpoint_short_circuit": False,
        "summary": summary,
        "batch_summaries": batch_summaries,
        "rows_seen": summary["candidate_source_rows_read"],
        "rows_written": count_rows_written_from_summary(summary),
        "cursor": cursor,
        "mode": args.mode,
        "limit": limit,
        "mapping_artifact": mapping_artifact,
    }


def build_duplicate_fixture_prelude(fixture_ids: Mapping[str, str]) -> str:
    existing_rows = [
        {
            "id": fixture_ids["stable_rerun"],
            "full_name": "Stable Existing Candidate",
            "linkedin_username": None,
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "id": fixture_ids["username_match_existing"],
            "full_name": "Username Existing Candidate",
            "linkedin_username": "task7a-username-match",
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "id": fixture_ids["url_match_existing"],
            "full_name": "URL Existing Candidate",
            "linkedin_username": None,
            "linkedin_url": "https://www.linkedin.com/in/task7a-url-match",
            "linkedin_url_normalized": "https://www.linkedin.com/in/task7a-url-match",
        },
        {
            "id": fixture_ids["legacy_id_conflict_existing"],
            "full_name": "Legacy Id Existing Candidate",
            "linkedin_username": None,
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "id": fixture_ids["legacy_id_conflict_target"],
            "full_name": "Legacy Id Conflict Target",
            "linkedin_username": "task7a-legacy-id-conflict",
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "id": fixture_ids["conflicting_existing_username"],
            "full_name": "Existing Username A",
            "linkedin_username": "task7a-existing-a",
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
        {
            "id": fixture_ids["conflicting_existing_url"],
            "full_name": "Existing URL B",
            "linkedin_username": None,
            "linkedin_url": "https://www.linkedin.com/in/task7a-existing-a",
            "linkedin_url_normalized": "https://www.linkedin.com/in/task7a-existing-a",
        },
        {
            "id": fixture_ids["existing_nonlegacy_primary_candidate"],
            "full_name": "Existing Nonlegacy Primary Candidate",
            "linkedin_username": None,
            "linkedin_url": None,
            "linkedin_url_normalized": None,
        },
    ]

    values_sql = []
    for row in existing_rows:
        values_sql.append(
            "("
            + ", ".join(
                [
                    f"{sql_text_literal(row['id'])}::uuid",
                    sql_text_literal(row["full_name"]),
                    sql_text_literal(row["linkedin_username"]),
                    sql_text_literal(row["linkedin_url"]),
                    sql_text_literal(row["linkedin_url_normalized"]),
                    sql_text_literal(PROFILE_SOURCE_VALUE),
                    "'[]'::jsonb",
                ]
            )
            + ")"
        )

    existing_email_values = [
        "("
        + ", ".join(
            [
                f"{sql_text_literal(fixture_ids['existing_nonlegacy_primary_candidate'])}::uuid",
                sql_text_literal("retained-primary@example.com"),
                "lower('retained-primary@example.com')::extensions.citext",
                sql_text_literal("personal"),
                sql_text_literal("linkedin_import"),
                "true",
                "1",
            ]
        )
        + ")"
    ]

    return f"""
insert into public.candidate_profiles_v2 (
    id,
    full_name,
    linkedin_username,
    linkedin_url,
    linkedin_url_normalized,
    source,
    source_record_refs
)
values
{",\n".join(values_sql)};

insert into public.candidate_emails_v2 (
    candidate_id,
    email_raw,
    email_normalized,
    email_type,
    email_source,
    is_primary,
    verification_attempts
)
values
{",\n".join(existing_email_values)};
"""


def build_duplicate_fixture_records() -> tuple[
    dict[str, str],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    fixture_ids = {
        name: str(uuid4())
        for name in [
            "stable_rerun",
            "username_match_existing",
            "url_match_existing",
            "legacy_id_conflict_existing",
            "legacy_id_conflict_target",
            "conflicting_existing_username",
            "conflicting_existing_url",
            "username_match_incoming",
            "url_match_incoming",
            "conflicting_incoming_identity",
            "conflicting_existing_identity",
            "email_duplicate_candidate",
            "cross_candidate_a",
            "cross_candidate_b",
            "multi_primary_candidate",
            "no_primary_candidate",
            "fallback_duplicate_candidate",
            "existing_nonlegacy_primary_candidate",
        ]
    }

    candidate_records = [
        {
            "source_candidate_id": fixture_ids["stable_rerun"],
            "full_name": "Stable Existing Candidate",
            "first_name": "Stable",
            "last_name": "Existing",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": "Stable headline",
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Stable Existing Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["username_match_incoming"],
            "full_name": "Username Match Candidate",
            "first_name": "Username",
            "last_name": "Match",
            "raw_linkedin_username": "task7a-username-match",
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Username Match Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["url_match_incoming"],
            "full_name": "URL Match Candidate",
            "first_name": "URL",
            "last_name": "Match",
            "raw_linkedin_username": None,
            "raw_linkedin_url": "https://linkedin.com/in/task7a-url-match/",
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "URL Match Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["conflicting_incoming_identity"],
            "full_name": "Conflicting Incoming Candidate",
            "first_name": "Conflict",
            "last_name": "Incoming",
            "raw_linkedin_username": "task7a-conflict-a",
            "raw_linkedin_url": "https://www.linkedin.com/in/task7a-conflict-b/",
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Conflicting Incoming Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["legacy_id_conflict_existing"],
            "full_name": "Legacy Id Conflict Incoming",
            "first_name": "Legacy",
            "last_name": "Conflict",
            "raw_linkedin_username": "task7a-legacy-id-conflict",
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Legacy Id Conflict Incoming",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["conflicting_existing_identity"],
            "full_name": "Conflicting Existing Candidate",
            "first_name": "Conflict",
            "last_name": "Existing",
            "raw_linkedin_username": "task7a-existing-a",
            "raw_linkedin_url": "https://www.linkedin.com/in/task7a-existing-a/",
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Conflicting Existing Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["email_duplicate_candidate"],
            "full_name": "Email Duplicate Candidate",
            "first_name": "Email",
            "last_name": "Duplicate",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Email Duplicate Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["cross_candidate_a"],
            "full_name": "Cross Candidate A",
            "first_name": "Cross",
            "last_name": "A",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Cross Candidate A",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["cross_candidate_b"],
            "full_name": "Cross Candidate B",
            "first_name": "Cross",
            "last_name": "B",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Cross Candidate B",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["multi_primary_candidate"],
            "full_name": "Multi Primary Candidate",
            "first_name": "Multi",
            "last_name": "Primary",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Multi Primary Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
        {
            "source_candidate_id": fixture_ids["no_primary_candidate"],
            "full_name": "No Primary Candidate",
            "first_name": "No",
            "last_name": "Primary",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "No Primary Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": "preferred@example.com",
        },
        {
            "source_candidate_id": fixture_ids["fallback_duplicate_candidate"],
            "full_name": "Fallback Duplicate Candidate",
            "first_name": "Fallback",
            "last_name": "Duplicate",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Fallback Duplicate Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": "dup@example.com",
        },
        {
            "source_candidate_id": fixture_ids["existing_nonlegacy_primary_candidate"],
            "full_name": "Existing Nonlegacy Primary Candidate",
            "first_name": "Existing",
            "last_name": "Primary",
            "raw_linkedin_username": None,
            "raw_linkedin_url": None,
            "headline": None,
            "summary": None,
            "location": None,
            "profile_picture_url": None,
            "phone": None,
            "education_summary": None,
            "education_schools": None,
            "education_degrees": None,
            "education_fields": None,
            "skills_text": None,
            "top_skills": None,
            "linkedin_enrichment_status": None,
            "linkedin_enrichment_date": None,
            "legacy_source": "legacy_fixture",
            "raw_full_name": "Existing Nonlegacy Primary Candidate",
            "legacy_created_at": None,
            "legacy_updated_at": None,
            "fallback_email_raw": None,
        },
    ]

    email_records = [
        {
            "source_candidate_id": fixture_ids["email_duplicate_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-email-duplicate-1",
            "email_raw": "Person@Example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": "2024-01-01T00:00:00Z",
            "verification_attempts": 1,
            "last_verification_attempt": "2024-01-01T00:00:00Z",
            "raw_response": None,
            "source_created_at": "2024-01-01T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["email_duplicate_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-email-duplicate-2",
            "email_raw": "mailto:person@example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": True,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": "2024-02-01T00:00:00Z",
            "verification_attempts": 2,
            "last_verification_attempt": "2024-02-01T00:00:00Z",
            "raw_response": None,
            "source_created_at": "2024-01-02T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["cross_candidate_a"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-cross-a",
            "email_raw": "shared@example.com",
            "raw_email_type": "business",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": True,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-03T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["cross_candidate_b"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-cross-b",
            "email_raw": "shared@example.com",
            "raw_email_type": "business",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": True,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-03T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["multi_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-multi-primary-1",
            "email_raw": "alpha@example.com",
            "raw_email_type": "business",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": True,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": "2024-01-10T00:00:00Z",
            "verification_attempts": 1,
            "last_verification_attempt": "2024-01-10T00:00:00Z",
            "raw_response": None,
            "source_created_at": "2024-01-10T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["multi_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-multi-primary-2",
            "email_raw": "beta@example.com",
            "raw_email_type": "business",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": True,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": "2024-02-10T00:00:00Z",
            "verification_attempts": 3,
            "last_verification_attempt": "2024-02-10T00:00:00Z",
            "raw_response": None,
            "source_created_at": "2024-01-11T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["no_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-no-primary-1",
            "email_raw": "other@example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-12T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["no_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-no-primary-2",
            "email_raw": "preferred@example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-13T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["fallback_duplicate_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-fallback-duplicate",
            "email_raw": "dup@example.com",
            "raw_email_type": "business",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-14T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["existing_nonlegacy_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-existing-primary-1",
            "email_raw": "other-existing-primary@example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-15T00:00:00Z",
        },
        {
            "source_candidate_id": fixture_ids["existing_nonlegacy_primary_candidate"],
            "source_row_kind": "candidate_email",
            "source_row_id": "fixture-existing-primary-2",
            "email_raw": "another-existing-primary@example.com",
            "raw_email_type": "personal",
            "raw_email_source": "legacy_candidate_emails",
            "raw_is_primary": False,
            "quality": None,
            "result": None,
            "resultcode": None,
            "subresult": None,
            "verification_date": None,
            "verification_attempts": 0,
            "last_verification_attempt": None,
            "raw_response": None,
            "source_created_at": "2024-01-16T00:00:00Z",
        },
    ]

    expectations = {
        "candidate_expectations": {
            fixture_ids["stable_rerun"]: {
                "match_action": "match_existing",
                "match_basis": "legacy_id",
            },
            fixture_ids["username_match_incoming"]: {
                "match_action": "match_existing",
                "match_basis": "linkedin_username",
            },
            fixture_ids["url_match_incoming"]: {
                "match_action": "match_existing",
                "match_basis": "linkedin_url_normalized",
            },
            fixture_ids["conflicting_incoming_identity"]: {
                "match_action": "skip",
                "ambiguity_type": "conflicting_incoming_linkedin_identity",
            },
            fixture_ids["legacy_id_conflict_existing"]: {
                "match_action": "skip",
                "ambiguity_type": "legacy_id_conflicts_with_linkedin_identity",
            },
            fixture_ids["conflicting_existing_identity"]: {
                "match_action": "skip",
                "ambiguity_type": "conflicting_existing_linkedin_identity",
            },
            fixture_ids["existing_nonlegacy_primary_candidate"]: {
                "match_action": "match_existing",
                "match_basis": "legacy_id",
            },
        },
        "email_duplicate_candidate": fixture_ids["email_duplicate_candidate"],
        "cross_candidate_a": fixture_ids["cross_candidate_a"],
        "cross_candidate_b": fixture_ids["cross_candidate_b"],
        "multi_primary_candidate": fixture_ids["multi_primary_candidate"],
        "no_primary_candidate": fixture_ids["no_primary_candidate"],
        "fallback_duplicate_candidate": fixture_ids["fallback_duplicate_candidate"],
        "existing_nonlegacy_primary_candidate": fixture_ids[
            "existing_nonlegacy_primary_candidate"
        ],
    }

    return fixture_ids, candidate_records, email_records, expectations


def run_duplicate_validation(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> dict[str, Any]:
    fixture_ids, candidate_records, email_records, expectations = (
        build_duplicate_fixture_records()
    )
    prelude = build_duplicate_fixture_prelude(fixture_ids)
    batch_output = run_batch(
        target_db,
        candidate_records,
        email_records,
        commit_writes=False,
        prelude_sql=prelude,
    )

    candidate_results = {
        row["source_candidate_id"]: row for row in batch_output["candidate_results"]
    }
    email_results_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in batch_output["email_results"]:
        resolved_candidate_id = row.get("resolved_candidate_id")
        if resolved_candidate_id is not None:
            email_results_by_candidate[str(resolved_candidate_id)].append(row)

    primary_plans = {
        row["resolved_candidate_id"]: row for row in batch_output["primary_plans"]
    }

    case_results: list[dict[str, Any]] = []
    passed_count = 0

    for source_candidate_id, expectation in expectations["candidate_expectations"].items():
        actual = candidate_results.get(source_candidate_id, {})
        passed = True
        mismatches: list[str] = []
        for key, expected_value in expectation.items():
            if actual.get(key) != expected_value:
                passed = False
                mismatches.append(
                    f"{key}: expected {expected_value!r}, received {actual.get(key)!r}"
                )
        if passed:
            passed_count += 1
        case_results.append(
            {
                "case_name": f"candidate:{source_candidate_id}",
                "passed": passed,
                "expected": expectation,
                "actual": actual,
                "mismatches": mismatches,
            }
        )

    duplicate_candidate_id = expectations["email_duplicate_candidate"]
    duplicate_candidate_rows = email_results_by_candidate.get(duplicate_candidate_id, [])
    duplicate_case_passed = (
        len(duplicate_candidate_rows) == 1
        and duplicate_candidate_rows[0].get("duplicate_collapsed_count") == 1
    )
    if duplicate_case_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:duplicate_within_candidate",
            "passed": duplicate_case_passed,
            "expected": {"email_result_count": 1, "duplicate_collapsed_count": 1},
            "actual": duplicate_candidate_rows,
            "mismatches": []
            if duplicate_case_passed
            else ["Expected exactly one surviving email row with duplicate_collapsed_count=1."],
        }
    )

    cross_a_rows = email_results_by_candidate.get(expectations["cross_candidate_a"], [])
    cross_b_rows = email_results_by_candidate.get(expectations["cross_candidate_b"], [])
    cross_conflict_passed = (
        len(cross_a_rows) == 1
        and len(cross_b_rows) == 1
        and cross_a_rows[0].get("email_action") == "skip_conflict"
        and cross_b_rows[0].get("email_action") == "skip_conflict"
    )
    if cross_conflict_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:cross_candidate_conflict",
            "passed": cross_conflict_passed,
            "expected": {"email_action": "skip_conflict"},
            "actual": {"candidate_a": cross_a_rows, "candidate_b": cross_b_rows},
            "mismatches": []
            if cross_conflict_passed
            else ["Expected both cross-candidate rows to be skipped as conflicts."],
        }
    )

    multi_primary_plan = primary_plans.get(expectations["multi_primary_candidate"], {})
    multi_primary_rows = email_results_by_candidate.get(
        expectations["multi_primary_candidate"], []
    )
    multi_primary_passed = (
        multi_primary_plan.get("source_primary_count") == 2
        and multi_primary_plan.get("chosen_primary_email") == "beta@example.com"
        and any(
            row.get("email_normalized") == "beta@example.com" and row.get("is_primary")
            for row in multi_primary_rows
        )
    )
    if multi_primary_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:multi_primary_resolution",
            "passed": multi_primary_passed,
            "expected": {
                "source_primary_count": 2,
                "chosen_primary_email": "beta@example.com",
            },
            "actual": {
                "primary_plan": multi_primary_plan,
                "email_results": multi_primary_rows,
            },
            "mismatches": []
            if multi_primary_passed
            else ["Expected multi-primary resolution to choose beta@example.com."],
        }
    )

    no_primary_plan = primary_plans.get(expectations["no_primary_candidate"], {})
    no_primary_passed = (
        no_primary_plan.get("source_primary_count") == 0
        and no_primary_plan.get("chosen_primary_email") == "preferred@example.com"
        and no_primary_plan.get("final_primary_count") == 1
    )
    if no_primary_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:no_primary_fallback_hint",
            "passed": no_primary_passed,
            "expected": {
                "source_primary_count": 0,
                "chosen_primary_email": "preferred@example.com",
                "final_primary_count": 1,
            },
            "actual": no_primary_plan,
            "mismatches": []
            if no_primary_passed
            else ["Expected fallback email hint to drive the primary choice."],
        }
    )

    retained_primary_plan = primary_plans.get(
        expectations["existing_nonlegacy_primary_candidate"], {}
    )
    retained_primary_rows = email_results_by_candidate.get(
        expectations["existing_nonlegacy_primary_candidate"], []
    )
    retained_primary_passed = (
        retained_primary_plan.get("source_primary_count") == 0
        and retained_primary_plan.get("chosen_primary_email") is None
        and retained_primary_plan.get("final_primary_count") == 1
        and len(retained_primary_rows) == 2
        and all(not row.get("is_primary") for row in retained_primary_rows)
    )
    if retained_primary_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:retain_preexisting_nonlegacy_primary",
            "passed": retained_primary_passed,
            "expected": {
                "source_primary_count": 0,
                "chosen_primary_email": None,
                "final_primary_count": 1,
                "incoming_email_results": 2,
            },
            "actual": {
                "primary_plan": retained_primary_plan,
                "email_results": retained_primary_rows,
            },
            "mismatches": []
            if retained_primary_passed
            else [
                "Expected an existing non-legacy primary email to remain primary when"
                " the incoming legacy-managed rows do not resolve a new primary."
            ],
        }
    )

    fallback_duplicate_rows = email_results_by_candidate.get(
        expectations["fallback_duplicate_candidate"], []
    )
    fallback_duplicate_passed = (
        len(fallback_duplicate_rows) == 1
        and isinstance(fallback_duplicate_rows[0].get("contributors"), list)
        and len(fallback_duplicate_rows[0]["contributors"]) == 1
        and fallback_duplicate_rows[0]["contributors"][0].get("source_row_kind")
        == "candidate_email"
    )
    if fallback_duplicate_passed:
        passed_count += 1
    case_results.append(
        {
            "case_name": "email:candidate_email_vs_fallback_duplicate",
            "passed": fallback_duplicate_passed,
            "expected": {
                "contributors": [{"source_row_kind": "candidate_email"}],
            },
            "actual": fallback_duplicate_rows,
            "mismatches": []
            if fallback_duplicate_passed
            else ["Expected fallback duplicate to be suppressed before email writes."],
        }
    )

    total_cases = len(case_results)
    return {
        "cases_run": total_cases,
        "cases_passed": passed_count,
        "cases_failed": total_cases - passed_count,
        "all_passed": passed_count == total_cases,
        "results": case_results,
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
            app_name="candidate-profiles-backfill-source",
        )
        target_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="candidate-profiles-backfill-target",
        )
    else:
        source_dsn, source_env_used = load_dsn(
            args.source_dsn_env,
            fallback_env=args.target_dsn_env,
        )
        target_dsn, target_env_used = load_dsn(args.target_dsn_env)
        source_db = PsqlClient(source_dsn, app_name="candidate-profiles-backfill-source")
        target_db = PsqlClient(target_dsn, app_name="candidate-profiles-backfill-target")

    checkpoint = JsonCheckpoint(args.checkpoint_name)
    report_dir = Path(args.report_dir).resolve()

    try:
        main_pass = run_main_pass(
            args,
            source_db=source_db,
            target_db=target_db,
            checkpoint=checkpoint,
        )
        duplicate_validation = (
            run_duplicate_validation(target_db)
            if should_run_duplicate_validation(args)
            else None
        )
    except PsqlError as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - defensive runtime reporting
        LOGGER.exception("candidate profile/email backfill failed: %s", exc)
        return 1

    report = {
        "generated_at": utc_now(),
        "script_name": Path(__file__).name,
        "task_scope": "7a" if args.mode == "dry-run" else "7b_or_7c_apply",
        "mode": args.mode,
        "limit": main_pass["limit"],
        "batch_size": args.batch_size,
        "checkpoint_name": args.checkpoint_name,
        "source_dsn_env_used": source_env_used,
        "target_dsn_env_used": target_env_used,
        "legacy_source_tables": {
            "candidates": LEGACY_CANDIDATES_TABLE,
            "candidate_emails": LEGACY_CANDIDATE_EMAILS_TABLE,
        },
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
                    if key
                    not in {
                        "candidate_results",
                        "email_results",
                        "primary_plans",
                        "mapping_artifact",
                    }
                }
                for batch_summary in main_pass["batch_summaries"]
            ],
        },
        "duplicate_validation": duplicate_validation,
    }

    report_scope = (
        "candidate_profiles_emails_preflight"
        if args.mode == "dry-run"
        else "candidate_profiles_emails_backfill_apply"
    )
    report_path = write_json_report(report_dir, report, scope=report_scope)
    markdown_path = write_text_report(
        report_dir,
        render_markdown_report(report),
        scope=report_scope,
        extension="md",
    )
    mapping_path = write_json_report(
        report_dir,
        main_pass["mapping_artifact"],
        scope="candidate_profiles_emails_candidate_map",
    )
    LOGGER.info("wrote QA report to %s", report_path)
    LOGGER.info("wrote QA markdown report to %s", markdown_path)
    LOGGER.info("wrote candidate mapping artifact to %s", mapping_path)

    if duplicate_validation is not None and not duplicate_validation["all_passed"]:
        LOGGER.error("duplicate-validation fixtures failed; inspect %s", report_path)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
