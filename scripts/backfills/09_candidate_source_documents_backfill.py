#!/usr/bin/env python3
"""Checkpoint-aware candidate source-document backfill script.

Task coverage:
- Task 9a: implement the LinkedIn baseline source-document backfill script,
  run a deterministic dry-run on the first 100 legacy candidates, and run
  source-document duplicate/versioning fixtures in a rolled-back transaction
- Task 9b: later reuse this script in apply mode for a 100-row pilot write
- Task 9c: later reuse this script in apply mode for the full migration

Design notes:
- source reads are deterministic and ordered by legacy `candidates.id` ascending
- this script only reads the legacy `candidates` table and only writes
  `linkedin_profile` rows into `candidate_source_documents`
- resumes, recruiter notes, transcript summaries, chunks, embeddings, and
  aggregate search documents are intentionally out of scope for Task 9
- dry-run uses the real insert/supersede/ambiguity logic inside a transaction
  that is rolled back, so decision behavior matches a real run for the
  inspected batch without committing writes
- duplicate validation uses temporary fixture writes inside a rolled-back
  transaction and never mutates real legacy source data
- the script avoids Python DB dependencies and talks to Postgres via `psql`
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
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

DEFAULT_CHECKPOINT_NAME = "09_candidate_source_documents_backfill"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_PREFLIGHT_LIMIT = 100
DEFAULT_SOURCE_ENV = "LEGACY_DATABASE_URL"
DEFAULT_TARGET_ENV = "DATABASE_URL"
LEGACY_CANDIDATES_TABLE = "public.candidates"
REPORT_DIR = REPO_ROOT / "reports" / "qa"
AMBIGUITY_SOURCE_SYSTEM = "legacy_candidate_source_documents"
LINKEDIN_SOURCE_TYPE = "linkedin_profile"
LINKEDIN_TRUST_LEVEL = "baseline"
LINKEDIN_TITLE = "LinkedIn profile"


SOURCE_FIELD_ORDER = (
    "headline",
    "profile_summary",
    "location",
    "current_title",
    "current_company",
    "all_skills_text",
    "top_skills",
    "education",
    "work_experience",
    "previous_companies",
)


SECTION_DEFINITIONS = (
    ("headline", "Headline"),
    ("profile_summary", "Profile Summary"),
    ("location", "Location"),
    ("current_title", "Current Title"),
    ("current_company", "Current Company"),
    ("skills", "Skills"),
    ("education", "Education"),
    ("work_experience", "Work Experience"),
    ("previous_companies", "Previous Companies"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill LinkedIn baseline candidate source documents."
    )
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help=(
            "Execution mode. `dry-run` performs the real per-batch logic inside "
            "rolled-back transactions. `apply` commits writes to "
            "candidate_source_documents and canonicalization_ambiguities."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Alias for --mode=dry-run; included for the Task 9a contract.",
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
        help="Deterministic source candidate batch size.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional max legacy candidate row count for this run. Defaults to "
            "100 for dry-run mode and unlimited for apply mode."
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
        "--candidate-map-path",
        default=None,
        help=(
            "Path to the approved Task 7c candidate-resolution mapping artifact. "
            "Required so source documents link to the approved canonical candidate id."
        ),
    )
    parser.add_argument(
        "--run-duplicate-validation",
        action="store_true",
        help=(
            "Run rolled-back source-document duplicate/versioning fixtures after "
            "the main pass."
        ),
    )
    parser.add_argument(
        "--skip-duplicate-validation",
        action="store_true",
        help=(
            "Skip duplicate-validation fixtures. Dry-run mode runs them by default "
            "because Task 9a requires them."
        ),
    )
    parser.add_argument(
        "--confirm-duplicate-fixture-writes",
        action="store_true",
        help=(
            "Required when duplicate validation is enabled because fixtures write "
            "temporary rows inside a transaction that is rolled back."
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

    if args.dry_run:
        if args.mode != "dry-run":
            parser.error("--dry-run cannot be combined with --mode=apply.")
        args.mode = "dry-run"

    if args.batch_size <= 0:
        parser.error("--batch-size must be > 0.")

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be > 0 when provided.")

    if args.mode == "apply" and not args.confirm_apply:
        parser.error("--confirm-apply is required with --mode=apply.")

    if args.run_duplicate_validation and args.skip_duplicate_validation:
        parser.error(
            "--run-duplicate-validation and --skip-duplicate-validation cannot be used together."
        )

    if should_run_duplicate_validation(args) and not args.confirm_duplicate_fixture_writes:
        parser.error(
            "--confirm-duplicate-fixture-writes is required when duplicate validation is enabled."
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


def effective_limit(args: argparse.Namespace) -> int | None:
    if args.limit is not None:
        return args.limit
    if args.mode == "dry-run":
        return DEFAULT_PREFLIGHT_LIMIT
    return None


def blank_to_none(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return value


def clean_uuid_text(value: Any) -> str | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    return str(cleaned)


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


def preserve_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def sql_text_literal(value: str | None) -> str:
    if value is None:
        return "null"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_jsonb_literal(value: Any) -> str:
    return f"{sql_text_literal(json.dumps(value, sort_keys=True))}::jsonb"


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


def build_source_batch_sql(
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
        {source_select_expression('linkedin_username', available_columns, table_alias='c')} as raw_linkedin_username,
        {source_select_expression('linkedin_url', available_columns, table_alias='c')} as raw_linkedin_url,
        {source_select_expression('headline', available_columns, table_alias='c')} as headline,
        {source_select_expression('profile_summary', available_columns, table_alias='c')} as profile_summary,
        {source_select_expression('location', available_columns, table_alias='c')} as location,
        {source_select_expression('current_title', available_columns, table_alias='c')} as current_title,
        {source_select_expression('current_company', available_columns, table_alias='c')} as current_company,
        {source_select_expression('all_skills_text', available_columns, table_alias='c')} as all_skills_text,
        {source_select_expression('top_skills', available_columns, table_alias='c', null_expr='null::text[]')} as top_skills,
        {source_select_expression('education', available_columns, table_alias='c')} as education,
        {source_select_expression('work_experience', available_columns, table_alias='c', null_expr='null::jsonb')} as work_experience,
        {source_select_expression('previous_companies', available_columns, table_alias='c', null_expr='null::jsonb')} as previous_companies,
        {source_select_expression('linkedin_data', available_columns, table_alias='c', null_expr='null::jsonb')} as linkedin_data,
        {source_select_expression('linkedin_enrichment_status', available_columns, table_alias='c')} as linkedin_enrichment_status,
        {source_select_expression('linkedin_enrichment_date', available_columns, table_alias='c', null_expr='null::timestamptz')} as linkedin_enrichment_date,
        {source_select_expression('created_at', available_columns, table_alias='c', null_expr='null::timestamptz')} as legacy_created_at,
        {source_select_expression('updated_at', available_columns, table_alias='c', null_expr='null::timestamptz')} as legacy_updated_at
    from {LEGACY_CANDIDATES_TABLE} as c
    where {cursor_clause}
    order by c.id asc
    limit {batch_size}
) as t;
"""


def fetch_source_batch(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    cursor: str | None,
    batch_size: int,
    available_columns: set[str],
) -> list[dict[str, Any]]:
    sql = build_source_batch_sql(
        cursor,
        batch_size,
        available_columns=available_columns,
    )
    return source_db.query_json_rows(sql)


def normalize_line_text(text: str) -> str | None:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    for raw_line in text.split("\n"):
        cleaned = re.sub(r"[ \t]+", " ", raw_line.strip())
        if cleaned:
            lines.append(cleaned)
    if not lines:
        return None
    return "\n".join(lines)


def indent_multiline(text: str, *, spaces: int = 2) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line if line else line for line in text.split("\n"))


def format_source_value(value: Any) -> str | None:
    value = blank_to_none(value)
    if value is None:
        return None

    if isinstance(value, str):
        return normalize_line_text(value)

    if isinstance(value, (int, float, bool)):
        return str(value)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        parts = [format_source_value(item) for item in value]
        cleaned_parts = [part for part in parts if part]
        if not cleaned_parts:
            return None
        return "\n".join(cleaned_parts)

    if isinstance(value, Mapping):
        lines: list[str] = []
        for key in sorted(value):
            formatted = format_source_value(value.get(key))
            if formatted is None:
                continue
            key_text = str(key)
            if "\n" in formatted:
                lines.append(f"{key_text}:\n{indent_multiline(formatted)}")
            else:
                lines.append(f"{key_text}: {formatted}")
        if not lines:
            return None
        return "\n".join(lines)

    return normalize_line_text(str(value))


def normalize_document_text(raw_text: str | None) -> str | None:
    if raw_text is None:
        return None
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    sections: list[str] = []
    current_lines: list[str] = []

    def flush_current() -> None:
        nonlocal current_lines
        cleaned = [
            re.sub(r"\s+", " ", line.strip())
            for line in current_lines
            if line.strip()
        ]
        if cleaned:
            sections.append("\n".join(cleaned))
        current_lines = []

    for line in raw_text.split("\n"):
        if line.startswith("## ") and current_lines:
            flush_current()
        current_lines.append(line)
    flush_current()

    if not sections:
        return None
    return "\n\n".join(sections)


def build_sectioned_text(raw_record: Mapping[str, Any]) -> tuple[str | None, str | None, list[str]]:
    work_experience_text = format_source_value(raw_record.get("work_experience"))
    all_skills_text = format_source_value(raw_record.get("all_skills_text"))
    top_skills_text = format_source_value(raw_record.get("top_skills"))

    values_by_key: dict[str, str | None] = {
        "headline": format_source_value(raw_record.get("headline")),
        "profile_summary": format_source_value(raw_record.get("profile_summary")),
        "location": format_source_value(raw_record.get("location")),
        "current_title": format_source_value(raw_record.get("current_title")),
        "current_company": format_source_value(raw_record.get("current_company")),
        "skills": all_skills_text or top_skills_text,
        "education": format_source_value(raw_record.get("education")),
        "work_experience": work_experience_text,
        "previous_companies": None
        if work_experience_text
        else format_source_value(raw_record.get("previous_companies")),
    }

    sections: list[str] = []
    source_fields_used: list[str] = []
    for key, title in SECTION_DEFINITIONS:
        value = values_by_key[key]
        if value is None:
            continue
        sections.append(f"## {title}\n{value}")
        if key == "skills":
            source_fields_used.append("all_skills_text" if all_skills_text else "top_skills")
        else:
            source_fields_used.append(key)

    raw_text = "\n\n".join(sections) if sections else None
    normalized_text = normalize_document_text(raw_text)
    return raw_text, normalized_text, source_fields_used


def sha256_text(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def choose_effective_at(raw_record: Mapping[str, Any]) -> str | None:
    return (
        clean_optional_timestamptz(raw_record.get("linkedin_enrichment_date"))
        or clean_optional_timestamptz(raw_record.get("legacy_updated_at"))
        or clean_optional_timestamptz(raw_record.get("legacy_created_at"))
    )


def build_fallback_raw_payload(
    raw_record: Mapping[str, Any],
    *,
    source_fields_used: Sequence[str],
) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for field_name in SOURCE_FIELD_ORDER:
        fields[field_name] = preserve_jsonable(raw_record.get(field_name))
    return {
        "source_table": "candidates",
        "legacy_candidate_id": str(raw_record["source_candidate_id"]),
        "source_fields_used": list(source_fields_used),
        "raw_source_fields": fields,
    }


def prepare_source_row(
    raw_record: Mapping[str, Any],
    *,
    candidate_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_candidate_id = str(raw_record["source_candidate_id"])
    candidate_resolution = candidate_map.get(source_candidate_id, {})
    candidate_match_action = blank_to_none(candidate_resolution.get("match_action"))
    candidate_match_basis = blank_to_none(candidate_resolution.get("match_basis"))
    candidate_skip_reason = blank_to_none(candidate_resolution.get("skip_reason"))
    resolved_candidate_id = clean_uuid_text(candidate_resolution.get("resolved_candidate_id"))

    if candidate_match_action not in {"create_new", "match_existing"}:
        resolved_candidate_id = None
        if candidate_skip_reason is None:
            candidate_skip_reason = "missing_candidate_link"

    raw_text, normalized_text, source_fields_used = build_sectioned_text(raw_record)
    external_source_ref = f"legacy:candidates:{source_candidate_id}:linkedin_profile"

    raw_payload = preserve_jsonable(raw_record.get("linkedin_data"))
    if raw_payload is None:
        raw_payload = build_fallback_raw_payload(
            raw_record,
            source_fields_used=source_fields_used,
        )

    metadata_json = {
        "document_identity_key": external_source_ref,
        "source_table": "candidates",
        "legacy_candidate_id": source_candidate_id,
        "source_fields_used": source_fields_used,
        "raw_linkedin_username": blank_to_none(raw_record.get("raw_linkedin_username")),
        "raw_linkedin_url": blank_to_none(raw_record.get("raw_linkedin_url")),
        "legacy_created_at": clean_optional_timestamptz(raw_record.get("legacy_created_at")),
        "legacy_updated_at": clean_optional_timestamptz(raw_record.get("legacy_updated_at")),
        "linkedin_enrichment_date": clean_optional_timestamptz(
            raw_record.get("linkedin_enrichment_date")
        ),
        "linkedin_enrichment_status": blank_to_none(
            raw_record.get("linkedin_enrichment_status")
        ),
        "content_sha256": sha256_text(normalized_text),
    }

    return {
        "source_candidate_id": source_candidate_id,
        "resolved_candidate_id": resolved_candidate_id,
        "candidate_match_action": candidate_match_action,
        "candidate_match_basis": candidate_match_basis,
        "candidate_skip_reason": candidate_skip_reason,
        "source_type": LINKEDIN_SOURCE_TYPE,
        "source_subtype": None,
        "title": LINKEDIN_TITLE,
        "source_url": blank_to_none(raw_record.get("raw_linkedin_url")),
        "external_source_ref": external_source_ref,
        "raw_payload": raw_payload,
        "raw_text": raw_text,
        "normalized_text": normalized_text,
        "metadata_json": metadata_json,
        "trust_level": LINKEDIN_TRUST_LEVEL,
        "effective_at": choose_effective_at(raw_record),
        "raw_linkedin_username": blank_to_none(raw_record.get("raw_linkedin_username")),
        "raw_linkedin_url": blank_to_none(raw_record.get("raw_linkedin_url")),
        "source_fields_used": source_fields_used,
    }


def prepare_source_batch(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_map: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [prepare_source_row(row, candidate_map=candidate_map) for row in raw_rows]


def next_cursor(
    prepared_batch: list[dict[str, Any]],
    current_cursor: str | None,
) -> str | None:
    if not prepared_batch:
        return current_cursor
    return str(prepared_batch[-1]["source_candidate_id"])


def find_candidate_map_path(provided_path: str | None) -> Path:
    if provided_path is None:
        raise FileNotFoundError(
            "Task 9 requires an explicit approved Task 7c candidate-resolution mapping artifact. "
            "Pass --candidate-map-path."
        )

    path = Path(provided_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Candidate map artifact not found: {path}")
    return path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_candidate_map(candidate_map_path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(candidate_map_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Candidate map artifact {candidate_map_path} contains invalid JSON."
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError(
            f"Candidate map artifact {candidate_map_path} must contain a JSON object keyed by legacy candidate id."
        )

    normalized: dict[str, dict[str, Any]] = {}
    for legacy_candidate_id, raw_entry in payload.items():
        if not isinstance(raw_entry, dict):
            continue
        normalized[str(legacy_candidate_id)] = {
            "match_action": raw_entry.get("match_action"),
            "match_basis": raw_entry.get("match_basis"),
            "resolved_candidate_id": clean_uuid_text(raw_entry.get("resolved_candidate_id")),
            "skip_reason": raw_entry.get("skip_reason"),
        }
    return normalized


def validate_checkpoint_resume_metadata(
    checkpoint: JsonCheckpoint,
    *,
    expected_metadata: Mapping[str, Any],
    force_rerun: bool,
) -> None:
    state = checkpoint.load()
    if state.get("status") == "pending" or force_rerun:
        return

    actual_metadata = state.get("metadata", {})
    mismatches: list[str] = []
    for key, expected_value in expected_metadata.items():
        actual_value = actual_metadata.get(key)
        if actual_value is not None and actual_value != expected_value:
            mismatches.append(
                f"{key}: checkpoint={actual_value!r}, expected={expected_value!r}"
            )

    if mismatches:
        raise RuntimeError(
            "Checkpoint metadata is not compatible with the requested run. "
            "Use --force-rerun to reset it. Mismatches: " + "; ".join(mismatches)
        )


def fetch_target_prerequisites(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> dict[str, Any]:
    sql = """
select json_build_object(
  'candidate_source_documents_exists', to_regclass('public.candidate_source_documents') is not null,
  'candidate_profiles_v2_exists', to_regclass('public.candidate_profiles_v2') is not null,
  'canonicalization_ambiguities_exists', to_regclass('public.canonicalization_ambiguities') is not null,
  'build_candidate_source_document_identity_key_exists', to_regprocedure('public.build_candidate_source_document_identity_key(text,text,text,text,jsonb)') is not null,
  'build_candidate_source_document_content_hash_exists', to_regprocedure('public.build_candidate_source_document_content_hash(text,text,text,jsonb)') is not null,
  'decide_candidate_source_document_action_exists', to_regprocedure('public.decide_candidate_source_document_action(uuid,text,text,text,text,text,text,jsonb)') is not null,
  'record_canonicalization_ambiguity_exists', to_regprocedure('public.record_canonicalization_ambiguity(text,text,text,text,jsonb,uuid[],text)') is not null,
  'normalize_candidate_linkedin_url_exists', to_regprocedure('public.normalize_candidate_linkedin_url(text)') is not null,
  'normalize_search_text_exists', to_regprocedure('public.normalize_search_text(text)') is not null
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
        raise RuntimeError(
            "Target database is missing required Task 9 prerequisites: "
            + ", ".join(sorted(missing))
            + "."
        )


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

{prelude_block}create temp table tmp_source_document_input (
    source_candidate_id uuid not null,
    resolved_candidate_id uuid,
    candidate_match_action text,
    candidate_match_basis text,
    candidate_skip_reason text,
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
    effective_at timestamptz,
    raw_linkedin_username text,
    raw_linkedin_url text,
    source_fields_used jsonb
) on commit drop;

insert into tmp_source_document_input (
    source_candidate_id,
    resolved_candidate_id,
    candidate_match_action,
    candidate_match_basis,
    candidate_skip_reason,
    source_type,
    source_subtype,
    title,
    source_url,
    external_source_ref,
    raw_payload,
    raw_text,
    normalized_text,
    metadata_json,
    trust_level,
    effective_at,
    raw_linkedin_username,
    raw_linkedin_url,
    source_fields_used
)
select
    source_candidate_id::uuid,
    nullif(resolved_candidate_id, '')::uuid,
    candidate_match_action,
    candidate_match_basis,
    candidate_skip_reason,
    source_type,
    source_subtype,
    title,
    source_url,
    external_source_ref,
    raw_payload,
    raw_text,
    normalized_text,
    coalesce(metadata_json, '{{}}'::jsonb),
    trust_level,
    nullif(effective_at, '')::timestamptz,
    raw_linkedin_username,
    raw_linkedin_url,
    coalesce(source_fields_used, '[]'::jsonb)
from jsonb_to_recordset({payload_literal}) as x(
    source_candidate_id text,
    resolved_candidate_id text,
    candidate_match_action text,
    candidate_match_basis text,
    candidate_skip_reason text,
    source_type text,
    source_subtype text,
    title text,
    source_url text,
    external_source_ref text,
    raw_payload jsonb,
    raw_text text,
    normalized_text text,
    metadata_json jsonb,
    trust_level text,
    effective_at text,
    raw_linkedin_username text,
    raw_linkedin_url text,
    source_fields_used jsonb
)
order by source_candidate_id asc;

create temp table tmp_source_document_results (
    source_candidate_id uuid not null,
    resolved_candidate_id uuid,
    source_type text not null,
    source_document_id uuid,
    matched_document_id uuid,
    decision text not null,
    effective_action text not null,
    document_identity_key text,
    content_hash text,
    next_document_version integer,
    ambiguity_type text,
    skip_reason text,
    source_url text,
    external_source_ref text,
    normalized_text_sample text,
    ambiguity_logged boolean not null default false
) on commit drop;

do $plpgsql$
declare
    rec tmp_source_document_input%rowtype;
    decision_rec record;
    v_candidate_exists boolean;
    v_source_url text;
    v_document_id uuid;
    v_now timestamptz := now();
    v_normalized_input jsonb;
    v_matched_ids uuid[];
begin
    for rec in
        select *
        from tmp_source_document_input
        order by source_candidate_id asc
    loop
        if rec.resolved_candidate_id is null then
            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                decision,
                effective_action,
                skip_reason,
                source_url,
                external_source_ref,
                normalized_text_sample
            ) values (
                rec.source_candidate_id,
                null,
                rec.source_type,
                'skip',
                'skip',
                coalesce(rec.candidate_skip_reason, 'missing_candidate_link'),
                null,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500)
            );
            continue;
        end if;

        select exists (
            select 1
            from public.candidate_profiles_v2
            where id = rec.resolved_candidate_id
        ) into v_candidate_exists;

        if not v_candidate_exists then
            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                decision,
                effective_action,
                skip_reason,
                source_url,
                external_source_ref,
                normalized_text_sample
            ) values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_type,
                'skip',
                'skip',
                'orphan_missing_candidate_profile',
                null,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500)
            );
            continue;
        end if;

        v_source_url := public.normalize_candidate_linkedin_url(rec.source_url);

        select *
        into decision_rec
        from public.decide_candidate_source_document_action(
            rec.resolved_candidate_id,
            rec.source_type,
            rec.source_subtype,
            rec.title,
            v_source_url,
            rec.external_source_ref,
            rec.normalized_text,
            coalesce(rec.metadata_json, '{{}}'::jsonb)
        );

        if decision_rec.decision = 'ambiguous' then
            select coalesce(array_agg(id order by id), '{{}}'::uuid[])
            into v_matched_ids
            from public.candidate_source_documents
            where candidate_id = rec.resolved_candidate_id
              and source_type = rec.source_type
              and is_active;

            v_normalized_input := jsonb_build_object(
                'candidate_id', rec.resolved_candidate_id,
                'legacy_candidate_id', rec.source_candidate_id,
                'source_type', rec.source_type,
                'document_identity_key', decision_rec.document_identity_key,
                'external_source_ref', rec.external_source_ref,
                'source_url', v_source_url,
                'content_hash', decision_rec.content_hash,
                'ambiguity_family', decision_rec.ambiguity_type
            );

            perform public.record_canonicalization_ambiguity(
                'candidate_source_document',
                decision_rec.ambiguity_type,
                '{AMBIGUITY_SOURCE_SYSTEM}',
                rec.external_source_ref,
                v_normalized_input,
                v_matched_ids,
                'manual_review'
            );

            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                matched_document_id,
                decision,
                effective_action,
                document_identity_key,
                content_hash,
                next_document_version,
                ambiguity_type,
                source_url,
                external_source_ref,
                normalized_text_sample,
                ambiguity_logged
            ) values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_type,
                null,
                decision_rec.decision,
                'ambiguous',
                decision_rec.document_identity_key,
                decision_rec.content_hash,
                decision_rec.next_document_version,
                decision_rec.ambiguity_type,
                v_source_url,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500),
                true
            );
            continue;
        end if;

        if decision_rec.decision = 'no_op' then
            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                source_document_id,
                matched_document_id,
                decision,
                effective_action,
                document_identity_key,
                content_hash,
                next_document_version,
                source_url,
                external_source_ref,
                normalized_text_sample
            ) values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_type,
                decision_rec.matched_document_id,
                decision_rec.matched_document_id,
                decision_rec.decision,
                'no_op',
                decision_rec.document_identity_key,
                decision_rec.content_hash,
                decision_rec.next_document_version,
                v_source_url,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500)
            );
            continue;
        end if;

        if decision_rec.decision = 'supersede' then
            update public.candidate_source_documents
            set
                is_active = false,
                superseded_at = v_now
            where id = decision_rec.matched_document_id
              and is_active;

            insert into public.candidate_source_documents (
                candidate_id,
                source_type,
                source_subtype,
                title,
                source_url,
                external_source_ref,
                raw_payload,
                raw_text,
                normalized_text,
                metadata_json,
                trust_level,
                document_version,
                is_active,
                effective_at,
                superseded_at,
                ingested_at
            ) values (
                rec.resolved_candidate_id,
                rec.source_type,
                rec.source_subtype,
                rec.title,
                v_source_url,
                rec.external_source_ref,
                rec.raw_payload,
                rec.raw_text,
                rec.normalized_text,
                coalesce(rec.metadata_json, '{{}}'::jsonb),
                rec.trust_level,
                decision_rec.next_document_version,
                true,
                rec.effective_at,
                null,
                v_now
            ) returning id into v_document_id;

            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                source_document_id,
                matched_document_id,
                decision,
                effective_action,
                document_identity_key,
                content_hash,
                next_document_version,
                source_url,
                external_source_ref,
                normalized_text_sample
            ) values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_type,
                v_document_id,
                decision_rec.matched_document_id,
                decision_rec.decision,
                'supersede',
                decision_rec.document_identity_key,
                decision_rec.content_hash,
                decision_rec.next_document_version,
                v_source_url,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500)
            );
            continue;
        end if;

        if decision_rec.decision = 'parallel' then
            insert into public.candidate_source_documents (
                candidate_id,
                source_type,
                source_subtype,
                title,
                source_url,
                external_source_ref,
                raw_payload,
                raw_text,
                normalized_text,
                metadata_json,
                trust_level,
                document_version,
                is_active,
                effective_at,
                superseded_at,
                ingested_at
            ) values (
                rec.resolved_candidate_id,
                rec.source_type,
                rec.source_subtype,
                rec.title,
                v_source_url,
                rec.external_source_ref,
                rec.raw_payload,
                rec.raw_text,
                rec.normalized_text,
                coalesce(rec.metadata_json, '{{}}'::jsonb),
                rec.trust_level,
                decision_rec.next_document_version,
                true,
                rec.effective_at,
                null,
                v_now
            ) returning id into v_document_id;

            insert into tmp_source_document_results (
                source_candidate_id,
                resolved_candidate_id,
                source_type,
                source_document_id,
                matched_document_id,
                decision,
                effective_action,
                document_identity_key,
                content_hash,
                next_document_version,
                source_url,
                external_source_ref,
                normalized_text_sample
            ) values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_type,
                v_document_id,
                null,
                decision_rec.decision,
                'insert',
                decision_rec.document_identity_key,
                decision_rec.content_hash,
                decision_rec.next_document_version,
                v_source_url,
                rec.external_source_ref,
                left(coalesce(rec.normalized_text, ''), 500)
            );
            continue;
        end if;

        insert into tmp_source_document_results (
            source_candidate_id,
            resolved_candidate_id,
            source_type,
            matched_document_id,
            decision,
            effective_action,
            document_identity_key,
            content_hash,
            next_document_version,
            ambiguity_type,
            skip_reason,
            source_url,
            external_source_ref,
            normalized_text_sample
        ) values (
            rec.source_candidate_id,
            rec.resolved_candidate_id,
            rec.source_type,
            decision_rec.matched_document_id,
            decision_rec.decision,
            'skip',
            decision_rec.document_identity_key,
            decision_rec.content_hash,
            decision_rec.next_document_version,
            decision_rec.ambiguity_type,
            'unexpected_document_decision',
            v_source_url,
            rec.external_source_ref,
            left(coalesce(rec.normalized_text, ''), 500)
        );
    end loop;
end
$plpgsql$;

select row_to_json(t)::text
from (
    select
        source_candidate_id::text as source_candidate_id,
        resolved_candidate_id::text as resolved_candidate_id,
        source_type,
        source_document_id::text as source_document_id,
        matched_document_id::text as matched_document_id,
        decision,
        effective_action,
        document_identity_key,
        content_hash,
        next_document_version,
        ambiguity_type,
        skip_reason,
        source_url,
        external_source_ref,
        normalized_text_sample,
        ambiguity_logged
    from tmp_source_document_results
    order by source_candidate_id asc
) as t;

{final_statement}
"""


def run_batch(
    target_db: PsqlClient | SupabaseLinkedClient,
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
    prepared_batch: Sequence[Mapping[str, Any]],
    rows: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    rows_list = list(rows)
    decision_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    skip_reason_counts: Counter[str] = Counter()
    ambiguity_type_counts: Counter[str] = Counter()
    source_type_counts: Counter[str] = Counter()
    samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    normalized_text_samples: list[dict[str, Any]] = []

    for row in rows_list:
        decision = str(row.get("decision"))
        action = str(row.get("effective_action"))
        decision_counts[decision] += 1
        action_counts[action] += 1

        source_type = row.get("source_type")
        if source_type is not None:
            source_type_counts[str(source_type)] += 1

        skip_reason = row.get("skip_reason")
        if skip_reason:
            skip_reason_counts[str(skip_reason)] += 1

        ambiguity_type = row.get("ambiguity_type")
        if ambiguity_type:
            ambiguity_type_counts[str(ambiguity_type)] += 1

        if len(samples[action]) < sample_limit:
            samples[action].append(
                {
                    "source_candidate_id": row.get("source_candidate_id"),
                    "resolved_candidate_id": row.get("resolved_candidate_id"),
                    "source_document_id": row.get("source_document_id"),
                    "matched_document_id": row.get("matched_document_id"),
                    "decision": row.get("decision"),
                    "effective_action": row.get("effective_action"),
                    "document_identity_key": row.get("document_identity_key"),
                    "content_hash": row.get("content_hash"),
                    "next_document_version": row.get("next_document_version"),
                    "ambiguity_type": row.get("ambiguity_type"),
                    "skip_reason": row.get("skip_reason"),
                    "source_url": row.get("source_url"),
                    "external_source_ref": row.get("external_source_ref"),
                }
            )

        if (
            row.get("normalized_text_sample")
            and len(normalized_text_samples) < sample_limit
        ):
            normalized_text_samples.append(
                {
                    "source_candidate_id": row.get("source_candidate_id"),
                    "resolved_candidate_id": row.get("resolved_candidate_id"),
                    "normalized_text_sample": row.get("normalized_text_sample"),
                }
            )

    unresolved_actions = action_counts.get("skip", 0) + action_counts.get("ambiguous", 0)
    docs_inserted = action_counts.get("insert", 0) + action_counts.get("supersede", 0)

    return {
        "legacy_candidate_rows_read": len(prepared_batch),
        "linkedin_profile_docs_normalized": len(prepared_batch),
        "linkedin_profile_docs_inserted_or_would_insert": docs_inserted,
        "linkedin_profile_docs_new_insert_or_would_insert": action_counts.get("insert", 0),
        "linkedin_profile_docs_superseded_or_would_supersede": action_counts.get("supersede", 0),
        "no_op_outcomes": action_counts.get("no_op", 0),
        "ambiguous_outcomes": action_counts.get("ambiguous", 0),
        "orphan_source_skips": skip_reason_counts.get("orphan_missing_candidate_profile", 0),
        "missing_candidate_map_skips": skip_reason_counts.get("missing_candidate_link", 0),
        "candidates_that_would_end_without_required_active_linkedin_profile": unresolved_actions,
        "decision_counts": dict(decision_counts),
        "action_counts": dict(action_counts),
        "skip_reason_counts": dict(skip_reason_counts),
        "ambiguity_type_counts": dict(ambiguity_type_counts),
        "source_type_counts": dict(source_type_counts),
        "samples": dict(samples),
        "normalized_text_samples": normalized_text_samples,
    }


def base_summary(*, sample_limit: int) -> dict[str, Any]:
    return {
        "legacy_candidate_rows_read": 0,
        "linkedin_profile_docs_normalized": 0,
        "linkedin_profile_docs_inserted_or_would_insert": 0,
        "linkedin_profile_docs_new_insert_or_would_insert": 0,
        "linkedin_profile_docs_superseded_or_would_supersede": 0,
        "no_op_outcomes": 0,
        "ambiguous_outcomes": 0,
        "orphan_source_skips": 0,
        "missing_candidate_map_skips": 0,
        "candidates_that_would_end_without_required_active_linkedin_profile": 0,
        "decision_counts": {},
        "action_counts": {},
        "skip_reason_counts": {},
        "ambiguity_type_counts": {},
        "source_type_counts": {},
        "samples": {},
        "normalized_text_samples": [],
        "sample_limit": sample_limit,
    }


def merge_summary(base: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    for key in (
        "legacy_candidate_rows_read",
        "linkedin_profile_docs_normalized",
        "linkedin_profile_docs_inserted_or_would_insert",
        "linkedin_profile_docs_new_insert_or_would_insert",
        "linkedin_profile_docs_superseded_or_would_supersede",
        "no_op_outcomes",
        "ambiguous_outcomes",
        "orphan_source_skips",
        "missing_candidate_map_skips",
        "candidates_that_would_end_without_required_active_linkedin_profile",
    ):
        base[key] += int(incoming[key])

    for key in (
        "decision_counts",
        "action_counts",
        "skip_reason_counts",
        "ambiguity_type_counts",
        "source_type_counts",
    ):
        counter = Counter(base[key])
        counter.update(incoming[key])
        base[key] = dict(counter)

    for action, action_samples in incoming["samples"].items():
        existing = base["samples"].setdefault(action, [])
        remaining = max(0, base["sample_limit"] - len(existing))
        if remaining:
            existing.extend(action_samples[:remaining])

    remaining_text_samples = max(
        0,
        base["sample_limit"] - len(base["normalized_text_samples"]),
    )
    if remaining_text_samples:
        base["normalized_text_samples"].extend(
            incoming["normalized_text_samples"][:remaining_text_samples]
        )

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
        "# Candidate Source Documents Backfill QA Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Script: `{report['script_name']}`",
        f"- Mode: `{report['mode']}`",
        f"- Limit: `{report['limit']}`",
        f"- Batch size: `{report['batch_size']}`",
        f"- Checkpoint: `{report['checkpoint_name']}`",
        f"- Candidate map path: `{report['candidate_map_path']}`",
        f"- Candidate map SHA256: `{report['candidate_map_sha256']}`",
        f"- Legacy source table: `{report['legacy_source_table']}`",
        f"- Destination table: `{report['destination_table']}`",
        f"- Included source family: `{LINKEDIN_SOURCE_TYPE}`",
        f"- Excluded source families: `{json.dumps(report['excluded_source_families'])}`",
        "",
        "## Overall Counts",
        "",
        f"- Legacy candidate rows read: `{overall['legacy_candidate_rows_read']}`",
        f"- LinkedIn profile docs normalized: `{overall['linkedin_profile_docs_normalized']}`",
        f"- LinkedIn docs inserted / would insert, including supersede versions: `{overall['linkedin_profile_docs_inserted_or_would_insert']}`",
        f"- New LinkedIn docs inserted / would insert: `{overall['linkedin_profile_docs_new_insert_or_would_insert']}`",
        f"- LinkedIn docs superseded / would supersede: `{overall['linkedin_profile_docs_superseded_or_would_supersede']}`",
        f"- No-op outcomes: `{overall['no_op_outcomes']}`",
        f"- Ambiguous outcomes: `{overall['ambiguous_outcomes']}`",
        f"- Orphan source skips: `{overall['orphan_source_skips']}`",
        f"- Missing candidate-map skips: `{overall['missing_candidate_map_skips']}`",
        f"- Candidates that would end without required active LinkedIn doc: `{overall['candidates_that_would_end_without_required_active_linkedin_profile']}`",
        f"- Decision counts: `{json.dumps(overall['decision_counts'], sort_keys=True)}`",
        f"- Action counts: `{json.dumps(overall['action_counts'], sort_keys=True)}`",
        f"- Skip reason counts: `{json.dumps(overall['skip_reason_counts'], sort_keys=True)}`",
        f"- Ambiguity type counts: `{json.dumps(overall['ambiguity_type_counts'], sort_keys=True)}`",
        f"- Source type counts: `{json.dumps(overall['source_type_counts'], sort_keys=True)}`",
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


def build_duplicate_fixture_prelude(fixture_ids: Mapping[str, str]) -> str:
    candidate_values = ",\n".join(
        "("
        + ", ".join(
            [
                f"{sql_text_literal(candidate_id)}::uuid",
                sql_text_literal(f"Fixture Candidate {index}"),
                sql_text_literal("fixture"),
            ]
        )
        + ")"
        for index, candidate_id in enumerate(
            [
                fixture_ids["candidate_first_insert"],
                fixture_ids["candidate_no_op"],
                fixture_ids["candidate_supersede"],
                fixture_ids["candidate_ambiguous"],
            ],
            start=1,
        )
    )

    existing_rows = [
        {
            "id": fixture_ids["doc_no_op"],
            "candidate_id": fixture_ids["candidate_no_op"],
            "title": "Existing LinkedIn profile",
            "source_url": "https://www.linkedin.com/in/fixture-no-op",
            "external_source_ref": "legacy:candidates:" + fixture_ids["candidate_no_op"] + ":linkedin_profile",
            "normalized_text": "Same LinkedIn Content",
            "document_version": 1,
        },
        {
            "id": fixture_ids["doc_supersede"],
            "candidate_id": fixture_ids["candidate_supersede"],
            "title": "Existing LinkedIn profile",
            "source_url": "https://www.linkedin.com/in/fixture-supersede",
            "external_source_ref": "legacy:candidates:" + fixture_ids["candidate_supersede"] + ":linkedin_profile",
            "normalized_text": "Old LinkedIn Content",
            "document_version": 2,
        },
        {
            "id": fixture_ids["doc_ambiguous_a"],
            "candidate_id": fixture_ids["candidate_ambiguous"],
            "title": "Existing LinkedIn profile A",
            "source_url": "https://www.linkedin.com/in/fixture-ambiguous",
            "external_source_ref": "legacy:candidates:" + fixture_ids["candidate_ambiguous"] + ":linkedin_profile:a",
            "normalized_text": "Ambiguous LinkedIn Content A",
            "document_version": 1,
        },
        {
            "id": fixture_ids["doc_ambiguous_b"],
            "candidate_id": fixture_ids["candidate_ambiguous"],
            "title": "Existing LinkedIn profile B",
            "source_url": "https://www.linkedin.com/in/fixture-ambiguous-alt",
            "external_source_ref": "legacy:candidates:" + fixture_ids["candidate_ambiguous"] + ":linkedin_profile:b",
            "normalized_text": "Ambiguous LinkedIn Content B",
            "document_version": 1,
        },
    ]

    doc_values: list[str] = []
    for row in existing_rows:
        metadata = {
            "document_identity_key": row["external_source_ref"],
            "source_table": "fixture",
            "legacy_candidate_id": row["candidate_id"],
        }
        doc_values.append(
            "("
            + ", ".join(
                [
                    f"{sql_text_literal(row['id'])}::uuid",
                    f"{sql_text_literal(row['candidate_id'])}::uuid",
                    sql_text_literal(LINKEDIN_SOURCE_TYPE),
                    sql_text_literal(row["title"]),
                    sql_text_literal(row["source_url"]),
                    sql_text_literal(row["external_source_ref"]),
                    sql_text_literal("Fixture raw text"),
                    sql_text_literal(row["normalized_text"]),
                    sql_jsonb_literal(metadata),
                    sql_text_literal(LINKEDIN_TRUST_LEVEL),
                    str(row["document_version"]),
                    "true",
                ]
            )
            + ")"
        )

    return f"""
insert into public.candidate_profiles_v2 (
    id,
    full_name,
    source
)
values
{candidate_values};

insert into public.candidate_source_documents (
    id,
    candidate_id,
    source_type,
    title,
    source_url,
    external_source_ref,
    raw_text,
    normalized_text,
    metadata_json,
    trust_level,
    document_version,
    is_active
)
values
{",\n".join(doc_values)};
"""


def build_fixture_record(
    *,
    source_candidate_id: str,
    resolved_candidate_id: str | None,
    normalized_text: str,
    raw_url: str,
    match_action: str = "match_existing",
    skip_reason: str | None = None,
) -> dict[str, Any]:
    external_source_ref = f"legacy:candidates:{source_candidate_id}:linkedin_profile"
    metadata_json = {
        "document_identity_key": external_source_ref,
        "source_table": "candidates",
        "legacy_candidate_id": source_candidate_id,
        "source_fields_used": ["headline"],
        "raw_linkedin_username": None,
        "raw_linkedin_url": raw_url,
        "legacy_created_at": None,
        "legacy_updated_at": None,
        "linkedin_enrichment_date": None,
        "linkedin_enrichment_status": None,
        "content_sha256": sha256_text(normalized_text),
    }
    return {
        "source_candidate_id": source_candidate_id,
        "resolved_candidate_id": resolved_candidate_id,
        "candidate_match_action": match_action,
        "candidate_match_basis": "fixture",
        "candidate_skip_reason": skip_reason,
        "source_type": LINKEDIN_SOURCE_TYPE,
        "source_subtype": None,
        "title": LINKEDIN_TITLE,
        "source_url": raw_url,
        "external_source_ref": external_source_ref,
        "raw_payload": {"fixture": True},
        "raw_text": normalized_text,
        "normalized_text": normalized_text,
        "metadata_json": metadata_json,
        "trust_level": LINKEDIN_TRUST_LEVEL,
        "effective_at": None,
        "raw_linkedin_username": None,
        "raw_linkedin_url": raw_url,
        "source_fields_used": ["headline"],
    }


def build_duplicate_fixture_records() -> tuple[
    dict[str, str],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    fixture_ids = {
        name: str(uuid4())
        for name in (
            "candidate_first_insert",
            "candidate_no_op",
            "candidate_supersede",
            "candidate_ambiguous",
            "candidate_orphan",
            "doc_no_op",
            "doc_supersede",
            "doc_ambiguous_a",
            "doc_ambiguous_b",
        )
    }

    incoming_records = [
        build_fixture_record(
            source_candidate_id=fixture_ids["candidate_first_insert"],
            resolved_candidate_id=fixture_ids["candidate_first_insert"],
            normalized_text="Brand New LinkedIn Content",
            raw_url="https://www.linkedin.com/in/fixture-first-insert",
        ),
        build_fixture_record(
            source_candidate_id=fixture_ids["candidate_no_op"],
            resolved_candidate_id=fixture_ids["candidate_no_op"],
            normalized_text="same linkedin content",
            raw_url="https://linkedin.com/in/fixture-no-op?trk=public-profile",
        ),
        build_fixture_record(
            source_candidate_id=fixture_ids["candidate_supersede"],
            resolved_candidate_id=fixture_ids["candidate_supersede"],
            normalized_text="New LinkedIn Content",
            raw_url="https://www.linkedin.com/in/fixture-supersede",
        ),
        build_fixture_record(
            source_candidate_id=fixture_ids["candidate_ambiguous"],
            resolved_candidate_id=fixture_ids["candidate_ambiguous"],
            normalized_text="Ambiguous Incoming LinkedIn Content",
            raw_url="https://www.linkedin.com/in/fixture-ambiguous",
        ),
        build_fixture_record(
            source_candidate_id=fixture_ids["candidate_orphan"],
            resolved_candidate_id=fixture_ids["candidate_orphan"],
            normalized_text="Orphan LinkedIn Content",
            raw_url="https://www.linkedin.com/in/fixture-orphan",
        ),
    ]

    expectations = {
        fixture_ids["candidate_first_insert"]: {
            "decision": "parallel",
            "effective_action": "insert",
            "next_document_version": 1,
        },
        fixture_ids["candidate_no_op"]: {
            "decision": "no_op",
            "effective_action": "no_op",
            "matched_document_id": fixture_ids["doc_no_op"],
            "next_document_version": 1,
        },
        fixture_ids["candidate_supersede"]: {
            "decision": "supersede",
            "effective_action": "supersede",
            "matched_document_id": fixture_ids["doc_supersede"],
            "next_document_version": 3,
        },
        fixture_ids["candidate_ambiguous"]: {
            "decision": "ambiguous",
            "effective_action": "ambiguous",
            "ambiguity_type": "multiple_active_linkedin_profiles",
            "ambiguity_logged": True,
        },
        fixture_ids["candidate_orphan"]: {
            "decision": "skip",
            "effective_action": "skip",
            "skip_reason": "orphan_missing_candidate_profile",
        },
    }

    return fixture_ids, incoming_records, expectations


def run_duplicate_validation(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> dict[str, Any]:
    fixture_ids, prepared_batch, expectations = build_duplicate_fixture_records()
    prelude_sql = build_duplicate_fixture_prelude(fixture_ids)
    rows = run_batch(
        target_db,
        prepared_batch,
        commit_writes=False,
        prelude_sql=prelude_sql,
    )

    by_candidate_id = {str(row["source_candidate_id"]): row for row in rows}
    case_results: list[dict[str, Any]] = []
    passed_count = 0

    for source_candidate_id, expectation in expectations.items():
        actual = by_candidate_id.get(source_candidate_id, {})
        mismatches: list[str] = []
        passed = True
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
                "source_candidate_id": source_candidate_id,
                "passed": passed,
                "expected": expectation,
                "actual": actual,
                "mismatches": mismatches,
            }
        )

    return {
        "cases_run": len(expectations),
        "cases_passed": passed_count,
        "cases_failed": len(expectations) - passed_count,
        "all_passed": passed_count == len(expectations),
        "results": case_results,
    }


def run_main_pass(
    args: argparse.Namespace,
    *,
    source_db: PsqlClient | SupabaseLinkedClient,
    target_db: PsqlClient | SupabaseLinkedClient,
    checkpoint: JsonCheckpoint,
    candidate_map: Mapping[str, Mapping[str, Any]],
    candidate_map_sha256: str,
) -> dict[str, Any]:
    dry_run = args.mode == "dry-run"
    limit = effective_limit(args)
    checkpoint_metadata = {
        "script_name": Path(__file__).name,
        "mode": args.mode,
        "candidate_map_sha256": candidate_map_sha256,
        "source_family": LINKEDIN_SOURCE_TYPE,
    }
    validate_checkpoint_resume_metadata(
        checkpoint,
        expected_metadata=checkpoint_metadata,
        force_rerun=args.force_rerun,
    )

    available_candidate_columns = fetch_source_table_columns(
        source_db,
        schema_name="public",
        table_name="candidates",
    )
    validate_target_prerequisites(target_db)

    if dry_run:
        state, should_run = checkpoint.begin(
            batch_size=min(args.batch_size, limit or args.batch_size),
            dry_run=True,
            force_rerun=args.force_rerun,
            metadata=checkpoint_metadata,
        )
    else:
        state, should_run = checkpoint.begin(
            batch_size=args.batch_size,
            dry_run=False,
            force_rerun=args.force_rerun,
            metadata=checkpoint_metadata,
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
            current_batch_size = min(current_batch_size, remaining)
        elif remaining is not None:
            current_batch_size = min(current_batch_size, remaining)

        raw_batch = fetch_source_batch(
            source_db,
            cursor=cursor,
            batch_size=current_batch_size,
            available_columns=available_candidate_columns,
        )
        if not raw_batch:
            source_exhausted = True
            break

        prepared_batch = prepare_source_batch(raw_batch, candidate_map=candidate_map)
        batch_number += 1
        batch_results = run_batch(
            target_db,
            prepared_batch,
            commit_writes=not dry_run,
        )
        batch_summary = summarize_results(
            prepared_batch,
            batch_results,
            sample_limit=args.sample_limit,
        )
        batch_summary["batch_number"] = batch_number
        batch_summary["rows_seen"] = len(prepared_batch)
        batch_summaries.append(batch_summary)
        merge_summary(summary, batch_summary)

        total_rows_seen += len(prepared_batch)
        rows_written = (
            batch_summary["action_counts"].get("insert", 0)
            + batch_summary["action_counts"].get("supersede", 0)
        )
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
                last_seen_key=prepared_batch[-1]["source_candidate_id"],
                rows_seen=len(prepared_batch),
                rows_processed=len(prepared_batch),
                rows_written=rows_written,
                rows_skipped=batch_summary["action_counts"].get("skip", 0)
                + batch_summary["action_counts"].get("no_op", 0)
                + batch_summary["action_counts"].get("ambiguous", 0),
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
                    "progress": {"has_more": True},
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

    candidate_map_path = find_candidate_map_path(args.candidate_map_path)
    candidate_map = load_candidate_map(candidate_map_path)
    candidate_map_sha256 = file_sha256(candidate_map_path)

    if args.linked_workdir:
        linked_workdir = Path(args.linked_workdir).resolve()
        source_env_used = "SUPABASE_LINKED_WORKDIR"
        target_env_used = "SUPABASE_LINKED_WORKDIR"
        source_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="candidate-source-documents-backfill-source",
        )
        target_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="candidate-source-documents-backfill-target",
        )
    else:
        source_dsn, source_env_used = load_dsn(
            args.source_dsn_env,
            fallback_env=args.target_dsn_env,
        )
        target_dsn, target_env_used = load_dsn(args.target_dsn_env)
        source_db = PsqlClient(
            source_dsn,
            app_name="candidate-source-documents-backfill-source",
        )
        target_db = PsqlClient(
            target_dsn,
            app_name="candidate-source-documents-backfill-target",
        )

    checkpoint = JsonCheckpoint(args.checkpoint_name)
    report_dir = Path(args.report_dir).resolve()

    try:
        main_pass = run_main_pass(
            args,
            source_db=source_db,
            target_db=target_db,
            checkpoint=checkpoint,
            candidate_map=candidate_map,
            candidate_map_sha256=candidate_map_sha256,
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
        LOGGER.exception("candidate source-documents backfill failed: %s", exc)
        return 1

    report = {
        "generated_at": utc_now(),
        "script_name": Path(__file__).name,
        "task_scope": "9a" if args.mode == "dry-run" else "9b_or_9c_apply",
        "mode": args.mode,
        "limit": main_pass["limit"],
        "batch_size": args.batch_size,
        "checkpoint_name": args.checkpoint_name,
        "candidate_map_path": str(candidate_map_path),
        "candidate_map_sha256": candidate_map_sha256,
        "candidate_map_entries": len(candidate_map),
        "source_dsn_env_used": source_env_used,
        "target_dsn_env_used": target_env_used,
        "legacy_source_table": LEGACY_CANDIDATES_TABLE,
        "destination_table": "public.candidate_source_documents",
        "ambiguity_table": "public.canonicalization_ambiguities",
        "included_source_families": [LINKEDIN_SOURCE_TYPE],
        "excluded_source_families": [
            "resume",
            "manual_profile_note",
            "recruiter_note_raw",
            "recruiter_note_summary",
            "transcript_summary",
            "candidate_communications",
            "future_artifacts",
        ],
        "explicit_source_field_exclusions": [
            "candidates.resume_text",
            "candidates.notes",
            "recruiter_candidates.notes",
            "candidates.ai_summary",
            "candidate_communications.*",
        ],
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
        "candidate_source_documents_preflight"
        if args.mode == "dry-run"
        else "candidate_source_documents_backfill_apply"
    )
    report_path = write_json_report(report_dir, report, scope=report_scope)
    markdown_path = write_text_report(
        report_dir,
        render_markdown_report(report),
        scope=report_scope,
        extension="md",
    )
    LOGGER.info("wrote QA report to %s", report_path)
    LOGGER.info("wrote QA markdown report to %s", markdown_path)

    if duplicate_validation is not None and not duplicate_validation["all_passed"]:
        LOGGER.error("duplicate-validation fixtures failed; inspect %s", report_path)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
