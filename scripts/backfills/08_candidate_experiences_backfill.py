#!/usr/bin/env python3
"""Checkpoint-aware candidate experience backfill script.

Task coverage:
- Task 8a: implement the script, run a preflight dry-run on the first 100
  flattened experience rows, and run duplicate-validation fixtures in a rolled-
  back transaction
- Task 8b: later reuse this script in apply mode for a 100-row pilot write
- Task 8c: later reuse this script in apply mode for the full migration

Design notes:
- source reads are deterministic and ordered by legacy `candidates.id` asc, then
  chosen source array index asc
- `work_experience` is the primary source array; `linkedin_data.data.experience`
  is the fallback only when `work_experience` is null or empty
- dry-run uses the real insert/update logic inside a transaction that is rolled
  back, so duplicate handling, ambiguity logging, and reorder updates behave like
  a real run for the inspected batch
- duplicate validation uses temporary fixture writes inside a rolled-back
  transaction and never mutates the real legacy source table
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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from uuid import UUID, uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.checkpoint import JsonCheckpoint
from scripts.lib.psql import PsqlClient, PsqlError, SupabaseLinkedClient, load_dsn

LOGGER = logging.getLogger(__name__)

DEFAULT_CHECKPOINT_NAME = "08_candidate_experiences_backfill"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_SOURCE_CANDIDATE_BATCH_SIZE = 100
DEFAULT_PREFLIGHT_LIMIT = 100
DEFAULT_SOURCE_ENV = "LEGACY_DATABASE_URL"
DEFAULT_TARGET_ENV = "DATABASE_URL"
LEGACY_CANDIDATES_TABLE = "public.candidates"
REPORT_DIR = REPO_ROOT / "reports" / "qa"
DEFAULT_CANDIDATE_MAP_GLOB = "*candidate_profiles_emails_candidate_map*.json"
AMBIGUITY_SOURCE_SYSTEM = "legacy_candidate_experiences"
LINKED_SOURCE_CANDIDATE_BATCH_SIZE = DEFAULT_SOURCE_CANDIDATE_BATCH_SIZE

MONTH_LOOKUP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

SOURCE_PATH_WORK_EXPERIENCE = "work_experience"
SOURCE_PATH_LINKEDIN_EXPERIENCE = "linkedin_data.data.experience"

TITLE_KEYS = ("title", "position_title", "position", "role")
DESCRIPTION_KEYS = ("description", "summary", "details")
LOCATION_KEYS = ("location", "geo_location", "geoLocation")
COMPANY_NAME_KEYS = ("company", "companyName", "company_name", "organization")
COMPANY_ID_KEYS = ("company_id", "companyId", "linkedin_company_id", "linkedinCompanyId")
COMPANY_USERNAME_KEYS = (
    "company_linkedin_username",
    "companyLinkedinUsername",
    "company_username",
    "companyUsername",
    "linkedin_username",
    "linkedinUsername",
)
COMPANY_URL_KEYS = (
    "company_linkedin_url",
    "companyLinkedinUrl",
    "company_url",
    "companyUrl",
    "linkedin_company_url",
    "linkedinCompanyUrl",
    "linkedin_url",
    "linkedinUrl",
    "url",
)
START_DATE_KEYS = ("start_date", "startDate", "start")
END_DATE_KEYS = ("end_date", "endDate", "end")
CURRENT_KEYS = ("is_current", "isCurrent", "current")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill canonical candidate experiences from legacy candidate payloads."
    )
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help=(
            "Execution mode. `dry-run` performs the real per-batch logic inside a "
            "rolled-back transaction. `apply` commits writes to candidate_experiences_v2."
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
        help="Deterministic flattened-source batch size.",
    )
    parser.add_argument(
        "--source-candidate-batch-size",
        type=int,
        default=DEFAULT_SOURCE_CANDIDATE_BATCH_SIZE,
        help=(
            "How many source candidates to fetch per DB round-trip before Python "
            "flattens them into experience rows."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional max flattened source-row count for this run. Defaults to 100 "
            "for dry-run mode and unlimited for apply mode."
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
            "Required for this script so experience linkage always uses the explicitly approved map."
        ),
    )
    parser.add_argument(
        "--run-duplicate-validation",
        action="store_true",
        help=(
            "Run rolled-back duplicate-validation fixtures against candidate_experiences_v2 "
            "after the main pass."
        ),
    )
    parser.add_argument(
        "--skip-duplicate-validation",
        action="store_true",
        help=(
            "Skip duplicate-validation fixtures. Dry-run mode runs them by default "
            "because Task 8a requires them."
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
        "--shard-count",
        type=int,
        default=1,
        help=(
            "Optional worker shard count for apply mode. Sharding partitions work by "
            "resolved canonical candidate id from the approved Task 7 map."
        ),
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Zero-based shard index when --shard-count > 1.",
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

    if args.source_candidate_batch_size <= 0:
        parser.error("--source-candidate-batch-size must be > 0.")

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be > 0 when provided.")

    if args.shard_count <= 0:
        parser.error("--shard-count must be > 0.")

    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        parser.error("--shard-index must satisfy 0 <= shard-index < shard-count.")

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

    if args.shard_count > 1:
        if args.mode != "apply":
            parser.error("Sharded runs are supported only with --mode=apply.")
        if args.run_duplicate_validation:
            parser.error(
                "--run-duplicate-validation is not supported with sharded apply runs."
            )
        if args.limit is not None:
            parser.error("--limit is not supported with sharded apply runs.")

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


def preserve_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def preserve_raw_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return None


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


def clean_uuid_text(value: Any) -> str | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    return str(cleaned)


def uuid_int(value: str) -> int:
    return UUID(value).int


def shard_suffix(shard_count: int, shard_index: int) -> str:
    return f"__shard_{shard_index}_of_{shard_count}"


def effective_checkpoint_name(base_name: str, *, shard_count: int, shard_index: int) -> str:
    if shard_count <= 1:
        return base_name
    return f"{base_name}{shard_suffix(shard_count, shard_index)}"


def stable_shard_index(candidate_id: str, shard_count: int) -> int:
    return uuid_int(candidate_id) % shard_count


def validate_candidate_map_for_sharding(
    candidate_map: Mapping[str, Mapping[str, Any]],
) -> None:
    for source_candidate_id, entry in candidate_map.items():
        action = normalized_string(entry.get("match_action"))
        resolved_candidate_id = clean_uuid_text(entry.get("resolved_candidate_id"))
        if action in {"create_new", "match_existing"} and resolved_candidate_id is None:
            raise ValueError(
                "Task 8 sharding requires every mapped Task 7 candidate-map entry to "
                f"include resolved_candidate_id. Offending source candidate: {source_candidate_id}"
            )


def build_shard_manifest(
    candidate_map: Mapping[str, Mapping[str, Any]],
    *,
    shard_count: int,
    shard_index: int,
) -> dict[str, Any] | None:
    if shard_count <= 1:
        return None

    validate_candidate_map_for_sharding(candidate_map)

    ordered_source_candidate_ids: list[str] = []
    resolved_candidate_ids_in_shard: set[str] = set()
    mapped_source_candidate_total = 0
    mapped_source_candidate_count = 0
    unmapped_source_candidate_count = 0

    for source_candidate_id, entry in candidate_map.items():
        action = normalized_string(entry.get("match_action"))
        resolved_candidate_id = clean_uuid_text(entry.get("resolved_candidate_id"))

        if action in {"create_new", "match_existing"} and resolved_candidate_id is not None:
            candidate_shard_index = stable_shard_index(resolved_candidate_id, shard_count)
            mapped_source_candidate_total += 1
            if candidate_shard_index != shard_index:
                continue
            mapped_source_candidate_count += 1
            resolved_candidate_ids_in_shard.add(resolved_candidate_id)
            ordered_source_candidate_ids.append(source_candidate_id)
            continue

        if shard_index != 0:
            continue
        unmapped_source_candidate_count += 1
        ordered_source_candidate_ids.append(source_candidate_id)

    ordered_source_candidate_ids.sort(key=uuid_int)

    return {
        "shard_count": shard_count,
        "shard_index": shard_index,
        "ordered_source_candidate_ids": ordered_source_candidate_ids,
        "candidate_index_by_id": {
            candidate_id: index
            for index, candidate_id in enumerate(ordered_source_candidate_ids)
        },
        "resolved_candidate_count": len(resolved_candidate_ids_in_shard),
        "mapped_source_candidate_total": mapped_source_candidate_total,
        "mapped_source_candidate_count": mapped_source_candidate_count,
        "unmapped_source_candidate_count": unmapped_source_candidate_count,
        "includes_unmapped": shard_index == 0 and unmapped_source_candidate_count > 0,
    }


def validate_checkpoint_resume_metadata(
    checkpoint: JsonCheckpoint,
    *,
    expected_metadata: Mapping[str, Any],
    force_rerun: bool,
    strict_keys: set[str] | None = None,
) -> None:
    state = checkpoint.load()
    if state.get("status") == "pending" or force_rerun:
        return

    actual_metadata = state.get("metadata", {})
    mismatches: list[str] = []
    for key, expected_value in expected_metadata.items():
        actual_value = actual_metadata.get(key)
        if strict_keys and key in strict_keys and actual_value != expected_value:
            mismatches.append(
                f"{key}: checkpoint={actual_value!r}, expected={expected_value!r}"
            )
            continue
        if actual_value is not None and actual_value != expected_value:
            mismatches.append(
                f"{key}: checkpoint={actual_value!r}, expected={expected_value!r}"
            )

    if mismatches:
        raise RuntimeError(
            "Checkpoint metadata is not compatible with the requested run. "
            "Use --force-rerun to reset it. Mismatches: " + "; ".join(mismatches)
        )


def sql_text_literal(value: str | None) -> str:
    if value is None:
        return "null"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_jsonb_literal(value: Any) -> str:
    return f"{sql_text_literal(json.dumps(value, sort_keys=True))}::jsonb"


def coerce_int(value: Any) -> int | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    try:
        return int(str(cleaned))
    except (TypeError, ValueError):
        return None


def parse_boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in {"true", "t", "1", "yes", "y"}:
            return True
        if cleaned in {"false", "f", "0", "no", "n"}:
            return False
    return None


def normalized_string(value: Any) -> str | None:
    cleaned = blank_to_none(value)
    if cleaned is None:
        return None
    return str(cleaned)


def nested_get(mapping: Mapping[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def first_present(mapping: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        if key in mapping:
            value = mapping.get(key)
            if value is not None:
                return value
    return None


def extract_text_field(source: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    raw_value = first_present(source, keys)
    if isinstance(raw_value, Mapping):
        for nested_key in ("text", "value", "name", "title", "label", "display"):
            nested_value = raw_value.get(nested_key)
            cleaned = normalized_string(nested_value)
            if cleaned is not None:
                return cleaned
        return None
    return normalized_string(raw_value)


def parse_month_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        if 1 <= value <= 12:
            return value
        return None
    if isinstance(value, float):
        numeric = int(value)
        if 1 <= numeric <= 12:
            return numeric
        return None
    if isinstance(value, str):
        cleaned = value.strip().lower().replace(".", "")
        if not cleaned:
            return None
        if cleaned.isdigit():
            numeric = int(cleaned)
            if 1 <= numeric <= 12:
                return numeric
            return None
        return MONTH_LOOKUP.get(cleaned)
    return None


_DATE_PATTERNS_DAY = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%b %d %Y",
    "%B %d %Y",
    "%d %b %Y",
    "%d %B %Y",
)


def parse_date_string(raw_value: str, *, is_end_date: bool) -> tuple[str | None, str]:
    text = raw_value.strip()
    if not text:
        return None, "unknown"

    lowered = re.sub(r"\s+", " ", text.lower()).strip()
    lowered = lowered.replace(".", "")

    if is_end_date and lowered in {"present", "current", "ongoing", "now", "today"}:
        return None, "present"

    try:
        parsed_day = datetime.fromisoformat(text)
        return parsed_day.date().isoformat(), "day"
    except ValueError:
        pass

    if re.fullmatch(r"\d{4}-\d{2}", text):
        year, month = text.split("-")
        try:
            parsed = date(int(year), int(month), 1)
        except ValueError:
            return None, "unknown"
        return parsed.isoformat(), "month"

    if re.fullmatch(r"\d{4}/\d{2}", text):
        year, month = text.split("/")
        try:
            parsed = date(int(year), int(month), 1)
        except ValueError:
            return None, "unknown"
        return parsed.isoformat(), "month"

    if re.fullmatch(r"\d{4}", text):
        return date(int(text), 1, 1).isoformat(), "year"

    for pattern in _DATE_PATTERNS_DAY:
        try:
            parsed = datetime.strptime(text, pattern).date()
            return parsed.isoformat(), "day"
        except ValueError:
            continue

    month_year_match = re.fullmatch(r"([A-Za-z]+)\s+(\d{4})", text)
    if month_year_match:
        month = parse_month_value(month_year_match.group(1))
        year = int(month_year_match.group(2))
        if month is not None:
            return date(year, month, 1).isoformat(), "month"

    year_month_name_match = re.fullmatch(r"(\d{4})\s+([A-Za-z]+)", text)
    if year_month_name_match:
        year = int(year_month_name_match.group(1))
        month = parse_month_value(year_month_name_match.group(2))
        if month is not None:
            return date(year, month, 1).isoformat(), "month"

    return None, "unknown"


def parse_date_mapping(
    raw_value: Mapping[str, Any],
    *,
    is_end_date: bool,
) -> tuple[str | None, str]:
    nested_value = raw_value.get("value")
    if nested_value is not None and nested_value is not raw_value:
        return parse_experience_date_value(nested_value, is_end_date=is_end_date)

    nested_date = raw_value.get("date")
    if nested_date is not None and nested_date is not raw_value:
        return parse_experience_date_value(nested_date, is_end_date=is_end_date)

    text_candidate = extract_text_field(raw_value, ("text", "label", "display", "raw"))
    if text_candidate is not None:
        parsed_text = parse_date_string(text_candidate, is_end_date=is_end_date)
        if parsed_text != (None, "unknown"):
            return parsed_text

    year = coerce_int(raw_value.get("year"))
    month = parse_month_value(raw_value.get("month"))
    day = coerce_int(raw_value.get("day"))

    if year is not None:
        try:
            if month is not None and day is not None:
                return date(year, month, day).isoformat(), "day"
            if month is not None:
                return date(year, month, 1).isoformat(), "month"
            return date(year, 1, 1).isoformat(), "year"
        except ValueError:
            return None, "unknown"

    precision = normalized_string(raw_value.get("precision"))
    if precision is not None:
        normalized_precision = precision.strip().lower()
        if is_end_date and normalized_precision == "present":
            return None, "present"

    return None, "unknown"


def parse_experience_date_value(
    raw_value: Any,
    *,
    is_end_date: bool,
) -> tuple[str | None, str]:
    if raw_value is None:
        return None, "unknown"
    if isinstance(raw_value, str):
        return parse_date_string(raw_value, is_end_date=is_end_date)
    if isinstance(raw_value, Mapping):
        return parse_date_mapping(raw_value, is_end_date=is_end_date)
    if isinstance(raw_value, (int, float)):
        numeric = int(raw_value)
        if 1000 <= numeric <= 9999:
            return date(numeric, 1, 1).isoformat(), "year"
    return None, "unknown"


def extract_company_name(item: Mapping[str, Any]) -> str | None:
    raw_value = first_present(item, COMPANY_NAME_KEYS)
    if isinstance(raw_value, Mapping):
        for nested_key in ("name", "companyName", "company_name", "text", "label", "display"):
            candidate = normalized_string(raw_value.get(nested_key))
            if candidate is not None:
                return candidate
        return None
    return normalized_string(raw_value)


def extract_company_identity_inputs(item: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    raw_company_id = normalized_string(first_present(item, COMPANY_ID_KEYS))
    raw_company_username = normalized_string(first_present(item, COMPANY_USERNAME_KEYS))
    raw_company_url = normalized_string(first_present(item, COMPANY_URL_KEYS))

    company_mapping = first_present(item, COMPANY_NAME_KEYS)
    if isinstance(company_mapping, Mapping):
        if raw_company_id is None:
            raw_company_id = normalized_string(
                first_present(company_mapping, COMPANY_ID_KEYS)
            )
        if raw_company_username is None:
            raw_company_username = normalized_string(
                first_present(company_mapping, COMPANY_USERNAME_KEYS)
            )
        if raw_company_url is None:
            raw_company_url = normalized_string(
                first_present(company_mapping, COMPANY_URL_KEYS)
            )

    return raw_company_id, raw_company_username, raw_company_url


def is_structurally_empty(prepared_row: Mapping[str, Any]) -> bool:
    return not any(
        prepared_row.get(key)
        for key in (
            "title",
            "description",
            "location",
            "raw_company_name",
            "incoming_linkedin_id",
            "incoming_linkedin_username",
            "incoming_linkedin_url",
            "start_date_raw",
            "end_date_raw",
        )
    ) and not bool(prepared_row.get("raw_is_current"))


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


def source_select_expression(
    column_name: str,
    available_columns: set[str],
    *,
    table_alias: str,
    null_expr: str,
) -> str:
    if column_name in available_columns:
        return f"{table_alias}.{column_name}"
    return null_expr


def source_jsonb_expression(
    column_name: str,
    available_columns: set[str],
    *,
    table_alias: str,
) -> str:
    if column_name in available_columns:
        return f"{table_alias}.{column_name}::jsonb"
    return "null::jsonb"


def build_source_batch_sql(
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    *,
    available_columns: set[str],
) -> str:
    candidate_created_at = source_select_expression(
        "created_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    candidate_updated_at = source_select_expression(
        "updated_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    work_experience_expr = source_jsonb_expression(
        "work_experience",
        available_columns,
        table_alias="c",
    )
    linkedin_data_expr = source_jsonb_expression(
        "linkedin_data",
        available_columns,
        table_alias="c",
    )

    cursor_clause = "true"
    if cursor is not None:
        cursor_candidate_id = str(cursor["source_candidate_id"])
        cursor_array_index = int(cursor["source_array_index"])
        cursor_clause = f"""(
            flattened.source_candidate_id::uuid > {sql_text_literal(cursor_candidate_id)}::uuid
            or (
                flattened.source_candidate_id::uuid = {sql_text_literal(cursor_candidate_id)}::uuid
                and flattened.source_array_index > {cursor_array_index}
            )
        )"""

    return f"""
with source_candidates as (
    select
        c.id::text as source_candidate_id,
        {candidate_created_at} as candidate_created_at,
        {candidate_updated_at} as candidate_updated_at,
        case
            when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
                then {sql_text_literal(SOURCE_PATH_WORK_EXPERIENCE)}
            when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
                then {sql_text_literal(SOURCE_PATH_LINKEDIN_EXPERIENCE)}
            else null
        end as source_path,
        case
            when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
                then {work_experience_expr}
            when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
                then ({linkedin_data_expr} -> 'data' -> 'experience')
            else '[]'::jsonb
        end as chosen_source_array
    from {LEGACY_CANDIDATES_TABLE} as c
),
flattened as (
    select
        sc.source_candidate_id,
        sc.candidate_created_at,
        sc.candidate_updated_at,
        sc.source_path,
        case
            when sc.source_path = {sql_text_literal(SOURCE_PATH_WORK_EXPERIENCE)} then 'work_experience'
            when sc.source_path = {sql_text_literal(SOURCE_PATH_LINKEDIN_EXPERIENCE)} then 'linkedin_data_experience'
            else null
        end as source_family,
        (exp.ordinality - 1)::integer as source_array_index,
        exp.item as raw_experience_item
    from source_candidates as sc
    cross join lateral jsonb_array_elements(sc.chosen_source_array) with ordinality as exp(item, ordinality)
    where sc.source_path is not null
)
select row_to_json(t)::text
from (
    select
        flattened.source_candidate_id,
        flattened.candidate_created_at,
        flattened.candidate_updated_at,
        flattened.source_path,
        flattened.source_family,
        flattened.source_array_index,
        flattened.raw_experience_item
    from flattened
    where {cursor_clause}
    order by flattened.source_candidate_id::uuid asc, flattened.source_array_index asc
    limit {batch_size}
) as t;
"""


def build_source_candidate_batch_sql(
    cursor_candidate_id: str | None,
    batch_size: int,
    *,
    available_columns: set[str],
    include_current_candidate: bool,
) -> str:
    candidate_created_at = source_select_expression(
        "created_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    candidate_updated_at = source_select_expression(
        "updated_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    work_experience_expr = source_jsonb_expression(
        "work_experience",
        available_columns,
        table_alias="c",
    )
    linkedin_data_expr = source_jsonb_expression(
        "linkedin_data",
        available_columns,
        table_alias="c",
    )

    cursor_clause = "true"
    if cursor_candidate_id is not None:
        operator = ">=" if include_current_candidate else ">"
        cursor_clause = (
            f"c.id::uuid {operator} {sql_text_literal(cursor_candidate_id)}::uuid"
        )

    return f"""
select
    c.id::text as source_candidate_id,
    {candidate_created_at} as candidate_created_at,
    {candidate_updated_at} as candidate_updated_at,
    case
        when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
            then {sql_text_literal(SOURCE_PATH_WORK_EXPERIENCE)}
        when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
            then {sql_text_literal(SOURCE_PATH_LINKEDIN_EXPERIENCE)}
        else null
    end as source_path,
    case
        when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
            then 'work_experience'
        when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
            then 'linkedin_data_experience'
        else null
    end as source_family,
    case
        when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
            then {work_experience_expr}
        when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
         and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
            then ({linkedin_data_expr} -> 'data' -> 'experience')
        else '[]'::jsonb
    end as chosen_source_array
from {LEGACY_CANDIDATES_TABLE} as c
where {cursor_clause}
  and (
        (
            jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
            and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
        )
        or
        (
            jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
            and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
        )
    )
order by c.id::uuid asc
limit {batch_size};
"""


def fetch_source_candidate_batch(
    source_db: SupabaseLinkedClient,
    *,
    cursor_candidate_id: str | None,
    batch_size: int,
    available_columns: set[str],
    include_current_candidate: bool,
) -> list[dict[str, Any]]:
    sql = build_source_candidate_batch_sql(
        cursor_candidate_id,
        batch_size,
        available_columns=available_columns,
        include_current_candidate=include_current_candidate,
    )
    return source_db.query_json_rows(sql)


def build_source_candidate_subset_sql(
    source_candidate_ids: Sequence[str],
    *,
    available_columns: set[str],
) -> str:
    if not source_candidate_ids:
        raise ValueError("source_candidate_ids must be non-empty.")

    candidate_created_at = source_select_expression(
        "created_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    candidate_updated_at = source_select_expression(
        "updated_at",
        available_columns,
        table_alias="c",
        null_expr="null::timestamptz",
    )
    work_experience_expr = source_jsonb_expression(
        "work_experience",
        available_columns,
        table_alias="c",
    )
    linkedin_data_expr = source_jsonb_expression(
        "linkedin_data",
        available_columns,
        table_alias="c",
    )
    requested_ids_literal = ", ".join(
        f"{sql_text_literal(candidate_id)}::uuid" for candidate_id in source_candidate_ids
    )

    return f"""
with requested_candidates as (
    select
        requested.candidate_id,
        requested.ordinality
    from unnest(array[{requested_ids_literal}]::uuid[]) with ordinality as requested(candidate_id, ordinality)
),
source_candidates as (
    select
        requested.ordinality,
        c.id::text as source_candidate_id,
        {candidate_created_at} as candidate_created_at,
        {candidate_updated_at} as candidate_updated_at,
        case
            when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
                then {sql_text_literal(SOURCE_PATH_WORK_EXPERIENCE)}
            when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
                then {sql_text_literal(SOURCE_PATH_LINKEDIN_EXPERIENCE)}
            else null
        end as source_path,
        case
            when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
                then 'work_experience'
            when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
                then 'linkedin_data_experience'
            else null
        end as source_family,
        case
            when jsonb_typeof(coalesce({work_experience_expr}, 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce({work_experience_expr}, '[]'::jsonb)) > 0
                then {work_experience_expr}
            when jsonb_typeof(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), 'null'::jsonb)) = 'array'
             and jsonb_array_length(coalesce(({linkedin_data_expr} -> 'data' -> 'experience'), '[]'::jsonb)) > 0
                then ({linkedin_data_expr} -> 'data' -> 'experience')
            else '[]'::jsonb
        end as chosen_source_array
    from requested_candidates as requested
    join {LEGACY_CANDIDATES_TABLE} as c
      on c.id = requested.candidate_id
)
select row_to_json(t)::text
from (
    select
        sc.source_candidate_id,
        sc.candidate_created_at,
        sc.candidate_updated_at,
        sc.source_path,
        sc.source_family,
        sc.chosen_source_array
    from source_candidates as sc
    where sc.source_path is not null
    order by sc.ordinality asc
) as t;
"""


def fetch_source_candidate_subset(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    source_candidate_ids: Sequence[str],
    available_columns: set[str],
) -> list[dict[str, Any]]:
    if not source_candidate_ids:
        return []

    sql = build_source_candidate_subset_sql(
        source_candidate_ids,
        available_columns=available_columns,
    )
    return source_db.query_json_rows(sql)


def flatten_candidate_rows_batch(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    cursor: Mapping[str, Any] | None,
    batch_size: int,
) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    cursor_candidate_id = str(cursor["source_candidate_id"]) if cursor is not None else None
    cursor_source_index = (
        int(cursor["source_array_index"]) if cursor is not None else None
    )

    for candidate_row in candidate_rows:
        source_candidate_id = str(candidate_row["source_candidate_id"])
        chosen_source_array = candidate_row.get("chosen_source_array")
        if isinstance(chosen_source_array, str):
            try:
                chosen_source_array = json.loads(chosen_source_array)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    "Linked source batch returned non-decodable chosen_source_array "
                    f"for candidate {source_candidate_id}."
                ) from exc
        if not isinstance(chosen_source_array, list):
            raise RuntimeError(
                "Linked source batch returned an unexpected chosen_source_array type "
                f"for candidate {source_candidate_id}: "
                f"{type(chosen_source_array).__name__}."
            )

        start_index = 0
        if (
            cursor_candidate_id is not None
            and source_candidate_id == cursor_candidate_id
            and cursor_source_index is not None
        ):
            start_index = cursor_source_index + 1

        for source_array_index in range(start_index, len(chosen_source_array)):
            flattened.append(
                {
                    "source_candidate_id": source_candidate_id,
                    "candidate_created_at": candidate_row.get("candidate_created_at"),
                    "candidate_updated_at": candidate_row.get("candidate_updated_at"),
                    "source_path": candidate_row.get("source_path"),
                    "source_family": candidate_row.get("source_family"),
                    "source_array_index": source_array_index,
                    "raw_experience_item": chosen_source_array[source_array_index],
                }
            )
            if len(flattened) >= batch_size:
                return flattened

    return flattened


def shard_manifest_start_index(
    shard_manifest: Mapping[str, Any],
    *,
    cursor: Mapping[str, Any] | None,
) -> int:
    if cursor is None:
        return 0

    source_candidate_id = str(cursor["source_candidate_id"])
    candidate_index_by_id = shard_manifest["candidate_index_by_id"]
    if source_candidate_id not in candidate_index_by_id:
        raise RuntimeError(
            "Checkpoint cursor does not belong to this shard manifest. "
            f"cursor_source_candidate_id={source_candidate_id}"
        )
    return int(candidate_index_by_id[source_candidate_id])


def fetch_source_batch_via_candidate_rows(
    source_db: SupabaseLinkedClient,
    *,
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    available_columns: set[str],
    source_candidate_batch_size: int,
) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    cursor_candidate_id = (
        str(cursor["source_candidate_id"]) if cursor is not None else None
    )
    include_current_candidate = cursor is not None
    resume_cursor = cursor

    while len(flattened) < batch_size:
        candidate_rows = fetch_source_candidate_batch(
            source_db,
            cursor_candidate_id=cursor_candidate_id,
            batch_size=source_candidate_batch_size,
            available_columns=available_columns,
            include_current_candidate=include_current_candidate,
        )
        if not candidate_rows:
            break

        flattened.extend(
            flatten_candidate_rows_batch(
                candidate_rows,
                cursor=resume_cursor,
                batch_size=batch_size - len(flattened),
            )
        )
        if len(flattened) >= batch_size:
            break

        resume_cursor = None
        include_current_candidate = False
        cursor_candidate_id = str(candidate_rows[-1]["source_candidate_id"])

        if len(candidate_rows) < source_candidate_batch_size:
            break

    return flattened


def fetch_source_batch_via_manifest(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    shard_manifest: Mapping[str, Any],
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    available_columns: set[str],
    source_candidate_batch_size: int,
) -> list[dict[str, Any]]:
    ordered_source_candidate_ids = shard_manifest["ordered_source_candidate_ids"]
    if not ordered_source_candidate_ids:
        return []

    flattened: list[dict[str, Any]] = []
    manifest_position = shard_manifest_start_index(shard_manifest, cursor=cursor)
    resume_cursor = cursor

    while (
        len(flattened) < batch_size
        and manifest_position < len(ordered_source_candidate_ids)
    ):
        candidate_id_slice = ordered_source_candidate_ids[
            manifest_position : manifest_position + source_candidate_batch_size
        ]
        candidate_rows = fetch_source_candidate_subset(
            source_db,
            source_candidate_ids=candidate_id_slice,
            available_columns=available_columns,
        )
        if candidate_rows:
            flattened.extend(
                flatten_candidate_rows_batch(
                    candidate_rows,
                    cursor=resume_cursor,
                    batch_size=batch_size - len(flattened),
                )
            )
            if len(flattened) >= batch_size:
                break

        manifest_position += len(candidate_id_slice)
        resume_cursor = None

    return flattened


def fetch_source_batch(
    source_db: PsqlClient | SupabaseLinkedClient,
    *,
    cursor: Mapping[str, Any] | None,
    batch_size: int,
    available_columns: set[str],
    source_candidate_batch_size: int,
    shard_manifest: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if shard_manifest is not None:
        return fetch_source_batch_via_manifest(
            source_db,
            shard_manifest=shard_manifest,
            cursor=cursor,
            batch_size=batch_size,
            available_columns=available_columns,
            source_candidate_batch_size=source_candidate_batch_size,
        )

    if isinstance(source_db, SupabaseLinkedClient):
        return fetch_source_batch_via_candidate_rows(
            source_db,
            cursor=cursor,
            batch_size=batch_size,
            available_columns=available_columns,
            source_candidate_batch_size=source_candidate_batch_size,
        )

    sql = build_source_batch_sql(
        cursor=cursor,
        batch_size=batch_size,
        available_columns=available_columns,
    )
    return source_db.query_json_rows(sql)


def next_cursor(
    prepared_batch: list[dict[str, Any]],
    current_cursor: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not prepared_batch:
        return current_cursor
    last = prepared_batch[-1]
    return {
        "source_candidate_id": str(last["source_candidate_id"]),
        "source_array_index": int(last["source_array_index"]),
    }


def find_candidate_map_path(provided_path: str | None) -> Path:
    if provided_path is None:
        raise FileNotFoundError(
            "Task 8 requires an explicit approved Task 7c candidate-resolution mapping artifact. "
            "Pass --candidate-map-path."
        )

    path = Path(provided_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Candidate map artifact not found: {path}")
    return path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
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


def prepare_source_row(
    raw_row: Mapping[str, Any],
    *,
    candidate_map: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_candidate_id = str(raw_row["source_candidate_id"])
    source_path = str(raw_row["source_path"])
    source_family = normalized_string(raw_row.get("source_family"))
    source_array_index = int(raw_row["source_array_index"])
    raw_item = raw_row.get("raw_experience_item")
    if not isinstance(raw_item, Mapping):
        raw_item = {"raw_value": preserve_jsonable(raw_item)}

    title = extract_text_field(raw_item, TITLE_KEYS)
    description = extract_text_field(raw_item, DESCRIPTION_KEYS)
    location = extract_text_field(raw_item, LOCATION_KEYS)
    raw_company_name = extract_company_name(raw_item)
    incoming_linkedin_id, incoming_linkedin_username, incoming_linkedin_url = (
        extract_company_identity_inputs(raw_item)
    )

    raw_start_value = first_present(raw_item, START_DATE_KEYS)
    raw_end_value = first_present(raw_item, END_DATE_KEYS)
    raw_is_current_value = first_present(raw_item, CURRENT_KEYS)
    raw_is_current = parse_boolish(raw_is_current_value)

    start_date_raw, start_date_precision_raw = parse_experience_date_value(
        raw_start_value,
        is_end_date=False,
    )
    end_date_raw, end_date_precision_raw = parse_experience_date_value(
        raw_end_value,
        is_end_date=True,
    )

    candidate_resolution = candidate_map.get(source_candidate_id, {})
    resolved_candidate_id = clean_uuid_text(
        candidate_resolution.get("resolved_candidate_id")
    )
    candidate_match_action = normalized_string(candidate_resolution.get("match_action"))
    candidate_match_basis = normalized_string(candidate_resolution.get("match_basis"))
    candidate_skip_reason = normalized_string(candidate_resolution.get("skip_reason"))

    if candidate_match_action not in {"create_new", "match_existing"}:
        resolved_candidate_id = None
        if candidate_skip_reason is None:
            candidate_skip_reason = "missing_candidate_link"

    prepared = {
        "source_candidate_id": source_candidate_id,
        "resolved_candidate_id": resolved_candidate_id,
        "candidate_match_action": candidate_match_action,
        "candidate_match_basis": candidate_match_basis,
        "candidate_skip_reason": candidate_skip_reason,
        "source_path": source_path,
        "source_family": source_family,
        "source_array_index": source_array_index,
        "experience_index": source_array_index,
        "title": title,
        "description": description,
        "location": location,
        "raw_company_name": raw_company_name,
        "incoming_linkedin_id": incoming_linkedin_id,
        "incoming_linkedin_username": incoming_linkedin_username,
        "incoming_linkedin_url": incoming_linkedin_url,
        "start_date_raw": start_date_raw,
        "start_date_precision_raw": start_date_precision_raw,
        "end_date_raw": end_date_raw,
        "end_date_precision_raw": end_date_precision_raw,
        "raw_is_current": raw_is_current,
        "raw_item": preserve_jsonable(raw_item),
        "raw_company_identity_inputs": {
            "linkedin_id": incoming_linkedin_id,
            "linkedin_username": incoming_linkedin_username,
            "linkedin_url": incoming_linkedin_url,
            "company_name": raw_company_name,
        },
        "raw_date_payload": {
            "start": preserve_jsonable(raw_start_value),
            "end": preserve_jsonable(raw_end_value),
            "is_current": preserve_jsonable(raw_is_current_value),
        },
        "candidate_created_at": clean_optional_timestamptz(raw_row.get("candidate_created_at")),
        "candidate_updated_at": clean_optional_timestamptz(raw_row.get("candidate_updated_at")),
    }
    prepared["structurally_empty"] = is_structurally_empty(prepared)
    return prepared


def prepare_source_batch(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_map: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [prepare_source_row(row, candidate_map=candidate_map) for row in raw_rows]


def fetch_target_prerequisites(
    target_db: PsqlClient | SupabaseLinkedClient,
) -> dict[str, Any]:
    sql = """
select json_build_object(
  'candidate_experiences_v2_exists', to_regclass('public.candidate_experiences_v2') is not null,
  'candidate_profiles_v2_exists', to_regclass('public.candidate_profiles_v2') is not null,
  'companies_v2_exists', to_regclass('public.companies_v2') is not null,
  'canonicalization_ambiguities_exists', to_regclass('public.canonicalization_ambiguities') is not null,
  'resolve_company_match_exists', to_regprocedure('public.resolve_company_match(text,text,text,text)') is not null,
  'record_canonicalization_ambiguity_exists', to_regprocedure('public.record_canonicalization_ambiguity(text,text,text,text,jsonb,uuid[],text)') is not null,
  'normalize_experience_date_precision_exists', to_regprocedure('public.normalize_experience_date_precision(text,boolean,boolean)') is not null,
  'normalize_experience_date_exists', to_regprocedure('public.normalize_experience_date(date,text,boolean,boolean)') is not null,
  'normalize_experience_is_current_exists', to_regprocedure('public.normalize_experience_is_current(boolean,text)') is not null,
  'build_candidate_experience_source_hash_exists', to_regprocedure('public.build_candidate_experience_source_hash(uuid,text,uuid,text,date,text,date,text,boolean)') is not null,
  'normalize_company_name_exists', to_regprocedure('public.normalize_company_name(text)') is not null,
  'normalize_search_text_exists', to_regprocedure('public.normalize_search_text(text)') is not null,
  'normalize_linkedin_username_exists', to_regprocedure('public.normalize_linkedin_username(text)') is not null,
  'normalize_company_linkedin_url_exists', to_regprocedure('public.normalize_company_linkedin_url(text)') is not null,
  'extract_company_linkedin_username_from_url_exists', to_regprocedure('public.extract_company_linkedin_username_from_url(text)') is not null
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
            "Target database is missing required Task 8 prerequisites: "
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

create or replace function pg_temp.upsert_candidate_experience_ambiguity(
    p_ambiguity_type text,
    p_source_record_ref text,
    p_normalized_input jsonb,
    p_matched_record_ids uuid[],
    p_recommended_action text default 'manual_review'
)
returns uuid
language plpgsql
as $$
declare
    v_existing_id uuid;
begin
    select id
    into v_existing_id
    from public.canonicalization_ambiguities
    where entity_type = 'candidate_experience'
      and ambiguity_type = p_ambiguity_type
      and coalesce(source_system, '') = '{AMBIGUITY_SOURCE_SYSTEM}'
      and coalesce(source_record_ref, '') = coalesce(p_source_record_ref, '')
      and status = 'open'
    order by created_at desc
    limit 1;

    if v_existing_id is not null then
        update public.canonicalization_ambiguities
        set
            normalized_input = coalesce(p_normalized_input, '{{}}'::jsonb),
            matched_record_ids = p_matched_record_ids,
            recommended_action = coalesce(p_recommended_action, 'manual_review')
        where id = v_existing_id;
        return v_existing_id;
    end if;

    return public.record_canonicalization_ambiguity(
        'candidate_experience',
        p_ambiguity_type,
        '{AMBIGUITY_SOURCE_SYSTEM}',
        p_source_record_ref,
        p_normalized_input,
        p_matched_record_ids,
        p_recommended_action
    );
end;
$$;

{prelude_block}create temp table tmp_experience_input (
    source_candidate_id uuid not null,
    resolved_candidate_id uuid,
    candidate_match_action text,
    candidate_match_basis text,
    candidate_skip_reason text,
    source_path text not null,
    source_family text,
    source_array_index integer not null,
    experience_index integer not null,
    title text,
    description text,
    location text,
    raw_company_name text,
    incoming_linkedin_id text,
    incoming_linkedin_username text,
    incoming_linkedin_url text,
    start_date_raw date,
    start_date_precision_raw text,
    end_date_raw date,
    end_date_precision_raw text,
    raw_is_current boolean,
    raw_item jsonb,
    raw_company_identity_inputs jsonb,
    raw_date_payload jsonb,
    candidate_created_at timestamptz,
    candidate_updated_at timestamptz,
    structurally_empty boolean not null default false
) on commit drop;

insert into tmp_experience_input (
    source_candidate_id,
    resolved_candidate_id,
    candidate_match_action,
    candidate_match_basis,
    candidate_skip_reason,
    source_path,
    source_family,
    source_array_index,
    experience_index,
    title,
    description,
    location,
    raw_company_name,
    incoming_linkedin_id,
    incoming_linkedin_username,
    incoming_linkedin_url,
    start_date_raw,
    start_date_precision_raw,
    end_date_raw,
    end_date_precision_raw,
    raw_is_current,
    raw_item,
    raw_company_identity_inputs,
    raw_date_payload,
    candidate_created_at,
    candidate_updated_at,
    structurally_empty
)
select
    source_candidate_id::uuid,
    nullif(resolved_candidate_id, '')::uuid,
    candidate_match_action,
    candidate_match_basis,
    candidate_skip_reason,
    source_path,
    source_family,
    source_array_index,
    experience_index,
    title,
    description,
    location,
    raw_company_name,
    incoming_linkedin_id,
    incoming_linkedin_username,
    incoming_linkedin_url,
    nullif(start_date_raw, '')::date,
    start_date_precision_raw,
    nullif(end_date_raw, '')::date,
    end_date_precision_raw,
    raw_is_current,
    raw_item,
    raw_company_identity_inputs,
    raw_date_payload,
    nullif(candidate_created_at, '')::timestamptz,
    nullif(candidate_updated_at, '')::timestamptz,
    structurally_empty
from jsonb_to_recordset({payload_literal}) as x(
    source_candidate_id text,
    resolved_candidate_id text,
    candidate_match_action text,
    candidate_match_basis text,
    candidate_skip_reason text,
    source_path text,
    source_family text,
    source_array_index integer,
    experience_index integer,
    title text,
    description text,
    location text,
    raw_company_name text,
    incoming_linkedin_id text,
    incoming_linkedin_username text,
    incoming_linkedin_url text,
    start_date_raw text,
    start_date_precision_raw text,
    end_date_raw text,
    end_date_precision_raw text,
    raw_is_current boolean,
    raw_item jsonb,
    raw_company_identity_inputs jsonb,
    raw_date_payload jsonb,
    candidate_created_at text,
    candidate_updated_at text,
    structurally_empty boolean
)
order by source_candidate_id asc, source_array_index asc;

create temp table tmp_experience_results (
    source_candidate_id uuid not null,
    resolved_candidate_id uuid,
    source_path text not null,
    source_family text,
    source_array_index integer not null,
    experience_index integer not null,
    experience_row_id uuid,
    effective_action text not null,
    experience_match_basis text,
    company_resolution_status text,
    company_match_basis text,
    ambiguity_type text,
    skip_reason text,
    company_id uuid,
    source_hash text,
    normalized_title text,
    normalized_company_name text,
    source_company_linkedin_username text,
    start_date date,
    start_date_precision text,
    end_date date,
    end_date_precision text,
    is_current boolean,
    ambiguity_logged boolean not null default false
) on commit drop;

do $plpgsql$
declare
    rec tmp_experience_input%rowtype;
    company_match_rec record;
    existing_row public.candidate_experiences_v2%rowtype;
    v_candidate_exists boolean;
    v_company_id uuid;
    v_source_company_linkedin_username text;
    v_start_date date;
    v_start_precision text;
    v_end_date date;
    v_end_precision text;
    v_is_current boolean;
    v_source_hash text;
    v_source_payload jsonb;
    v_normalized_title text;
    v_normalized_company_name text;
    v_ambiguity_source_ref text;
    v_normalized_ambiguity_input jsonb;
    v_existing_by_ref_id uuid;
    v_existing_by_hash_id uuid;
    v_existing_ref_count integer;
    v_conflicting_hash_id uuid;
    v_ambiguity_match_ids uuid[];
    v_result_action text;
    v_experience_match_basis text;
    v_display_title text;
    v_display_description text;
    v_display_location text;
    v_display_company_name text;
    v_display_company_username text;
    v_needs_update boolean;
    v_incoming_identity jsonb;
    v_company_inputs jsonb;
begin
    for rec in
        select *
        from tmp_experience_input
        order by source_candidate_id asc, source_array_index asc
    loop
        if rec.resolved_candidate_id is null then
            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                null,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                null,
                'skip',
                null,
                null,
                null,
                null,
                coalesce(rec.candidate_skip_reason, 'missing_candidate_link'),
                null,
                null,
                public.normalize_search_text(rec.title),
                public.normalize_company_name(rec.raw_company_name),
                null,
                null,
                null,
                null,
                null,
                false,
                false
            );
            continue;
        end if;

        select exists (
            select 1
            from public.candidate_profiles_v2
            where id = rec.resolved_candidate_id
        )
        into v_candidate_exists;

        if not coalesce(v_candidate_exists, false) then
            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                null,
                'skip',
                null,
                null,
                null,
                null,
                'missing_candidate_link',
                null,
                public.normalize_search_text(rec.title),
                public.normalize_company_name(rec.raw_company_name),
                null,
                null,
                null,
                null,
                null,
                false,
                false
            );
            continue;
        end if;

        v_is_current := public.normalize_experience_is_current(
            rec.raw_is_current,
            rec.end_date_precision_raw
        );
        v_start_precision := public.normalize_experience_date_precision(
            rec.start_date_precision_raw,
            false,
            false
        );
        v_start_date := public.normalize_experience_date(
            rec.start_date_raw,
            rec.start_date_precision_raw,
            false,
            false
        );
        v_end_precision := public.normalize_experience_date_precision(
            rec.end_date_precision_raw,
            true,
            v_is_current
        );
        v_end_date := public.normalize_experience_date(
            rec.end_date_raw,
            rec.end_date_precision_raw,
            true,
            v_is_current
        );
        v_normalized_title := public.normalize_search_text(rec.title);
        v_normalized_company_name := public.normalize_company_name(rec.raw_company_name);

        if rec.structurally_empty then
            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                null,
                'skip',
                null,
                null,
                null,
                null,
                'structurally_empty',
                null,
                v_normalized_title,
                v_normalized_company_name,
                null,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                false
            );
            continue;
        end if;

        select *
        into company_match_rec
        from public.resolve_company_match(
            rec.incoming_linkedin_id,
            rec.incoming_linkedin_username,
            rec.incoming_linkedin_url,
            rec.raw_company_name
        );

        if company_match_rec.decision is null then
            raise exception 'resolve_company_match returned no decision for candidate_id=% source_path=% array_index=%',
                rec.source_candidate_id,
                rec.source_path,
                rec.source_array_index;
        end if;

        v_company_id := null;
        if company_match_rec.decision = 'match_existing' then
            v_company_id := company_match_rec.matched_company_id;
        end if;

        v_source_company_linkedin_username := coalesce(
            public.normalize_linkedin_username(rec.incoming_linkedin_username),
            public.extract_company_linkedin_username_from_url(rec.incoming_linkedin_url)
        );

        v_source_hash := public.build_candidate_experience_source_hash(
            rec.resolved_candidate_id,
            rec.title,
            v_company_id,
            rec.raw_company_name,
            v_start_date,
            v_start_precision,
            v_end_date,
            v_end_precision,
            v_is_current
        );

        v_incoming_identity := jsonb_build_object(
            'title', v_normalized_title,
            'raw_company_name', v_normalized_company_name,
            'start_date', to_jsonb(v_start_date),
            'start_date_precision', to_jsonb(v_start_precision),
            'end_date', to_jsonb(v_end_date),
            'end_date_precision', to_jsonb(v_end_precision),
            'is_current', to_jsonb(v_is_current)
        );
        v_company_inputs := jsonb_build_object(
            'linkedin_id', to_jsonb(nullif(lower(btrim(coalesce(rec.incoming_linkedin_id, ''))), '')),
            'linkedin_username', to_jsonb(company_match_rec.normalized_linkedin_username),
            'linkedin_url_normalized', to_jsonb(company_match_rec.normalized_linkedin_url),
            'company_name', to_jsonb(company_match_rec.normalized_name)
        );
        v_source_payload := jsonb_build_object(
            'source_table', 'candidates',
            'legacy_candidate_id', rec.source_candidate_id::text,
            'source_path', rec.source_path,
            'source_array_index', rec.source_array_index,
            'source_family', rec.source_family,
            'experience_identity', v_incoming_identity,
            'raw_experience_fragment', rec.raw_item,
            'raw_company_identity_inputs', rec.raw_company_identity_inputs,
            'raw_date_payload', rec.raw_date_payload
        );
        v_ambiguity_source_ref := rec.resolved_candidate_id::text || ':' || v_source_hash;
        v_normalized_ambiguity_input := jsonb_build_object(
            'legacy_candidate_id', rec.source_candidate_id::text,
            'candidate_id', rec.resolved_candidate_id::text,
            'source_path', rec.source_path,
            'source_array_index', rec.source_array_index,
            'experience_identity', v_incoming_identity,
            'company_resolution_inputs', v_company_inputs
        );
        if company_match_rec.decision = 'ambiguous' then
            select array_agg(distinct c.id order by c.id)
            into v_ambiguity_match_ids
            from public.companies_v2 as c
            where (
                rec.incoming_linkedin_id is not null
                and c.linkedin_id = lower(btrim(rec.incoming_linkedin_id))
            )
            or (
                company_match_rec.normalized_linkedin_username is not null
                and c.linkedin_username = company_match_rec.normalized_linkedin_username
            )
            or (
                company_match_rec.normalized_linkedin_url is not null
                and c.linkedin_url_normalized = company_match_rec.normalized_linkedin_url
            )
            or (
                company_match_rec.ambiguity_type = 'multiple_normalized_name_matches'
                and company_match_rec.normalized_name is not null
                and c.normalized_name = company_match_rec.normalized_name
            );

            perform pg_temp.upsert_candidate_experience_ambiguity(
                company_match_rec.ambiguity_type,
                v_ambiguity_source_ref,
                v_normalized_ambiguity_input,
                v_ambiguity_match_ids,
                'manual_review'
            );
        end if;

        select count(*), min(id::text)::uuid
        into v_existing_ref_count, v_existing_by_ref_id
        from public.candidate_experiences_v2
        where candidate_id = rec.resolved_candidate_id
          and coalesce(source_payload ->> 'source_table', '') = 'candidates'
          and coalesce(source_payload ->> 'legacy_candidate_id', '') = rec.source_candidate_id::text
          and coalesce(source_payload ->> 'source_path', '') = rec.source_path
          and coalesce(source_payload ->> 'source_array_index', '') = rec.source_array_index::text;

        if v_existing_ref_count > 1 then
            perform pg_temp.upsert_candidate_experience_ambiguity(
                'multiple_source_record_reference_matches',
                v_ambiguity_source_ref,
                v_normalized_ambiguity_input,
                array(
                    select id
                    from public.candidate_experiences_v2
                    where candidate_id = rec.resolved_candidate_id
                      and coalesce(source_payload ->> 'source_table', '') = 'candidates'
                      and coalesce(source_payload ->> 'legacy_candidate_id', '') = rec.source_candidate_id::text
                      and coalesce(source_payload ->> 'source_path', '') = rec.source_path
                      and coalesce(source_payload ->> 'source_array_index', '') = rec.source_array_index::text
                    order by id
                ),
                'manual_review'
            );

            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                null,
                'skip',
                null,
                case
                    when company_match_rec.decision = 'match_existing' then 'matched'
                    when company_match_rec.decision = 'ambiguous' then 'ambiguous'
                    else 'unresolved'
                end,
                company_match_rec.match_basis,
                'multiple_source_record_reference_matches',
                'multiple_source_record_reference_matches',
                v_company_id,
                v_source_hash,
                v_normalized_title,
                v_normalized_company_name,
                v_source_company_linkedin_username,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                true
            );
            continue;
        end if;

        select id
        into v_existing_by_hash_id
        from public.candidate_experiences_v2
        where candidate_id = rec.resolved_candidate_id
          and source_hash = v_source_hash;

        if v_existing_by_ref_id is not null
           and v_existing_by_hash_id is not null
           and v_existing_by_ref_id <> v_existing_by_hash_id then
            perform pg_temp.upsert_candidate_experience_ambiguity(
                'source_record_ref_conflicts_with_source_hash',
                v_ambiguity_source_ref,
                v_normalized_ambiguity_input,
                array[v_existing_by_ref_id, v_existing_by_hash_id],
                'manual_review'
            );

            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                null,
                'skip',
                null,
                case
                    when company_match_rec.decision = 'match_existing' then 'matched'
                    when company_match_rec.decision = 'ambiguous' then 'ambiguous'
                    else 'unresolved'
                end,
                company_match_rec.match_basis,
                'source_record_ref_conflicts_with_source_hash',
                'source_record_ref_conflicts_with_source_hash',
                v_company_id,
                v_source_hash,
                v_normalized_title,
                v_normalized_company_name,
                v_source_company_linkedin_username,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                true
            );
            continue;
        end if;

        if v_existing_by_ref_id is null and v_existing_by_hash_id is null then
            insert into public.candidate_experiences_v2 (
                candidate_id,
                company_id,
                experience_index,
                title,
                description,
                location,
                raw_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                source_payload,
                source_hash
            )
            values (
                rec.resolved_candidate_id,
                v_company_id,
                rec.experience_index,
                rec.title,
                rec.description,
                rec.location,
                rec.raw_company_name,
                v_source_company_linkedin_username,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                v_source_payload,
                v_source_hash
            )
            returning * into existing_row;

            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                existing_row.id,
                'create_new',
                null,
                case
                    when company_match_rec.decision = 'match_existing' then 'matched'
                    when company_match_rec.decision = 'ambiguous' then 'ambiguous'
                    else 'unresolved'
                end,
                company_match_rec.match_basis,
                case when company_match_rec.decision = 'ambiguous' then company_match_rec.ambiguity_type else null end,
                null,
                v_company_id,
                v_source_hash,
                v_normalized_title,
                v_normalized_company_name,
                v_source_company_linkedin_username,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                company_match_rec.decision = 'ambiguous'
            );
            continue;
        end if;

        v_experience_match_basis := case
            when v_existing_by_ref_id is not null and v_existing_by_hash_id is not null
                then 'source_record_ref_and_source_hash'
            when v_existing_by_ref_id is not null
                then 'source_record_ref'
            when v_existing_by_hash_id is not null
                then 'source_hash'
            else null
        end;

        select *
        into existing_row
        from public.candidate_experiences_v2
        where id = coalesce(v_existing_by_ref_id, v_existing_by_hash_id)
        for update;

        if not found then
            raise exception 'Existing experience lookup failed for candidate_id=% source_path=% array_index=%',
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_array_index;
        end if;

        select id
        into v_conflicting_hash_id
        from public.candidate_experiences_v2
        where candidate_id = rec.resolved_candidate_id
          and source_hash = v_source_hash
          and id <> existing_row.id;

        if v_conflicting_hash_id is not null then
            perform pg_temp.upsert_candidate_experience_ambiguity(
                'source_hash_collision_on_update',
                v_ambiguity_source_ref,
                v_normalized_ambiguity_input,
                array[existing_row.id, v_conflicting_hash_id],
                'manual_review'
            );

            insert into tmp_experience_results (
                source_candidate_id,
                resolved_candidate_id,
                source_path,
                source_family,
                source_array_index,
                experience_index,
                experience_row_id,
                effective_action,
                experience_match_basis,
                company_resolution_status,
                company_match_basis,
                ambiguity_type,
                skip_reason,
                company_id,
                source_hash,
                normalized_title,
                normalized_company_name,
                source_company_linkedin_username,
                start_date,
                start_date_precision,
                end_date,
                end_date_precision,
                is_current,
                ambiguity_logged
            )
            values (
                rec.source_candidate_id,
                rec.resolved_candidate_id,
                rec.source_path,
                rec.source_family,
                rec.source_array_index,
                rec.experience_index,
                existing_row.id,
                'skip',
                v_experience_match_basis,
                case
                    when company_match_rec.decision = 'match_existing' then 'matched'
                    when company_match_rec.decision = 'ambiguous' then 'ambiguous'
                    else 'unresolved'
                end,
                company_match_rec.match_basis,
                'source_hash_collision_on_update',
                'source_hash_collision_on_update',
                v_company_id,
                v_source_hash,
                v_normalized_title,
                v_normalized_company_name,
                v_source_company_linkedin_username,
                v_start_date,
                v_start_precision,
                v_end_date,
                v_end_precision,
                v_is_current,
                true
            );
            continue;
        end if;

        v_display_title := existing_row.title;
        if existing_row.title is null and rec.title is not null then
            v_display_title := rec.title;
        elsif rec.title is not null
              and public.normalize_search_text(existing_row.title) is distinct from public.normalize_search_text(rec.title) then
            v_display_title := rec.title;
        end if;

        v_display_description := existing_row.description;
        if existing_row.description is null and rec.description is not null then
            v_display_description := rec.description;
        elsif rec.description is not null
              and public.normalize_search_text(existing_row.description) is distinct from public.normalize_search_text(rec.description) then
            v_display_description := rec.description;
        end if;

        v_display_location := existing_row.location;
        if existing_row.location is null and rec.location is not null then
            v_display_location := rec.location;
        elsif rec.location is not null
              and public.normalize_search_text(existing_row.location) is distinct from public.normalize_search_text(rec.location) then
            v_display_location := rec.location;
        end if;

        v_display_company_name := existing_row.raw_company_name;
        if existing_row.raw_company_name is null and rec.raw_company_name is not null then
            v_display_company_name := rec.raw_company_name;
        elsif rec.raw_company_name is not null
              and public.normalize_company_name(existing_row.raw_company_name) is distinct from public.normalize_company_name(rec.raw_company_name) then
            v_display_company_name := rec.raw_company_name;
        end if;

        v_display_company_username := existing_row.source_company_linkedin_username;
        if existing_row.source_company_linkedin_username is null and v_source_company_linkedin_username is not null then
            v_display_company_username := v_source_company_linkedin_username;
        elsif v_source_company_linkedin_username is not null
              and public.normalize_linkedin_username(existing_row.source_company_linkedin_username) is distinct from public.normalize_linkedin_username(v_source_company_linkedin_username) then
            v_display_company_username := v_source_company_linkedin_username;
        end if;

        v_needs_update := false;
        if existing_row.experience_index is distinct from rec.experience_index then
            v_needs_update := true;
        end if;
        if existing_row.company_id is distinct from v_company_id then
            v_needs_update := true;
        end if;
        if existing_row.title is distinct from v_display_title then
            v_needs_update := true;
        end if;
        if existing_row.description is distinct from v_display_description then
            v_needs_update := true;
        end if;
        if existing_row.location is distinct from v_display_location then
            v_needs_update := true;
        end if;
        if existing_row.raw_company_name is distinct from v_display_company_name then
            v_needs_update := true;
        end if;
        if existing_row.source_company_linkedin_username is distinct from v_display_company_username then
            v_needs_update := true;
        end if;
        if existing_row.start_date is distinct from v_start_date then
            v_needs_update := true;
        end if;
        if existing_row.start_date_precision is distinct from v_start_precision then
            v_needs_update := true;
        end if;
        if existing_row.end_date is distinct from v_end_date then
            v_needs_update := true;
        end if;
        if existing_row.end_date_precision is distinct from v_end_precision then
            v_needs_update := true;
        end if;
        if existing_row.is_current is distinct from v_is_current then
            v_needs_update := true;
        end if;
        if existing_row.source_hash is distinct from v_source_hash then
            v_needs_update := true;
        end if;

        if v_needs_update then
            update public.candidate_experiences_v2
            set
                company_id = v_company_id,
                experience_index = rec.experience_index,
                title = v_display_title,
                description = v_display_description,
                location = v_display_location,
                raw_company_name = v_display_company_name,
                source_company_linkedin_username = v_display_company_username,
                start_date = v_start_date,
                start_date_precision = v_start_precision,
                end_date = v_end_date,
                end_date_precision = v_end_precision,
                is_current = v_is_current,
                source_payload = v_source_payload,
                source_hash = v_source_hash
            where id = existing_row.id;

            v_result_action := 'match_existing';
        else
            v_result_action := 'no_op';
        end if;

        insert into tmp_experience_results (
            source_candidate_id,
            resolved_candidate_id,
            source_path,
            source_family,
            source_array_index,
            experience_index,
            experience_row_id,
            effective_action,
            experience_match_basis,
            company_resolution_status,
            company_match_basis,
            ambiguity_type,
            skip_reason,
            company_id,
            source_hash,
            normalized_title,
            normalized_company_name,
            source_company_linkedin_username,
            start_date,
            start_date_precision,
            end_date,
            end_date_precision,
            is_current,
            ambiguity_logged
        )
        values (
            rec.source_candidate_id,
            rec.resolved_candidate_id,
            rec.source_path,
            rec.source_family,
            rec.source_array_index,
            rec.experience_index,
            existing_row.id,
            v_result_action,
            v_experience_match_basis,
            case
                when company_match_rec.decision = 'match_existing' then 'matched'
                when company_match_rec.decision = 'ambiguous' then 'ambiguous'
                else 'unresolved'
            end,
            company_match_rec.match_basis,
            case when company_match_rec.decision = 'ambiguous' then company_match_rec.ambiguity_type else null end,
            null,
            v_company_id,
            v_source_hash,
            v_normalized_title,
            v_normalized_company_name,
            v_display_company_username,
            v_start_date,
            v_start_precision,
            v_end_date,
            v_end_precision,
            v_is_current,
            company_match_rec.decision = 'ambiguous'
        );
    end loop;
end
$plpgsql$;

select row_to_json(t)::text
from (
    select
        source_candidate_id::text as source_candidate_id,
        resolved_candidate_id::text as resolved_candidate_id,
        source_path,
        source_family,
        source_array_index,
        experience_index,
        experience_row_id::text as experience_row_id,
        effective_action,
        experience_match_basis,
        company_resolution_status,
        company_match_basis,
        ambiguity_type,
        skip_reason,
        company_id::text as company_id,
        source_hash,
        normalized_title,
        normalized_company_name,
        source_company_linkedin_username,
        start_date::text as start_date,
        start_date_precision,
        end_date::text as end_date,
        end_date_precision,
        is_current,
        ambiguity_logged
    from tmp_experience_results
    order by source_candidate_id::uuid asc, source_array_index asc
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
    action_counts: Counter[str] = Counter()
    company_resolution_counts: Counter[str] = Counter()
    experience_match_basis_counts: Counter[str] = Counter()
    company_match_basis_counts: Counter[str] = Counter()
    ambiguity_type_counts: Counter[str] = Counter()
    source_path_counts: Counter[str] = Counter()
    date_precision_counts: Counter[str] = Counter()
    current_role_counts: Counter[str] = Counter()
    samples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    distinct_candidates = {str(row["source_candidate_id"]) for row in prepared_batch}

    for row in rows_list:
        action = str(row.get("effective_action"))
        action_counts[action] += 1

        company_resolution_status = normalized_string(row.get("company_resolution_status"))
        if company_resolution_status is not None:
            company_resolution_counts[company_resolution_status] += 1

        experience_match_basis = normalized_string(row.get("experience_match_basis"))
        if experience_match_basis is not None:
            experience_match_basis_counts[experience_match_basis] += 1

        company_match_basis = normalized_string(row.get("company_match_basis"))
        if company_match_basis is not None:
            company_match_basis_counts[company_match_basis] += 1

        ambiguity_type = normalized_string(row.get("ambiguity_type"))
        if ambiguity_type is not None:
            ambiguity_type_counts[ambiguity_type] += 1

        source_path = normalized_string(row.get("source_path"))
        if source_path is not None:
            source_path_counts[source_path] += 1

        for precision_key in (
            normalized_string(row.get("start_date_precision")),
            normalized_string(row.get("end_date_precision")),
        ):
            if precision_key is not None:
                date_precision_counts[precision_key] += 1

        current_role_counts["current" if row.get("is_current") else "not_current"] += 1

        if len(samples[action]) < sample_limit:
            samples[action].append(
                {
                    "source_candidate_id": row.get("source_candidate_id"),
                    "resolved_candidate_id": row.get("resolved_candidate_id"),
                    "source_path": row.get("source_path"),
                    "source_array_index": row.get("source_array_index"),
                    "experience_row_id": row.get("experience_row_id"),
                    "company_resolution_status": row.get("company_resolution_status"),
                    "experience_match_basis": row.get("experience_match_basis"),
                    "company_match_basis": row.get("company_match_basis"),
                    "ambiguity_type": row.get("ambiguity_type"),
                    "skip_reason": row.get("skip_reason"),
                    "company_id": row.get("company_id"),
                    "source_hash": row.get("source_hash"),
                    "start_date_precision": row.get("start_date_precision"),
                    "end_date_precision": row.get("end_date_precision"),
                    "is_current": row.get("is_current"),
                }
            )

    work_experience_rows = source_path_counts.get(SOURCE_PATH_WORK_EXPERIENCE, 0)
    linkedin_fallback_rows = source_path_counts.get(SOURCE_PATH_LINKEDIN_EXPERIENCE, 0)

    return {
        "candidates_read": len(distinct_candidates),
        "flattened_source_rows_read": len(prepared_batch),
        "rows_normalized": len(prepared_batch),
        "rows_sourced_from_work_experience": work_experience_rows,
        "rows_sourced_from_linkedin_fallback": linkedin_fallback_rows,
        "rows_inserted": action_counts.get("create_new", 0),
        "rows_matched_updated": action_counts.get("match_existing", 0),
        "rows_no_op": action_counts.get("no_op", 0),
        "rows_skipped": action_counts.get("skip", 0),
        "rows_missing_candidate_links": sum(
            1 for row in rows_list if row.get("skip_reason") == "missing_candidate_link"
        ),
        "rows_with_matched_canonical_companies": company_resolution_counts.get("matched", 0),
        "rows_with_unresolved_company_links": company_resolution_counts.get("unresolved", 0),
        "rows_with_ambiguous_company_resolution": company_resolution_counts.get("ambiguous", 0),
        "rows_matched_by_source_record_ref": experience_match_basis_counts.get("source_record_ref", 0)
        + experience_match_basis_counts.get("source_record_ref_and_source_hash", 0),
        "rows_matched_by_source_hash": experience_match_basis_counts.get("source_hash", 0)
        + experience_match_basis_counts.get("source_record_ref_and_source_hash", 0),
        "source_path_counts": dict(source_path_counts),
        "action_counts": dict(action_counts),
        "company_resolution_counts": dict(company_resolution_counts),
        "experience_match_basis_counts": dict(experience_match_basis_counts),
        "company_match_basis_counts": dict(company_match_basis_counts),
        "ambiguity_type_counts": dict(ambiguity_type_counts),
        "date_precision_counts": dict(date_precision_counts),
        "current_role_counts": dict(current_role_counts),
        "samples": dict(samples),
    }


def merge_summary(base: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    base["candidates_read"] += int(incoming["candidates_read"])
    base["flattened_source_rows_read"] += int(incoming["flattened_source_rows_read"])
    base["rows_normalized"] += int(incoming["rows_normalized"])
    base["rows_sourced_from_work_experience"] += int(
        incoming["rows_sourced_from_work_experience"]
    )
    base["rows_sourced_from_linkedin_fallback"] += int(
        incoming["rows_sourced_from_linkedin_fallback"]
    )
    base["rows_inserted"] += int(incoming["rows_inserted"])
    base["rows_matched_updated"] += int(incoming["rows_matched_updated"])
    base["rows_no_op"] += int(incoming["rows_no_op"])
    base["rows_skipped"] += int(incoming["rows_skipped"])
    base["rows_missing_candidate_links"] += int(incoming["rows_missing_candidate_links"])
    base["rows_with_matched_canonical_companies"] += int(
        incoming["rows_with_matched_canonical_companies"]
    )
    base["rows_with_unresolved_company_links"] += int(
        incoming["rows_with_unresolved_company_links"]
    )
    base["rows_with_ambiguous_company_resolution"] += int(
        incoming["rows_with_ambiguous_company_resolution"]
    )
    base["rows_matched_by_source_record_ref"] += int(
        incoming["rows_matched_by_source_record_ref"]
    )
    base["rows_matched_by_source_hash"] += int(incoming["rows_matched_by_source_hash"])

    for key in (
        "source_path_counts",
        "action_counts",
        "company_resolution_counts",
        "experience_match_basis_counts",
        "company_match_basis_counts",
        "ambiguity_type_counts",
        "date_precision_counts",
        "current_role_counts",
    ):
        counter = Counter(base[key])
        counter.update(incoming[key])
        base[key] = dict(counter)

    for action, action_samples in incoming["samples"].items():
        existing = base["samples"].setdefault(action, [])
        remaining = max(0, base["sample_limit"] - len(existing))
        if remaining:
            existing.extend(action_samples[:remaining])

    return base


def base_summary(*, sample_limit: int) -> dict[str, Any]:
    return {
        "candidates_read": 0,
        "flattened_source_rows_read": 0,
        "rows_normalized": 0,
        "rows_sourced_from_work_experience": 0,
        "rows_sourced_from_linkedin_fallback": 0,
        "rows_inserted": 0,
        "rows_matched_updated": 0,
        "rows_no_op": 0,
        "rows_skipped": 0,
        "rows_missing_candidate_links": 0,
        "rows_with_matched_canonical_companies": 0,
        "rows_with_unresolved_company_links": 0,
        "rows_with_ambiguous_company_resolution": 0,
        "rows_matched_by_source_record_ref": 0,
        "rows_matched_by_source_hash": 0,
        "source_path_counts": {},
        "action_counts": {},
        "company_resolution_counts": {},
        "experience_match_basis_counts": {},
        "company_match_basis_counts": {},
        "ambiguity_type_counts": {},
        "date_precision_counts": {},
        "current_role_counts": {},
        "samples": {},
        "sample_limit": sample_limit,
    }


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
    sharding = report.get("sharding") or {"enabled": False}
    lines = [
        "# Candidate Experiences Backfill QA Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Script: `{report['script_name']}`",
        f"- Mode: `{report['mode']}`",
        f"- Limit: `{report['limit']}`",
        f"- Batch size: `{report['batch_size']}`",
        f"- Source candidate batch size: `{report['source_candidate_batch_size']}`",
        f"- Checkpoint: `{report['checkpoint_name']}`",
        f"- Candidate map path: `{report['candidate_map_path']}`",
        f"- Candidate map SHA256: `{report['candidate_map_sha256']}`",
    ]

    if sharding.get("enabled"):
        lines.extend(
            [
                "",
                "## Sharding",
                "",
                f"- Shard count: `{sharding['shard_count']}`",
                f"- Shard index: `{sharding['shard_index']}`",
                f"- Effective checkpoint: `{sharding['checkpoint_name_effective']}`",
                f"- Base checkpoint: `{sharding['checkpoint_name_base']}`",
                f"- Manifest source candidates: `{sharding['manifest_source_candidate_count']}`",
                f"- Manifest mapped source candidates: `{sharding['manifest_mapped_source_candidate_count']}`",
                f"- Global mapped source candidates: `{sharding['manifest_mapped_source_candidate_total']}`",
                f"- Manifest unmapped source candidates: `{sharding['manifest_unmapped_source_candidate_count']}`",
                f"- Manifest resolved canonical candidates: `{sharding['manifest_resolved_candidate_count']}`",
                f"- Includes unmapped source candidates: `{sharding['includes_unmapped']}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Overall Counts",
            "",
            f"- Candidates read: `{overall['candidates_read']}`",
            f"- Flattened source rows read: `{overall['flattened_source_rows_read']}`",
            f"- Rows normalized: `{overall['rows_normalized']}`",
            f"- Rows sourced from work_experience: `{overall['rows_sourced_from_work_experience']}`",
            f"- Rows sourced from linkedin_data.data.experience: `{overall['rows_sourced_from_linkedin_fallback']}`",
            f"- Rows inserted: `{overall['rows_inserted']}`",
            f"- Rows matched/updated: `{overall['rows_matched_updated']}`",
            f"- Rows no-op on rerun: `{overall['rows_no_op']}`",
            f"- Rows skipped: `{overall['rows_skipped']}`",
            f"- Rows with missing candidate links: `{overall['rows_missing_candidate_links']}`",
            f"- Rows with matched canonical companies: `{overall['rows_with_matched_canonical_companies']}`",
            f"- Rows with unresolved company links: `{overall['rows_with_unresolved_company_links']}`",
            f"- Rows with ambiguous company resolution: `{overall['rows_with_ambiguous_company_resolution']}`",
            f"- Rows matched by source record reference: `{overall['rows_matched_by_source_record_ref']}`",
            f"- Rows matched by source hash: `{overall['rows_matched_by_source_hash']}`",
            f"- Source path counts: `{json.dumps(overall['source_path_counts'], sort_keys=True)}`",
            f"- Action counts: `{json.dumps(overall['action_counts'], sort_keys=True)}`",
            f"- Company resolution counts: `{json.dumps(overall['company_resolution_counts'], sort_keys=True)}`",
            f"- Experience match basis counts: `{json.dumps(overall['experience_match_basis_counts'], sort_keys=True)}`",
            f"- Company match basis counts: `{json.dumps(overall['company_match_basis_counts'], sort_keys=True)}`",
            f"- Ambiguity type counts: `{json.dumps(overall['ambiguity_type_counts'], sort_keys=True)}`",
            f"- Date precision counts: `{json.dumps(overall['date_precision_counts'], sort_keys=True)}`",
            f"- Current role counts: `{json.dumps(overall['current_role_counts'], sort_keys=True)}`",
        ]
    )

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
    company_rows = [
        {
            "id": fixture_ids["company_acme"],
            "name": "Acme Incorporated",
            "linkedin_id": "fixture-acme-001",
            "linkedin_username": "fixture-acme",
            "linkedin_url": "https://www.linkedin.com/company/fixture-acme",
        },
        {
            "id": fixture_ids["company_dupco_a"],
            "name": "DupCo LLC",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": None,
        },
        {
            "id": fixture_ids["company_dupco_b"],
            "name": "DupCo Inc.",
            "linkedin_id": None,
            "linkedin_username": None,
            "linkedin_url": None,
        },
    ]

    candidate_rows = [
        fixture_ids["candidate_existing"],
        fixture_ids["candidate_punctuation"],
        fixture_ids["candidate_reorder"],
        fixture_ids["candidate_ambiguous_company"],
    ]

    company_values: list[str] = []
    for row in company_rows:
        company_values.append(
            "("
            + ", ".join(
                [
                    f"{sql_text_literal(row['id'])}::uuid",
                    sql_text_literal(row["name"]),
                    f"public.normalize_company_name({sql_text_literal(row['name'])})",
                    sql_text_literal(row["linkedin_id"]),
                    sql_text_literal(row["linkedin_username"]),
                    sql_text_literal(row["linkedin_url"]),
                    (
                        f"public.normalize_company_linkedin_url({sql_text_literal(row['linkedin_url'])})"
                        if row["linkedin_url"] is not None
                        else "null"
                    ),
                    sql_text_literal("fixture"),
                    sql_text_literal(
                        "linkedin_id"
                        if row["linkedin_id"]
                        else "linkedin_username"
                        if row["linkedin_username"]
                        else "name"
                    ),
                    "'[]'::jsonb",
                ]
            )
            + ")"
        )

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
        for index, candidate_id in enumerate(candidate_rows, start=1)
    )

    existing_experience_values = [
        {
            "candidate_id": fixture_ids["candidate_existing"],
            "company_id": fixture_ids["company_acme"],
            "experience_index": 0,
            "title": "Senior Engineer",
            "description": "Builds platform services",
            "location": "New York",
            "raw_company_name": "Acme Incorporated",
            "source_company_linkedin_username": "fixture-acme",
            "start_date": "2020-01-01",
            "start_date_precision": "month",
            "end_date": "2021-01-01",
            "end_date_precision": "month",
            "is_current": False,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_array_index": 0,
        },
        {
            "candidate_id": fixture_ids["candidate_existing"],
            "company_id": fixture_ids["company_acme"],
            "experience_index": 1,
            "title": "Engineering Manager",
            "description": "Original description",
            "location": "Boston",
            "raw_company_name": "Acme Incorporated",
            "source_company_linkedin_username": "fixture-acme",
            "start_date": "2021-01-01",
            "start_date_precision": "year",
            "end_date": None,
            "end_date_precision": "present",
            "is_current": True,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_array_index": 1,
        },
        {
            "candidate_id": fixture_ids["candidate_punctuation"],
            "company_id": fixture_ids["company_acme"],
            "experience_index": 0,
            "title": "Engineer - Platform",
            "description": None,
            "location": None,
            "raw_company_name": "Acme Incorporated",
            "source_company_linkedin_username": "fixture-acme",
            "start_date": "2019-01-01",
            "start_date_precision": "year",
            "end_date": "2020-01-01",
            "end_date_precision": "year",
            "is_current": False,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_array_index": 0,
        },
        {
            "candidate_id": fixture_ids["candidate_reorder"],
            "company_id": fixture_ids["company_acme"],
            "experience_index": 5,
            "title": "Principal Engineer",
            "description": "Reorder fixture",
            "location": None,
            "raw_company_name": "Acme Incorporated",
            "source_company_linkedin_username": "fixture-acme",
            "start_date": "2018-01-01",
            "start_date_precision": "year",
            "end_date": "2019-01-01",
            "end_date_precision": "year",
            "is_current": False,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_array_index": 5,
        },
    ]

    experience_values_sql: list[str] = []
    for row in existing_experience_values:
        source_payload_literal = sql_jsonb_literal(
            {
                "source_table": "candidates",
                "legacy_candidate_id": row["candidate_id"],
                "source_path": row["source_path"],
                "source_array_index": row["source_array_index"],
                "source_family": "fixture",
                "experience_identity": {
                    "title": row["title"].lower(),
                    "raw_company_name": "acme",
                    "start_date": row["start_date"],
                    "start_date_precision": row["start_date_precision"],
                    "end_date": row["end_date"],
                    "end_date_precision": row["end_date_precision"],
                    "is_current": row["is_current"],
                },
                "raw_experience_fragment": {"fixture": True},
                "raw_company_identity_inputs": {"company_name": row["raw_company_name"]},
                "raw_date_payload": {
                    "start": row["start_date"],
                    "end": row["end_date"],
                    "is_current": row["is_current"],
                },
            }
        )
        source_hash_expr = (
            "public.build_candidate_experience_source_hash("
            f"{sql_text_literal(row['candidate_id'])}::uuid,"
            f"{sql_text_literal(row['title'])},"
            f"{sql_text_literal(row['company_id'])}::uuid,"
            f"{sql_text_literal(row['raw_company_name'])},"
            f"{sql_text_literal(row['start_date'])}::date,"
            f"{sql_text_literal(row['start_date_precision'])},"
            + (
                f"{sql_text_literal(row['end_date'])}::date,"
                if row["end_date"] is not None
                else "null::date,"
            )
            + f"{sql_text_literal(row['end_date_precision'])},"
            + ("true" if row["is_current"] else "false")
            + ")"
        )
        experience_values_sql.append(
            "("
            + ", ".join(
                [
                    f"{sql_text_literal(row['candidate_id'])}::uuid",
                    f"{sql_text_literal(row['company_id'])}::uuid",
                    str(row["experience_index"]),
                    sql_text_literal(row["title"]),
                    sql_text_literal(row["description"]),
                    sql_text_literal(row["location"]),
                    sql_text_literal(row["raw_company_name"]),
                    sql_text_literal(row["source_company_linkedin_username"]),
                    f"{sql_text_literal(row['start_date'])}::date",
                    sql_text_literal(row["start_date_precision"]),
                    (
                        f"{sql_text_literal(row['end_date'])}::date"
                        if row["end_date"] is not None
                        else "null::date"
                    ),
                    sql_text_literal(row["end_date_precision"]),
                    "true" if row["is_current"] else "false",
                    source_payload_literal,
                    source_hash_expr,
                ]
            )
            + ")"
        )

    return f"""
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
{",\n".join(company_values)};

insert into public.candidate_profiles_v2 (
    id,
    full_name,
    source
)
values
{candidate_values};

insert into public.candidate_experiences_v2 (
    candidate_id,
    company_id,
    experience_index,
    title,
    description,
    location,
    raw_company_name,
    source_company_linkedin_username,
    start_date,
    start_date_precision,
    end_date,
    end_date_precision,
    is_current,
    source_payload,
    source_hash
)
values
{",\n".join(experience_values_sql)};
"""


def build_duplicate_fixture_records() -> tuple[
    dict[str, str],
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    fixture_ids = {
        name: str(uuid4())
        for name in (
            "candidate_existing",
            "candidate_punctuation",
            "candidate_reorder",
            "candidate_ambiguous_company",
            "company_acme",
            "company_dupco_a",
            "company_dupco_b",
            "missing_candidate_source",
        )
    }

    incoming_records = [
        {
            "source_candidate_id": fixture_ids["candidate_existing"],
            "resolved_candidate_id": fixture_ids["candidate_existing"],
            "candidate_match_action": "match_existing",
            "candidate_match_basis": "legacy_id",
            "candidate_skip_reason": None,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 0,
            "experience_index": 0,
            "title": "  senior engineer  ",
            "description": "Builds platform services",
            "location": "New York",
            "raw_company_name": " ACME incorporated ",
            "incoming_linkedin_id": "fixture-acme-001",
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2020-01-01",
            "start_date_precision_raw": "month",
            "end_date_raw": "2021-01-01",
            "end_date_precision_raw": "month",
            "raw_is_current": False,
            "raw_item": {"fixture_case": "noise_duplicate"},
            "raw_company_identity_inputs": {"linkedin_id": "fixture-acme-001", "company_name": " ACME incorporated "},
            "raw_date_payload": {"start": "2020-01", "end": "2021-01", "is_current": False},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
        {
            "source_candidate_id": fixture_ids["candidate_existing"],
            "resolved_candidate_id": fixture_ids["candidate_existing"],
            "candidate_match_action": "match_existing",
            "candidate_match_basis": "legacy_id",
            "candidate_skip_reason": None,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 1,
            "experience_index": 1,
            "title": "Engineering Manager",
            "description": "Updated description only",
            "location": "Boston",
            "raw_company_name": "Acme Incorporated",
            "incoming_linkedin_id": "fixture-acme-001",
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2021-01-01",
            "start_date_precision_raw": "year",
            "end_date_raw": None,
            "end_date_precision_raw": "present",
            "raw_is_current": True,
            "raw_item": {"fixture_case": "description_only_change"},
            "raw_company_identity_inputs": {"linkedin_id": "fixture-acme-001", "company_name": "Acme Incorporated"},
            "raw_date_payload": {"start": "2021", "end": "Present", "is_current": True},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
        {
            "source_candidate_id": fixture_ids["candidate_punctuation"],
            "resolved_candidate_id": fixture_ids["candidate_punctuation"],
            "candidate_match_action": "match_existing",
            "candidate_match_basis": "legacy_id",
            "candidate_skip_reason": None,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 1,
            "experience_index": 1,
            "title": "Engineer / Platform",
            "description": None,
            "location": None,
            "raw_company_name": "Acme Incorporated",
            "incoming_linkedin_id": "fixture-acme-001",
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2019-01-01",
            "start_date_precision_raw": "year",
            "end_date_raw": "2020-01-01",
            "end_date_precision_raw": "year",
            "raw_is_current": False,
            "raw_item": {"fixture_case": "punctuation_distinct"},
            "raw_company_identity_inputs": {"linkedin_id": "fixture-acme-001", "company_name": "Acme Incorporated"},
            "raw_date_payload": {"start": "2019", "end": "2020", "is_current": False},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
        {
            "source_candidate_id": fixture_ids["candidate_reorder"],
            "resolved_candidate_id": fixture_ids["candidate_reorder"],
            "candidate_match_action": "match_existing",
            "candidate_match_basis": "legacy_id",
            "candidate_skip_reason": None,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 1,
            "experience_index": 1,
            "title": "Principal Engineer",
            "description": "Reorder fixture",
            "location": None,
            "raw_company_name": "Acme Incorporated",
            "incoming_linkedin_id": "fixture-acme-001",
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2018-01-01",
            "start_date_precision_raw": "year",
            "end_date_raw": "2019-01-01",
            "end_date_precision_raw": "year",
            "raw_is_current": False,
            "raw_item": {"fixture_case": "reorder_only"},
            "raw_company_identity_inputs": {"linkedin_id": "fixture-acme-001", "company_name": "Acme Incorporated"},
            "raw_date_payload": {"start": "2018", "end": "2019", "is_current": False},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
        {
            "source_candidate_id": fixture_ids["missing_candidate_source"],
            "resolved_candidate_id": None,
            "candidate_match_action": "skip",
            "candidate_match_basis": "ambiguous",
            "candidate_skip_reason": "missing_candidate_link",
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 0,
            "experience_index": 0,
            "title": "Orphan Experience",
            "description": None,
            "location": None,
            "raw_company_name": "Acme Incorporated",
            "incoming_linkedin_id": "fixture-acme-001",
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2022-01-01",
            "start_date_precision_raw": "year",
            "end_date_raw": None,
            "end_date_precision_raw": "present",
            "raw_is_current": True,
            "raw_item": {"fixture_case": "missing_candidate_link"},
            "raw_company_identity_inputs": {"linkedin_id": "fixture-acme-001", "company_name": "Acme Incorporated"},
            "raw_date_payload": {"start": "2022", "end": "Present", "is_current": True},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
        {
            "source_candidate_id": fixture_ids["candidate_ambiguous_company"],
            "resolved_candidate_id": fixture_ids["candidate_ambiguous_company"],
            "candidate_match_action": "match_existing",
            "candidate_match_basis": "legacy_id",
            "candidate_skip_reason": None,
            "source_path": SOURCE_PATH_WORK_EXPERIENCE,
            "source_family": "fixture",
            "source_array_index": 0,
            "experience_index": 0,
            "title": "Product Manager",
            "description": None,
            "location": None,
            "raw_company_name": "DupCo LLC",
            "incoming_linkedin_id": None,
            "incoming_linkedin_username": None,
            "incoming_linkedin_url": None,
            "start_date_raw": "2022-01-01",
            "start_date_precision_raw": "year",
            "end_date_raw": None,
            "end_date_precision_raw": "present",
            "raw_is_current": True,
            "raw_item": {"fixture_case": "ambiguous_company_resolution"},
            "raw_company_identity_inputs": {"company_name": "DupCo LLC"},
            "raw_date_payload": {"start": "2022", "end": "Present", "is_current": True},
            "candidate_created_at": None,
            "candidate_updated_at": None,
            "structurally_empty": False,
        },
    ]

    expectations = {
        fixture_ids["candidate_existing"] + ":0": {
            "effective_action": "no_op",
            "experience_match_basis": "source_record_ref_and_source_hash",
            "company_resolution_status": "matched",
            "company_match_basis": "linkedin_id",
        },
        fixture_ids["candidate_existing"] + ":1": {
            "effective_action": "match_existing",
            "experience_match_basis": "source_record_ref_and_source_hash",
            "company_resolution_status": "matched",
            "company_match_basis": "linkedin_id",
        },
        fixture_ids["candidate_punctuation"] + ":1": {
            "effective_action": "create_new",
            "company_resolution_status": "matched",
            "company_match_basis": "linkedin_id",
        },
        fixture_ids["candidate_reorder"] + ":1": {
            "effective_action": "match_existing",
            "experience_match_basis": "source_hash",
            "company_resolution_status": "matched",
            "company_match_basis": "linkedin_id",
            "experience_index": 1,
        },
        fixture_ids["missing_candidate_source"] + ":0": {
            "effective_action": "skip",
            "skip_reason": "missing_candidate_link",
        },
        fixture_ids["candidate_ambiguous_company"] + ":0": {
            "effective_action": "create_new",
            "company_resolution_status": "ambiguous",
            "ambiguity_type": "multiple_normalized_name_matches",
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

    by_case_key = {
        f"{row['source_candidate_id']}:{row['source_array_index']}": row for row in rows
    }
    case_results: list[dict[str, Any]] = []
    passed_count = 0

    for case_key, expectation in expectations.items():
        actual = by_case_key.get(case_key, {})
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
                "case_key": case_key,
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
    candidate_map: Mapping[str, Mapping[str, Any]],
    candidate_map_sha256: str,
    shard_manifest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    dry_run = args.mode == "dry-run"
    limit = effective_limit(args)
    checkpoint_metadata = {
        "script_name": Path(__file__).name,
        "mode": args.mode,
        "candidate_map_sha256": candidate_map_sha256,
        "shard_count": args.shard_count,
        "shard_index": args.shard_index,
        "sharded_run": shard_manifest is not None,
    }
    if shard_manifest is not None:
        checkpoint_metadata.update(
            {
                "shard_source_candidate_count": len(
                    shard_manifest["ordered_source_candidate_ids"]
                ),
                "shard_resolved_candidate_count": shard_manifest[
                    "resolved_candidate_count"
                ],
                "shard_includes_unmapped": shard_manifest["includes_unmapped"],
            }
        )

    validate_checkpoint_resume_metadata(
        checkpoint,
        expected_metadata=checkpoint_metadata,
        force_rerun=args.force_rerun,
        strict_keys=set(checkpoint_metadata) if shard_manifest is not None else None,
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
    seen_candidate_ids: set[str] = set()
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

        raw_batch = fetch_source_batch(
            source_db,
            cursor=cursor,
            batch_size=current_batch_size,
            available_columns=available_candidate_columns,
            source_candidate_batch_size=args.source_candidate_batch_size,
            shard_manifest=shard_manifest,
        )
        if not raw_batch:
            source_exhausted = True
            break

        prepared_batch = prepare_source_batch(raw_batch, candidate_map=candidate_map)
        batch_number += 1
        seen_candidate_ids.update(
            str(row["source_candidate_id"]) for row in prepared_batch
        )
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
        summary["candidates_read"] = len(seen_candidate_ids)

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
                last_seen_key=f"{prepared_batch[-1]['source_candidate_id']}:{prepared_batch[-1]['source_array_index']}",
                rows_seen=len(prepared_batch),
                rows_processed=len(prepared_batch),
                rows_written=rows_written,
                rows_skipped=batch_summary["action_counts"].get("skip", 0)
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
    shard_manifest = build_shard_manifest(
        candidate_map,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
    )

    if args.linked_workdir:
        linked_workdir = Path(args.linked_workdir).resolve()
        source_env_used = "SUPABASE_LINKED_WORKDIR"
        target_env_used = "SUPABASE_LINKED_WORKDIR"
        source_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="candidate-experiences-backfill-source",
        )
        target_db = SupabaseLinkedClient(
            linked_workdir,
            app_name="candidate-experiences-backfill-target",
        )
    else:
        source_dsn, source_env_used = load_dsn(
            args.source_dsn_env,
            fallback_env=args.target_dsn_env,
        )
        target_dsn, target_env_used = load_dsn(args.target_dsn_env)
        source_db = PsqlClient(source_dsn, app_name="candidate-experiences-backfill-source")
        target_db = PsqlClient(target_dsn, app_name="candidate-experiences-backfill-target")

    checkpoint_name = effective_checkpoint_name(
        args.checkpoint_name,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
    )
    checkpoint = JsonCheckpoint(checkpoint_name)
    report_dir = Path(args.report_dir).resolve()

    try:
        main_pass = run_main_pass(
            args,
            source_db=source_db,
            target_db=target_db,
            checkpoint=checkpoint,
            candidate_map=candidate_map,
            candidate_map_sha256=candidate_map_sha256,
            shard_manifest=shard_manifest,
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
        LOGGER.exception("candidate experiences backfill failed: %s", exc)
        return 1

    report = {
        "generated_at": utc_now(),
        "script_name": Path(__file__).name,
        "task_scope": "8a" if args.mode == "dry-run" else "8b_or_8c_apply",
        "mode": args.mode,
        "limit": main_pass["limit"],
        "batch_size": args.batch_size,
        "source_candidate_batch_size": args.source_candidate_batch_size,
        "checkpoint_name": checkpoint.path.stem,
        "checkpoint_name_base": args.checkpoint_name,
        "candidate_map_path": str(candidate_map_path),
        "candidate_map_sha256": candidate_map_sha256,
        "candidate_map_entries": len(candidate_map),
        "source_dsn_env_used": source_env_used,
        "target_dsn_env_used": target_env_used,
        "legacy_source_table": LEGACY_CANDIDATES_TABLE,
        "sharding": {
            "enabled": shard_manifest is not None,
            "shard_count": args.shard_count,
            "shard_index": args.shard_index,
            "checkpoint_name_base": args.checkpoint_name,
            "checkpoint_name_effective": checkpoint.path.stem,
            "manifest_source_candidate_count": (
                len(shard_manifest["ordered_source_candidate_ids"])
                if shard_manifest is not None
                else None
            ),
            "manifest_resolved_candidate_count": (
                shard_manifest["resolved_candidate_count"]
                if shard_manifest is not None
                else None
            ),
            "manifest_mapped_source_candidate_count": (
                shard_manifest["mapped_source_candidate_count"]
                if shard_manifest is not None
                else None
            ),
            "manifest_mapped_source_candidate_total": (
                shard_manifest["mapped_source_candidate_total"]
                if shard_manifest is not None
                else None
            ),
            "manifest_unmapped_source_candidate_count": (
                shard_manifest["unmapped_source_candidate_count"]
                if shard_manifest is not None
                else None
            ),
            "includes_unmapped": (
                shard_manifest["includes_unmapped"]
                if shard_manifest is not None
                else False
            ),
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
                    if key != "sample_limit"
                }
                for batch_summary in main_pass["batch_summaries"]
            ],
        },
        "duplicate_validation": duplicate_validation,
    }

    report_scope = (
        "candidate_experiences_preflight"
        if args.mode == "dry-run"
        else "candidate_experiences_backfill_apply"
    )
    if shard_manifest is not None:
        report_scope = f"{report_scope}{shard_suffix(args.shard_count, args.shard_index)}"
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
