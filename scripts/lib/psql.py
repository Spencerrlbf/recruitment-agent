"""Minimal psql-backed helpers for checkpoint-aware migration scripts."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


class PsqlError(RuntimeError):
    """Raised when a psql invocation fails or returns unexpected output."""


def load_dsn(
    primary_env: str,
    *,
    fallback_env: str | None = None,
) -> tuple[str, str]:
    """Return a DSN from env vars, preferring `primary_env`."""

    primary_value = os.getenv(primary_env)
    if primary_value:
        return primary_value, primary_env

    if fallback_env:
        fallback_value = os.getenv(fallback_env)
        if fallback_value:
            return fallback_value, fallback_env

    if fallback_env:
        raise RuntimeError(
            f"Missing database connection string. Set `{primary_env}` or `{fallback_env}`."
        )

    raise RuntimeError(f"Missing database connection string. Set `{primary_env}`.")


@dataclass(frozen=True)
class PsqlClient:
    """Thin wrapper around `psql` for scripts that avoid Python DB dependencies."""

    dsn: str
    app_name: str

    def run_sql(self, sql: str) -> str:
        """Execute SQL and return stdout."""

        env = os.environ.copy()
        env["PGAPPNAME"] = self.app_name

        command = [
            "psql",
            self.dsn,
            "-X",
            "--no-psqlrc",
            "--set",
            "ON_ERROR_STOP=1",
            "-P",
            "pager=off",
            "-q",
            "-t",
            "-A",
            "-f",
            "-",
        ]

        completed = subprocess.run(
            command,
            input=sql,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        if completed.returncode != 0:
            raise PsqlError(
                "psql command failed.\n"
                f"app_name={self.app_name}\n"
                f"stderr={completed.stderr.strip()}\n"
                f"stdout={completed.stdout.strip()}"
            )

        return completed.stdout

    def query_json_rows(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL that emits one JSON object per line."""

        output = self.run_sql(sql)
        rows: list[dict[str, Any]] = []

        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            parsed = json.loads(line)
            if not isinstance(parsed, dict):
                raise PsqlError(
                    "Expected one JSON object per output line from psql, "
                    f"received: {type(parsed).__name__}"
                )
            rows.append(parsed)

        return rows

    def query_json_row(self, sql: str) -> dict[str, Any] | None:
        """Execute SQL expected to emit zero or one JSON row."""

        rows = self.query_json_rows(sql)
        if not rows:
            return None
        if len(rows) != 1:
            raise PsqlError(f"Expected exactly one JSON row, received {len(rows)}.")
        return rows[0]


@dataclass(frozen=True)
class SupabaseLinkedClient:
    """Query a linked Supabase project via the Supabase CLI."""

    workdir: Path
    app_name: str

    def _is_retryable_failure(self, completed: subprocess.CompletedProcess[str]) -> bool:
        combined_output = f"{completed.stderr}\n{completed.stdout}"
        retry_markers = (
            "unexpected status 429",
            "unexpected status 500",
            "unexpected status 502",
            "unexpected status 503",
            "unexpected status 504",
            "Too Many Requests",
            "ThrottlerException",
            "Failed to perform authorization check",
        )
        return any(marker in combined_output for marker in retry_markers)

    def _run_cli(self, sql: str) -> str:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".sql",
            delete=False,
        ) as handle:
            handle.write(sql)
            handle.flush()
            temp_path = Path(handle.name)

        try:
            command = [
                "supabase",
                "db",
                "query",
                "--linked",
                "--output",
                "json",
                "--workdir",
                str(self.workdir),
                "--file",
                str(temp_path),
            ]
            max_attempts = max(1, int(os.getenv("SUPABASE_LINKED_MAX_ATTEMPTS", "6")))
            retry_base_seconds = max(
                0.25, float(os.getenv("SUPABASE_LINKED_RETRY_BASE_SECONDS", "2"))
            )
            retry_max_seconds = max(
                retry_base_seconds,
                float(os.getenv("SUPABASE_LINKED_RETRY_MAX_SECONDS", "30")),
            )

            completed: subprocess.CompletedProcess[str] | None = None
            for attempt in range(1, max_attempts + 1):
                completed = subprocess.run(
                    command,
                    text=True,
                    capture_output=True,
                    env=os.environ.copy(),
                    check=False,
                )
                if completed.returncode == 0:
                    return completed.stdout

                if attempt >= max_attempts or not self._is_retryable_failure(completed):
                    break

                sleep_seconds = min(
                    retry_max_seconds,
                    retry_base_seconds * (2 ** (attempt - 1)),
                )
                LOGGER.warning(
                    "supabase linked query throttled; retrying app_name=%s attempt=%s/%s sleep_seconds=%.1f",
                    self.app_name,
                    attempt,
                    max_attempts,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

        assert completed is not None
        if completed.returncode != 0:
            raise PsqlError(
                "supabase db query --linked failed.\n"
                f"app_name={self.app_name}\n"
                f"stderr={completed.stderr.strip()}\n"
                f"stdout={completed.stdout.strip()}"
            )

        return completed.stdout

    def query_json_rows(self, sql: str) -> list[dict[str, Any]]:
        output = self._run_cli(sql)
        decoder = json.JSONDecoder()
        payload: dict[str, Any] | None = None

        for index, char in enumerate(output):
            if char != "{":
                continue
            try:
                candidate, _ = decoder.raw_decode(output[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and "rows" in candidate:
                payload = candidate
                break

        if payload is None:
            raise PsqlError(
                "Could not parse JSON response from supabase db query --linked.\n"
                f"output={output.strip()}"
            )

        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            raise PsqlError("Expected `rows` to be a list in Supabase CLI JSON output.")

        parsed_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise PsqlError(
                    "Expected each row from Supabase CLI JSON output to be an object."
                )

            if len(row) == 1:
                only_value = next(iter(row.values()))
                if isinstance(only_value, dict):
                    parsed_rows.append(only_value)
                    continue
                if isinstance(only_value, str):
                    try:
                        decoded_value = json.loads(only_value)
                    except json.JSONDecodeError:
                        decoded_value = None
                    if isinstance(decoded_value, dict):
                        parsed_rows.append(decoded_value)
                        continue

            parsed_rows.append(row)

        return parsed_rows

    def query_json_row(self, sql: str) -> dict[str, Any] | None:
        rows = self.query_json_rows(sql)
        if not rows:
            return None
        if len(rows) != 1:
            raise PsqlError(f"Expected exactly one JSON row, received {len(rows)}.")
        return rows[0]
