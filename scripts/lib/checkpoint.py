"""File-based JSON checkpoints for resumable migration and backfill scripts."""

from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

DEFAULT_CHECKPOINT_DIR = Path(__file__).resolve().parents[1] / "checkpoints"


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp suitable for checkpoint metadata."""

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def deep_merge(base: dict[str, Any], updates: Mapping[str, Any]) -> dict[str, Any]:
    """Merge nested mappings without discarding existing checkpoint fields."""

    merged = deepcopy(base)
    for key, value in updates.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def normalize_checkpoint_name(name: str) -> str:
    """Ensure checkpoint names stay within the checkpoint directory."""

    cleaned = name.strip()
    if not cleaned:
        raise ValueError("Checkpoint name must be non-empty.")

    separators = {os.sep}
    if os.altsep:
        separators.add(os.altsep)

    if any(separator in cleaned for separator in separators):
        raise ValueError("Checkpoint name must not contain path separators.")

    if cleaned.endswith(".json"):
        return cleaned
    return f"{cleaned}.json"


class JsonCheckpoint:
    """JSON checkpoint helper with atomic writes and resumable batch progress."""

    def __init__(
        self,
        name: str,
        checkpoint_dir: Path | None = None,
    ) -> None:
        self.directory = (checkpoint_dir or DEFAULT_CHECKPOINT_DIR).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / normalize_checkpoint_name(name)

    def default_state(self) -> dict[str, Any]:
        """Return the baseline checkpoint structure used for new jobs."""

        return {
            "checkpoint_name": self.path.stem,
            "schema_version": 1,
            "status": "pending",
            "dry_run": False,
            "run_attempt": 0,
            "started_at": None,
            "updated_at": None,
            "completed_at": None,
            "progress": {
                "batch_number": 0,
                "batch_size": None,
                "cursor": None,
                "last_seen_key": None,
                "rows_seen": 0,
                "rows_processed": 0,
                "rows_written": 0,
                "rows_skipped": 0,
                "has_more": True,
            },
            "metadata": {},
            "summary": {},
        }

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> dict[str, Any]:
        """Load checkpoint state or return a default state when absent."""

        state = self.default_state()
        if not self.path.exists():
            return state

        with self.path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            raise ValueError(f"Checkpoint {self.path} must contain a JSON object.")

        return deep_merge(state, data)

    def save(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """Persist state with an atomic replace so partial writes are avoided."""

        next_state = deep_merge(self.default_state(), state)
        next_state["updated_at"] = utc_now()

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=self.directory,
            prefix=f"{self.path.stem}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(next_state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
            temp_name = handle.name

        os.replace(temp_name, self.path)
        return next_state

    def update(self, updates: Mapping[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        """Load, merge, and atomically save updated checkpoint state."""

        merged_updates = dict(updates or {})
        if kwargs:
            merged_updates.update(kwargs)

        return self.save(deep_merge(self.load(), merged_updates))

    def begin(
        self,
        *,
        batch_size: int | None = None,
        dry_run: bool = False,
        force_rerun: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Start or resume a run.

        Safe rerun behavior:
        - if a checkpoint is already completed, the helper returns `should_run=False`
          unless `force_rerun=True`
        - when forced, progress fields are reset while preserving a small audit trail
        """

        state = self.load()

        if state["status"] == "completed" and not force_rerun:
            return state, False

        if force_rerun and (
            state["status"] != "pending"
            or state["progress"]["cursor"] is not None
            or int(state.get("run_attempt", 0)) > 0
        ):
            previous_completed_at = state.get("completed_at")
            previous_attempts = int(state.get("run_attempt", 0))
            state = self.default_state()
            state["metadata"] = {
                "forced_rerun_from_completed_at": previous_completed_at,
            }
            state["run_attempt"] = previous_attempts

        state["status"] = "running"
        state["dry_run"] = dry_run
        state["run_attempt"] = int(state["run_attempt"]) + 1
        state["completed_at"] = None
        state["started_at"] = state["started_at"] or utc_now()

        if batch_size is not None:
            state["progress"]["batch_size"] = batch_size

        if metadata:
            state["metadata"] = deep_merge(state["metadata"], metadata)

        if dry_run:
            state["updated_at"] = utc_now()
            return state, True

        return self.save(state), True

    def record_batch(
        self,
        *,
        batch_number: int,
        batch_size: int | None,
        cursor: Any,
        last_seen_key: Any,
        rows_seen: int,
        rows_processed: int,
        rows_written: int,
        rows_skipped: int,
        has_more: bool,
    ) -> dict[str, Any]:
        """Record cumulative progress after a durable batch write."""

        state = self.load()
        progress = state["progress"]

        progress["batch_number"] = batch_number
        progress["batch_size"] = batch_size
        progress["cursor"] = cursor
        progress["last_seen_key"] = last_seen_key
        progress["rows_seen"] = int(progress["rows_seen"]) + rows_seen
        progress["rows_processed"] = int(progress["rows_processed"]) + rows_processed
        progress["rows_written"] = int(progress["rows_written"]) + rows_written
        progress["rows_skipped"] = int(progress["rows_skipped"]) + rows_skipped
        progress["has_more"] = has_more

        state["status"] = "running"
        return self.save(state)

    def mark_completed(self, *, summary: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Mark a checkpoint complete so future runs are safe by default."""

        state = self.load()
        state["status"] = "completed"
        state["completed_at"] = utc_now()
        state["progress"]["has_more"] = False

        if summary:
            state["summary"] = deep_merge(state["summary"], summary)

        return self.save(state)
