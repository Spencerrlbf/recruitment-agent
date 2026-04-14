#!/usr/bin/env python3
"""Reusable scaffold for checkpoint-aware backfill scripts.

This template is intentionally safe to copy before domain logic exists:
- the source batch loader returns no rows by default
- dry-run mode does not mutate checkpoint files
- completed checkpoints short-circuit reruns unless force-rerun is requested
- the write path is structured around idempotent operations
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.lib.checkpoint import JsonCheckpoint

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Checkpoint-aware backfill scaffold.")
    parser.add_argument(
        "--checkpoint-name",
        default="backfill_template",
        help="Checkpoint file stem stored under scripts/checkpoints/.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Deterministic batch size for each source fetch.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview work without mutating checkpoints or destination data.",
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Reset a completed checkpoint and start from the beginning.",
    )
    return parser.parse_args()


def fetch_source_batch(*, cursor: Any, batch_size: int) -> list[dict[str, Any]]:
    """Return the next deterministic batch from the source system.

    Replace this stub with a stable, ordered query such as:
    - `WHERE id > :cursor ORDER BY id ASC LIMIT :batch_size`
    - or another monotonic cursor that supports resume without gaps
    """

    _ = (cursor, batch_size)
    return []


def build_operations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate source rows into idempotent write operations."""

    return [{"record_id": record.get("id"), "action": "upsert"} for record in records]


def apply_operations(
    operations: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Apply idempotent writes.

    Replace this stub with UPSERTs, MERGEs, or other deterministic write logic.
    """

    if dry_run:
        LOGGER.info("dry-run: would apply %s operations", len(operations))
        return {"rows_written": 0, "rows_skipped": len(operations)}

    return {"rows_written": len(operations), "rows_skipped": 0}


def next_cursor(records: list[dict[str, Any]], current_cursor: Any) -> Any:
    """Advance the resume cursor from the last row in a deterministic batch."""

    if not records:
        return current_cursor
    return records[-1].get("id")


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    checkpoint = JsonCheckpoint(args.checkpoint_name)
    state, should_run = checkpoint.begin(
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        force_rerun=args.force_rerun,
        metadata={"script_name": Path(__file__).name},
    )

    if not should_run:
        LOGGER.info(
            "checkpoint %s already completed at %s; use --force-rerun to start over",
            checkpoint.path.name,
            state.get("completed_at"),
        )
        return 0

    if args.dry_run:
        LOGGER.info("dry-run enabled: checkpoint state will not be written")

    batch_number = int(state["progress"]["batch_number"])
    cursor = state["progress"]["cursor"]

    while True:
        records = fetch_source_batch(cursor=cursor, batch_size=args.batch_size)
        if not records:
            if args.dry_run:
                LOGGER.info("dry-run complete: no source rows returned")
            else:
                checkpoint.mark_completed(
                    summary={"message": "completed without additional source rows"}
                )
                LOGGER.info("checkpoint marked complete")
            return 0

        batch_number += 1
        operations = build_operations(records)
        summary = apply_operations(operations, dry_run=args.dry_run)
        cursor = next_cursor(records, cursor)

        if args.dry_run:
            LOGGER.info(
                "dry-run batch=%s cursor=%s summary=%s",
                batch_number,
                json.dumps(cursor),
                json.dumps(summary, sort_keys=True),
            )
        else:
            checkpoint.record_batch(
                batch_number=batch_number,
                batch_size=args.batch_size,
                cursor=cursor,
                last_seen_key=records[-1].get("id"),
                rows_seen=len(records),
                rows_processed=len(operations),
                rows_written=summary["rows_written"],
                rows_skipped=summary["rows_skipped"],
                has_more=len(records) == args.batch_size,
            )

        if len(records) < args.batch_size:
            if args.dry_run:
                LOGGER.info("dry-run complete: final partial batch reached")
            else:
                checkpoint.mark_completed(summary={"message": "final partial batch reached"})
                LOGGER.info("checkpoint marked complete")
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
