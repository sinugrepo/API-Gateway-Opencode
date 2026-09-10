"""SQLite token-usage tracking (WAL, thread-safe)."""
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from starlette.status import HTTP_400_BAD_REQUEST

from app.core.config import USAGE_DB_PATH
from app.core.logging_utils import _log


_VALID_PERIODS = ("today", "3h", "6h", "1d", "7d", "30d")


_usage_db: Optional[sqlite3.Connection] = None


_usage_db_lock = threading.Lock()


def _get_usage_db_unlocked() -> sqlite3.Connection:
    """Return a persistent WAL-mode SQLite connection (caller must hold _usage_db_lock)."""
    global _usage_db
    if _usage_db is None:
        _usage_db = sqlite3.connect(USAGE_DB_PATH, timeout=10.0, check_same_thread=False)
        _usage_db.execute("PRAGMA journal_mode=WAL")
        _usage_db.execute("PRAGMA synchronous=NORMAL")
        _usage_db.execute(
            """
            CREATE TABLE IF NOT EXISTS token_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                request_id TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        _usage_db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_token_usage_timestamp
            ON token_usage(timestamp)
            """
        )
        _usage_db.commit()
    else:
        try:
            _usage_db.execute("SELECT 1")
        except sqlite3.ProgrammingError:
            _usage_db = sqlite3.connect(USAGE_DB_PATH, timeout=10.0, check_same_thread=False)
            _usage_db.execute("PRAGMA journal_mode=WAL")
            _usage_db.execute("PRAGMA synchronous=NORMAL")
    return _usage_db


def _get_usage_db() -> sqlite3.Connection:
    """Public wrapper that acquires the lock for external callers."""
    with _usage_db_lock:
        return _get_usage_db_unlocked()


def _record_usage(
    *,
    request_id: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
) -> None:
    """Persist token usage from a successful chat completion response."""
    with _usage_db_lock:
        conn = _get_usage_db_unlocked()
        try:
            conn.execute(
                """
                INSERT INTO token_usage
                    (timestamp, request_id, model,
                     prompt_tokens, completion_tokens, total_tokens)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    request_id,
                    model,
                    int(prompt_tokens or 0),
                    int(completion_tokens or 0),
                    int(total_tokens or 0),
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _query_usage(start_time: float, end_time: float) -> Dict[str, Any]:
    """Aggregate token usage inside a [start_time, end_time] window."""
    with _usage_db_lock:
        conn = _get_usage_db_unlocked()
        conn.row_factory = sqlite3.Row
        try:
            total_row = conn.execute(
                """
                SELECT
                    COUNT(DISTINCT request_id) AS request_count,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM token_usage
                WHERE timestamp >= ? AND timestamp <= ?
                """,
                (start_time, end_time),
            ).fetchone()

            model_rows = conn.execute(
                """
                SELECT
                    model,
                    COUNT(DISTINCT request_id) AS request_count,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM token_usage
                WHERE timestamp >= ? AND timestamp <= ?
                GROUP BY model
                ORDER BY total_tokens DESC
                """,
                (start_time, end_time),
            ).fetchall()

            return {
                "request_count": int(total_row["request_count"] or 0),
                "prompt_tokens": int(total_row["prompt_tokens"] or 0),
                "completion_tokens": int(total_row["completion_tokens"] or 0),
                "total_tokens": int(total_row["total_tokens"] or 0),
                "by_model": {
                    row["model"]: {
                        "request_count": int(row["request_count"]),
                        "prompt_tokens": int(row["prompt_tokens"]),
                        "completion_tokens": int(row["completion_tokens"]),
                        "total_tokens": int(row["total_tokens"]),
                    }
                    for row in model_rows
                },
            }
        finally:
            conn.row_factory = None


def _query_usage_history(start_time: float, end_time: float) -> List[Dict[str, Any]]:
    """Return time-bucketed usage for charting.

    Uses 1-hour buckets for ranges up to 48h, 1-day buckets beyond.
    Bucketing is pushed into SQL to avoid loading all rows into memory.
    """
    span_hours = (end_time - start_time) / 3600
    if span_hours <= 48:
        bucket_seconds = 3600
    else:
        bucket_seconds = 86400

    with _usage_db_lock:
        conn = _get_usage_db_unlocked()
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                f"""
                SELECT
                    CAST((CAST(timestamp AS REAL) - ?) / ? AS INTEGER) AS bucket_idx,
                    COUNT(DISTINCT request_id) AS request_count,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens
                FROM token_usage
                WHERE timestamp >= ? AND timestamp <= ?
                GROUP BY bucket_idx
                ORDER BY bucket_idx ASC
                """,
                (start_time, bucket_seconds, start_time, end_time),
            ).fetchall()
        finally:
            conn.row_factory = None

    num_buckets = max(int((end_time - start_time) / bucket_seconds) + 1, 0)
    buckets: List[Dict[str, Any]] = []
    for idx in range(num_buckets):
        bucket_start = start_time + idx * bucket_seconds
        bucket_end = min(bucket_start + bucket_seconds, end_time)
        d = datetime.fromtimestamp(bucket_start)
        label = d.strftime("%H:%M") if span_hours <= 48 else d.strftime("%b %d")
        buckets.append({
            "start": bucket_start,
            "end": bucket_end,
            "label": label,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "request_count": 0,
        })

    row_map = {int(r["bucket_idx"]): r for r in rows}
    for idx, bucket in enumerate(buckets):
        r = row_map.get(idx)
        if r:
            bucket["request_count"] = int(r["request_count"])
            bucket["prompt_tokens"] = int(r["prompt_tokens"])
            bucket["completion_tokens"] = int(r["completion_tokens"])
            bucket["total_tokens"] = int(r["total_tokens"])

    return buckets


def _safe_record(
    *,
    request_id: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
) -> None:
    """Wrapper that logs recording failures without raising."""
    try:
        _record_usage(
            request_id=request_id,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )
    except Exception as exc:  # noqa: BLE001 - never crash the request path
        _log("USAGE", f"Failed to record usage: {exc}")


def _resolve_period(
    period: str, now: datetime,
    start: Optional[str] = None, end: Optional[str] = None,
) -> Tuple[str, float, float]:
    """Map a period keyword to (description, start_epoch, end_epoch).

    If `start` and `end` are provided (ISO date strings like "2026-07-25"),
    they override `period` entirely. This allows custom date range queries.
    """
    if start and end:
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=now.tzinfo)
            end_dt = datetime.strptime(end, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=now.tzinfo,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Invalid date format. Use YYYY-MM-DD: {exc}",
            )
        return (
            f"custom range {start} to {end}",
            start_dt.timestamp(),
            end_dt.timestamp(),
        )

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    mapping: Dict[str, Tuple[str, float, float]] = {
        "today": (
            "today (00:00 local time to now)",
            today_start.timestamp(),
            now.timestamp(),
        ),
        "3h": (
            "last 3 hours",
            (now - timedelta(hours=3)).timestamp(),
            now.timestamp(),
        ),
        "6h": (
            "last 6 hours",
            (now - timedelta(hours=6)).timestamp(),
            now.timestamp(),
        ),
        "1d": (
            "last 1 day",
            (now - timedelta(days=1)).timestamp(),
            now.timestamp(),
        ),
        "7d": (
            "last 7 days",
            (now - timedelta(days=7)).timestamp(),
            now.timestamp(),
        ),
        "30d": (
            "last 30 days",
            (now - timedelta(days=30)).timestamp(),
            now.timestamp(),
        ),
    }
    if period not in mapping:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail=(
                f"Invalid period '{period}'. "
                f"Supported: {', '.join(_VALID_PERIODS)}"
            ),
        )
    description, start_ts, end_ts = mapping[period]
    return description, start_ts, end_ts
