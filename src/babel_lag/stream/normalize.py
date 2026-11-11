"""Flatten a sanitized event into the bronze row shape shared by every engine.

The Lakeflow bronze (SQL over Auto Loader), the Spark Structured Streaming bronze (from Kafka)
and the delta-rs bronze (`free`) all produce exactly these columns; silver and gold logic starts
from them and is written once (`rollup/sql`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pyarrow as pa

BRONZE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_dt", pa.timestamp("us", tz="UTC")),
        ("stream", pa.string()),
        ("wiki", pa.string()),
        ("event_type", pa.string()),
        ("namespace", pa.int32()),
        ("page_id", pa.int64()),
        ("page_title", pa.string()),
        ("rev_id", pa.int64()),
        ("is_bot", pa.bool_()),
        ("is_redirect", pa.bool_()),
        ("length_new", pa.int64()),
        ("editor_hash", pa.string()),
        ("source_file", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def _int(v: Any) -> int | None:
    try:
        return int(v) if v is not None and not isinstance(v, bool) else None
    except (TypeError, ValueError):
        return None


def _dt(v: Any) -> datetime | None:
    """EventStreams `meta.dt` is ISO 8601 UTC (`...Z`); None when missing or malformed."""
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")) if v else None
    except ValueError:
        return None


def flatten(ev: dict[str, Any]) -> dict[str, Any]:
    meta = ev.get("meta") or {}
    stream = str(meta.get("stream", ""))
    # recentchange carries its own type (edit, new, log, categorize); the others are named by stream
    etype = ev.get("type") if stream.endswith("recentchange") else stream.rsplit(".", 1)[-1]
    return {
        "event_id": meta.get("id"),
        "event_dt": _dt(meta.get("dt")),
        "stream": stream,
        "wiki": ev.get("wiki") or ev.get("database"),
        "event_type": etype,
        "namespace": _int(ev.get("namespace", ev.get("page_namespace"))),
        "page_id": _int(ev.get("page_id")),
        "page_title": ev.get("title") or ev.get("page_title"),
        "rev_id": _int(ev.get("rev_id")),
        "is_bot": bool(ev["bot"]) if ev.get("bot") is not None else None,
        "is_redirect": ev.get("page_is_redirect"),
        "length_new": _int(ev.get("rev_len", ev.get("length_new"))),
        "editor_hash": ev.get("editor_hash"),
    }
