"""MediaWiki history dump parser (78-column TSV, verified on the 2026-10 zuwiki file).

Keeps revision events in the article namespace (0) only, as compact rows. Editor usernames, user
ids, IP addresses and comments are dropped here: the only trace of the editor is a salted hash
(`BABEL_EDITOR_SALT`) for distinct-editor counts. Malformed lines go to quarantine with a reason.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from pydantic import BaseModel

from babel_lag.stream.sanitize import editor_hash

N_COLS = 78
# 0-based positions (wikitech "Schema details" order, confirmed against a real file)
WIKI, ENTITY, ETYPE, TS = 0, 2, 3, 4
USER_ID, USER_TEXT, USER_BOT_BY = 6, 9, 15
PAGE_ID, PAGE_TITLE, PAGE_NS, PAGE_REDIRECT, PAGE_DELETED, PAGE_CREATED = 28, 30, 33, 35, 36, 37
REV_ID, REV_PARENT, REV_BYTES = 60, 61, 65
REV_DELETED_WITH_PAGE, REV_REVERTED, REV_IS_REVERT = 70, 72, 75


class HistoryRevision(BaseModel):
    """One article-namespace revision; no personal data."""

    wiki: str
    page_id: int
    page_title: str
    rev_id: int
    rev_parent_id: int | None
    rev_ts: datetime
    rev_bytes: int | None
    is_bot: bool
    is_reverted: bool
    is_revert: bool
    page_is_redirect: bool
    page_is_deleted: bool
    page_created_at: datetime | None
    rev_deleted_with_page: bool
    editor_hash: str | None


SCHEMA = pa.schema(
    [
        ("wiki", pa.string()),
        ("page_id", pa.int64()),
        ("page_title", pa.string()),
        ("rev_id", pa.int64()),
        ("rev_parent_id", pa.int64()),
        ("rev_ts", pa.timestamp("us", tz="UTC")),
        ("rev_bytes", pa.int64()),
        ("is_bot", pa.bool_()),
        ("is_reverted", pa.bool_()),
        ("is_revert", pa.bool_()),
        ("page_is_redirect", pa.bool_()),
        ("page_is_deleted", pa.bool_()),
        ("page_created_at", pa.timestamp("us", tz="UTC")),
        ("rev_deleted_with_page", pa.bool_()),
        ("editor_hash", pa.string()),
    ]
)
assert list(HistoryRevision.model_fields) == SCHEMA.names


def _ts(v: str) -> datetime | None:
    return datetime.strptime(v[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC) if v else None


def _int(v: str) -> int | None:
    return int(v) if v else None


def parse_line(line: str) -> dict[str, Any] | None:
    """A row dict for an article revision, None for other events; raises ValueError if bad."""
    c = line.rstrip("\n").split("\t")
    if len(c) != N_COLS:
        raise ValueError(f"expected {N_COLS} columns, got {len(c)}")
    if c[ENTITY] != "revision" or c[PAGE_NS] != "0":
        return None
    rev_ts = _ts(c[TS])
    if rev_ts is None or not c[PAGE_ID] or not c[REV_ID]:
        raise ValueError("revision without timestamp, page id or revision id")
    editor = c[USER_ID] or c[USER_TEXT]  # anonymous editors have only their IP: hashed, never kept
    return {
        "wiki": c[WIKI],
        "page_id": int(c[PAGE_ID]),
        "page_title": c[PAGE_TITLE],
        "rev_id": int(c[REV_ID]),
        "rev_parent_id": _int(c[REV_PARENT]),
        "rev_ts": rev_ts,
        "rev_bytes": _int(c[REV_BYTES]),
        "is_bot": c[USER_BOT_BY] != "",
        "is_reverted": c[REV_REVERTED] == "true",
        "is_revert": c[REV_IS_REVERT] == "true",
        "page_is_redirect": c[PAGE_REDIRECT] == "true",
        "page_is_deleted": c[PAGE_DELETED] == "true",
        "page_created_at": _ts(c[PAGE_CREATED]),
        "rev_deleted_with_page": c[REV_DELETED_WITH_PAGE] == "true",
        "editor_hash": editor_hash(f"{c[WIKI]}:{editor}") if editor else None,
    }


def parse(
    lines: Iterable[str], quarantine: list[dict[str, Any]], chunk: int = 200_000
) -> Iterator[pa.Table]:
    """Arrow tables of `chunk` rows; bad lines appended to `quarantine` (line number, reason)."""
    rows: list[dict[str, Any]] = []
    for n, line in enumerate(lines, 1):
        try:
            row = parse_line(line)
        except ValueError as exc:
            quarantine.append({"line": n, "reason": str(exc)[:200]})
            continue
        if row is not None:
            rows.append(row)
            if len(rows) >= chunk:
                yield pa.Table.from_pylist(rows, SCHEMA)
                rows = []
    if rows:
        yield pa.Table.from_pylist(rows, SCHEMA)
