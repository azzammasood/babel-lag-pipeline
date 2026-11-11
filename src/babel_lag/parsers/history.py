"""MediaWiki history dump parser (78-column TSV, verified on the 2026-10 zuwiki file).

DuckDB reads the decompressed TSV (multithreaded; the Python line parser managed 1.6 MB/s of
bz2 input, DuckDB is bounded by decompression). Keeps revision events in the article namespace
(0) only, as compact rows. Editor usernames, user ids, IP addresses and comments are never
selected: the only trace of the editor is a salted hash (`BABEL_EDITOR_SALT`) for distinct
counts, computed inside DuckDB. Lines with the wrong column count are rejected by DuckDB and go
to quarantine with its reason (line number and error, never the line itself).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from pydantic import BaseModel

N_COLS = 78
# 1-based column numbers (wikitech "Schema details" order, confirmed against a real file)
COL = {
    "wiki": 1, "entity": 3, "ts": 5, "user_id": 7, "user_text": 10, "user_bot_by": 16,
    "page_id": 29, "page_title": 31, "page_ns": 34, "page_redirect": 36, "page_deleted": 37,
    "page_created": 38, "rev_id": 61, "rev_parent": 62, "rev_bytes": 66,
    "rev_deleted_with_page": 71, "rev_reverted": 73, "rev_is_revert": 76,
}  # fmt: skip


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


def _c(name: str) -> str:
    return f"column{COL[name] - 1:02d}"


def _ts(name: str) -> str:
    return f"CAST(strptime(left(nullif({_c(name)}, ''), 19), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)"


def _sql(path: Path) -> str:
    cols = ", ".join(f"'column{i:02d}': 'VARCHAR'" for i in range(N_COLS))
    editor = f"coalesce(nullif({_c('user_id')}, ''), nullif({_c('user_text')}, ''))"
    salt_ok = bool(os.environ.get("BABEL_EDITOR_SALT"))
    hashed = (f"CASE WHEN {editor} IS NULL THEN NULL ELSE left(sha256($salt || '|' || "
              f"{_c('wiki')} || ':' || {editor}), 16) END") if salt_ok else "NULL"  # fmt: skip
    return f"""
    SELECT {_c("wiki")} AS wiki,
           CAST({_c("page_id")} AS BIGINT) AS page_id,
           {_c("page_title")} AS page_title,
           CAST({_c("rev_id")} AS BIGINT) AS rev_id,
           CAST(nullif({_c("rev_parent")}, '') AS BIGINT) AS rev_parent_id,
           timezone('UTC', {_ts("ts")}) AS rev_ts,
           CAST(nullif({_c("rev_bytes")}, '') AS BIGINT) AS rev_bytes,
           {_c("user_bot_by")} <> '' AS is_bot,
           {_c("rev_reverted")} = 'true' AS is_reverted,
           {_c("rev_is_revert")} = 'true' AS is_revert,
           {_c("page_redirect")} = 'true' AS page_is_redirect,
           {_c("page_deleted")} = 'true' AS page_is_deleted,
           timezone('UTC', {_ts("page_created")}) AS page_created_at,
           {_c("rev_deleted_with_page")} = 'true' AS rev_deleted_with_page,
           {hashed} AS editor_hash,
           $source_file AS source_file, $source_sha256 AS source_sha256,
           CAST($fetched_at AS TIMESTAMPTZ) AS fetched_at
    FROM read_csv('{path.as_posix()}', delim = '\t', quote = '', escape = '', header = false,
                  columns = {{{cols}}}, null_padding = false, strict_mode = true,
                  ignore_errors = true, store_rejects = true, parallel = true)
    WHERE {_c("entity")} = 'revision' AND {_c("page_ns")} = '0'
      AND {_c("page_id")} <> '' AND {_c("rev_id")} <> '' AND {_c("ts")} <> ''
    """


def parse_file(path: Path, out: Path, quarantine: list[dict[str, Any]], lineage: dict[str, str],
               memory_limit: str = "2GB") -> int:  # fmt: skip
    """Decompressed TSV -> Parquet (SCHEMA + source_file, source_sha256, fetched_at)."""
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit}'; SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    params = dict(lineage)
    if os.environ.get("BABEL_EDITOR_SALT"):
        params["salt"] = os.environ["BABEL_EDITOR_SALT"]
    con.execute(f"COPY ({_sql(path)}) TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)",
                params)  # fmt: skip
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}')").fetchall()[0][0]
    # error_type only: DuckDB's message can quote the raw line, which may name an editor
    for line, etype in con.sql(
        "SELECT DISTINCT line, error_type FROM reject_errors ORDER BY line"
    ).fetchall():
        quarantine.append({"line": int(line), "reason": f"{etype}: expected {N_COLS} columns"})
    con.close()
    return int(rows)
