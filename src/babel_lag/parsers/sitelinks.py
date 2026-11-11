"""Wikidata `wb_items_per_site` SQL dump parser (ADR in spike section 5).

Rows are `(ips_row_id, ips_item_id, 'ips_site_id', 'ips_site_page')` tuples of INSERT statements,
either many per line (classic mysqldump) or one per line (the 2026 dumps, verified on the file).
Only the slice's sites are kept. Titles are stored with underscores, the form used by the
history and pageview dumps, so joins go through (wiki, title).
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

import pyarrow as pa
from pydantic import BaseModel

TUPLE = re.compile(rb"\((\d+),(\d+),'((?:[^'\\]|\\.)*)','((?:[^'\\]|\\.)*)'\)")
UNESCAPE = re.compile(rb"\\(.)")


class Sitelink(BaseModel):
    item_id: int
    wiki: str
    page_title: str


SCHEMA = pa.schema([("item_id", pa.int64()), ("wiki", pa.string()), ("page_title", pa.string())])
assert list(Sitelink.model_fields) == SCHEMA.names


def _unescape(b: bytes) -> str:
    return UNESCAPE.sub(lambda m: m.group(1), b).decode("utf-8")


def parse(lines: Iterable[bytes], sites: set[str], quarantine: list[dict[str, Any]]) -> pa.Table:
    wanted = {s.encode() for s in sites}
    rows = []
    for n, line in enumerate(lines, 1):
        if not (line.startswith(b"(") or line.startswith(b"INSERT INTO")):
            continue
        for m in TUPLE.finditer(line):
            if m.group(3) not in wanted:
                continue
            try:
                title = _unescape(m.group(4)).replace(" ", "_")
                rows.append(
                    {"item_id": int(m.group(2)), "wiki": m.group(3).decode(), "page_title": title}
                )
            except UnicodeDecodeError as exc:
                quarantine.append({"line": n, "reason": f"title not UTF-8: {exc}"[:200]})
    return pa.Table.from_pylist(rows, SCHEMA)
