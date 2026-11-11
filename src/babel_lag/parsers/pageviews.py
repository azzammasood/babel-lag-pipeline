"""pageview_complete daily `user` file parser (space-separated, 6 fields, verified in the spike).

`<project> <title> <page_id|null> <access> <daily_total> <hourly_counts>`. Only the slice's
Wikipedias are kept; access types are summed per (wiki, page id, title, day). Pageviews carry no
personal data (aggregated by Wikimedia); bad lines go to quarantine with a reason.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from datetime import date
from typing import Any

import pyarrow as pa
from pydantic import BaseModel


class PageviewDay(BaseModel):
    wiki: str
    page_id: int | None
    page_title: str
    day: date
    views: int


SCHEMA = pa.schema(
    [
        ("wiki", pa.string()),
        ("page_id", pa.int64()),
        ("page_title", pa.string()),
        ("day", pa.date32()),
        ("views", pa.int64()),
    ]
)
assert list(PageviewDay.model_fields) == SCHEMA.names


def parse(
    lines: Iterable[bytes], day: date, projects: dict[str, str], quarantine: list[dict[str, Any]]
) -> pa.Table:
    """`projects` maps `ur.wikipedia` -> `urwiki` (the slice); other projects are skipped."""
    keys = {p.encode(): w for p, w in projects.items()}
    views: Counter[tuple[str, int | None, str]] = Counter()
    for n, line in enumerate(lines, 1):
        parts = line.rstrip(b"\n").split(b" ")
        wiki = keys.get(parts[0])
        if wiki is None:
            continue
        try:
            if len(parts) != 6:
                raise ValueError(f"expected 6 fields, got {len(parts)}")
            pid = None if parts[2] == b"null" else int(parts[2])
            views[(wiki, pid, parts[1].decode("utf-8"))] += int(parts[4])
        except (ValueError, UnicodeDecodeError) as exc:
            quarantine.append({"line": n, "reason": str(exc)[:200]})
    rows = [
        {"wiki": w, "page_id": p, "page_title": t, "day": day, "views": v}
        for (w, p, t), v in views.items()
    ]
    return pa.Table.from_pylist(rows, SCHEMA)
