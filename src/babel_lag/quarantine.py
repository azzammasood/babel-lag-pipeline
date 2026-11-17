"""Quarantine for records that cannot be parsed (CLAUDE.md: never invent, never drop silently).

Stream lines can carry usernames or IPs, so a stream quarantine record keeps only where the line
came from (SSE id or batch file), its size and the reason, never its content.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from babel_lag.paths import DATA


def stream_line(stage: str, reason: str, ref: str | None, nbytes: int) -> None:
    """Append one record to data/quarantine/stream/<stage>_<yyyymmdd>.ndjson."""
    now = datetime.now(UTC)
    q = DATA / "quarantine" / "stream"
    q.mkdir(parents=True, exist_ok=True)
    rec = {"stage": stage, "ref": ref, "bytes": nbytes, "reason": reason, "at": now.isoformat()}
    with (q / f"{stage}_{now:%Y%m%d}.ndjson").open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
