"""Spike: stream one pageview_complete day from the umu.se mirror; rows, wikis, parse speed.

Usage: uv run python -I scripts/spike/pageview_day.py 20261001
Nothing is kept on disk; writes data/spike/pageview_day.json.
"""

from __future__ import annotations

import bz2
import collections
import datetime as dt
import json
import sys
import time

import httpx

sys.path.insert(0, "src")
from babel_lag.env import load_dotenv  # noqa: E402
from babel_lag.paths import DATA  # noqa: E402
from babel_lag.policy import user_agent  # noqa: E402

MIRROR = "https://ftp.acc.umu.se/mirror/wikimedia.org/other/pageview_complete"


def main(day: str) -> None:
    url = f"{MIRROR}/{day[:4]}/{day[:4]}-{day[4:6]}/pageviews-{day}-user.bz2"
    dec = bz2.BZ2Decompressor()
    rows_by_project: collections.Counter[str] = collections.Counter()
    views_by_project: collections.Counter[str] = collections.Counter()
    agents: collections.Counter[str] = collections.Counter()
    bad, nbytes, buf = 0, 0, b""
    no_page_id = 0
    t0 = time.monotonic()
    with httpx.stream("GET", url, headers={"User-Agent": user_agent()}, timeout=120) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes(1 << 20):
            nbytes += len(chunk)
            data = dec.decompress(chunk)
            while dec.eof:  # multistream bz2
                rest = dec.unused_data
                dec = bz2.BZ2Decompressor()
                data += dec.decompress(rest)
            buf += data
            lines = buf.split(b"\n")
            buf = lines.pop()
            for line in lines:
                parts = line.split(b" ")
                if len(parts) != 6:
                    bad += 1
                    continue
                proj = parts[0].decode()
                rows_by_project[proj] += 1
                views_by_project[proj] += int(parts[4])
                agents[parts[3].decode()] += 1
                if parts[2] == b"null":
                    no_page_id += 1
    secs = time.monotonic() - t0
    wp = {k: v for k, v in rows_by_project.items() if k.endswith(".wikipedia")}
    out = {
        "day": day,
        "measured_at": dt.datetime.now(dt.UTC).isoformat(),
        "compressed_mb": round(nbytes / 1e6, 1),
        "seconds_download_and_parse": round(secs, 1),
        "rows": sum(rows_by_project.values()),
        "rows_per_s": round(sum(rows_by_project.values()) / secs),
        "unparseable_rows": bad,
        "rows_without_page_id": no_page_id,
        "projects": len(rows_by_project),
        "wikipedias": len(wp),
        "wikipedia_rows": sum(wp.values()),
        "top25_wikipedia_rows": dict(collections.Counter(wp).most_common(25)),
        "top25_wikipedia_views": {
            k: v for k, v in views_by_project.most_common(60) if k.endswith(".wikipedia")
        },
        "agent_types": dict(agents),
    }
    (DATA / "spike").mkdir(parents=True, exist_ok=True)
    (DATA / "spike" / "pageview_day.json").write_text(json.dumps(out, indent=1), "utf-8")
    print(
        {
            k: out[k]
            for k in (
                "compressed_mb",
                "seconds_download_and_parse",
                "rows",
                "rows_per_s",
                "unparseable_rows",
                "rows_without_page_id",
                "projects",
                "wikipedias",
                "wikipedia_rows",
            )
        }
    )


if __name__ == "__main__":
    load_dotenv()
    main(sys.argv[1])
