"""Spike: exact sizes of the bulk dumps from nginx directory listings (no data downloads).

Usage: uv run python scripts/spike/dump_sizes.py history 2026-10
       uv run python scripts/spike/dump_sizes.py pageviews
       uv run python scripts/spike/dump_sizes.py wikidata
Writes data/spike/<kind>_sizes.json; prints a short summary. 1 request/second (policy).
"""

from __future__ import annotations

import datetime as dt
import json
import re
import sys
import time

import httpx

from babel_lag.env import load_dotenv
from babel_lag.paths import DATA
from babel_lag.policy import user_agent

BASE = "https://dumps.wikimedia.org"
# One nginx listing row per line: name, date, size. Anchored per line so the `../` row (which has
# no date) cannot swallow the next row (milestone 1 review blocker).
ROW = re.compile(
    r'^<a href="([^"]+)">[^<]*</a>[ \t]+(\d{2}-\w{3}-\d{4} \d{2}:\d{2})[ \t]+(-|\d+)\s*$',
    re.M,
)
OUT = DATA / "spike"


def listing(client: httpx.Client, path: str) -> list[tuple[str, int | None]]:
    time.sleep(1.0)
    r = client.get(BASE + path)
    r.raise_for_status()
    return [(n, None if s == "-" else int(s)) for n, _, s in ROW.findall(r.text) if n != "../"]


def history(snapshot: str, client: httpx.Client) -> dict:
    root = f"/other/mediawiki_history/{snapshot}/"
    wikis = [n.rstrip("/") for n, s in listing(client, root) if s is None]
    per_wiki: dict[str, dict] = {}
    for i, w in enumerate(wikis):
        files = listing(client, root + w + "/")
        if any(s is None for _, s in files):  # nested dirs (none expected, but be safe)
            files = files + [
                (f"{d}{n}", s)
                for d, _ in files
                if _ is None
                for n, s in listing(client, root + w + "/" + d)
            ]
        files = [(n, s) for n, s in files if s is not None and n.endswith(".bz2")]
        per_wiki[w] = {"files": len(files), "bytes": sum(s for _, s in files)}
        if i % 100 == 0:
            print(f"{i}/{len(wikis)} wikis", file=sys.stderr)
    total = sum(v["bytes"] for v in per_wiki.values())
    top = sorted(per_wiki.items(), key=lambda kv: -kv[1]["bytes"])
    return {
        "snapshot": snapshot,
        "wikis": len(per_wiki),
        "files": sum(v["files"] for v in per_wiki.values()),
        "total_bytes": total,
        "total_gb": round(total / 1e9, 1),
        "wikipedia_bytes": sum(
            v["bytes"]
            for k, v in per_wiki.items()
            if k.endswith("wiki")
            and k not in ("wikidatawiki", "commonswiki", "metawiki", "specieswiki", "mediawikiwiki")
        ),
        "top30_gb": {k: round(v["bytes"] / 1e9, 2) for k, v in top[:30]},
        "per_wiki": per_wiki,
    }


def pageviews(client: httpx.Client) -> dict:
    root = "/other/pageview_complete/"
    years = [n.rstrip("/") for n, s in listing(client, root) if s is None and n[:4].isdigit()]
    per_day: dict[str, dict[str, int]] = {}
    for y in years:
        for m, _ in [x for x in listing(client, f"{root}{y}/") if x[1] is None]:
            for n, s in listing(client, f"{root}{y}/{m}"):
                mt = re.match(r"pageviews-(\d{8})-(\w+)\.bz2$", n)
                if mt and s is not None:
                    per_day.setdefault(mt.group(1), {})[mt.group(2)] = s
        print(f"year {y}: {len(per_day)} days", file=sys.stderr)
    days = sorted(per_day)
    user = [per_day[d].get("user", 0) for d in days]
    by_year: dict[str, float] = {}
    for d in days:
        by_year[d[:4]] = by_year.get(d[:4], 0) + sum(per_day[d].values()) / 1e9
    return {
        "first_day": days[0],
        "last_day": days[-1],
        "days": len(days),
        "kinds": sorted({k for v in per_day.values() for k in v}),
        "user_total_gb": round(sum(user) / 1e9, 1),
        "all_kinds_total_gb": round(sum(sum(v.values()) for v in per_day.values()) / 1e9, 1),
        "user_mb_per_day_first_last_median": [
            round(user[0] / 1e6),
            round(user[-1] / 1e6),
            round(sorted(user)[len(user) // 2] / 1e6),
        ],
        "gb_by_year_all_kinds": {k: round(v, 1) for k, v in by_year.items()},
    }


def wikidata(client: httpx.Client) -> dict:
    out = {}
    for path in ("/wikidatawiki/latest/", "/wikidatawiki/entities/"):
        for n, s in listing(client, path):
            if s and any(
                k in n
                for k in (
                    "wb_items_per_site",
                    "page_props",
                    "all.json.gz",
                    "latest-all.json.bz2",
                    "truthy-BETA.nt.gz",
                    "latest-lexemes",
                )
            ):
                out[path + n] = round(s / 1e9, 3)
    for w in ("enwiki", "dewiki", "urwiki"):
        for n, s in listing(client, f"/{w}/latest/"):
            if s and ("page_props.sql.gz" in n or n.endswith("-page.sql.gz")):
                out[f"/{w}/latest/{n}"] = round(s / 1e9, 3)
    return {"files_gb": out}


if __name__ == "__main__":
    load_dotenv()
    kind = sys.argv[1]
    client = httpx.Client(headers={"User-Agent": user_agent()}, timeout=60, follow_redirects=True)
    result = (
        history(sys.argv[2], client)
        if kind == "history"
        else pageviews(client)
        if kind == "pageviews"
        else wikidata(client)
    )
    result["measured_at"] = dt.datetime.now(dt.UTC).isoformat()
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{kind}_sizes.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "per_wiki"})[:2500])
