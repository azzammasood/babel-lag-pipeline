"""Live topic tracking (brief section 6): topics spreading across languages right now.

1. New articles from the live stream: `page-create` events in the article namespace, not bots, not
   redirects, on Wikipedias, in the last 48 hours of event time.
2. Each (wiki, title) is resolved to its Wikidata item with `wbgetentities` (50 titles per call,
   one call per second, `maxlag`, the project User-Agent; policy.yml). Items are often linked hours
   after the article is created, so a miss is retried after 3 hours until the article is 48 h old.
   The resolution state lives in data/state/live_items.parquet (carried by the publish cache).
3. `gold_live_spread`: items that gained articles in 2+ languages in the window, with each
   language's creation time and its lag in hours after the first.

Only public article titles and item ids are used; no editor data is involved.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from babel_lag.lake import delta
from babel_lag.paths import STATE
from babel_lag.policy import load_policy, user_agent

API = "https://www.wikidata.org/w/api.php"
WINDOW = timedelta(hours=48)
RETRY = timedelta(hours=3)
STATE_FILE = STATE / "live_items.parquet"
STATE_SCHEMA = pa.schema(
    [
        ("wiki", pa.string()),
        ("page_title", pa.string()),
        ("item_id", pa.int64()),
        ("checked_at", pa.timestamp("us", tz="UTC")),
    ]
)
# fmt: skip


def new_articles(now: datetime) -> list[tuple[str, str, datetime]]:
    if not delta.exists("silver_live_events"):
        return []
    from babel_lag.backfill import wikipedias

    wikis = list(wikipedias())
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("s", delta.dataset("silver_live_events"))
    rows = con.execute(
        "SELECT wiki, page_title, min(event_dt) FROM s WHERE event_type = 'page-create' "
        "AND namespace = 0 AND NOT coalesce(is_bot, false) AND NOT coalesce(is_redirect, false) "
        "AND page_title IS NOT NULL AND event_dt >= ? AND wiki IN (SELECT unnest(?)) "
        "GROUP BY wiki, page_title",
        [now - WINDOW, wikis],
    ).fetchall()
    con.close()
    return [(w, t, dt) for w, t, dt in rows]


def _lookup(client: httpx.Client, wiki: str, titles: list[str]) -> dict[str, int]:
    """title (underscored) -> numeric item id, for the titles that have an item."""
    r = client.get(API, params={
        "action": "wbgetentities", "sites": wiki, "titles": "|".join(t.replace("_", " ")
                                                                       for t in titles),
        "props": "sitelinks", "sitefilter": wiki, "format": "json", "maxlag": 5,
    })  # fmt: skip
    r.raise_for_status()
    out = {}
    for qid, ent in (r.json().get("entities") or {}).items():
        link = (ent.get("sitelinks") or {}).get(wiki)
        if qid.startswith("Q") and link:
            out[link["title"].replace(" ", "_")] = int(qid[1:])
    return out


def resolve(now: datetime | None = None, max_calls: int = 300) -> dict[str, int]:
    """Resolve new articles to items (bounded by max_calls per run); returns counts."""
    now = now or datetime.now(UTC)
    arts = new_articles(now)
    known: dict[tuple[str, str], dict[str, Any]] = {}
    if STATE_FILE.exists():
        for r in pq.read_table(STATE_FILE).to_pylist():
            known[(r["wiki"], r["page_title"])] = r
    due: dict[str, list[str]] = {}
    for wiki, title, _ in arts:
        k = known.get((wiki, title))
        if k is None or (k["item_id"] is None and now - k["checked_at"] >= RETRY):
            due.setdefault(wiki, []).append(title)
    rps = float(load_policy()["hosts"]["www.wikidata.org"]["requests_per_second"])
    calls = 0
    with httpx.Client(headers={"User-Agent": user_agent()}, timeout=30) as client:
        for wiki, titles in sorted(due.items()):
            for i in range(0, len(titles), 50):
                if calls >= max_calls:
                    break
                batch = titles[i : i + 50]
                found = _lookup(client, wiki, batch)
                calls += 1
                for t in batch:
                    known[(wiki, t)] = {"wiki": wiki, "page_title": t,
                                        "item_id": found.get(t), "checked_at": now}  # fmt: skip
                time.sleep(1 / rps)
    cutoff = now - 2 * WINDOW  # keep the state bounded
    rows = [r for r in known.values() if r["checked_at"] >= cutoff]
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, STATE_SCHEMA), STATE_FILE)
    spread = _spread(arts, known)
    return {"new_articles": len(arts), "api_calls": calls,
            "resolved": sum(1 for r in rows if r["item_id"] is not None),
            "spreading_items": spread}  # fmt: skip


def _spread(arts: list[tuple[str, str, datetime]], known: dict[tuple[str, str], Any]) -> int:
    rows = []
    for wiki, title, dt in arts:
        k = known.get((wiki, title))
        if k and k["item_id"] is not None:
            rows.append({"item_id": k["item_id"], "wiki": wiki, "created_at": dt})
    t = pa.Table.from_pylist(
        rows,
        pa.schema(
            [
                ("item_id", pa.int64()),
                ("wiki", pa.string()),
                ("created_at", pa.timestamp("us", tz="UTC")),
            ]
        ),
    )
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("a", t)
    out = con.execute(
        "WITH f AS (SELECT item_id, min(created_at) AS first_created_at, "
        "count(DISTINCT wiki) AS languages FROM a GROUP BY item_id "
        "HAVING count(DISTINCT wiki) >= 2) "
        "SELECT a.item_id, a.wiki, min(a.created_at) AS created_at, f.first_created_at, "
        "round(epoch(min(a.created_at) - f.first_created_at) / 3600.0, 2) AS lag_hours, "
        "CAST(f.languages AS BIGINT) AS languages FROM a JOIN f USING (item_id) "
        "GROUP BY a.item_id, a.wiki, f.first_created_at, f.languages ORDER BY 6 DESC, 1, 5"
    ).to_arrow_table()
    con.close()
    delta._write("gold_live_spread", out)
    return len({r["item_id"] for r in out.to_pylist()})
