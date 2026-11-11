"""Spike: live EventStreams rate by stream and wiki, plus `since` replay depth.

Usage: uv run python scripts/spike/live_rate.py rate --seconds 600
       uv run python scripts/spike/live_rate.py since
Writes data/spike/live_rate.json or data/spike/since_depth.json. Prints only a short summary.
"""

from __future__ import annotations

import collections
import datetime as dt
import json
import sys
import time

import httpx

from babel_lag.env import load_dotenv
from babel_lag.paths import DATA
from babel_lag.policy import user_agent

STREAMS = "recentchange,revision-create,page-create"
BASE = "https://stream.wikimedia.org/v2/stream/"
OUT = DATA / "spike"


def sse(client: httpx.Client, url: str, params: dict[str, str]):
    """Yield (id, data) per SSE event."""
    with client.stream("GET", url, params=params, headers={"Accept": "text/event-stream"}) as r:
        r.raise_for_status()
        ev_id, data = None, []
        for line in r.iter_lines():
            if line.startswith("id:"):
                ev_id = line[3:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())
            elif line == "" and data:
                yield ev_id, "\n".join(data)
                ev_id, data = None, []


def rate(seconds: int) -> None:
    by_stream: collections.Counter[str] = collections.Counter()
    by_wiki: collections.Counter[str] = collections.Counter()
    bots: collections.Counter[str] = collections.Counter()
    rc_types: collections.Counter[str] = collections.Counter()
    nbytes: collections.Counter[str] = collections.Counter()
    lag_s: list[float] = []
    keys_seen: dict[str, set[str]] = {}
    reconnects, last_id = 0, None
    t0 = time.monotonic()
    client = httpx.Client(headers={"User-Agent": user_agent()}, timeout=httpx.Timeout(30, read=60))
    while time.monotonic() - t0 < seconds:
        params = {"last-event-id": last_id} if last_id else {}
        try:
            for ev_id, raw in sse(client, BASE + STREAMS, params):
                last_id = ev_id
                try:
                    e = json.loads(raw)
                except json.JSONDecodeError:
                    by_stream["_unparseable"] += 1
                    continue
                s = e.get("meta", {}).get("stream", "?")
                by_stream[s] += 1
                nbytes[s] += len(raw)
                wiki = e.get("wiki") or e.get("database") or "?"
                by_wiki[wiki] += 1
                keys_seen.setdefault(s, set()).update(e.keys())
                bot = e.get("bot")
                if bot is None:
                    bot = e.get("performer", {}).get("user_is_bot")
                bots[f"{s}:{bool(bot)}"] += 1
                if s == "mediawiki.recentchange":
                    rc_types[e.get("type", "?")] += 1
                ts = e.get("meta", {}).get("dt")
                if ts and len(lag_s) < 200000:
                    t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    lag_s.append((dt.datetime.now(dt.UTC) - t).total_seconds())
                if time.monotonic() - t0 >= seconds:
                    break
        except (httpx.HTTPError, httpx.StreamError) as exc:
            reconnects += 1
            print(f"reconnect after {type(exc).__name__}", file=sys.stderr)
            time.sleep(min(60, 2**reconnects))
    elapsed = time.monotonic() - t0
    total = sum(by_stream.values())
    lag_s.sort()
    out = {
        "measured_at": dt.datetime.now(dt.UTC).isoformat(),
        "seconds": round(elapsed, 1),
        "events": total,
        "events_per_s": round(total / elapsed, 2),
        "by_stream_per_s": {k: round(v / elapsed, 2) for k, v in by_stream.items()},
        "bytes_per_event": {k: round(nbytes[k] / by_stream[k]) for k in nbytes},
        "wikis_seen": len(by_wiki),
        "top_wikis_per_s": {k: round(v / elapsed, 3) for k, v in by_wiki.most_common(30)},
        "bot_counts": dict(bots),
        "recentchange_types": dict(rc_types),
        "receive_lag_s_p50_p95": [lag_s[len(lag_s) // 2], lag_s[int(len(lag_s) * 0.95)]]
        if lag_s
        else None,
        "reconnects": reconnects,
        "top_level_keys": {k: sorted(v) for k, v in keys_seen.items()},
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "live_rate.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(
        json.dumps(
            {
                k: out[k]
                for k in (
                    "events_per_s",
                    "by_stream_per_s",
                    "wikis_seen",
                    "reconnects",
                    "receive_lag_s_p50_p95",
                )
            }
        )
    )


def since_depth() -> None:
    client = httpx.Client(headers={"User-Agent": user_agent()}, timeout=httpx.Timeout(30, read=60))
    now = dt.datetime.now(dt.UTC)
    results = {}
    for stream in ("recentchange", "revision-create", "page-create"):
        res = {}
        for days in (1, 3, 7, 14, 21, 28, 31, 35, 45):
            since = (now - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                _, raw = next(sse(client, BASE + stream, {"since": since}))
                first = json.loads(raw)["meta"]["dt"]
            except (httpx.HTTPError, StopIteration, KeyError) as exc:
                first = f"error: {type(exc).__name__}"
            res[days] = {"since": since, "first_event_dt": first}
            time.sleep(1)
        results[stream] = res
    OUT.mkdir(parents=True, exist_ok=True)
    out = {"measured_at": now.isoformat(), "results": results}
    (OUT / "since_depth.json").write_text(json.dumps(out, indent=1), encoding="utf-8")
    for s, r in results.items():
        print(s, {d: v["first_event_dt"][:19] for d, v in r.items()})


if __name__ == "__main__":
    load_dotenv()
    if sys.argv[1] == "rate":
        rate(int(sys.argv[sys.argv.index("--seconds") + 1]) if "--seconds" in sys.argv else 600)
    else:
        since_depth()
