"""Resumable async SSE collector for Wikimedia EventStreams (brief section 3, ADR-005).

One connection to `recentchange,revision-create,page-create` (etiquette: one live connection per
profile). Every event is sanitized (`sanitize.py`) before it is held anywhere. Events are grouped
into NDJSON micro-batches per stream; a batch file is written once under a unique name
`<stream>/<yyyy>/<mm>/<dd>/<first-event-dt>_<run-id>-<seq>.ndjson` and never rewritten (Auto
Loader skips overwritten files). The checkpoint (the server's last event id, plus the event time
as a `since` fallback) advances only after the batch holding that event is on disk, so a crash
replays events rather than losing them; silver de-duplicates on the event id.

The server closes connections every so often; the client reconnects with exponential backoff
(policy.yml) and resumes from the checkpoint.

Modes: `catchup` (the `free` profile on Actions) stops once it reaches events newer than its own
start time or after `max_seconds`; `follow` (the `full` profile) runs until stopped. Sinks: batch
files always; Kafka (Avro, schema registry) when `kafka` is given.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from babel_lag.paths import LANDING, STATE
from babel_lag.policy import load_policy, user_agent
from babel_lag.stream.sanitize import sanitize

BASE = "https://stream.wikimedia.org/v2/stream/"
STREAMS = ("mediawiki.recentchange", "mediawiki.revision-create", "mediawiki.page-create")
SHORT = {s: s.split(".", 1)[1] for s in STREAMS}


def run_id() -> str:
    gh = os.environ.get("GITHUB_RUN_ID")
    if gh:
        return f"gh{gh}a{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return "local" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")


def compact_dt(dt: str) -> str:
    """`2026-11-11T05:00:00.123Z` -> `20261111T050000Z` (file-name safe on Windows)."""
    return dt[:19].replace("-", "").replace(":", "") + "Z"


@dataclass
class Checkpoint:
    path: Path
    last_event_id: str | None = None
    last_event_dt: str | None = None

    @classmethod
    def load(cls, path: Path) -> Checkpoint:
        if path.exists():
            raw = json.loads(path.read_text("utf-8"))
            return cls(path, raw.get("last_event_id"), raw.get("last_event_dt"))
        return cls(path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        body = {
            "last_event_id": self.last_event_id,
            "last_event_dt": self.last_event_dt,
            "saved_at": datetime.now(UTC).isoformat(),
        }
        tmp.write_text(json.dumps(body), "utf-8")
        tmp.replace(self.path)


@dataclass
class Stats:
    events: int = 0
    bytes_in: int = 0
    unparseable: int = 0
    reconnects: int = 0
    batches: list[str] = field(default_factory=list)
    bytes_out: int = 0
    first_dt: str | None = None
    last_dt: str | None = None
    receive_lag_s: list[float] = field(default_factory=list)  # now - meta.dt, sampled
    # per stream: [events, min event time, max event time] (streams replay at different positions)
    per_stream: dict[str, list[Any]] = field(default_factory=dict)

    def rate(self) -> float | None:
        """Live events per second: each stream's events over its own event-time span, summed."""
        total = 0.0
        for n, lo, hi in self.per_stream.values():
            span = (_parse_dt(hi) - _parse_dt(lo)).total_seconds()
            if span >= 60:
                total += n / span
        return round(total, 2) if total else None


class BatchWriter:
    """Holds sanitized events per stream and writes each batch once under a unique name."""

    def __init__(self, root: Path, rid: str) -> None:
        self.root, self.rid, self.seq = root, rid, 0
        self.buf: dict[str, list[dict[str, Any]]] = {}

    def add(self, ev: dict[str, Any]) -> None:
        self.buf.setdefault(SHORT.get(ev["meta"].get("stream", ""), "other"), []).append(ev)

    def size(self) -> int:
        return sum(len(v) for v in self.buf.values())

    def flush(self) -> list[tuple[Path, int]]:
        out = []
        for stream, events in self.buf.items():
            if not events:
                continue
            first = events[0]["meta"]["dt"]
            self.seq += 1
            rel = Path(stream, first[:4], first[5:7], first[8:10])
            path = self.root / rel / f"{compact_dt(first)}_{self.rid}-{self.seq:04d}.ndjson"
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():  # never rewrite a batch (ADR-005)
                raise FileExistsError(path)
            data = b"".join(json.dumps(e, ensure_ascii=False).encode() + b"\n" for e in events)
            tmp = path.with_suffix(".part")
            tmp.write_bytes(data)
            tmp.replace(path)
            out.append((path, len(data)))
        self.buf = {}
        return out


async def sse_events(
    client: httpx.AsyncClient, url: str, params: dict[str, str], headers: dict[str, str]
) -> AsyncIterator[tuple[str | None, str]]:
    """Yield (event id, data) pairs from one SSE connection until the server closes it."""
    async with client.stream("GET", url, params=params, headers=headers) as r:
        r.raise_for_status()
        eid: str | None = None
        data: list[str] = []
        async for line in r.aiter_lines():
            if line == "":
                if data:
                    yield eid, "\n".join(data)
                eid, data = None, []
            elif line.startswith("id:"):
                eid = line[3:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].lstrip())


async def collect(
    mode: str = "catchup",
    max_seconds: float = 600,
    max_events: int | None = None,
    batch_events: int = 20_000,
    batch_seconds: float = 60,
    initial_lookback: timedelta = timedelta(hours=1),
    landing: Path = LANDING / "live",
    checkpoint: Path = STATE / "collector.json",
    kafka: Callable[[dict[str, Any]], None] | None = None,
    hash_editor: bool = True,
) -> Stats:
    hosts = load_policy()["hosts"]["stream.wikimedia.org"]
    cp = Checkpoint.load(checkpoint)
    writer = BatchWriter(landing, run_id())
    stats = Stats()
    started = datetime.now(UTC)
    stop_dt = started.strftime("%Y-%m-%dT%H:%M:%S")
    deadline = time.monotonic() + max_seconds
    backoff = float(hosts["backoff_initial_s"])
    last_flush = time.monotonic()
    pending_id, pending_dt = cp.last_event_id, cp.last_event_dt
    done = False
    caught_up: set[str] = set()

    def flush() -> None:
        nonlocal last_flush
        for path, nbytes in writer.flush():
            stats.batches.append(str(path))
            stats.bytes_out += nbytes
        cp.last_event_id, cp.last_event_dt = pending_id, pending_dt
        cp.save()
        last_flush = time.monotonic()

    timeout = httpx.Timeout(30.0, read=60.0)
    url = BASE + ",".join(STREAMS)
    ua = {"User-Agent": user_agent()}

    async def run() -> None:
        nonlocal done, pending_id, pending_dt, backoff
        async with httpx.AsyncClient(timeout=timeout, headers=ua) as client:
            while not done and time.monotonic() < deadline:
                params: dict[str, str] = {}
                headers: dict[str, str] = {}
                if pending_id:
                    headers["Last-Event-ID"] = pending_id
                else:
                    start = (started - initial_lookback).strftime("%Y-%m-%dT%H:%M:%SZ")
                    params["since"] = pending_dt or start
                try:
                    async for eid, raw in sse_events(client, url, params, headers):
                        stats.bytes_in += len(raw)
                        try:
                            ev = json.loads(raw)
                            dt = ev["meta"]["dt"]
                        except (ValueError, KeyError, TypeError):
                            stats.unparseable += 1
                            continue
                        if ev["meta"].get("domain") == "canary":  # WMF health-check events
                            continue
                        writer.add(sanitize(ev, hash_editor=hash_editor))
                        if kafka is not None:
                            kafka(writer.buf[SHORT.get(ev["meta"]["stream"], "other")][-1])
                        stats.events += 1
                        # min and max event time: streams interleave out of order during a replay
                        ps = stats.per_stream.setdefault(ev["meta"].get("stream", ""), [0, dt, dt])
                        ps[0], ps[1], ps[2] = ps[0] + 1, min(ps[1], dt), max(ps[2], dt)
                        stats.first_dt = min(stats.first_dt or dt, dt)
                        stats.last_dt = max(stats.last_dt or dt, dt)
                        if stats.events % 500 == 1:
                            lag = (datetime.now(UTC) - _parse_dt(dt)).total_seconds()
                            stats.receive_lag_s.append(round(lag, 3))
                        pending_id, pending_dt = eid or pending_id, dt
                        backoff = float(hosts["backoff_initial_s"])
                        if (
                            writer.size() >= batch_events
                            or time.monotonic() - last_flush > batch_seconds
                        ):
                            flush()
                        if mode == "catchup" and dt[:19] >= stop_dt:
                            caught_up.add(ev["meta"].get("stream", ""))
                            done = caught_up >= set(STREAMS)  # streams replay at different speeds
                        if max_events and stats.events >= max_events:
                            done = True
                        if done or time.monotonic() >= deadline:
                            break
                except (httpx.HTTPError, OSError) as exc:
                    stats.reconnects += 1
                    wait = min(backoff, float(hosts["backoff_max_s"])) * (0.5 + random.random())
                    print(f"stream closed ({type(exc).__name__}); reconnect in {wait:.1f} s")
                    await asyncio.sleep(wait)
                    backoff *= 2
                    continue
                if not done and time.monotonic() < deadline:
                    stats.reconnects += 1  # server closed cleanly; resume from the checkpoint

    try:
        async with asyncio.timeout(max_seconds + 30):
            await run()
    except TimeoutError:
        pass  # hard time box (a stalled read or reconnect loop): what arrived is flushed below
    flush()
    return stats


def _parse_dt(dt: str) -> datetime:
    return datetime.fromisoformat(dt.replace("Z", "+00:00"))
