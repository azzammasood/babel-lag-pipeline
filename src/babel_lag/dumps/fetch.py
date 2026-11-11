"""Bounded-disk dump ingestion for the slice: download one file, hash it, parse it to compact
Parquet with lineage columns, delete the download (stream and delete, brief section 1).

Etiquette (policy.yml): the umu.se mirror for `other/` trees, the main site for `latest/`; at most
2 concurrent downloads; the descriptive User-Agent with contact.
"""

from __future__ import annotations

import bz2
import gzip
import hashlib
import json
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from babel_lag.parsers import history, pageviews, sitelinks
from babel_lag.paths import DATA
from babel_lag.policy import load_policy, load_slice, slice_wikis, user_agent

STAGING = DATA / "staging"
DOWNLOADS = DATA / "downloads"


@dataclass
class FileResult:
    source: str
    url: str
    bytes_in: int
    sha256: str
    rows_out: int
    quarantined: int
    seconds_download: float
    seconds_parse: float
    output: str


def download(url: str, dest: Path) -> tuple[int, str, float]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    h, n, t0 = hashlib.sha256(), 0, time.monotonic()
    with (
        httpx.stream(
            "GET", url, headers={"User-Agent": user_agent()}, timeout=120, follow_redirects=True
        ) as r,
        dest.open("wb") as fh,
    ):
        r.raise_for_status()
        for chunk in r.iter_bytes(1 << 20):
            h.update(chunk)
            fh.write(chunk)
            n += len(chunk)
    return n, h.hexdigest(), time.monotonic() - t0


def _lineage(t: pa.Table, url: str, sha: str, fetched: datetime) -> pa.Table:
    n = t.num_rows
    t = t.append_column("source_file", pa.array([url] * n, pa.string()))
    t = t.append_column("source_sha256", pa.array([sha] * n, pa.string()))
    return t.append_column("fetched_at", pa.array([fetched] * n, pa.timestamp("us", tz="UTC")))


def _quarantine(source: str, name: str, url: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    q = DATA / "quarantine" / source
    q.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps({"source_file": url, **r}) + "\n" for r in rows)
    (q / f"{name}.ndjson").write_text(body, "utf-8")


def _ingest(
    source: str,
    url: str,
    name: str,
    parse: Callable[[Path, list[dict[str, Any]]], Iterator[pa.Table]],
) -> FileResult:
    out = STAGING / source / f"{name}.parquet"
    raw = DOWNLOADS / f"{source}-{name}"
    nbytes, sha, secs = download(url, raw)
    fetched = datetime.now(UTC)
    quarantine: list[dict[str, Any]] = []
    t0, rows = time.monotonic(), 0
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".part")
    writer: pq.ParquetWriter | None = None
    try:
        for table in parse(raw, quarantine):
            table = _lineage(table, url, sha, fetched)
            if writer is None:
                writer = pq.ParquetWriter(tmp, table.schema, compression="zstd")
            writer.write_table(table)
            rows += table.num_rows
    finally:
        if writer is not None:
            writer.close()
        raw.unlink(missing_ok=True)  # stream and delete
    if writer is not None:
        tmp.replace(out)
    _quarantine(source, name, url, quarantine)
    return FileResult(
        source, url, nbytes, sha, rows, len(quarantine), round(secs, 1),
        round(time.monotonic() - t0, 1), str(out),
    )  # fmt: skip


def _bz2_text(path: Path) -> Iterator[str]:
    with bz2.open(path, "rt", encoding="utf-8", errors="strict", newline="\n") as fh:
        yield from fh


def history_job(wiki: str) -> Callable[[], FileResult]:
    snap = load_slice()["history_snapshot"]
    base = load_slice()["mirrors"]["other"]
    url = f"{base}/mediawiki_history/{snap}/{wiki}/{snap}.{wiki}.all-time.tsv.bz2"

    def parse(raw: Path, q: list[dict[str, Any]]) -> Iterator[pa.Table]:
        return history.parse(_bz2_text(raw), q)

    return lambda: _ingest("history", url, wiki, parse)


def pageview_job(day: str) -> Callable[[], FileResult]:
    base = load_slice()["mirrors"]["other"]
    url = f"{base}/pageview_complete/{day[:4]}/{day[:4]}-{day[4:6]}/pageviews-{day}-user.bz2"
    projects = {f"{w.removesuffix('wiki')}.wikipedia": w for w in slice_wikis()}
    d = date(int(day[:4]), int(day[4:6]), int(day[6:]))

    def parse(raw: Path, q: list[dict[str, Any]]) -> Iterator[pa.Table]:
        with bz2.open(raw, "rb") as fh:
            yield pageviews.parse(fh, d, projects, q)

    return lambda: _ingest("pageviews", url, day, parse)


def sitelinks_job() -> Callable[[], FileResult]:
    base = load_slice()["mirrors"]["latest"]
    url = f"{base}/wikidatawiki/latest/wikidatawiki-latest-wb_items_per_site.sql.gz"

    def parse(raw: Path, q: list[dict[str, Any]]) -> Iterator[pa.Table]:
        with gzip.open(raw, "rb") as fh:
            yield sitelinks.parse(fh, set(slice_wikis()), q)

    return lambda: _ingest("sitelinks", url, "wb_items_per_site", parse)


def run_jobs(jobs: list[Callable[[], FileResult]], log: Path) -> list[FileResult]:
    """Run jobs with the etiquette cap on concurrent downloads; append each result to `log`."""
    cap = int(load_policy()["hosts"]["ftp.acc.umu.se"]["max_concurrent"])
    results: list[FileResult] = []
    log.parent.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=min(cap, 2)) as pool:
        for res in pool.map(lambda j: j(), jobs):
            results.append(res)
            with log.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(asdict(res)) + "\n")
            print(f"{res.source} {Path(res.output).stem}: {res.rows_out:,} rows, "
                  f"{res.bytes_in / 1e6:.1f} MB, q={res.quarantined}")  # fmt: skip
    return results
