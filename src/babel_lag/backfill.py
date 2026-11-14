"""Milestone 3 backfill: every Wikipedia's full history -> compact page facts, on Actions runners
(ADR-004, ADR-006; the repo is public, so minutes are unlimited). Resumable and bounded:

- `plan`: bin the 377 Wikipedias (config/wikipedias.yml, measured listing) into chunks of at
  most ~25 GB of bz2, skipping wikis whose output already sits in the release.
- `chunk`: per wiki, list its files on the mirror, then per file download -> lbzip2 -> DuckDB
  aggregate to one row per page (the `silver_page_creations` shape) -> delete; combine a wiki's
  file partials into `pages_<wiki>.parquet` plus a stats JSON (bytes, rows, seconds, sha256).
- `assemble`: all page facts + sitelinks of every Wikipedia (DuckDB over the SQL dump) + 7 days
  of `user` pageviews -> the core SQL steps 04-07 with REFERENCE_SET `all377` -> per-language gold
  (small Parquet) for the publish workflow.

No editor names or IPs leave the parser (only page-level counts are kept).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import httpx
import yaml

from babel_lag.dumps.fetch import DOWNLOADS, _bunzip, download
from babel_lag.parsers import history
from babel_lag.paths import CONFIG, DATA
from babel_lag.policy import load_slice, user_agent

OUT = DATA / "backfill"
CHUNK_BYTES = 25e9


def wikipedias() -> dict[str, int]:
    raw = yaml.safe_load((CONFIG / "wikipedias.yml").read_text("utf-8"))["wikipedias"]
    return {w: int(v[1]) for w, v in raw.items()}


def plan(done: set[str] | None = None) -> list[list[str]]:
    """Greedy bins of wikis (largest first), each at most CHUNK_BYTES unless one wiki is larger."""
    done = done or set()
    bins: list[list[str]] = []
    sizes: list[float] = []
    for w, b in sorted(wikipedias().items(), key=lambda kv: -kv[1]):
        if w in done:
            continue
        for i, s in enumerate(sizes):
            if s + b <= CHUNK_BYTES:
                bins[i].append(w)
                sizes[i] += b
                break
        else:
            bins.append([w])
            sizes.append(b)
    return bins


def list_files(wiki: str) -> list[str]:
    snap = load_slice()["history_snapshot"]
    base = f"{load_slice()['mirrors']['other']}/mediawiki_history/{snap}/{wiki}/"
    r = httpx.get(base, headers={"User-Agent": user_agent()}, timeout=60, follow_redirects=True)
    r.raise_for_status()
    names = sorted(set(re.findall(r'href="([^"/]+\.tsv\.bz2)"', r.text)))
    return [base + n for n in names]


PAGES_SQL = """
SELECT wiki, page_id, max(page_title) AS page_title,
       min(rev_ts) AS created_at, min_by(is_bot, rev_ts) AS created_by_bot,
       bool_or(page_is_redirect) AS is_redirect, bool_or(page_is_deleted) AS is_deleted,
       CAST(count(*) AS BIGINT) AS revisions,
       CAST(sum(CASE WHEN is_bot THEN 0 ELSE 1 END) AS BIGINT) AS human_revisions,
       CAST(sum(CASE WHEN is_reverted THEN 1 ELSE 0 END) AS BIGINT) AS reverted_revisions,
       max(rev_ts) AS last_rev_ts, max_by(rev_bytes, rev_ts) AS last_rev_bytes
FROM revs GROUP BY wiki, page_id
"""
COMBINE_SQL = """
SELECT wiki, page_id, max(page_title) AS page_title,
       min(created_at) AS created_at, min_by(created_by_bot, created_at) AS created_by_bot,
       bool_or(is_redirect) AS is_redirect, bool_or(is_deleted) AS is_deleted,
       CAST(sum(revisions) AS BIGINT) AS revisions,
       CAST(sum(human_revisions) AS BIGINT) AS human_revisions,
       CAST(sum(reverted_revisions) AS BIGINT) AS reverted_revisions,
       max(last_rev_ts) AS last_rev_ts, max_by(last_rev_bytes, last_rev_ts) AS last_rev_bytes
FROM read_parquet(?) GROUP BY wiki, page_id
"""


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    tmp = DATA / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET memory_limit = '8GB'")
    con.execute(f"SET temp_directory = '{tmp.as_posix()}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET enable_progress_bar = false")
    return con


def chunk(wikis: list[str], out: Path = OUT) -> list[dict[str, Any]]:
    out.mkdir(parents=True, exist_ok=True)
    stats = []
    for wiki in wikis:
        target = out / f"pages_{wiki}.parquet"
        if target.exists():
            continue
        t0, files = time.monotonic(), list_files(wiki)
        parts, nbytes, shas, quarantined = [], 0, [], 0
        for i, url in enumerate(files):
            raw, tsv = DOWNLOADS / f"bf-{wiki}-{i}.bz2", DOWNLOADS / f"bf-{wiki}-{i}.tsv"
            n, sha, _ = download(url, raw)
            nbytes, shas = nbytes + n, [*shas, sha]
            try:
                _bunzip(raw, tsv)
                raw.unlink()
                part = out / f"part_{wiki}_{i}.parquet"
                con = _con()
                fetched = datetime.now(UTC).isoformat()
                params = {"source_file": url, "source_sha256": sha, "fetched_at": fetched}
                if os.environ.get("BABEL_EDITOR_SALT"):
                    params["salt"] = os.environ["BABEL_EDITOR_SALT"]
                sql = f"WITH revs AS ({history._sql(tsv)}) {PAGES_SQL}"
                con.execute(f"COPY ({sql}) TO '{part.as_posix()}' (FORMAT parquet)", params)
                quarantined += con.execute(
                    "SELECT count(DISTINCT line) FROM reject_errors").fetchall()[0][0]  # fmt: skip
                con.close()
                parts.append(part)
            finally:
                raw.unlink(missing_ok=True)
                tsv.unlink(missing_ok=True)
        con = _con()
        con.execute(f"COPY ({COMBINE_SQL}) TO '{target.as_posix()}' (FORMAT parquet, "
                    "COMPRESSION zstd)", [[p.as_posix() for p in parts]])  # fmt: skip
        pages = con.execute(f"SELECT count(*) FROM '{target.as_posix()}'").fetchall()[0][0]
        con.close()
        for p in parts:
            p.unlink()
        s = {"wiki": wiki, "files": len(files), "bytes_in": nbytes, "pages": int(pages),
             "quarantined": int(quarantined), "seconds": round(time.monotonic() - t0, 1),
             "sha256_of_shas": hashlib.sha256("".join(shas).encode()).hexdigest(),
             "finished_at": datetime.now(UTC).isoformat()}  # fmt: skip
        (out / f"stats_{wiki}.json").write_text(json.dumps(s), "utf-8")
        stats.append(s)
        print(json.dumps(s))
    return stats


# The 2026 dump writes one `(row,item,'site','title'),` tuple per line (verified). `|` cannot occur
# in a MediaWiki title, so it serves as a never-matching delimiter: each line is one column.
SITELINKS_SQL = r"""
WITH raw AS (
  SELECT regexp_extract(line, '^\((\d+),(\d+),''([a-z_]+)'',''((?:[^''\\]|\\.)*)''\)',
                        ['row', 'item', 'site', 'title']) AS x
  FROM read_csv(?, columns = {'line': 'VARCHAR'}, delim = '|', quote = '', escape = '',
                header = false, auto_detect = false, ignore_errors = true)
  WHERE starts_with(line, '(')
)
SELECT CAST(x.item AS BIGINT) AS item_id, x.site AS wiki,
       replace(regexp_replace(x.title, '\\(.)', '\1', 'g'), ' ', '_') AS page_title
FROM raw
WHERE x.site IN (SELECT unnest(CAST(? AS VARCHAR[])))
"""


def parse_sitelinks(path: Path, sites: list[str]) -> Any:
    """Sitelinks of `sites` from the (gzipped) SQL dump, as an Arrow table (DuckDB parser)."""
    con = _con()
    out = con.execute(SITELINKS_SQL, [path.as_posix(), sites]).to_arrow_table()
    con.close()
    return out


BACKTEST_SQL = """
WITH picks AS (
  SELECT item_id, max(languages) AS languages, min(first_created_at) AS first_created_at
  FROM lag
  WHERE first_created_at >= TIMESTAMPTZ '2020-01-01' AND first_created_at < TIMESTAMPTZ '2025-01-01'
  GROUP BY item_id
  ORDER BY languages DESC, item_id
  LIMIT 20
)
SELECT l.item_id, l.wiki, l.created_at, l.first_created_at, l.lag_seconds,
       round(l.lag_seconds / 86400.0, 2) AS lag_days, p.languages
FROM lag l JOIN picks p USING (item_id)
ORDER BY p.languages DESC, l.item_id, l.lag_seconds
"""


def assemble(days: int = 7, out: Path = OUT) -> dict[str, int]:
    """All-language gold from the page facts (the backfill) + sitelinks + recent pageviews."""
    from babel_lag.parsers import pageviews
    from babel_lag.rollup import engine

    wikis = sorted(wikipedias())
    pages = sorted(out.glob("pages_*.parquet"))
    if len(pages) < len(wikis):
        missing = sorted(set(wikis) - {p.stem.removeprefix("pages_") for p in pages})
        raise RuntimeError(f"{len(missing)} wikis not backfilled yet: {missing[:10]}")
    sl_url = (f"{load_slice()['mirrors']['latest']}/wikidatawiki/latest/"
              "wikidatawiki-latest-wb_items_per_site.sql.gz")  # fmt: skip
    gz = DOWNLOADS / "sitelinks-all.sql.gz"
    download(sl_url, gz)
    sitelinks = parse_sitelinks(gz, wikis)
    gz.unlink()
    projects = {f"{w.removesuffix('wiki')}.wikipedia": w for w in wikis}
    views = []
    end = date.today() - timedelta(days=2)
    for k in range(days):
        d = end - timedelta(days=k)
        day = d.strftime("%Y%m%d")
        base = load_slice()["mirrors"]["other"]
        url = f"{base}/pageview_complete/{day[:4]}/{day[:4]}-{day[4:6]}/pageviews-{day}-user.bz2"
        raw = DOWNLOADS / f"pv-{day}.bz2"
        download(url, raw)
        import bz2

        with bz2.open(raw, "rb") as fh:
            views.append(pageviews.parse(fh, d, projects, []))
        raw.unlink()
    import pyarrow as pa
    import pyarrow.dataset as pds

    from babel_lag.stream.normalize import BRONZE_SCHEMA

    os.environ["BABEL_REFERENCE_SET"] = "all377"
    inputs = {
        "silver_page_creations": pds.dataset([str(p) for p in pages], format="parquet"),
        "silver_sitelinks": sitelinks,
        "silver_pageviews_daily": pa.concat_tables(views),
        "bronze_live_events": BRONZE_SCHEMA.empty_table(),
        "silver_page_revisions": history.SCHEMA.empty_table(),
    }
    steps = {"gold_coverage_lag", "gold_language_lag", "gold_item_pageviews"}
    steps.add("gold_language_demand")
    res = engine.run_duckdb(inputs, only=steps)
    gold = out / "gold"
    gold.mkdir(parents=True, exist_ok=True)
    import pyarrow.parquet as pq

    # Backtests (published criteria, docs/method.md): the 20 items first created 2020-2024 that
    # reached the most languages, with every language's lag to the first article.
    bt = duckdb.connect()
    bt.register("lag", res["gold_coverage_lag"])
    backtests = bt.execute(BACKTEST_SQL).to_arrow_table()
    bt.close()
    pq.write_table(backtests, gold / "gold_backtests.parquet")
    counts = {"gold_backtests": backtests.num_rows}
    for table in ("gold_language_lag", "gold_language_demand"):
        pq.write_table(res[table], gold / f"{table}.parquet")
        counts[table] = res[table].num_rows
    counts.update(pages=sum(pq.read_metadata(p).num_rows for p in pages),
                  sitelinks=sitelinks.num_rows, coverage_rows=res["gold_coverage_lag"].num_rows,
                  pageview_rows=inputs["silver_pageviews_daily"].num_rows)  # fmt: skip
    (gold / "assemble_stats.json").write_text(
        json.dumps({**counts, "days": days, "finished_at": datetime.now(UTC).isoformat()}), "utf-8"
    )
    return counts
