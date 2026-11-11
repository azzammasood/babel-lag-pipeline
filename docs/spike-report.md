# Spike report (milestone 1)

Measured from the owner's machine, 2026-11-11 (UTC), with the probes in `scripts/spike/`. Raw
results: `data/spike/*.json` (git-ignored). Numbers marked *est.* are derived, not measured.

## 1. Live EventStreams rate (`live_rate.py rate --seconds 600`)
One connection to `recentchange,revision-create,page-create`, 600.2 s, 05:12-05:22 UTC.
- **46.07 events/s** in all: recentchange 28.46/s, revision-create 16.29/s, page-create 1.32/s.
  27,651 events, 0 unparseable, **0 reconnects** in 10 minutes.
- 134 distinct wikis in 10 minutes (the long tail of ~1,000 wikis is rarely active).
  Top: commonswiki 20.8/s, wikidatawiki 9.0/s, enwiki 4.5/s, arwiki 1.4/s, incubatorwiki 0.8/s.
  Commons and Wikidata are about 2/3 of the firehose; Wikipedias are the minority.
- Mean payload: revision-create 1,637 B, recentchange 1,308 B, page-create 1,364 B per event
  (*est.* ~4.0 M events/day, ~5.6 GB/day raw JSON across the three streams).
- Bot share: recentchange 42% (`bot` flag), revision-create 49%, page-create 37%
  (`performer.user_is_bot`).
- recentchange types: edit 52%, categorize 37%, log 7%, new 3%.
- Receive lag (now minus `meta.dt`): p50 0.66 s, p95 3.46 s.
- **Personal data fields:** recentchange `user`; revision-create and page-create `performer`
  (`user_text`, `user_id`, groups, edit count, registration time). Anonymous edits put an IP in
  `user`/`performer.user_text`. `comment`/`parsedcomment` are free text that can name people.
  All are dropped at ingestion (salted hash of the user only where a distinct count is needed).

## 2. `since` replay depth (`live_rate.py since`)
Asked for 1, 3, 7, 14, 21, 28, 31, 35, 45 days back at 05:22 UTC on 2026-11-11.
- 1, 3 and 7 days back: replay starts at the requested time.
- 14 days or more: replay starts at the retention floor, **2026-11-03 05:15-12:15 UTC** depending
  on the stream, i.e. **about 7.7-8.0 days** of history today.
- Design rule: the `free` catch-up must run at least once every 7 days or it loses events; the
  watchdog alerts after 3 days without a successful run.

## 3. History snapshot size
*(pending: crawl of `/other/mediawiki_history/2026-10/` running)*

## 4. Pageview complete
- One day (2026-10-01, `user` agent file) from the umu.se mirror: **706.6 MB** compressed,
  **60,724,996 rows**, 0 unparseable, 2,870,988 rows (4.7%) without a page id (`null`);
  1,009 projects, **371 Wikipedias** holding 45,765,955 rows (75%).
- Download plus single-threaded Python bz2 parse: 236.3 s, **257k rows/s** (CPU-bound; the
  mirror delivers faster than one core decompresses).
- Total size by year: *(pending: listing crawl running)*

## 5. Wikidata sitelinks
- `wikidatawiki-latest-wb_items_per_site.sql.gz`: **1.917 GB** (item id, site id, page title).
- Alternatives: `page_props.sql.gz` 1.576 GB for wikidatawiki itself, but per-wiki
  `page_props` (enwiki 0.486 GB, dewiki 0.106 GB, urwiki 0.021 GB) would be needed for every wiki;
  full JSON dump `latest-all.json.bz2` **103.7 GB**.
- **Choice:** `wb_items_per_site` (one file, 54x smaller than the JSON dump). It gives titles, not
  page ids; joins to history and pageviews go through (wiki, normalised title), and history rows
  carry page id and title. Coverage numbers come with the first parse in milestone 2.

## 6. Download throughput (100 MB range requests, nothing kept)
| Host | Path | MB/s |
| --- | --- | --- |
| ftp.acc.umu.se (mirror) | `other/pageview_complete` | **11.3** |
| dumps.wikimedia.org | `other/pageview_complete` | 4.2 |
| dumps.wikimedia.org | `wikidatawiki/latest` | 3.8 |
| dumps.wikimedia.your.org | `wikidatawiki/latest` | 2.9 |
umu.se and clarkson.edu do not mirror `latest/` paths (404); umu.se mirrors `other/`.
Choice: the `other/` trees (history, pageviews) from umu.se; `latest/` files from the main site.

## 7. Databricks Free Edition
New Free Edition account (owner, 2026-11-11). `databricks_volume.py`, from the owner's machine,
05:34 UTC, total 165.9 s:
- Control plane OK; one warehouse, "Serverless Starter Warehouse", 2X-Small, STOPPED; it started
  on request (RUNNING) and answered SQL.
- Created schemas `workspace.babel_landing`, `workspace.babel_bronze` and the managed volume
  `babel_landing.live` over REST; uploaded a sanitized NDJSON micro-batch (560,093 B, 1,020
  events) with the Files API in **2.47 s**.
- Created the serverless Lakeflow pipeline `babel-bronze-live` (Auto Loader `read_files` on a
  stream, three expectations) and triggered it: full refresh **COMPLETED in 46.9 s**, table
  holds **1,020 rows** (exact). Second batch (1,021 events) uploaded, incremental update
  **COMPLETED in 12.8 s**, table holds **2,041 rows** (exact; no duplicates, nothing dropped).
- No quota message seen. Free storage size limit not published; measured as volumes grow.
- Still to verify: the same from GitHub Actions (needs the repo), and quota behaviour under the
  milestone 2 cadence.

## 8. GitHub Actions runners
From Keystone's measured runs: standard runners have 4 CPUs, 16 GB RAM, 14 GB SSD; 6 h per job;
a private repo on the free plan has 2,000 min/month, public repos unlimited (ADR-003).
Keystone processed 1,873 GB over 37.9 job hours this way.

## 9. Local machine and Compose profiles
51.1 GB free (budget 35.8 GB), 16.8 GB RAM, 8 logical CPUs. Docker 28.0.4 installed; the daemon
was not running. Image sizes and idle RAM per profile: *(pending: measured when the stack is
first pulled in milestone 2)*
