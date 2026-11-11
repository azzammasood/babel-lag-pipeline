# Spike report (milestone 1)

Measured from the owner's machine, 2026-11-11 (UTC), with the probes in `scripts/spike/`. Raw
results: `data/spike/*.json` (git-ignored). Numbers marked *est.* are derived, not measured.

## 1. Live EventStreams rate (`live_rate.py rate --seconds 600`)
One connection to `recentchange,revision-create,page-create`, 600.2 s, about 05:18-05:28 UTC (`measured_at` 05:28:09 marks the end).
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
  Page titles outside namespace 0 (User:, User talk:) and log-event titles also name editors or
  IPs; they and `meta.uri` are dropped for those events (milestone 1 review).

## 2. `since` replay depth (`live_rate.py since`)
Asked for 1, 3, 7, 14, 21, 28, 31, 35, 45 days back at 05:28 UTC on 2026-11-11.
- 1, 3 and 7 days back: replay starts at the requested time.
- 14 days or more: replay starts at the retention floor, **2026-11-03 05:15-12:15 UTC** depending
  on the stream, i.e. **about 7.7-8.0 days** of history today. One outlier: recentchange at
  28 days started at 2026-11-03 19:47 (later than the floor); the floor is what counts.
- Design rule: the `free` catch-up must run at least once every 7 days or it loses events; the
  watchdog alerts after 3 days without a successful run.

## 3. History snapshot size
Exact byte sums from the dumps.wikimedia.org listings of snapshot **2026-10** (1,008 wikis,
2,898 `.tsv.bz2` files; every wiki has at least one file), measured 2026-11-11 05:52-06:10 UTC
with the corrected listing parser (the first crawl dropped each directory's first row; review):
- **676.4 GB** in all. **377 Wikipedias** (names ending `wiki`, minus wikidata, commons, meta,
  species, mediawiki, incubator, foundation, outreach, sources and test wikis): **354.6 GB**;
  enwiki 139.0 GB; small wikis ship one all-time file (urwiki 0.81 GB).
- Largest: wikidatawiki 168.7, enwiki 139.0, commonswiki 110.6, dewiki 25.4, frwiki 21.6,
  eswiki 15.4, ruwiki 13.9, itwiki 12.8, jawiki 9.9, zhwiki 8.1, plwiki 7.0 GB.
- Per-wiki sizes: `data/spike/history_sizes.json`. Rows per GB and parse speed are measured on
  the first chunks in milestone 2 (a ~20-language slice).

## 4. Pageview complete
- One day (2026-10-01, `user` agent file) from the umu.se mirror: **706.6 MB** compressed,
  **60,724,996 rows**, 0 unparseable, 2,870,988 rows (4.7%) without a page id (`null`);
  1,009 projects, **371 Wikipedias** holding 45,765,955 rows (75%).
- Download plus single-threaded Python bz2 parse: 236.3 s, **257k rows/s** (CPU-bound; the
  mirror delivers faster than one core decompresses).
- Listing of every day (corrected crawl): **5,449 days**, 2011-12-01..2026-11-10, kinds
  user/automated/spider. `user` files **2,917.3 GB** in all, all kinds 5,814.6 GB. `user` per
  day: first 518 MB, last 596 MB, median 515 MB. All kinds by year (GB): 2015 284.7,
  2016 416.2, 2018 425.3, 2020 499.2, 2022 593.5, 2023 332.9, 2024 306.4, 2026 444.5 (to 11-10).
- *est.* `user` files from 2015-07-01: ~2,137 GB (median day 515 MB x 4,150 days).

## 5. Wikidata sitelinks
- `wikidatawiki-latest-wb_items_per_site.sql.gz`: **1.917 GB** (item id, site id, page title).
- Alternatives: `page_props.sql.gz` 1.576 GB for wikidatawiki itself, but per-wiki
  `page_props` (enwiki 0.486 GB, dewiki 0.106 GB, urwiki 0.021 GB) would be needed for every wiki;
  full JSON dump `latest-all.json.bz2` **103.7 GB** (first listing; the re-crawl listed it at 0 B,
  presumably mid-regeneration).
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
New Free Edition account (owner, 2026-11-11). `databricks_volume.py` from the owner's machine.
First run 05:34 UTC (1,020 + 1,021 rows, exact); its data was purged after the review found
user-namespace titles could pass the sanitizer. Re-run with the fixed sanitizer, 05:47-05:49 UTC,
total 157.7 s:
- Control plane OK; one warehouse, "Serverless Starter Warehouse", 2X-Small; it starts on
  request (STOPPED -> RUNNING on the first run) and answers SQL.
- Schemas `workspace.babel_landing`, `workspace.babel_bronze` and the managed volume
  `babel_landing.live` created over REST; sanitized NDJSON micro-batch (480,519 B, 1,098
  events) uploaded with the Files API in **2.61 s**.
- Serverless Lakeflow pipeline `babel-bronze-live` (Auto Loader `read_files` on a stream, three
  expectations): full refresh **COMPLETED in 47.1 s**, **1,098 rows** (exact). Second batch
  (1,098 events): incremental update **COMPLETED in 24.1 s**, **2,196 rows** (exact).
- Privacy check in the workspace: 0 of 2,196 rows hold a title outside namespace 0 or on a log
  event, 0 hold a `User:`/`User talk:` title.
- No quota message seen. Free storage size limit not published; measured as volumes grow.
- Still to verify: the same from GitHub Actions (needs the repo), and quota behaviour under the
  milestone 2 cadence.

## 8. GitHub Actions runners
Measured by `spike-runner.yml`, run 42968606178 (2026-11-11 05:53 UTC, ubuntu-latest):
- **4 CPUs, 15 GB RAM, 145 GB root disk with 88 GB free** (more than the documented 14 GB SSD).
- One pageview day (706,636,328 B) from the umu.se mirror in **8.05 s = 87.7 MB/s**.
- Decompress the same day: `bzip2` single core **61.65 s** (11.5 MB/s compressed, 5 MB RSS);
  `lbzip2 -n 4` **13.88 s** (50.9 MB/s compressed, 79 MB RSS), 4.4x faster.
- Databricks from Actions: skipped in that run (secrets added at 06:23 UTC); to re-run.
- Limits: 6 h per job; a private repo on the free plan has 2,000 min/month, public repos are
  unlimited (ADR-003). Keystone processed 1,873 GB over 37.9 job hours on the same runners.

## 9. Local machine and Compose profiles
51.1 GB free (budget 35.8 GB), 16.8 GB RAM, 8 logical CPUs. Docker 28.0.4 installed; the daemon
was not running. Image sizes and idle RAM per profile: *(pending: measured when the stack is
first pulled in milestone 2)*
