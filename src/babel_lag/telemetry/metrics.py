"""`pipeline_metrics.json` (brief section 5c), built only from measured records: the run log,
the Delta tables, the quarantine files and the metadata build. Validated by its JSON Schema.

`engines[]` lists only services that appear in the run log (tool credibility: a tool is listed
only when it ran). Live rate = on-time events in the latest complete hour of gold windows divided
by 3600 s; end-to-end latency = publish time minus the newest event time in the published gold.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import jsonschema

from babel_lag.lake import delta
from babel_lag.paths import CONTRACTS, DATA
from babel_lag.telemetry.runlog import StageRecord, read

ROLES = {
    "eventstreams-collector": "live ingestion (async SSE, checkpoints, sanitizer)",
    "kafka": "live event log with Avro schema registry (full)",
    "delta-rs": "Delta Lake writer for bronze, silver and gold (free)",
    "duckdb": "core SQL engine (free)",
    "spark": "core SQL and Structured Streaming engine (full)",
    "databricks": "Unity Catalog volume, Lakeflow pipeline and SQL warehouse (full)",
    "dbt-duckdb": "dbt models and tests on DuckDB (free)",
    "dbt-databricks": "dbt models and tests on Databricks (full)",
    "soda-core": "data quality checks",
    "datacontract-cli": "ODCS contract lint",
    "openlineage": "lineage events to Marquez",
    "openmetadata": "catalog ingestion",
    "dump-fetcher": "history, pageview and sitelink dumps (stream and delete)",
    "babel-profile": "profile probe and fallback",
    "babel-export": "versioned JSON export",
    "dagster": "asset-graph orchestration showcase",
}


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return (v if v.tzinfo else v.replace(tzinfo=UTC)).astimezone(UTC).isoformat()
    return str(v)


def _rate(n: int | None, secs: float) -> float | None:
    return round(n / secs, 3) if n is not None and secs > 0 else None


def _stage(r: StageRecord) -> dict[str, Any]:
    moved = r.bytes_in if r.bytes_in is not None else r.bytes_out
    return {
        "stage": r.stage, "service": r.service, "version": r.version,
        "records_in": r.records_in, "records_out": r.records_out,
        "bytes_in": r.bytes_in, "bytes_out": r.bytes_out, "seconds": r.seconds,
        "records_per_second": _rate(r.records_out, r.seconds),
        "mb_per_second": _rate(moved / 1e6 if moved is not None else None, r.seconds),
        "status": r.status if r.status in ("ok", "failed") else "skipped",
    }  # fmt: skip


def _gold_live() -> dict[str, Any]:
    out: dict[str, Any] = {"window_start": None, "window_end": None, "events_per_second": None,
                           "latest_event_at": None, "wikis": None}  # fmt: skip
    if not delta.exists("gold_live_activity"):
        return out
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("g", delta.read("gold_live_activity"))
    con.register("s", delta.read("silver_live_events"))
    latest, wikis = con.execute("SELECT max(event_dt), count(DISTINCT wiki) FROM s").fetchall()[0]
    # the latest complete hour: four windows all strictly before the newest window
    row = con.execute(
        "WITH w AS (SELECT window_start, sum(events) AS e FROM g GROUP BY 1), "
        "m AS (SELECT max(window_start) AS mx FROM w) "
        "SELECT min(window_start), max(window_start), sum(e), count(*) FROM w, m "
        "WHERE window_start >= mx - INTERVAL 60 MINUTE AND window_start < mx"
    ).fetchall()[0]
    con.close()
    out.update(latest_event_at=latest, wikis=wikis)
    if row and row[3] == 4:
        out.update(
            window_start=row[0], window_end=row[1], events_per_second=round(row[2] / 3600, 2)
        )
    return out


def _quarantined() -> int:
    n = 0
    for f in (DATA / "quarantine").rglob("*.ndjson"):
        n += sum(1 for _ in f.open("rb"))
    return n


def _quality(records: list[StageRecord]) -> dict[str, int | None]:
    passed = failed = 0
    seen = False
    for r in records:
        if r.stage in ("dbt_build", "soda_scan", "contracts_lint") and r.status in ("ok", "failed"):
            seen = True
            passed += int(r.detail.get("passed", 0))
            failed += int(r.detail.get("failed", 0))
    viol = sum(int(r.detail.get("failed", 0)) for r in records if r.stage == "contracts_lint")
    return {"checks_passed": passed if seen else None, "checks_failed": failed if seen else None,
            "quarantined_rows": _quarantined(), "contract_violations": viol}  # fmt: skip


def build(
    meta_summary: dict[str, Any],
    decision: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    records = read()
    if not records:
        raise RuntimeError("empty run log: nothing measured yet")
    last_id = records[-1].run_id
    last = [r for r in records if r.run_id == last_id]
    prof = next((r for r in reversed(records) if r.stage == "profile" and r.detail.get(
        "produced_by")), None)  # fmt: skip
    if decision is None and prof is None:
        raise RuntimeError("no profile decision recorded")
    if decision is None:
        decision = {"profile": prof.detail["produced_by"], "fallback": prof.detail.get("fallback"),
                    "reason": prof.detail.get("reason", ""), "run_id": prof.run_id}  # fmt: skip
    used: dict[str, StageRecord] = {}
    for r in records:
        used[r.service] = r
    last_services = {r.service for r in last}
    engines = [{"name": s, "version": r.version, "role": ROLES.get(s, s),
                "active": s in last_services, "last_used": r.ended_at or None}
               for s, r in sorted(used.items())]  # fmt: skip
    by_run: dict[str, list[StageRecord]] = defaultdict(list)
    for r in records:
        by_run[r.run_id].append(r)
    runs = []
    for rid, rs in list(by_run.items())[-48:]:
        coll = [r for r in rs if r.stage == "collect"]
        runs.append({"run_id": rid, "ended_at": rs[-1].ended_at or None,
                     "status": "failed" if any(r.status == "failed" for r in rs) else "ok",
                     "events": coll[-1].records_out if coll else None,
                     "seconds": round(sum(r.seconds for r in rs), 1)})  # fmt: skip
    daily: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for r in records:
        if r.stage == "collect" and r.status == "ok":
            d = daily[r.ended_at[:10]]
            d[0] += r.records_out or 0
            d[1] += r.bytes_in or 0
    live = _gold_live()
    coll_last = next((r for r in reversed(last) if r.stage == "collect"), None)
    hist_rows = delta.read("silver_page_revisions").num_rows if delta.exists(
        "silver_page_revisions") else None  # fmt: skip
    latest = live["latest_event_at"]
    lag_s = (now - latest).total_seconds() if latest else None
    langs = delta.read("gold_language_lag").num_rows if delta.exists("gold_language_lag") else None
    collect_all = [r for r in records if r.stage == "collect" and r.status == "ok"]
    doc = {
        "metrics_version": "1.0",
        "generated_at": now.isoformat(),
        "profile": {"produced_by": decision["profile"], "fallback": bool(decision["fallback"]),
                    "reason": str(decision.get("reason", "")), "run_id": decision["run_id"]},
        "engines": engines,
        "totals": {
            "events": sum(r.records_out or 0 for r in collect_all),
            "bytes": sum(r.bytes_in or 0 for r in collect_all)
            + sum(r.bytes_in or 0 for r in records if r.stage.startswith("fetch_")),
            "files": sum(r.records_in or 0 for r in records if r.stage.startswith("fetch_"))
            + sum(len(r.detail.get("batches", [])) for r in collect_all),
            "history_rows": hist_rows,
            "wikis": live["wikis"],
            "languages": langs,
            "date_range": {"start": _iso(min((r.detail.get("first_dt") for r in collect_all
                                              if r.detail.get("first_dt")), default=None)),
                           "end": _iso(latest)},
        },
        "last_run": {"run_id": last_id, "started_at": last[0].started_at,
                     "ended_at": last[-1].ended_at or None,
                     "status": "failed" if any(r.status == "failed" for r in last) else "ok",
                     "stages": [_stage(r) for r in last]},
        "live": {"window_start": _iso(live["window_start"]), "window_end": _iso(live["window_end"]),
                 "events_per_second": live["events_per_second"],
                 "end_to_end_latency_s": round(lag_s, 1) if lag_s is not None else None,
                 "reconnects": coll_last.detail.get("reconnects") if coll_last else None,
                 "consumer_lag": next((r.detail.get("consumer_lag") for r in reversed(last)
                                       if "consumer_lag" in r.detail), None)},
        "history": {"runs": runs,
                    "daily": [{"day": d, "events": v[0], "bytes": v[1]}
                              for d, v in sorted(daily.items())[-30:]]},
        "freshness": {"latest_event_at": _iso(latest), "published_at": now.isoformat(),
                      "lag_s": round(lag_s, 1) if lag_s is not None else None},
        "quality": _quality(last),
        "cost": {"usd": 0, "free_quota_use_pct": None,
                 "free_quota_note": "Databricks Free Edition publishes no quota meter; GitHub "
                 "Actions minutes are not metered per run here."},
        "lineage_summary": {"nodes": meta_summary["nodes"], "edges": meta_summary["edges"],
                            "documented_pct": meta_summary["completeness"]["documented_pct"]},
    }  # fmt: skip
    validate(doc)
    return doc


def validate(doc: dict[str, Any]) -> None:
    schema = json.loads((CONTRACTS / "pipeline_metrics.schema.json").read_text("utf-8"))
    jsonschema.validate(doc, schema, format_checker=jsonschema.FormatChecker())


def write(doc: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=1), "utf-8")
