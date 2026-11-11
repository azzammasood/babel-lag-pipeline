"""Versioned JSON export (Keystone's envelope pattern): `data/export/v1/<dataset>.json`.

Rows come from the dbt marts: DuckDB (`free`) or Databricks SQL (`full`). `produced_by` comes from
the run log's latest profile decision; with none recorded an export refuses to be written. Every
document is validated against contracts/export_envelope.schema.json and the dataset's row schema.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import jsonschema

from babel_lag import __version__
from babel_lag.paths import CONTRACTS, DATA, EXPORT
from babel_lag.telemetry.runlog import read

ENVELOPE_VERSION = "1.1"
CORRECTIONS_URL = "https://github.com/azzammasood/babel-lag-pipeline/issues"
SOURCES = {
    "history": {"name": "MediaWiki history dumps (Wikimedia, CC0)",
                "url": "https://dumps.wikimedia.org/other/mediawiki_history/", "terms": "CC0"},
    "sitelinks": {"name": "Wikidata wb_items_per_site dump (Wikimedia, CC0)",
                  "url": "https://dumps.wikimedia.org/wikidatawiki/latest/", "terms": "CC0"},
    "pageviews": {"name": "Pageview complete dumps (Wikimedia, CC0)",
                  "url": "https://dumps.wikimedia.org/other/pageview_complete/", "terms": "CC0"},
    "eventstreams": {"name": "Wikimedia EventStreams (recentchange, revision-create, page-create)",
                     "url": "https://stream.wikimedia.org/v2/stream/", "terms": "CC0"},
}  # fmt: skip
DATASETS = {
    "languages": ("mart_language_index", ["history", "sitelinks", "pageviews"], "language"),
    "live": ("mart_live_hourly", ["eventstreams"], "wiki"),
}


class NoProfileRecorded(RuntimeError):
    pass


def recorded_profile() -> dict[str, Any]:
    for rec in reversed(read()):
        if rec.stage == "profile" and rec.status == "ok" and rec.detail.get("produced_by"):
            return {"profile": rec.detail["produced_by"], "fallback": rec.detail["fallback"],
                    "run_id": rec.run_id, "babel_version": __version__}  # fmt: skip
    raise NoProfileRecorded("no profile decision in data/telemetry/runs.jsonl")


def _json(v: Any) -> Any:
    if isinstance(v, datetime):
        return (v if v.tzinfo else v.replace(tzinfo=UTC)).astimezone(UTC).isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def mart_rows(model: str, profile: str) -> list[dict[str, Any]]:
    if profile == "full":
        from babel_lag.cloud.databricks import Workspace

        ws = Workspace.from_env()
        cols = [r[0] for r in ws.sql(f"DESCRIBE TABLE workspace.babel_marts.{model}")
                if r[0] and not r[0].startswith("#")]  # fmt: skip
        data = ws.sql(f"SELECT * FROM workspace.babel_marts.{model}")
        rows = [dict(zip(cols, r, strict=False)) for r in data]
        for r in rows:  # the statement API returns strings: restore numbers
            for k, v in r.items():
                if isinstance(v, str) and k not in ("wiki", "language", "hour"):
                    r[k] = float(v) if "." in v else int(v)
        return rows
    import duckdb

    con = duckdb.connect(str(DATA / "warehouse" / "babel.duckdb"), read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    res = con.sql(f"SELECT * FROM main.{model}")
    cols = res.columns
    rows = [dict(zip(cols, r, strict=True)) for r in res.fetchall()]
    con.close()
    return rows


def validate(doc: dict[str, Any]) -> None:
    schema = json.loads((CONTRACTS / "export_envelope.schema.json").read_text("utf-8"))
    jsonschema.validate(doc, schema, format_checker=jsonschema.FormatChecker())
    if doc["row_count"] != len(doc["rows"]):
        raise jsonschema.ValidationError("row_count does not match rows")
    row_schema = CONTRACTS / f"{doc['dataset']}_row.schema.json"
    if row_schema.exists():
        v = jsonschema.Draft202012Validator(
            json.loads(row_schema.read_text("utf-8")),
            format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER,
        )
        for i, row in enumerate(doc["rows"]):
            err = next(v.iter_errors(row), None)
            if err is not None:
                raise jsonschema.ValidationError(f"{doc['dataset']} row {i}: {err.message}")


def write_all(produced: dict[str, Any] | None = None, out: Path = EXPORT / "v1") -> dict[str, int]:
    produced = produced or recorded_profile()
    out.mkdir(parents=True, exist_ok=True)
    counts = {}
    for dataset, (model, sources, level) in DATASETS.items():
        rows = [{k: _json(v) for k, v in r.items()} for r in mart_rows(model, produced["profile"])]
        key = "hour" if dataset == "live" else None
        times = sorted(r[key] for r in rows) if key else []
        doc = {
            "envelope_version": ENVELOPE_VERSION,
            "dataset": dataset,
            "dataset_version": "v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "produced_by": produced,
            "window": {
                "start": times[0] if times else None,
                "end": times[-1] if times else None,
                "periods": [],
            },  # fmt: skip
            "sources": [SOURCES[s] for s in sources],
            "privacy": {"level": level, "people": "aggregate-only"},
            "corrections": CORRECTIONS_URL,
            "row_count": len(rows),
            "rows": sorted(rows, key=lambda r: tuple(str(v) for v in r.values())),
        }
        validate(doc)
        (out / f"{dataset}.json").write_text(json.dumps(doc, ensure_ascii=False), "utf-8")
        counts[dataset] = len(rows)
    return counts
