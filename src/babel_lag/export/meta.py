"""Static metadata export and ODCS contracts, generated from config/datasets.yml, the lineage
graph and the dbt model docs (brief section 5b).

Outputs (committed, the site renders them): `metadata/catalog.json`, `metadata/lineage.json`,
`metadata/glossary.json`, and one `contracts/<dataset>.odcs.yaml` per stored dataset.

Completeness = documented items / all items, where items are every dataset, every column that
really exists in a Delta table or dbt model (read from the tables, not from the docs), and every
documented column. A column present in data but missing from the docs lowers the number.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from babel_lag import __version__
from babel_lag.lineage.graph import COLUMNS, JOBS, datasets, edges
from babel_lag.paths import CONTRACTS, METADATA, ROOT

GLOSSARY = {
    "item": "A Wikidata item with at least one Wikipedia sitelink: the unit of a topic.",
    "sitelink": "A link from a Wikidata item to the article about it in one Wikipedia.",
    "coverage lag": "Time from an item's first article in the reference set to its article in "
    "language L (redirects, deleted and bot-created pages excluded).",
    "update lag": "Time from the start of an edit burst in the first language to a burst in "
    "language L (milestone 3).",
    "burst": "A window whose human edit count is far above the page's own baseline (robust "
    "threshold, parameters published in docs/method.md; milestone 3).",
    "demand without supply": "Readers looking for a topic in a language that has no article on "
    "it, measured by a published proxy (milestone 3).",
    "coverage depth": "Article size and revision count relative to the first language.",
    "reference set": "The languages whose earliest article defines an item's start; the "
    "20-language slice in milestone 2, all Wikipedias from milestone 3.",
    "late event": "A live event older than its batch's per-stream watermark minus 600 seconds.",
}
TYPE_MAP = {"string": "string", "large_string": "string", "int64": "integer", "int32": "integer",
            "bool": "boolean", "double": "number", "date32[day]": "date"}  # fmt: skip


def _logical(arrow_type: str) -> str:
    if arrow_type.startswith("timestamp"):
        return "timestamp"
    return TYPE_MAP.get(arrow_type, "string")


def real_columns() -> dict[str, dict[str, str]]:
    """Columns and Arrow types of every Delta table that exists locally."""
    from babel_lag.lake import delta

    out = {}
    for name in datasets():
        if name.split("_", 1)[0] in ("bronze", "silver", "gold") and delta.exists(name):
            from deltalake import DeltaTable

            schema = DeltaTable(str(delta.path_of(name))).schema().to_arrow()
            out[name] = {f.name: str(f.type) for f in schema}
    return out


def dbt_models() -> dict[str, dict[str, Any]]:
    """dbt models with their documented columns, and real columns when a manifest exists."""
    docs = yaml.safe_load((ROOT / "dbt/models/marts/marts.yml").read_text("utf-8"))
    out = {
        m["name"]: {
            "description": m.get("description", ""),
            "columns": {c["name"]: c.get("description", "") for c in m.get("columns", [])},
            "real": None,
        }
        for m in docs["models"]
    }
    catalog = ROOT / "dbt/target/catalog.json"
    if catalog.exists():
        for node in json.loads(catalog.read_text("utf-8"))["nodes"].values():
            name = node["metadata"]["name"]
            if name in out:
                out[name]["real"] = [c.lower() for c in node["columns"]]
    return out


def completeness(real: dict[str, dict[str, str]], models: dict[str, dict[str, Any]]) -> dict:
    total = documented = 0
    missing: list[str] = []
    for name, d in datasets().items():
        total += 1
        documented += bool(d.get("description"))
        cols = dict.fromkeys(real.get(name, {}), "") | (d.get("columns") or {})
        for c in cols:
            total += 1
            if (d.get("columns") or {}).get(c):
                documented += 1
            else:
                missing.append(f"{name}.{c}")
    for name, m in models.items():
        total += 1
        documented += bool(m["description"])
        for c in set(m["columns"]) | set(m["real"] or []):
            total += 1
            if m["columns"].get(c):
                documented += 1
            else:
                missing.append(f"{name}.{c}")
    return {"items": total, "documented": documented,
            "documented_pct": round(100 * documented / total, 1) if total else 0.0,
            "missing": sorted(missing)}  # fmt: skip


def contract(name: str, d: dict[str, Any], real: dict[str, str]) -> dict[str, Any]:
    cols = d.get("columns") or {}
    sla_minutes = {"30m": 30, "realtime": 1, "daily": 1440, "weekly": 10080, "monthly": 44640}
    return {
        "apiVersion": "v3.0.2",
        "kind": "DataContract",
        "id": f"babel-lag-{name.replace('_', '-')}",
        "name": name,
        "version": "1.0.0",
        "status": "active",
        "domain": d["domain"],
        "dataProduct": "babel-lag",
        "description": {
            "purpose": d["description"],
            "usage": "Aggregates for the Babel Lag site and method; never per-editor analysis.",
            "limitations": "Wikipedia only; milestone 2 covers a 20-language slice (ADR-007).",
        },
        "team": [{"username": d["owner"], "role": "owner"}],
        "servers": [
            {
                "server": "local",
                "type": "local",
                "format": "delta",
                "path": d["physical"].split(" ")[0],
            }
        ],  # fmt: skip
        "schema": [
            {
                "name": name,
                "physicalName": name,
                "physicalType": "table",
                "logicalType": "object",
                "description": d["description"],
                "tags": [d["layer"], d["classification"]],
                "properties": [
                    {
                        "name": c,
                        "logicalType": _logical(real.get(c, "string")),
                        "physicalType": real.get(c, "string"),
                        "description": text,
                        "classification": "restricted" if c == "editor_hash" else "public",
                    }
                    for c, text in cols.items()
                ],
            }
        ],
        "slaProperties": [
            {
                "property": "frequency",
                "value": sla_minutes.get(d["freshness_sla"], 1440),
                "unit": "m",
            }
        ],  # fmt: skip
        "tags": ["babel-lag", d["layer"]],
        "customProperties": [
            {"property": "license", "value": d["source_license"]},
            {"property": "containsPersonalData", "value": "false"},
        ],  # fmt: skip
    }


def build(write: bool = True) -> dict[str, Any]:
    real = real_columns()
    models = dbt_models()
    comp = completeness(real, models)
    now = datetime.now(UTC).isoformat()
    ds = datasets()
    catalog = {
        "generated_at": now,
        "babel_version": __version__,
        "completeness": {k: comp[k] for k in ("items", "documented", "documented_pct")},
        "datasets": [
            {
                "name": n, "layer": d["layer"], "namespace": d["namespace"],
                "physical": d["physical"], "description": d["description"],
                "owner": d["owner"], "domain": d["domain"],
                "classification": d["classification"], "freshness_sla": d["freshness_sla"],
                "columns": [{"name": c, "description": t, "type": real.get(n, {}).get(c)}
                            for c, t in (d.get("columns") or {}).items()],
            }
            for n, d in ds.items()
        ]
        + [
            {"name": n, "layer": "dbt", "namespace": "dbt", "physical": f"dbt model {n}",
             "description": m["description"], "owner": "babel-lag", "domain": "knowledge-spread",
             "classification": "public-aggregate", "freshness_sla": "30m",
             "columns": [{"name": c, "description": t, "type": None}
                         for c, t in m["columns"].items()]}
            for n, m in models.items()
        ],
    }  # fmt: skip
    nodes = [{"id": n, "kind": "dataset", "layer": d["layer"]} for n, d in ds.items()]
    nodes += [{"id": f"job:{j}", "kind": "job", "layer": "job"} for j in JOBS]
    nodes += [{"id": f"dbt:{m}", "kind": "dbt_model", "layer": "dbt"} for m in models]
    links = [{"from": i, "to": f"job:{j}"} for i, j, _ in edges()]
    links += [{"from": f"job:{j}", "to": o} for _, j, o in edges()]
    links = [dict(t) for t in {tuple(sorted(x.items())) for x in links}]
    links += [{"from": "gold_language_lag", "to": "dbt:mart_language_index"},
              {"from": "gold_item_pageviews", "to": "dbt:mart_language_index"},
              {"from": "dbt:mart_language_index", "to": "export_languages"},
              {"from": "gold_live_activity", "to": "dbt:mart_live_hourly"},
              {"from": "dbt:mart_live_hourly", "to": "export_live"}]  # fmt: skip
    columns = [{"to": f"{o}.{c}", "from": [f"{d}.{s}" for d, s in srcs]}
               for o, cols in COLUMNS.items() for c, srcs in cols.items()]  # fmt: skip
    lineage = {"generated_at": now, "nodes": nodes, "edges": sorted(links, key=lambda e: (
        e["from"], e["to"])), "columns": columns}  # fmt: skip
    glossary = {"generated_at": now, "terms": [{"term": k, "definition": v}
                                               for k, v in GLOSSARY.items()]}  # fmt: skip
    if write:
        METADATA.mkdir(exist_ok=True)
        for fname, doc in (("catalog", catalog), ("lineage", lineage), ("glossary", glossary)):
            (METADATA / f"{fname}.json").write_text(
                json.dumps(doc, indent=1, ensure_ascii=False) + "\n", "utf-8"
            )
        for n, d in ds.items():
            if d.get("columns"):
                path = CONTRACTS / f"{n}.odcs.yaml"
                path.write_text(yaml.safe_dump(contract(n, d, real.get(n, {})), sort_keys=False,
                                               allow_unicode=True, width=100), "utf-8")  # fmt: skip
    return {"completeness": comp, "nodes": len(nodes), "edges": len(lineage["edges"])}


def metadata_paths() -> list[Path]:
    return [METADATA / f"{n}.json" for n in ("catalog", "lineage", "glossary")]
