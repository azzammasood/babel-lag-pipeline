"""OpenMetadata ingestion over its REST API (compose `catalog` profile, brief section 5b).

Pushes the same metadata the site renders: a database service `babel-lag` with one schema per
layer, every dataset of config/datasets.yml as a table with column descriptions, owner team,
domain, classification tags and glossary terms, then table- and column-level lineage edges from
`lineage/graph.py` (live event to export field). Idempotent: every call is a PUT (create or update).

The local instance's default admin login is used (a localhost development container only).
"""

from __future__ import annotations

import base64
import os
from typing import Any

import httpx

from babel_lag.export.meta import GLOSSARY
from babel_lag.lineage.graph import COLUMNS, JOBS, datasets, edges
from babel_lag.telemetry.runlog import Stage

URL = os.environ.get("BABEL_OPENMETADATA", "http://127.0.0.1:8585")
SERVICE, DATABASE = "babel-lag", "lake"
TYPES = {"timestamp": "TIMESTAMP", "integer": "BIGINT", "boolean": "BOOLEAN", "number": "DOUBLE",
         "date": "DATE", "string": "STRING"}  # fmt: skip


class OM:
    def __init__(self) -> None:
        pw = base64.b64encode(os.environ.get("BABEL_OM_PASSWORD", "admin").encode()).decode()
        email = os.environ.get("BABEL_OM_USER", "admin@open-metadata.org")
        r = httpx.post(f"{URL}/api/v1/users/login", json={"email": email, "password": pw},
                       timeout=30)  # fmt: skip
        r.raise_for_status()
        token = r.json()["accessToken"]
        self.c = httpx.Client(base_url=f"{URL}/api/v1", timeout=60,
                              headers={"Authorization": f"Bearer {token}"})  # fmt: skip
        self.calls = 0

    def put(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        r = self.c.put(path, json=body)
        self.calls += 1
        if r.status_code >= 400:
            raise RuntimeError(f"PUT {path}: HTTP {r.status_code}: {r.text[:300]}")
        return r.json() if r.content else {}


def _dtype(name: str, real: dict[str, str]) -> str:
    from babel_lag.export.meta import _logical

    return TYPES[_logical(real.get(name, "string"))]


def push() -> dict[str, int]:
    from babel_lag.export.meta import real_columns

    with Stage("openmetadata", "openmetadata", "1.9") as st:
        om = OM()
        team = om.put("/teams", {"name": "babel-lag", "teamType": "Group",
                                 "description": "Owner of every Babel Lag dataset."})  # fmt: skip
        domain = om.put("/domains", {"name": "knowledge-spread", "domainType": "Source-aligned",
                                     "description": "How new knowledge spreads across the "
                                     "language editions of Wikipedia."})  # fmt: skip
        owners = [{"id": team["id"], "type": "team"}]
        om.put("/classifications", {"name": "BabelLag", "description": "Babel Lag data classes."})
        for tag, text in (("public-aggregate", "Aggregates only; no personal data."),
                          ("source-personal", "Upstream source carrying usernames and IPs; "
                           "sanitized before storage."),
                          ("pseudonymous-hash", "Salted hash for distinct counts; never "
                           "published.")):  # fmt: skip
            om.put("/tags", {"classification": "BabelLag", "name": tag, "description": text})
        om.put("/services/databaseServices", {
            "name": SERVICE, "serviceType": "CustomDatabase",
            "description": "Babel Lag lakehouse (Delta via delta-rs; Unity Catalog in full).",
            "connection": {"config": {"type": "CustomDatabase", "sourcePythonClass": "babel_lag"}},
        })  # fmt: skip
        om.put(
            "/databases",
            {
                "name": DATABASE,
                "service": SERVICE,
                "description": "Sources, landing, bronze, silver, gold, exports.",
            },
        )
        ds = datasets()
        real = real_columns()
        for layer in sorted({d["layer"] for d in ds.values()}):
            om.put("/databaseSchemas", {"name": layer, "database": f"{SERVICE}.{DATABASE}",
                                        "description": f"Babel Lag {layer} datasets."})  # fmt: skip
        ids: dict[str, str] = {}
        for name, d in ds.items():
            cols = d.get("columns") or {"_dataset": d["description"]}
            body = {
                "name": name,
                "databaseSchema": f"{SERVICE}.{DATABASE}.{d['layer']}",
                "description": f"{d['description']}\n\nPhysical: `{d['physical']}`. Freshness "
                f"SLA: {d['freshness_sla']}. License: {d['source_license']}.",
                "tableType": "Regular",
                "columns": [{"name": c.replace(".", "_"), "dataType": _dtype(c, real.get(name, {})),
                             "description": t,
                             "tags": [{"tagFQN": "BabelLag.pseudonymous-hash"}]
                             if c == "editor_hash" else []}
                            for c, t in cols.items()],
                "tags": [{"tagFQN": f"BabelLag.{d['classification']}"}],
                "owners": owners,
                "domains": [domain["fullyQualifiedName"]],
            }  # fmt: skip
            ids[name] = om.put("/tables", body)["id"]
        om.put("/glossaries", {"name": "babel-lag", "displayName": "Babel Lag",
                               "description": "Terms of the Babel Lag method.",
                               "owners": owners})  # fmt: skip
        for term, text in GLOSSARY.items():
            om.put("/glossaryTerms", {"glossary": "babel-lag", "name": term.replace(" ", "-"),
                                      "displayName": term, "description": text})  # fmt: skip
        n_edges = 0
        seen = set()
        for i, job, o in edges():
            if (i, o) in seen:
                continue
            seen.add((i, o))
            fq = f"{SERVICE}.{DATABASE}"
            cols = [
                {
                    "fromColumns": [f"{fq}.{ds[i]['layer']}.{i}.{c.replace('.', '_')}"],
                    "toColumn": f"{fq}.{ds[o]['layer']}.{o}.{oc.replace('.', '_')}",
                }
                for oc, srcs in COLUMNS.get(o, {}).items()
                for (si, c) in srcs
                if si == i
            ]
            om.put("/lineage", {"edge": {
                "fromEntity": {"id": ids[i], "type": "table"},
                "toEntity": {"id": ids[o], "type": "table"},
                "lineageDetails": {"description": f"job {job}: {JOBS[job]}"[:250],
                                   "columnsLineage": cols, "source": "Manual"}}})  # fmt: skip
            n_edges += 1
        st.records_out = len(ids)
        st.detail.update(tables=len(ids), edges=n_edges, calls=om.calls,
                         glossary_terms=len(GLOSSARY))  # fmt: skip
    return {"tables": len(ids), "edges": n_edges}
