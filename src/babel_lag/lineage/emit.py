"""OpenLineage events for every stage (brief section 5b; Keystone's emit pattern).

Each stage run writes a START and a COMPLETE (or FAIL) event, with schema facets from
config/datasets.yml and column-lineage facets from `graph.COLUMNS`, to
`data/lineage/events.jsonl` always; when `OPENLINEAGE_URL` is set (Marquez in the `lineage`
profile) the same events are posted to its `/api/v1/lineage` endpoint. A down Marquez never fails
a run: the file copy remains and `babel lineage replay` re-sends it.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

import httpx
import orjson
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    column_lineage_dataset,
    documentation_dataset,
    job_type_job,
    parent_run,
    schema_dataset,
)
from openlineage.client.serde import Serde
from openlineage.client.uuid import generate_new_uuid

from babel_lag.lineage.graph import COLUMNS, JOBS, datasets
from babel_lag.paths import DATA

JOB_NS = "babel-lag"
PRODUCER = "https://github.com/azzammasood/babel-lag-pipeline"


def events_path() -> Path:
    return DATA / "lineage" / "events.jsonl"


@cache
def pipeline_run_id() -> str:
    """One parent run per process (a CLI invocation), so Marquez groups a run's stages."""
    return os.environ.get("BABEL_PARENT_RUN_ID") or str(generate_new_uuid())


def ds_ref(name: str) -> tuple[str, str]:
    d = datasets()[name]
    return d["namespace"], name if d["namespace"] in ("babel-lag", "babel-lag-export") else d[
        "physical"
    ]


def _facets(name: str, with_columns: bool) -> dict[str, Any]:
    d = datasets()[name]
    out: dict[str, Any] = {"documentation": documentation_dataset.DocumentationDatasetFacet(
        description=d["description"])}  # fmt: skip
    cols = d.get("columns") or {}
    if cols:
        out["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(name=c, description=t)
                for c, t in cols.items()
            ]  # fmt: skip
        )
    if with_columns and name in COLUMNS:
        fields = {}
        for col, srcs in COLUMNS[name].items():
            inputs = []
            for ds, c in srcs:
                ns, nm = ds_ref(ds)
                inputs.append(column_lineage_dataset.InputField(namespace=ns, name=nm, field=c))
            fields[col] = column_lineage_dataset.Fields(
                inputFields=inputs, transformationDescription="", transformationType=""
            )
        out["columnLineage"] = column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)
    return out


def build(job: str, run_id: str, state: RunState, engine: str = "babel") -> dict[str, Any]:
    ins, outs = JOBS[job]
    ev = RunEvent(
        eventType=state,
        eventTime=datetime.now(UTC).isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "parent": parent_run.ParentRunFacet(
                    run=parent_run.Run(runId=pipeline_run_id()),
                    job=parent_run.Job(namespace=JOB_NS, name="babel_pipeline"),
                )
            },
        ),  # fmt: skip
        job=Job(
            namespace=JOB_NS,
            name=job,
            facets={
                "jobType": job_type_job.JobTypeJobFacet(
                    processingType="STREAMING" if "live" in job else "BATCH",
                    integration=engine.upper(),
                    jobType="JOB",
                )
            },
        ),  # fmt: skip
        inputs=[InputDataset(*ds_ref(i), facets=_facets(i, False)) for i in ins],
        outputs=[OutputDataset(*ds_ref(o), facets=_facets(o, True)) for o in outs],
        producer=PRODUCER,
    )
    return orjson.loads(Serde.to_json(ev))


def write(events: list[dict[str, Any]], forward: bool = True) -> int:
    """Append to the events file and post to Marquez when configured; returns failed posts."""
    path = events_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as fh:
        for ev in events:
            fh.write(orjson.dumps(ev) + b"\n")
    url = os.environ.get("OPENLINEAGE_URL")
    if not (forward and url):
        return 0
    failed = 0
    with httpx.Client(timeout=10) as client:
        for ev in events:
            try:
                client.post(f"{url.rstrip('/')}/api/v1/lineage", json=ev).raise_for_status()
            except httpx.HTTPError:
                failed += 1
    return failed


class LineageRun:
    def __init__(self, job: str, engine: str = "babel") -> None:
        self.job, self.engine, self.run_id = job, engine, str(generate_new_uuid())
        write([build(job, self.run_id, RunState.START, engine)])

    def finish(self, ok: bool) -> None:
        write([build(self.job, self.run_id, RunState.COMPLETE if ok else RunState.FAIL,
                     self.engine)])  # fmt: skip
