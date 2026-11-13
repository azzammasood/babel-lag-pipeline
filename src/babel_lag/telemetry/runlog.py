"""One measured record per stage run, the feed of `pipeline_metrics.json` (brief section 5c).

`Stage` wraps the work of one step: it measures wall-clock seconds, takes the records and bytes
the step counted, appends a line to `data/telemetry/runs.jsonl`, emits the OpenLineage START and
COMPLETE/FAIL pair for its job, and, when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (the `obs`
profile), records the same numbers as OpenTelemetry metrics. Only counted numbers are written;
what a stage cannot count stays null.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from types import TracebackType
from typing import Any

import orjson

from babel_lag.lineage.emit import LineageRun, pipeline_run_id
from babel_lag.lineage.graph import JOBS
from babel_lag.paths import DATA


def runs_path() -> Path:
    return DATA / "telemetry" / "runs.jsonl"


@dataclass
class StageRecord:
    run_id: str
    stage: str
    service: str
    version: str
    started_at: str
    ended_at: str = ""
    seconds: float = 0.0
    records_in: int | None = None
    records_out: int | None = None
    bytes_in: int | None = None
    bytes_out: int | None = None
    status: str = "running"
    detail: dict[str, Any] = field(default_factory=dict)


class Stage:
    def __init__(
        self, stage: str, service: str, version: str, job: str | tuple[str, ...] | None = None
    ) -> None:
        self.rec = StageRecord(pipeline_run_id(), stage, service, version,
                               datetime.now(UTC).isoformat())  # fmt: skip
        jobs = (job,) if isinstance(job, str) else (job or ())
        self.jobs = [j for j in jobs if j in JOBS]  # every lineage job this stage runs
        self._t0 = 0.0
        self._lineage: list[LineageRun] = []

    def __enter__(self) -> StageRecord:
        self._t0 = time.perf_counter()
        self._lineage = [LineageRun(j, self.rec.service) for j in self.jobs]
        return self.rec

    def __exit__(
        self, et: type[BaseException] | None, ev: BaseException | None, tb: TracebackType | None
    ) -> None:
        r = self.rec
        r.seconds = round(time.perf_counter() - self._t0, 3)
        r.ended_at = datetime.now(UTC).isoformat()
        r.status = "ok" if et is None else "failed"
        if ev is not None:
            r.detail["error"] = f"{type(ev).__name__}: {ev}"[:300]
        path = runs_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as fh:
            fh.write(orjson.dumps(asdict(r)) + b"\n")
        for run in self._lineage:
            run.finish(et is None)
        _otel(r)


def read() -> list[StageRecord]:
    path = runs_path()
    if not path.exists():
        return []
    return [StageRecord(**orjson.loads(line)) for line in path.read_bytes().splitlines() if line]


def iter_latest_run(records: list[StageRecord]) -> Iterator[StageRecord]:
    if records:
        last = records[-1].run_id
        yield from (r for r in records if r.run_id == last)


@cache
def _meter() -> Any:
    from opentelemetry import metrics
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=5000)
    provider = MeterProvider(resource=Resource.create({"service.name": "babel-lag"}),
                             metric_readers=[reader])  # fmt: skip
    metrics.set_meter_provider(provider)
    return provider.get_meter("babel_lag")


def _otel(r: StageRecord) -> None:
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    m = _meter()
    attrs = {"stage": r.stage, "service": r.service, "status": r.status}
    m.create_histogram("babel_stage_seconds", unit="s").record(r.seconds, attrs)
    if r.records_out is not None:
        m.create_counter("babel_stage_records").add(r.records_out, attrs)
    if r.bytes_in is not None:
        m.create_counter("babel_stage_bytes", unit="By").add(r.bytes_in, attrs)
    for key in ("events_per_second", "latency_p50_s", "consumer_lag"):
        if isinstance(r.detail.get(key), int | float):
            m.create_gauge(f"babel_live_{key}").set(r.detail[key], attrs)


def flush_otel() -> None:
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        from opentelemetry import metrics

        provider = metrics.get_meter_provider()
        if hasattr(provider, "force_flush"):
            provider.force_flush()
