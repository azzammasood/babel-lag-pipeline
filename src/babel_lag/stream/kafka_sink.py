"""Kafka sink for the `full` profile: sanitized events as Avro values, schema in the registry.

The subject `babel.live.events-value` is registered with BACKWARD compatibility (the registry's
default in compose); a schema change that breaks readers is refused by the registry. Keys are the
wiki, so one wiki's events stay ordered within a partition.
"""

from __future__ import annotations

import json
import os
from typing import Any

from babel_lag.paths import CONTRACTS

TOPIC = "babel.live.events"


class KafkaSink:
    def __init__(self) -> None:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        self._ctx = SerializationContext(TOPIC, MessageField.VALUE)
        registry = SchemaRegistryClient(
            {"url": os.environ.get("BABEL_SCHEMA_REGISTRY", "http://127.0.0.1:8081")}
        )
        schema = (CONTRACTS / "avro" / "live_event.avsc").read_text("utf-8")
        self._ser = AvroSerializer(registry, schema)
        self._p = Producer({"bootstrap.servers": os.environ.get("BABEL_KAFKA", "127.0.0.1:9092"),
                            "linger.ms": 200, "compression.type": "zstd",
                            "enable.idempotence": True})  # fmt: skip
        self.sent = 0

    def __call__(self, ev: dict[str, Any]) -> None:
        meta = ev["meta"]
        rec = {"event_id": meta["id"], "event_dt": meta["dt"], "stream": meta["stream"],
               "wiki": ev.get("wiki") or ev.get("database"),
               "payload": json.dumps(ev, ensure_ascii=False)}  # fmt: skip
        key = (rec["wiki"] or "").encode()
        self._p.produce(TOPIC, key=key, value=self._ser(rec, self._ctx))
        self.sent += 1
        if self.sent % 5000 == 0:
            self._p.poll(0)

    def close(self) -> None:
        self._p.flush(30)
