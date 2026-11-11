"""Shared fixtures for the `free` (DuckDB) vs `full` (Spark) parity test: small, hand-written,
covering duplicate events, a late event, bot edits, redirects, deleted and bot-created pages."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa

from babel_lag.parsers import history, pageviews, sitelinks
from babel_lag.stream.normalize import BRONZE_SCHEMA


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def _lineage(rows: list[dict], schema: pa.Schema) -> pa.Table:
    extra = {
        "source_file": "fixture",
        "source_sha256": "0" * 64,
        "fetched_at": ts("2026-11-11T00:00:00"),
    }
    for name, typ in (("source_file", pa.string()), ("source_sha256", pa.string()),
                      ("fetched_at", pa.timestamp("us", tz="UTC"))):  # fmt: skip
        schema = schema.append(pa.field(name, typ))
    return pa.Table.from_pylist([{**r, **extra} for r in rows], schema)


def _ev(
    eid,
    dt,
    stream,
    wiki,
    etype,
    ns=0,
    bot=False,
    redirect=None,
    editor="h1",
    f="b1",
    at="2026-11-11T05:20:00",
):  # noqa: E501
    return {
        "event_id": eid, "event_dt": ts(dt), "stream": f"mediawiki.{stream}", "wiki": wiki,
        "event_type": etype, "namespace": ns, "page_id": None, "page_title": "T", "rev_id": None,
        "is_bot": bot, "is_redirect": redirect, "length_new": 10, "editor_hash": editor,
        "source_file": f, "ingested_at": ts(at),
    }  # fmt: skip


BRONZE = [
    _ev("a", "2026-11-11T05:00:01", "recentchange", "urwiki", "edit"),
    _ev("b", "2026-11-11T05:03:00", "recentchange", "urwiki", "edit", bot=True, editor="h2"),
    _ev("c", "2026-11-11T05:16:00", "recentchange", "urwiki", "new", editor="h3"),
    _ev("c", "2026-11-11T05:16:00", "recentchange", "urwiki", "new", editor="h3", f="b2",
        at="2026-11-11T05:40:00"),  # duplicate, redelivered after a reconnect
    _ev("d", "2026-11-11T05:35:00", "recentchange", "zuwiki", "edit", f="b2", at="2026-11-11T05:40:00"),
    _ev("e", "2026-11-11T05:02:00", "recentchange", "zuwiki", "edit", f="b3",
        at="2026-11-11T06:00:00"),  # late: 33 minutes behind the watermark of its stream
    _ev("f", "2026-11-11T05:05:00", "page-create", "urwiki", "page-create", redirect=False),
    _ev("g", "2026-11-11T05:06:00", "page-create", "urwiki", "page-create", redirect=True,
        f="pc1"),  # a redirect is not a new article
    _ev("h", "2026-11-11T05:07:00", "revision-create", "zuwiki", "revision-create", ns=2, f="rv1"),
    _ev("a", "2026-11-11T05:00:01", "recentchange", "urwiki", "edit", f="b3",
        at="2026-11-11T06:00:00"),  # duplicate of the first event, late too: dropped as duplicate
]  # fmt: skip


def _rev(wiki, pid, title, rid, at, bot=False, redirect=False, deleted=False, reverted=False):
    return {
        "wiki": wiki, "page_id": pid, "page_title": title, "rev_id": rid, "rev_parent_id": None,
        "rev_ts": ts(at), "rev_bytes": 100, "is_bot": bot, "is_reverted": reverted,
        "is_revert": False, "page_is_redirect": redirect, "page_is_deleted": deleted,
        "page_created_at": None, "rev_deleted_with_page": deleted, "editor_hash": "x",
    }  # fmt: skip


REVISIONS = [
    _rev("urwiki", 1, "Pakistan", 10, "2005-01-01T00:00:00"),
    _rev("urwiki", 1, "Pakistan", 11, "2006-01-01T00:00:00", bot=True),
    _rev("zuwiki", 7, "IPakistan", 70, "2007-03-01T12:00:00"),
    _rev("zuwiki", 7, "IPakistan", 71, "2007-03-02T12:00:00", reverted=True),
    _rev("swwiki", 3, "Pakistani", 30, "2010-06-01T00:00:00"),
    _rev("urwiki", 2, "Moon", 20, "2004-02-01T00:00:00"),
    _rev("zuwiki", 8, "Inyanga", 80, "2004-02-01T00:00:00"),  # tie: both first
    _rev("swwiki", 4, "Mwezi", 40, "2012-01-01T00:00:00", bot=True),  # bot-created: excluded
    _rev("swwiki", 5, "Mwezi_(redirect)", 50, "2013-01-01T00:00:00", redirect=True),
    _rev("urwiki", 6, "Deleted", 60, "2003-01-01T00:00:00", deleted=True),
]
SITELINKS = [
    {"item_id": 843, "wiki": "urwiki", "page_title": "Pakistan"},
    {"item_id": 843, "wiki": "zuwiki", "page_title": "IPakistan"},
    {"item_id": 843, "wiki": "swwiki", "page_title": "Pakistani"},
    {"item_id": 405, "wiki": "urwiki", "page_title": "Moon"},
    {"item_id": 405, "wiki": "zuwiki", "page_title": "Inyanga"},
    {"item_id": 405, "wiki": "swwiki", "page_title": "Mwezi"},
    {"item_id": 99, "wiki": "urwiki", "page_title": "Deleted"},
]
PAGEVIEWS = [
    {"wiki": "urwiki", "page_id": 1, "page_title": "Pakistan", "day": date(2026, 11, 10), "views": 50},
    {"wiki": "urwiki", "page_id": 1, "page_title": "Pakistan", "day": date(2026, 11, 9), "views": 40},
    {"wiki": "zuwiki", "page_id": None, "page_title": "Inyanga", "day": date(2026, 11, 10), "views": 3},
    {"wiki": "swwiki", "page_id": 9, "page_title": "Not_linked", "day": date(2026, 11, 10), "views": 8},
]  # fmt: skip


def inputs() -> dict[str, pa.Table]:
    return {
        "bronze_live_events": pa.Table.from_pylist(BRONZE, BRONZE_SCHEMA),
        "silver_page_revisions": _lineage(REVISIONS, history.SCHEMA),
        "silver_sitelinks": _lineage(SITELINKS, sitelinks.SCHEMA),
        "silver_pageviews_daily": _lineage(PAGEVIEWS, pageviews.SCHEMA),
    }
