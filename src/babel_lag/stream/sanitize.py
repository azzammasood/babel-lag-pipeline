"""Drop personal data from EventStreams events at ingestion (brief section 4).

Allow-list, not deny-list: only the fields below survive. Usernames, IP addresses (anonymous
editors' `user`/`performer.user_text`), user ids, groups and free-text comments never leave this
function. A salted hash of the editor is kept only when a distinct-editor count is needed.

Titles are kept only for content pages (namespace 0) and never for log events: in the User and
User talk namespaces, and in block/newusers/rights logs, the title *is* a username or an IP.
`meta.uri` carries the title too, so it follows the same rule.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any

KEEP_TOP = {
    "wiki",
    "database",
    "type",
    "namespace",
    "page_namespace",
    "title",
    "page_title",
    "page_id",
    "rev_id",
    "rev_parent_id",
    "rev_timestamp",
    "rev_len",
    "rev_minor_edit",
    "page_is_redirect",
    "timestamp",
    "server_name",
    "bot",
    "minor",
    "patrolled",
}
KEEP_META = {"id", "dt", "stream", "domain", "uri"}
TITLE_FIELDS = ("title", "page_title")


def editor_hash(name: str | None) -> str | None:
    salt = os.environ.get("BABEL_EDITOR_SALT")
    if not name or not salt:
        return None
    # `salt|name`: the same string DuckDB hashes in the history parser (parsers/history.py)
    return hashlib.sha256(f"{salt}|{name}".encode()).hexdigest()[:16]


def sanitize(event: dict[str, Any], hash_editor: bool = False) -> dict[str, Any]:
    out = {k: v for k, v in event.items() if k in KEEP_TOP}
    out["meta"] = {k: v for k, v in (event.get("meta") or {}).items() if k in KEEP_META}
    perf = event.get("performer") or {}
    if "bot" not in out and "user_is_bot" in perf:
        out["bot"] = bool(perf["user_is_bot"])
    if "length" in event and isinstance(event["length"], dict):
        out["length_old"] = event["length"].get("old")
        out["length_new"] = event["length"].get("new")
    if "revision" in event and isinstance(event["revision"], dict):
        out["rev_id"] = event["revision"].get("new")
        out["rev_parent_id"] = event["revision"].get("old")
    ns = event.get("namespace", event.get("page_namespace"))
    if ns != 0 or event.get("type") == "log":
        for f in TITLE_FIELDS:
            out.pop(f, None)
        out["meta"].pop("uri", None)
    if hash_editor:
        out["editor_hash"] = editor_hash(event.get("user") or perf.get("user_text"))
    return out
