"""No username, IP address or comment survives ingestion (brief section 4): the sanitizer
unit check, then a scan of every silver and gold table and every export of the end-to-end run,
whose raw input carried the markers below."""

import json

from babel_lag.stream.sanitize import sanitize

MARKERS = ["SecretEditorName", "203.0.113.42", "2001:db8::7", "a comment naming SecretEditorName"]

RECENTCHANGE = {
    "meta": {"id": "e1", "dt": "2026-11-11T05:00:00Z", "stream": "mediawiki.recentchange"},
    "wiki": "enwiki",
    "type": "edit",
    "title": "X",
    "bot": False,
    "user": "203.0.113.42",
    "comment": MARKERS[3],
    "parsedcomment": MARKERS[3],
    "length": {"old": 10, "new": 20},
    "revision": {"old": 1, "new": 2},
}
REVISION = {
    "meta": {"id": "e2", "dt": "2026-11-11T05:00:01Z", "stream": "mediawiki.revision-create"},
    "database": "dewiki",
    "page_title": "Y",
    "rev_id": 3,
    "comment": MARKERS[3],
    "performer": {
        "user_text": "SecretEditorName",
        "user_id": 99,
        "user_is_bot": True,
        "user_groups": ["sysop"],
    },
}
ANON = {**REVISION, "performer": {"user_text": "2001:db8::7", "user_is_bot": False}}
USER_TALK = {
    **RECENTCHANGE,
    "namespace": 3,
    "title": "User talk:203.0.113.42",
    "meta": {**RECENTCHANGE["meta"], "uri": "https://en.wikipedia.org/wiki/User_talk:203.0.113.42"},
}
USER_PAGE = {**REVISION, "page_namespace": 2, "page_title": "User:SecretEditorName"}
BLOCK_LOG = {**RECENTCHANGE, "type": "log", "namespace": 0, "title": "SecretEditorName"}


def test_sanitize_drops_personal_data(monkeypatch) -> None:
    monkeypatch.setenv("BABEL_EDITOR_SALT", "test-salt")
    for ev in (RECENTCHANGE, REVISION, ANON, USER_TALK, USER_PAGE, BLOCK_LOG):
        for hashed in (False, True):
            text = json.dumps(sanitize(ev, hash_editor=hashed))
            assert not any(m in text for m in MARKERS), text
    out = sanitize(REVISION, hash_editor=True)
    article = sanitize({**RECENTCHANGE, "namespace": 0})
    assert article["title"] == "X"  # content titles survive: they are the topic, not the editor
    assert out["bot"] is True and out["rev_id"] == 3 and len(out["editor_hash"]) == 16


def test_no_personal_data_downstream(e2e) -> None:
    """Every silver/gold Delta file and every export file of the e2e run, as text."""
    import duckdb

    scanned = 0
    con = duckdb.connect()
    for table in sorted((e2e / "lake").glob("*/*")):
        if table.parent.name == "bronze":
            continue
        for f in table.rglob("*.parquet"):
            rows = con.execute(f"SELECT * FROM read_parquet('{f.as_posix()}')").fetchall()
            text = str(rows)
            assert not any(m in text for m in MARKERS), f
            scanned += 1
    for f in (e2e / "export").rglob("*.json"):
        text = f.read_text("utf-8")
        assert not any(m in text for m in MARKERS), f
        scanned += 1
    assert scanned >= 10
