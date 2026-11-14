"""Polite-client policy loader. Every fetcher reads config/policy.yml; nothing hardcodes limits."""

from __future__ import annotations

import os
from functools import cache
from typing import Any

import yaml

from babel_lag.paths import CONFIG


@cache
def load_policy() -> dict[str, Any]:
    with (CONFIG / "policy.yml").open(encoding="utf-8") as fh:
        policy: dict[str, Any] = yaml.safe_load(fh)
    return policy


def user_agent() -> str:
    """UA with contact; the email comes from BABEL_CONTACT_EMAIL so it never sits in git."""
    ua = load_policy()["user_agent"]
    email = os.environ.get("BABEL_CONTACT_EMAIL")
    return ua.replace(")", f"; {email})") if email else ua


@cache
def load_slice() -> dict[str, Any]:
    """The milestone 2 language slice (config/slice.yml, ADR-007)."""
    with (CONFIG / "slice.yml").open(encoding="utf-8") as fh:
        data: dict[str, Any] = yaml.safe_load(fh)
    return data


# Wiki database names whose pageview project code differs beyond `_` -> `-` (dump listing).
PAGEVIEW_PROJECT_EXCEPTIONS = {"be_x_oldwiki": "be-tarask"}


def pageview_project(wiki: str) -> str:
    """`zh_yuewiki` -> `zh-yue.wikipedia` (pageview dumps use the language code with hyphens)."""
    code = PAGEVIEW_PROJECT_EXCEPTIONS.get(wiki, wiki.removesuffix("wiki").replace("_", "-"))
    return f"{code}.wikipedia"


def slice_wikis() -> list[str]:
    return [f"{code}wiki" for code in load_slice()["languages"]]
