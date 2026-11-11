"""Repository paths. Bulk data lives in data/ (git-ignored); override with BABEL_DATA."""

from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
CONTRACTS = ROOT / "contracts"
METADATA = ROOT / "metadata"
DATA = Path(os.environ.get("BABEL_DATA", ROOT / "data"))
LAKE = DATA / "lake"  # Delta tables (bronze/silver/gold), written by delta-rs
LANDING = DATA / "landing"  # sanitized NDJSON micro-batches (ADR-005 names)
STATE = DATA / "state"  # collector checkpoints
EXPORT = DATA / "export"  # versioned JSON export (the site reads this)
