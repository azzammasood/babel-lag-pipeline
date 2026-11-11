"""End-to-end smoke (brief section 0 rule 10). Grows into the full pipeline smoke in milestone 2."""

import json

from typer.testing import CliRunner

from babel_lag.cli import app


def test_cli_machine_json() -> None:
    result = CliRunner().invoke(app, ["machine", "--json"])
    assert result.exit_code == 0, result.output
    info = json.loads(result.output)
    assert info["disk_budget_gb"] <= info["free_disk_gb"]
