"""Unit tests for the qiita end-user CLI scaffold.

This file grows alongside the user-CLI subcommands; for now it covers
just the parser-builds-and-help-runs contract. Subcommand-specific
tests land in follow-up commits.
"""

import pytest


def test_help_exits_cleanly(capsys):
    """`qiita --help` should print help and exit 0 even with no subcommands
    wired up. This is the cheapest smoke test that the parser is well-
    formed and the entry point is reachable."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    assert "qiita" in out
    assert "--base-url" in out


def test_no_subcommand_errors(capsys):
    """Without a subcommand argparse rejects the invocation. This locks in
    the required=True wiring on the subparser."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main([])
    # argparse exits 2 on required-arg-missing.
    assert exc_info.value.code == 2
