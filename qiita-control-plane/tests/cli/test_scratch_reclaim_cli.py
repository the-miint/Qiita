"""Argument and environment checks for `qiita-admin scratch reclaim`; none reach the database."""

import pytest

from qiita_control_plane.cli import admin as cli


def test_older_than_is_required(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["scratch", "reclaim"])
    assert exc.value.code == 2
    assert "--older-than" in capsys.readouterr().err


@pytest.mark.parametrize("days", ["0", "-3", "soon", "9999999999"])
def test_older_than_must_be_a_positive_integer(capsys, days):
    with pytest.raises(SystemExit) as exc:
        cli.main(["scratch", "reclaim", "--older-than", days])
    assert exc.value.code == 2
    assert "--older-than" in capsys.readouterr().err


@pytest.mark.parametrize("scratch", [None, "relative/scratch"])
def test_scratch_must_be_an_absolute_path(monkeypatch, capsys, scratch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://nowhere/none")
    if scratch is None:
        monkeypatch.delenv("PATH_SCRATCH", raising=False)
    else:
        monkeypatch.setenv("PATH_SCRATCH", scratch)

    assert cli.main(["scratch", "reclaim", "--older-than", "14"]) == 2
    assert "PATH_SCRATCH" in capsys.readouterr().err


def test_database_url_is_required(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert cli.main(["scratch", "reclaim", "--older-than", "14"]) == 2
    assert "DATABASE_URL" in capsys.readouterr().err
