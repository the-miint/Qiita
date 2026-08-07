"""Unit tests for the qiita-admin terminology subcommands. Nothing here runs
ROBOT; the load cases that reach Postgres carry the db marker."""

import asyncio
import json

import pytest
from qiita_common.models import TerminologyTermObsoletionKind

from qiita_control_plane.cli import admin as cli
from qiita_control_plane.terminology import (
    CLOSURE_TSV_COLUMNS,
    CLOSURE_TSV_FILENAME,
    MANIFEST_FILENAME,
    TERMS_TSV_FILENAME,
    _parse_terms_tsv,
    load_manifest,
    sha256_of_file,
)
from qiita_control_plane.testing.terminology import (
    TAXDUMP_ARCHIVE_FILENAME,
    parsed_term,
    write_robot_export_tsv,
    write_taxdump,
)

# The exact line an operator is shown. Pinned in full rather than rebuilt from
# the argv builder, so a change to what ROBOT is asked for is visible here as
# the operator-facing string it becomes.
_EXPECTED_ROBOT_COMMAND = (
    "robot export --input source.owl"
    " --header 'ID|LABEL|owl:deprecated|IAO:0100001|oboInOwl:hasAlternativeId'"
    " --include classes --export robot-export.tsv"
)


# =============================================================================
# terminology robot-command
# =============================================================================


def test_terminology_robot_command(capsys):
    """Tests the case where only the input is named: the printed command runs
    plain robot and writes the default export, with the pipe-bearing column
    selection quoted so a shell cannot read it as a pipeline."""
    rc = cli.main(["terminology", "robot-command", "--input", "source.owl"])

    assert rc == 0
    assert capsys.readouterr().out.strip() == _EXPECTED_ROBOT_COMMAND


def test_terminology_robot_command_executable(capsys):
    """Tests the case where a container runtime and export name are named:
    the runtime's tokens lead the command and the export name is carried
    through."""
    rc = cli.main(
        [
            "terminology",
            "robot-command",
            "--input",
            "uberon-base.owl",
            "--export",
            "uberon-export.tsv",
            "--executable",
            "apptainer exec /images/robot.sif robot",
        ]
    )

    assert rc == 0
    expected = (
        "apptainer exec /images/robot.sif robot export --input uberon-base.owl"
        " --header 'ID|LABEL|owl:deprecated|IAO:0100001|oboInOwl:hasAlternativeId'"
        " --include classes --export uberon-export.tsv"
    )
    assert capsys.readouterr().out.strip() == expected


# =============================================================================
# terminology prepare-owl
# =============================================================================


def test_terminology_prepare_owl(tmp_path, capsys):
    """Tests the case where an export is prepared: both release tables and a
    manifest declaring their digests are written, and the terms table reads
    back through the import-side parser."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(
        export_path,
        [
            ("UBERON:0001", "mouth", "", "", ""),
            ("UBERON:0002", "tooth", "", "", ""),
            ("UBERON:0003", "obsolete molar", "true^^xsd:boolean", "UBERON:0002", ""),
        ],
    )

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(export_path),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
        ]
    )

    assert rc == 0

    expected_terms = [
        parsed_term("UBERON:0001", "mouth"),
        parsed_term("UBERON:0002", "tooth"),
        parsed_term(
            "UBERON:0003",
            "obsolete molar",
            is_obsolete=True,
            replaced_by_term_id="UBERON:0002",
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
        ),
    ]
    assert _parse_terms_tsv(tmp_path / TERMS_TSV_FILENAME) == expected_terms

    closure_lines = (tmp_path / CLOSURE_TSV_FILENAME).read_text().splitlines()
    assert closure_lines == ["\t".join(CLOSURE_TSV_COLUMNS)]

    # The manifest must describe the files as written, so the load-side
    # verification of the same digests passes.
    manifest = load_manifest(tmp_path)
    assert manifest.name == "uberon"
    assert manifest.version == "2026-04-15"
    assert manifest.terms.path == TERMS_TSV_FILENAME
    assert manifest.terms.sha256 == sha256_of_file(tmp_path / TERMS_TSV_FILENAME)
    assert manifest.closure.path == CLOSURE_TSV_FILENAME
    assert manifest.closure.sha256 == sha256_of_file(tmp_path / CLOSURE_TSV_FILENAME)

    summary = json.loads(capsys.readouterr().out)
    assert summary == {
        "name": "uberon",
        "version": "2026-04-15",
        "terms_written": 3,
        "output_dir": str(tmp_path),
    }


def test_terminology_prepare_owl_term_id_prefix(tmp_path):
    """Tests the case where a term id prefix is given: classes imported from
    other vocabularies are left out of the written table."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(
        export_path,
        [
            ("UBERON:0001", "mouth", "", "", ""),
            ("CL:0000000", "cell", "", "", ""),
        ],
    )

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(export_path),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
            "--term-id-prefix",
            "UBERON:",
        ]
    )

    assert rc == 0
    assert _parse_terms_tsv(tmp_path / TERMS_TSV_FILENAME) == [parsed_term("UBERON:0001", "mouth")]


def test_terminology_prepare_owl_output_dir(tmp_path):
    """Tests the case where an output directory is named: the release files
    land there rather than beside the export."""
    export_dir = tmp_path / "incoming"
    export_dir.mkdir()
    write_robot_export_tsv(export_dir / "robot-export.tsv", [("UBERON:0001", "mouth", "", "", "")])
    output_dir = tmp_path / "staged"
    output_dir.mkdir()

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(export_dir / "robot-export.tsv"),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    assert (output_dir / TERMS_TSV_FILENAME).exists()
    assert (output_dir / MANIFEST_FILENAME).exists()
    assert not (export_dir / TERMS_TSV_FILENAME).exists()


def test_terminology_prepare_owl_missing_export(tmp_path, capsys):
    """Tests the case where the named export does not exist."""
    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(tmp_path / "absent.tsv"),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
        ]
    )

    assert rc == 1
    assert "No ROBOT export" in capsys.readouterr().err


def test_terminology_prepare_owl_malformed_export(tmp_path, capsys):
    """Tests the case where the export is missing a requested column."""
    export_path = tmp_path / "robot-export.tsv"
    export_path.write_text("ID\tLABEL\nUBERON:0001\tmouth\n")

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(export_path),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
        ]
    )

    assert rc == 1
    assert "missing column" in capsys.readouterr().err


def test_terminology_requires_subcommand():
    """Tests the case where no terminology subcommand is given."""
    with pytest.raises(SystemExit):
        cli.main(["terminology"])


# =============================================================================
# terminology prepare-taxdump
# =============================================================================


def test_terminology_prepare_taxdump(tmp_path, capsys):
    """Tests the case where a taxdump archive is prepared: both release tables
    and a manifest declaring their digests are written, and the terms table
    reads back through the import-side parser."""
    archive_path = write_taxdump(
        tmp_path,
        names=[
            ("2", "Bacteria", "Bacteria <bacteria>", "scientific name"),
            ("2", "eubacteria", "", "genbank common name"),
            ("9606", "Homo sapiens", "", "scientific name"),
        ],
        merged=[("30", "9606")],
        delnodes=[("777",)],
    )

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump-zip",
            str(archive_path),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 0

    expected_terms = [
        parsed_term("2", "Bacteria", alternate_label="eubacteria"),
        parsed_term("9606", "Homo sapiens"),
        parsed_term(
            "30",
            None,
            is_obsolete=True,
            replaced_by_term_id="9606",
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
        ),
        parsed_term(
            "777",
            None,
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
        ),
    ]
    assert _parse_terms_tsv(tmp_path / TERMS_TSV_FILENAME) == expected_terms

    closure_lines = (tmp_path / CLOSURE_TSV_FILENAME).read_text().splitlines()
    assert closure_lines == ["\t".join(CLOSURE_TSV_COLUMNS)]

    # The manifest must describe the files as written, so the load-side
    # verification of the same digests passes.
    manifest = load_manifest(tmp_path)
    assert manifest.name == "NCBI Taxonomy"
    assert manifest.version == "2026-08-01"
    assert manifest.terms.path == TERMS_TSV_FILENAME
    assert manifest.terms.sha256 == sha256_of_file(tmp_path / TERMS_TSV_FILENAME)
    assert manifest.closure.path == CLOSURE_TSV_FILENAME
    assert manifest.closure.sha256 == sha256_of_file(tmp_path / CLOSURE_TSV_FILENAME)

    summary = json.loads(capsys.readouterr().out)
    assert summary == {
        "name": "NCBI Taxonomy",
        "version": "2026-08-01",
        "terms_written": 4,
        "output_dir": str(tmp_path),
    }


def test_terminology_prepare_taxdump_output_dir(tmp_path):
    """Tests the case where an output directory is named: the release files
    land there rather than beside the archive."""
    archive_dir = tmp_path / "incoming"
    archive_dir.mkdir()
    archive_path = write_taxdump(archive_dir, names=[("2", "Bacteria", "", "scientific name")])
    output_dir = tmp_path / "staged"
    output_dir.mkdir()

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump-zip",
            str(archive_path),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 0
    assert (output_dir / TERMS_TSV_FILENAME).exists()
    assert (output_dir / MANIFEST_FILENAME).exists()
    assert not (archive_dir / TERMS_TSV_FILENAME).exists()


def test_terminology_prepare_taxdump_missing_archive(tmp_path, capsys):
    """Tests the case where the named archive does not exist."""
    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump-zip",
            str(tmp_path / TAXDUMP_ARCHIVE_FILENAME),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 1
    assert "No taxdump archive" in capsys.readouterr().err


def test_terminology_prepare_taxdump_not_an_archive(tmp_path, capsys):
    """Tests the case where the named path exists but is not an archive."""
    archive_path = tmp_path / TAXDUMP_ARCHIVE_FILENAME
    archive_path.write_text("2\t|\tBacteria\t|\n")

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump-zip",
            str(archive_path),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 1
    assert "not a zip file" in capsys.readouterr().err


def test_terminology_prepare_taxdump_contradictory_members(tmp_path, capsys):
    """Tests the case where the archive records one taxon as both live and
    deleted, which no taxdump can mean."""
    archive_path = write_taxdump(
        tmp_path,
        names=[("2", "Bacteria", "", "scientific name")],
        delnodes=[("2",)],
    )

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump-zip",
            str(archive_path),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 1
    assert "recorded in more than one member" in capsys.readouterr().err


# =============================================================================
# terminology load
# =============================================================================


def _prepare_release(tmp_path, capsys, *, name: str, version: str, export_rows) -> dict:
    """Produce a prepared release via the prepare-owl subcommand and return
    the paths of the three files it wrote.

    Drains the capture so the caller's own assertions see only the output of
    the command under test, not this preparation step's summary."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(export_path, export_rows)
    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(export_path),
            "--name",
            name,
            "--version",
            version,
        ]
    )
    assert rc == 0
    capsys.readouterr()
    return {
        "manifest": tmp_path / MANIFEST_FILENAME,
        "terms": tmp_path / TERMS_TSV_FILENAME,
        "closure": tmp_path / CLOSURE_TSV_FILENAME,
    }


def _load_argv(paths: dict, *extra: str) -> list[str]:
    """The load subcommand's argv for a prepared release."""
    return [
        "terminology",
        "load",
        "--manifest",
        str(paths["manifest"]),
        "--terms",
        str(paths["terms"]),
        "--closure",
        str(paths["closure"]),
        *extra,
    ]


def test_terminology_load_missing_database_url(tmp_path, monkeypatch, capsys):
    """Tests the case where DATABASE_URL is unset."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )

    rc = cli.main(_load_argv(paths))

    assert rc == 2
    assert "DATABASE_URL" in capsys.readouterr().err


def test_terminology_load_digest_mismatch(tmp_path, monkeypatch, capsys):
    """Tests the case where a release table no longer matches the manifest.

    DATABASE_URL points at nothing reachable, so a mismatch reported here is
    proof the files are checked before any connection is attempted.
    """
    monkeypatch.setenv("DATABASE_URL", "postgresql://nobody@127.0.0.1:1/absent")
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )
    paths["terms"].write_text("term_id\tlabel\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n")

    rc = cli.main(_load_argv(paths))

    assert rc == 1
    assert "sha256 mismatch" in capsys.readouterr().err


def test_terminology_load_declared_path_not_bare(tmp_path, monkeypatch, capsys):
    """Tests the case where the manifest declares a path with a directory in
    it, which the flat staging directory cannot represent."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://nobody@127.0.0.1:1/absent")
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )
    manifest = json.loads(paths["manifest"].read_text())
    manifest["terms"]["path"] = "nested/terms.tsv"
    paths["manifest"].write_text(json.dumps(manifest))

    rc = cli.main(_load_argv(paths))

    assert rc == 1
    assert "nested/terms.tsv" in capsys.readouterr().err


@pytest.mark.db
async def test_terminology_load(tmp_path, monkeypatch, capsys, postgres_url, created_terminologies):
    """Tests the case where a prepared release is loaded: every term is
    inserted and the counts are reported."""
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="cli_load_uberon",
        version="1.0.0",
        export_rows=[
            ("UBERON:0001", "mouth", "", "", ""),
            ("UBERON:0002", "tooth", "", "", ""),
        ],
    )

    rc = await asyncio.to_thread(cli.main, _load_argv(paths))

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    created_terminologies.append(result["terminology_idx"])
    assert result == {
        "terminology_idx": result["terminology_idx"],
        "terms_inserted": 2,
        "terms_label_updated": 0,
        "terms_newly_obsoleted": 0,
        "terms_newly_merged": 0,
        "terms_silently_dropped": 0,
        "closure_rows": 0,
    }


@pytest.mark.db
async def test_terminology_load_anomaly(
    tmp_path, monkeypatch, capsys, postgres_url, created_terminologies
):
    """Tests the case where a reload drops a term without deprecating it: the
    load refuses and names the dropped term."""
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    first = _prepare_release(
        tmp_path / "v1",
        capsys,
        name="cli_load_anomaly",
        version="1.0.0",
        export_rows=[
            ("UBERON:0001", "mouth", "", "", ""),
            ("UBERON:0002", "tooth", "", "", ""),
        ],
    )
    rc = await asyncio.to_thread(cli.main, _load_argv(first))
    assert rc == 0
    created_terminologies.append(json.loads(capsys.readouterr().out)["terminology_idx"])

    second = _prepare_release(
        tmp_path / "v2",
        capsys,
        name="cli_load_anomaly",
        version="2.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )

    rc = await asyncio.to_thread(cli.main, _load_argv(second))

    assert rc == 1
    assert "UBERON:0002" in capsys.readouterr().err


@pytest.mark.db
async def test_terminology_load_tolerate_anomalies(
    tmp_path, monkeypatch, capsys, postgres_url, created_terminologies
):
    """Tests the case where the same dropped term is tolerated: the load
    succeeds and reports it as silently dropped."""
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    first = _prepare_release(
        tmp_path / "v1",
        capsys,
        name="cli_load_tolerate",
        version="1.0.0",
        export_rows=[
            ("UBERON:0001", "mouth", "", "", ""),
            ("UBERON:0002", "tooth", "", "", ""),
        ],
    )
    rc = await asyncio.to_thread(cli.main, _load_argv(first))
    assert rc == 0
    created_terminologies.append(json.loads(capsys.readouterr().out)["terminology_idx"])

    second = _prepare_release(
        tmp_path / "v2",
        capsys,
        name="cli_load_tolerate",
        version="2.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )

    rc = await asyncio.to_thread(cli.main, _load_argv(second, "--tolerate-anomalies"))

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["terms_silently_dropped"] == 1
    assert result["terms_newly_obsoleted"] == 1
