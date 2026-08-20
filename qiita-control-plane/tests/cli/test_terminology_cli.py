"""Unit tests for the qiita-admin terminology subcommands. Nothing here runs
ROBOT; the load cases that reach Postgres carry the db marker."""

import asyncio
import json

import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models import (
    MAX_NAME_LENGTH,
    MAX_TERMINOLOGY_VERSION_LENGTH,
    TerminologyTermObsoletionKind,
)

from qiita_control_plane.cli import admin as cli
from qiita_control_plane.cli.admin import terminology as terminology_cli
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
    FIXTURE_TAXDUMP_ARCHIVE_FILENAME,
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

# Port 1 on the loopback host accepts no connections, so a case that must be
# refused before the database is reached fails the assertion rather than the
# connection if the refusal ever moves later.
_UNREACHABLE_DATABASE_URL = f"postgresql://nobody@{LOOPBACK_HOST}:1/absent"


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
    """Tests the case where a container runtime and export name are named: the
    runtime's tokens lead the command and the export name carries through."""
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
    """Tests the case where an export is prepared: the run writes both release
    tables and a manifest declaring their digests, and the terms table reads
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

    # The manifest must describe the files as written, so the load-side check
    # of the same digests passes.
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
    other vocabularies stay out of the written table."""
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


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--version", "v" * (MAX_TERMINOLOGY_VERSION_LENGTH + 1)),
        ("--name", "u" * (MAX_NAME_LENGTH + 1)),
    ],
    ids=["version", "name"],
)
def test_terminology_prepare_owl_identifier_too_long(tmp_path, capsys, option, value):
    """Tests the case where a release identifier is longer than a manifest can
    carry: the prepare refuses it, naming the option, and writes nothing, so
    the output directory never holds tables with no manifest."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(export_path, [("UBERON:0001", "mouth", "", "", "")])
    output_dir = tmp_path / "out"
    argv = [
        "terminology",
        "prepare-owl",
        "--export",
        str(export_path),
        "--name",
        "uberon",
        "--version",
        "2026-04-15",
        "--output-dir",
        str(output_dir),
    ]
    argv[argv.index(option) + 1] = value

    rc = cli.main(argv)

    assert rc == 2
    assert option in capsys.readouterr().err
    assert not output_dir.exists()


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
    assert "Expected Number of Columns: 5 Found: 2" in capsys.readouterr().err


@pytest.mark.parametrize(
    "subcommand,flag,expected_error",
    [
        ("prepare-owl", "--export", "is not a file"),
        ("prepare-taxdump", "--taxdump", "Not a taxdump archive file"),
    ],
)
def test_terminology_prepare_unreadable_input(tmp_path, capsys, subcommand, flag, expected_error):
    """Tests the case where the named source is a directory rather than a file
    the prepare can open: it reports the reason and exits 1 rather than
    raising."""
    a_directory = tmp_path / "a-directory"
    a_directory.mkdir()

    rc = cli.main(
        [
            "terminology",
            subcommand,
            flag,
            str(a_directory),
            "--name",
            "uberon",
            "--version",
            "1.0.0",
        ]
    )

    assert rc == 1
    assert expected_error in capsys.readouterr().err


def test_terminology_requires_subcommand():
    """Tests the case where no terminology subcommand is given."""
    with pytest.raises(SystemExit):
        cli.main(["terminology"])


# =============================================================================
# terminology prepare-taxdump
# =============================================================================


def test_terminology_prepare_taxdump(tmp_path, capsys):
    """Tests the case where a taxdump archive is prepared: the run writes both
    release tables and a manifest declaring their digests, and the terms table
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
            "--taxdump",
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

    # The manifest must describe the files as written, so the load-side check
    # of the same digests passes.
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
            "--taxdump",
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


def test_terminology_prepare_owl_creates_output_dir(tmp_path):
    """Tests the case where the named output directory does not exist: the run
    creates it, along with any missing parent, rather than refusing."""
    write_robot_export_tsv(tmp_path / "robot-export.tsv", [("UBERON:0001", "mouth", "", "", "")])
    output_dir = tmp_path / "absent" / "staged"

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(tmp_path / "robot-export.tsv"),
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


def test_terminology_prepare_taxdump_creates_output_dir(tmp_path):
    """Tests the case where the named output directory does not exist: the run
    creates it, along with any missing parent, rather than refusing."""
    archive_path = write_taxdump(tmp_path, names=[("2", "Bacteria", "", "scientific name")])
    output_dir = tmp_path / "absent" / "staged"

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump",
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


def test_terminology_prepare_owl_output_dir_uncreatable(tmp_path, capsys):
    """Tests the case where the output directory cannot be created because a
    file occupies its path: the run returns the precondition code, not a
    traceback."""
    write_robot_export_tsv(tmp_path / "robot-export.tsv", [("UBERON:0001", "mouth", "", "", "")])
    blocking_file = tmp_path / "blocking"
    blocking_file.write_text("not a directory")
    output_dir = blocking_file / "staged"

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(tmp_path / "robot-export.tsv"),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 2
    assert "output directory" in capsys.readouterr().err


def test_terminology_prepare_owl_write_failure(tmp_path, capsys, monkeypatch):
    """Tests the case where a release file cannot be written once the output
    directory exists: that counts as a failed run rather than an unmet
    precondition, so the codes differ."""
    write_robot_export_tsv(tmp_path / "robot-export.tsv", [("UBERON:0001", "mouth", "", "", "")])
    output_dir = tmp_path / "staged"

    def refuse_write(path, terms):
        raise OSError("no space left on device")

    monkeypatch.setattr(terminology_cli, "write_terms_tsv", refuse_write)

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(tmp_path / "robot-export.tsv"),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
            "--output-dir",
            str(output_dir),
        ]
    )

    assert rc == 1
    assert "no space left on device" in capsys.readouterr().err
    # Nothing published and nothing staged left behind, so a re-run starts clean.
    assert list(output_dir.iterdir()) == []


def test_terminology_prepare_owl_write_failure_keeps_prior_release(tmp_path, monkeypatch):
    """Tests the case where a release is prepared over one already in the output
    directory and the write fails part-way: the release already there survives
    intact, since a failed run cannot half-overwrite what it never replaced."""
    export_path = tmp_path / "robot-export.tsv"
    write_robot_export_tsv(export_path, [("UBERON:0001", "mouth", "", "", "")])
    output_dir = tmp_path / "staged"
    prepare_argv = [
        "terminology",
        "prepare-owl",
        "--export",
        str(export_path),
        "--name",
        "uberon",
        "--version",
        "2026-04-15",
        "--output-dir",
        str(output_dir),
    ]

    assert cli.main(prepare_argv) == 0
    published = {p.name: p.read_text() for p in output_dir.iterdir()}

    # A second run over the same directory, failing after it writes the terms
    # table and before it writes the manifest.
    write_robot_export_tsv(export_path, [("UBERON:0002", "molar", "", "", "")])

    def refuse_manifest(source_dir, manifest):
        raise OSError("no space left on device")

    monkeypatch.setattr(terminology_cli, "write_manifest", refuse_manifest)

    assert cli.main(prepare_argv) == 1

    result = {p.name: p.read_text() for p in output_dir.iterdir()}
    assert result == published


def test_terminology_prepare_taxdump_missing_archive(tmp_path, capsys):
    """Tests the case where the named archive does not exist."""
    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump",
            str(tmp_path / FIXTURE_TAXDUMP_ARCHIVE_FILENAME),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 1
    assert "No taxdump archive" in capsys.readouterr().err


def test_terminology_prepare_taxdump_not_an_archive(tmp_path, capsys):
    """Tests the case where the named path exists but is not a gzipped tar."""
    archive_path = tmp_path / FIXTURE_TAXDUMP_ARCHIVE_FILENAME
    archive_path.write_text("2\t|\tBacteria\t|\n")

    rc = cli.main(
        [
            "terminology",
            "prepare-taxdump",
            "--taxdump",
            str(archive_path),
            "--name",
            "NCBI Taxonomy",
            "--version",
            "2026-08-01",
        ]
    )

    assert rc == 1
    assert "gzip inflate failed" in capsys.readouterr().err


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
            "--taxdump",
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

    Drains the capture so only the output of the command under test remains,
    not this preparation step's summary."""
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


@pytest.mark.db
async def test_terminology_load_digest_mismatch(tmp_path, monkeypatch, capsys, postgres_url):
    """Tests the case where a release table no longer matches the manifest: the
    load refuses it, naming the mismatch."""
    monkeypatch.setenv("DATABASE_URL", postgres_url)
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )
    paths["terms"].write_text("term_id\tlabel\tis_obsolete\treplaced_by_term_id\tobsoletion_kind\n")

    rc = await asyncio.to_thread(cli.main, _load_argv(paths))

    assert rc == 1
    assert "sha256 mismatch" in capsys.readouterr().err


def _release_with_declared_terms_path(tmp_path, capsys, declared_path: str) -> dict:
    """A prepared release whose manifest declares `declared_path` for its terms
    table, for the cases refusing a declared path before any load begins."""
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )
    manifest = json.loads(paths["manifest"].read_text())
    manifest["terms"]["path"] = declared_path
    paths["manifest"].write_text(json.dumps(manifest))
    return paths


@pytest.mark.parametrize(
    "declared_path",
    ["nested/terms.tsv", MANIFEST_FILENAME, CLOSURE_TSV_FILENAME],
    ids=["carries_a_directory", "is_the_manifest", "is_the_other_table"],
)
def test_terminology_load_declared_path_refused(tmp_path, monkeypatch, capsys, declared_path):
    """Tests the case where the manifest declares a terms path the flat staging
    directory cannot hold: one carrying a directory, one naming the manifest the
    declared paths came from, and one already declared for the closure table.
    Each refusal names the offending path and precedes any load."""
    monkeypatch.setenv("DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    paths = _release_with_declared_terms_path(tmp_path, capsys, declared_path)

    rc = cli.main(_load_argv(paths))

    assert rc == 1
    assert declared_path in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["--manifest", "--terms", "--closure"])
@pytest.mark.parametrize("names_a_directory", [True, False], ids=["a_directory", "empty_string"])
def test_terminology_load_unreadable_file(tmp_path, monkeypatch, capsys, flag, names_a_directory):
    """Tests the case where one of the three named paths is not a file the load
    can open — a directory, or the empty string, which names the working
    directory. The load reports the operating system's reason and exits 1
    rather than raising."""
    monkeypatch.setenv("DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )
    a_directory = tmp_path / "a-directory"
    a_directory.mkdir()

    # Both spellings reach the same open of a directory, since the empty string
    # resolves to the working directory instead of failing as a path.
    argv = _load_argv(paths)
    argv[argv.index(flag) + 1] = str(a_directory) if names_a_directory else ""

    rc = cli.main(argv)

    assert rc == 1
    assert "Is a directory" in capsys.readouterr().err


def test_terminology_load_invalid_manifest(tmp_path, monkeypatch, capsys):
    """Tests the case where the manifest does not match the release schema: the
    load refuses it, reporting what failed validation rather than a
    traceback."""
    monkeypatch.setenv("DATABASE_URL", _UNREACHABLE_DATABASE_URL)
    paths = _prepare_release(
        tmp_path,
        capsys,
        name="uberon",
        version="1.0.0",
        export_rows=[("UBERON:0001", "mouth", "", "", "")],
    )

    # Drop a required field, so the manifest parses as JSON but not as a release.
    manifest = json.loads(paths["manifest"].read_text())
    del manifest["version"]
    paths["manifest"].write_text(json.dumps(manifest))

    rc = cli.main(_load_argv(paths))

    assert rc == 1
    assert "validation error for TerminologyManifest" in capsys.readouterr().err


@pytest.mark.db
async def test_terminology_load(tmp_path, monkeypatch, capsys, postgres_url, created_terminologies):
    """Tests the case where a prepared release is loaded: the load inserts
    every term and reports the counts."""
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
        "terms_alternate_label_updated": 0,
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
    load refuses, names the dropped term, and reports each anomaly kind's count
    alongside a capped sample rather than every offending value twice."""
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
    # The message is the first line; the payload is the JSON that follows it.
    message_line, payload_json = capsys.readouterr().err.split("\n", 1)
    assert "UBERON:0002" in message_line
    assert json.loads(payload_json) == {
        "silently_dropped_term_ids": {"count": 1, "sample": ["UBERON:0002"]},
        "unresolved_replaced_by": {"count": 0, "sample": []},
        "misaligned_replaced_by": {"count": 0, "sample": []},
        "unresolved_closure_endpoints": {"count": 0, "sample": []},
    }


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
    assert result == {
        "terminology_idx": result["terminology_idx"],
        "terms_inserted": 0,
        "terms_label_updated": 0,
        "terms_alternate_label_updated": 0,
        "terms_newly_obsoleted": 1,
        "terms_newly_merged": 0,
        "terms_silently_dropped": 1,
        "closure_rows": 0,
    }


def test_terminology_prepare_owl_build_failure(tmp_path, capsys, monkeypatch):
    """Tests the case where turning the export into term rows fails: the run
    reports a failure rather than letting a traceback escape, so one guard
    covers the build step and the read that feeds it."""
    write_robot_export_tsv(tmp_path / "robot-export.tsv", [("UBERON:0001", "mouth", "", "", "")])

    def refuse_build(exported_classes, *, term_id_prefix):
        raise ValueError("contradictory obsoletion encoding")

    monkeypatch.setattr(terminology_cli, "build_terms", refuse_build)

    rc = cli.main(
        [
            "terminology",
            "prepare-owl",
            "--export",
            str(tmp_path / "robot-export.tsv"),
            "--name",
            "uberon",
            "--version",
            "2026-04-15",
        ]
    )

    assert rc == 1
    assert "contradictory obsoletion encoding" in capsys.readouterr().err
    assert not (tmp_path / TERMS_TSV_FILENAME).exists()
