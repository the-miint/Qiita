"""qiita-admin CLI — terminology subcommands, which turn a release source
into the files a load consumes and apply them to the database.

No external toolchain is run from here: a step needing one prints the command
for an operator to run and reads back what that command wrote.
"""

import argparse
import asyncio
import dataclasses
import json
import shlex
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path

import duckdb
from pydantic import TypeAdapter, ValidationError
from qiita_common.models import (
    TerminologyManifest,
    TerminologyManifestFile,
    TerminologyName,
    TerminologyVersion,
)

from qiita_control_plane.repositories.terminology import (
    ParsedTerm,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    capped_offenders,
)
from qiita_control_plane.terminology import (
    CLOSURE_TSV_FILENAME,
    MANIFEST_FILENAME,
    TERMS_TSV_FILENAME,
    import_terminology,
    load_manifest,
    sha256_of_file,
    write_closure_tsv_stub,
    write_manifest,
    write_terms_tsv,
)
from qiita_control_plane.terminology_owl import build_terms
from qiita_control_plane.terminology_owl_robot import (
    DEFAULT_ROBOT_EXECUTABLE,
    parse_robot_export,
    robot_export_argv,
)
from qiita_control_plane.terminology_taxdump import build_terms_from_taxdump

from ._helpers import EXIT_PRECONDITION_FAILED, open_admin_pool, requires_database_url

# Default name for ROBOT's export, shared by the command that writes it and
# the one that reads it back so an operator need not retype it.
DEFAULT_ROBOT_EXPORT_FILENAME = "robot-export.tsv"

# Spell the argv default as the command line the flag accepts, so the flag's
# default and the argv builder's cannot drift apart.
DEFAULT_ROBOT_COMMAND_LINE = shlex.join(DEFAULT_ROBOT_EXECUTABLE)

# Names the directory a release is written in before publication. Dot-led, so
# an in-progress release does not read as part of its output directory.
_STAGING_DIR_PREFIX = ".qiita-staging-"

# Validators for the release-identifying strings on their own, so a name or
# version meets the manifest's bounds without a manifest to put it in.
_RELEASE_NAME_ADAPTER = TypeAdapter(TerminologyName)
_RELEASE_VERSION_ADAPTER = TypeAdapter(TerminologyVersion)


# Note that this command is *intended to be temporary*. It is a stop-gap to
# support the current terminology release process, which is not yet integrated
# into the apptainer/workflow/slurm pipeline for external software calls.
# Do not depend on it being here in the future.
def _handle_terminology_robot_command(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> int:
    """Print the ROBOT export command to run against the staged OWL.

    Emitted shell-quoted: the column selection contains pipe characters that
    a shell would otherwise read as pipelines.
    """
    argv = robot_export_argv(args.input, args.export, executable=shlex.split(args.executable))
    print(shlex.join(argv))
    return 0


def _check_release_identifiers(name: str, version: str) -> None:
    """Reject a name or version a release manifest could not carry.

    Raises ValueError naming the option at fault. Checked against the same
    bounds the manifest declares, so the two cannot disagree about what is
    acceptable.
    """
    for option, value, adapter in (
        ("--name", name, _RELEASE_NAME_ADAPTER),
        ("--version", version, _RELEASE_VERSION_ADAPTER),
    ):
        try:
            adapter.validate_python(value)
        except ValidationError as exc:
            raise ValueError(f"{option} cannot be carried by a release manifest: {exc}") from exc


def _write_release(
    output_dir: Path,
    terms: list[ParsedTerm],
    *,
    name: str,
    version: str,
) -> None:
    """Write `terms` and the closure stub into `output_dir` along with the
    manifest declaring both, and print what was written.

    `output_dir` must already exist. Nothing lands in it until all three files
    exist, so a write that fails part-way leaves whatever release was already
    there intact rather than half-overwritten.
    """
    # Stage inside the destination so publishing each file is a rename on one
    # filesystem; staging elsewhere would copy a table of any size back across.
    staging_dir = Path(tempfile.mkdtemp(dir=output_dir, prefix=_STAGING_DIR_PREFIX))
    try:
        terms_path = staging_dir / TERMS_TSV_FILENAME
        closure_path = staging_dir / CLOSURE_TSV_FILENAME
        write_terms_tsv(terms_path, terms)
        write_closure_tsv_stub(closure_path)

        # Digest the tables after writing them, so the manifest describes what
        # landed on disk rather than what was intended.
        terms_digest = sha256_of_file(terms_path)
        closure_digest = sha256_of_file(closure_path)
        declared_terms = TerminologyManifestFile(path=TERMS_TSV_FILENAME, sha256=terms_digest)
        declared_closure = TerminologyManifestFile(path=CLOSURE_TSV_FILENAME, sha256=closure_digest)
        manifest = TerminologyManifest(
            name=name,
            version=version,
            terms=declared_terms,
            closure=declared_closure,
        )
        write_manifest(staging_dir, manifest)

        # The manifest publishes last, so a publish interrupted part-way leaves a
        # directory a load refuses rather than one it reads against stale digests.
        for filename in (TERMS_TSV_FILENAME, CLOSURE_TSV_FILENAME, MANIFEST_FILENAME):
            (staging_dir / filename).replace(output_dir / filename)
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    summary = {
        "name": name,
        "version": version,
        "terms_written": len(terms),
        "output_dir": str(output_dir),
    }
    print(json.dumps(summary, indent=2))


def _handle_prepare(
    args: argparse.Namespace,
    *,
    input_path: Path,
    load_terms: Callable[[], list[ParsedTerm]],
    load_error_types: tuple[type[Exception], ...],
) -> int:
    """Read a release source through `load_terms` and write the release into
    the directory named on the command line, or beside `input_path`.

    Returns 2 when the name or version cannot go in a manifest or the output
    directory cannot be created, 1 when it cannot read the source or write a
    release file, and 0 on success. Checks the identifiers and reads the source
    before creating the output directory, so an unreadable source leaves no
    directory behind; a write that fails afterwards can leave that directory,
    but never a partial release in it.
    """
    output_dir: Path = args.output_dir or input_path.parent

    # Check ahead of reading the source, the expensive part on a full release,
    # and ahead of every write.
    try:
        _check_release_identifiers(args.name, args.version)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_PRECONDITION_FAILED

    try:
        terms = load_terms()
    except load_error_types as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"error: could not create output directory {output_dir}: {exc}", file=sys.stderr)
        return EXIT_PRECONDITION_FAILED

    # duckdb.Error alongside OSError: DuckDB writes a release table, so it
    # reports an unwritable destination as its own error rather than the
    # operating system's.
    try:
        _write_release(output_dir, terms, name=args.name, version=args.version)
    except (OSError, duckdb.Error) as exc:
        print(f"error: could not write release to {output_dir}: {exc}", file=sys.stderr)
        return 1
    return 0


def _handle_terminology_prepare_owl(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> int:
    """Turn a ROBOT export into the release tables plus the manifest that
    declares them, and print what was written.

    Returns 2 when the output directory cannot be created, and 1 when it cannot
    read the export, the export is malformed, or it cannot write a release file.
    """
    export_path: Path = args.export
    return _handle_prepare(
        args,
        input_path=export_path,
        load_terms=lambda: build_terms(
            parse_robot_export(export_path), term_id_prefix=args.term_id_prefix
        ),
        # duckdb.Error alongside the rest: DuckDB reads the export, so it
        # reports a row of the wrong width as its own error.
        load_error_types=(OSError, ValueError, duckdb.Error),
    )


def _handle_terminology_prepare_taxdump(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> int:
    """Turn an NCBI taxdump archive into the release tables plus the manifest
    that declares them, and print what was written.

    Returns 2 when the output directory cannot be created, and 1 when it cannot
    read the archive, the path is not an archive, a member's layout or content
    contradicts what the taxdump documents, or it cannot write a release file.
    """
    taxdump: Path = args.taxdump
    return _handle_prepare(
        args,
        input_path=taxdump,
        load_terms=lambda: build_terms_from_taxdump(taxdump),
        # duckdb.Error covers every way reading the archive can fail, since SQL
        # reads the members: an unreadable archive, an absent member, and a row
        # whose field count contradicts the documented layout.
        load_error_types=(OSError, ValueError, duckdb.Error),
    )


async def _load_terminology(
    database_url: str,
    source_dir: Path,
    *,
    tolerate_anomalies: bool,
) -> TerminologyImportResult:
    """Open the admin pool and apply the release staged in `source_dir`."""
    pool = await open_admin_pool(database_url)
    try:
        return await import_terminology(pool, source_dir, tolerate_anomalies=tolerate_anomalies)
    finally:
        await pool.close()


def _stage_release_files(
    staging_dir: Path,
    *,
    manifest_path: Path,
    terms_path: Path,
    closure_path: Path,
) -> TerminologyManifest:
    """Copy the three release files into `staging_dir` under the names the
    manifest declares, and return that manifest.

    Raises ValueError if the manifest declares a path that is the manifest's
    own name, or that both tables share, since a load reads the release from
    one flat directory holding all three files.
    """
    shutil.copyfile(manifest_path, staging_dir / MANIFEST_FILENAME)
    manifest = load_manifest(staging_dir)

    # The staged copies take their declared names, so the manifest's own name
    # would overwrite the file just staged.
    for declared in (manifest.terms, manifest.closure):
        if declared.path == MANIFEST_FILENAME:
            raise ValueError(
                f"manifest declares {declared.path!r} for a release table, which is"
                " the manifest's own name in the staging directory"
            )

    # One declared name per table: each stages under the name it declares, so
    # a shared name would let the second copy replace the first.
    if manifest.terms.path == manifest.closure.path:
        raise ValueError(
            f"manifest declares {manifest.terms.path!r} for both release tables;"
            " each is staged under its declared name, so one would replace the other"
        )

    shutil.copyfile(terms_path, staging_dir / manifest.terms.path)
    shutil.copyfile(closure_path, staging_dir / manifest.closure.path)
    return manifest


@requires_database_url
def _handle_terminology_load(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    database_url: str,
) -> int:
    """Apply a prepared release to the database and print what changed.

    Returns 1 when it cannot read a named file, the files do not match the
    manifest, the database is unreachable, or the release carries structural
    anomalies the run did not tolerate.
    """
    # Copy the three individually named files into a directory created here,
    # so loading needs no staging directory on the host.
    staging_dir = Path(tempfile.mkdtemp(prefix="qiita-terminology-"))
    try:
        try:
            _stage_release_files(
                staging_dir,
                manifest_path=args.manifest,
                terms_path=args.terms,
                closure_path=args.closure,
            )
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

        try:
            result = asyncio.run(
                _load_terminology(
                    database_url,
                    staging_dir,
                    tolerate_anomalies=args.tolerate_anomalies,
                )
            )
        except TerminologyImportAnomaly as exc:
            # Each kind reports its count and a capped sample: a release can
            # violate one of them for millions of terms, and enumerating them
            # all costs megabytes of stderr that hide which check even fired.
            # The same capping the message above already applied, so the two
            # halves of one error cannot name different offenders.
            payload = {}
            for attribute, rows in exc.reported_anomalies():
                count, sample = capped_offenders(rows)
                payload[attribute] = {"count": count, "sample": sample}
            print(f"error: {exc}", file=sys.stderr)
            print(json.dumps(payload, indent=2), file=sys.stderr)
            return 1
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    print(json.dumps(dataclasses.asdict(result), indent=2))
    return 0
