"""qiita-admin CLI — terminology subcommands for preparing and loading a
release.

`robot-command` emits the ROBOT export command for a human to run; nothing
here runs it. `prepare` turns the export that command produced into the
release tables and the manifest describing them. `load` applies those files
to the database.
"""

import argparse
import asyncio
import dataclasses
import json
import os
import shlex
import shutil
import sys
import tempfile
from pathlib import Path

from qiita_common.models import TerminologyManifest, TerminologyManifestFile

from qiita_control_plane.repositories.terminology import (
    TerminologyImportAnomaly,
    TerminologyImportResult,
)
from qiita_control_plane.terminology import (
    CLOSURE_TSV_FILENAME,
    MANIFEST_FILENAME,
    TERMS_TSV_FILENAME,
    import_terminology,
    load_manifest,
    sha256_of_file,
    verify_manifest_checksums,
    write_closure_tsv_stub,
    write_manifest,
    write_terms_tsv,
)
from qiita_control_plane.terminology_owl import build_terms
from qiita_control_plane.terminology_owl_robot import parse_robot_export, robot_export_argv

from ._helpers import open_admin_pool

# Default name for ROBOT's export, shared by the command that writes it and
# the one that reads it back so an operator need not retype it.
DEFAULT_ROBOT_EXPORT_FILENAME = "robot-export.tsv"


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


def _handle_terminology_prepare(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> int:
    """Turn a ROBOT export into the release tables plus the manifest that
    declares them, and print what was written.

    Returns 1 when the export is absent or malformed.
    """
    export_path: Path = args.export
    output_dir: Path = args.output_dir or export_path.parent

    try:
        exported_classes = parse_robot_export(export_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    terms = build_terms(exported_classes, term_id_prefix=args.term_id_prefix)

    terms_path = output_dir / TERMS_TSV_FILENAME
    closure_path = output_dir / CLOSURE_TSV_FILENAME
    write_terms_tsv(terms_path, terms)
    write_closure_tsv_stub(closure_path)

    # Digest the tables after writing them, so the manifest describes what
    # landed on disk rather than what was intended.
    manifest = TerminologyManifest(
        name=args.name,
        version=args.version,
        terms=TerminologyManifestFile(path=TERMS_TSV_FILENAME, sha256=sha256_of_file(terms_path)),
        closure=TerminologyManifestFile(
            path=CLOSURE_TSV_FILENAME, sha256=sha256_of_file(closure_path)
        ),
    )
    write_manifest(output_dir, manifest)

    summary = {
        "name": args.name,
        "version": args.version,
        "terms_written": len(terms),
        "output_dir": str(output_dir),
    }
    print(json.dumps(summary, indent=2))
    return 0


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

    Raises ValueError if the manifest declares a path that is not a bare
    filename, since the release is read from one flat directory.
    """
    shutil.copyfile(manifest_path, staging_dir / MANIFEST_FILENAME)
    manifest = load_manifest(staging_dir)

    # The declared names are what the staged copies are given, so a name
    # carrying a directory would land outside the flat staging directory.
    for declared in (manifest.terms, manifest.closure):
        if Path(declared.path).name != declared.path:
            raise ValueError(
                f"manifest declares {declared.path!r}, which is not a bare filename;"
                " a release is read from a single flat directory"
            )

    shutil.copyfile(terms_path, staging_dir / manifest.terms.path)
    shutil.copyfile(closure_path, staging_dir / manifest.closure.path)
    return manifest


def _handle_terminology_load(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> int:
    """Apply a prepared release to the database and print what changed.

    Returns 2 when DATABASE_URL is unset, and 1 when the files do not match
    the manifest, the database is unreachable, or the release carries
    structural anomalies that were not tolerated.
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2

    # The three files are named individually and copied into a directory
    # created here, so loading needs no staging directory on the host.
    staging_dir = Path(tempfile.mkdtemp(prefix="qiita-terminology-"))
    try:
        try:
            manifest = _stage_release_files(
                staging_dir,
                manifest_path=args.manifest,
                terms_path=args.terms,
                closure_path=args.closure,
            )
            # Verify here rather than leaving it to the import, so files that
            # do not match the manifest are refused before a connection to the
            # database is opened.
            verify_manifest_checksums(staging_dir, manifest)
        except (FileNotFoundError, ValueError) as exc:
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
            payload = {
                "silently_dropped_term_ids": exc.silently_dropped_term_ids,
                "unresolved_replaced_by": exc.unresolved_replaced_by,
                "misaligned_replaced_by": exc.misaligned_replaced_by,
            }
            print(f"error: {exc}", file=sys.stderr)
            print(json.dumps(payload, indent=2), file=sys.stderr)
            return 1
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)

    print(json.dumps(dataclasses.asdict(result), indent=2))
    return 0
