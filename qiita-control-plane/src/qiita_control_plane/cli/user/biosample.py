"""qiita user CLI — biosample subcommand handlers with biosample-specific logic."""

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BULK_BY_STUDY,
    PATH_BIOSAMPLE_BY_STUDY,
    PATH_STUDY_PREFIX,
)
from qiita_common.models import (
    BIOSAMPLE_ATTRIBUTE_FIELDS,
    BiosampleBulkImportRequest,
    BiosampleBulkImportResponse,
    BiosampleImportRequest,
)

from ...delimited_text import read_headed_tsv, write_tsv
from .. import _common
from ._helpers import _build_body

# The flag the TSV file arrives under, which every error about the file names.
_TSV_FILE_FLAG = "--tsv-file"
_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG = "--owner-biosample-id-field-name"
# Where DuckDB's read error turns to advice for whoever wrote the reading code.
_DUCKDB_ADVICE_MARKER = "Possible fixes"
# The fields of each created biosample that the output TSV carries, after the
# owner_biosample_id column.
_BULK_CREATE_OUTPUT_FIELDS = ("biosample_idx", "export_entity_id")
# Names that the owner_biosample_id column cannot take: the output columns and
# the attribute names. One rule ignores case for both, although routing a
# column to an attribute matches its exact name.
_REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES = (
    *_BULK_CREATE_OUTPUT_FIELDS,
    *BIOSAMPLE_ATTRIBUTE_FIELDS,
)
_OUTPUT_TSV_NAME = "created.tsv"


def _post_biosample(base_url: str, token: str, study_idx: int, body: dict) -> dict:
    """POST /api/v1/study/{study_idx}/biosample with the (already-pruned) body.

    The route currently requires owner_idx in the body explicitly; the CLI
    handler resolves it via whoami when --owner-idx is omitted so the
    caller can run `qiita biosample create` for themselves without first
    chasing their own principal_idx.
    """
    path = f"{PATH_STUDY_PREFIX}{PATH_BIOSAMPLE_BY_STUDY}".format(study_idx=study_idx)
    return _common.call("POST", base_url, token, path, json=body)


def _handle_biosample_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a biosample on a study, auto-defaulting owner_idx to the caller.

    The whoami lookup that resolves a missing --owner-idx shares the
    token-read path with the POST itself, so both calls happen inside the
    same `run_http_subcommand` invocation. Body construction runs after
    that resolution so BiosampleImportRequest sees a populated owner_idx.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        _common.resolve_owner_idx(args, args.base_url, token)
        body = _build_body(BiosampleImportRequest, args, parser)
        return _post_biosample(args.base_url, token, args.study_idx, body)

    return _common.run_http_subcommand(_run)


def _read_biosample_tsv(
    tsv_file: Path, owner_biosample_id_column: str, parser: argparse.ArgumentParser
) -> list[dict[str, str]]:
    """Read the TSV file at `tsv_file` into one dict per row, keyed on its
    stripped column headers. A file that cannot be read, that lacks the
    `owner_biosample_id_column` column, or that holds no rows exits through `parser.error`,
    naming the file and the fault."""
    try:
        tsv_rows = read_headed_tsv(tsv_file, source_name=_TSV_FILE_FLAG)
    except duckdb.Error as exc:
        detail = str(exc).split(_DUCKDB_ADVICE_MARKER)[0]
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file}: {' '.join(detail.split())}")
    except OSError as exc:
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file}: {exc}")
    except ValueError as exc:
        parser.error(str(exc))

    if not tsv_rows:
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file} holds no rows")
    if owner_biosample_id_column not in tsv_rows[0]:
        parser.error(
            f"{_TSV_FILE_FLAG} {tsv_file} has no {owner_biosample_id_column!r} column, which"
            f" {_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG} names"
        )
    return tsv_rows


def _bulk_create_rows(
    tsv_rows: list[dict[str, str]], owner_biosample_id_column: str, args: argparse.Namespace
) -> list[dict]:
    """One BiosampleImportRequest-shaped dict per TSV row. Cells are stripped
    and blank ones left out; attribute columns set those attributes, the
    `owner_biosample_id_column` column sets the owner_biosample_id, and every other column is
    metadata."""
    request_rows = []
    for tsv_row in tsv_rows:
        filled_cells = {name: cell.strip() for name, cell in tsv_row.items() if cell.strip()}

        # Route each filled cell to the owner_biosample_id, an attribute, or metadata.
        request_row: dict = {
            "owner_idx": args.owner_idx,
            "owner_biosample_id_field_name": owner_biosample_id_column,
        }
        metadata = {}
        for name, cell in filled_cells.items():
            if name == owner_biosample_id_column:
                request_row["owner_biosample_id_value"] = cell
            # A reserved name sets the attribute, so a study field of the same
            # name can never be filled from a TSV file.
            elif name in BIOSAMPLE_ATTRIBUTE_FIELDS:
                request_row[name] = cell
            else:
                metadata[name] = cell
        request_row["metadata"] = metadata
        if args.global_internal_names:
            request_row["global_internal_names"] = True
        request_rows.append(request_row)
    return request_rows


def _handle_biosample_bulk_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Create one biosample per row of a TSV file on a study, in one
    all-or-nothing request, then print each created biosample's identifiers as a TSV."""
    owner_biosample_id_column = args.owner_biosample_id_field_name.strip()
    folded_refused_names = {name.casefold() for name in _REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES}
    if owner_biosample_id_column.casefold() in folded_refused_names:
        parser.error(
            f"{_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG} {owner_biosample_id_column!r} cannot be"
            " used: ignoring case, it matches one of"
            f" {', '.join(_REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES)};"
            " `qiita biosample create` accepts it."
        )

    # Read the file first, so an unreadable file exits before any request.
    tsv_rows = _read_biosample_tsv(args.tsv_file, owner_biosample_id_column, parser)

    def _run(token: str) -> list[tuple[str, ...]]:
        _common.resolve_study_idx(args, parser, args.base_url, token)
        _common.resolve_owner_idx(args, args.base_url, token)
        request_rows = _bulk_create_rows(tsv_rows, owner_biosample_id_column, args)
        rows_namespace = argparse.Namespace(rows=request_rows)
        body = _build_body(BiosampleBulkImportRequest, rows_namespace, parser)
        bulk_path = f"{PATH_STUDY_PREFIX}{PATH_BIOSAMPLE_BULK_BY_STUDY}".format(
            study_idx=args.study_idx
        )
        created_raw = _common.call("POST", args.base_url, token, bulk_path, json=body)
        created = BiosampleBulkImportResponse.model_validate(created_raw)
        if len(created.results) != len(tsv_rows):
            print(
                f"error: the control plane returned {len(created.results)} result(s)"
                f" for {len(tsv_rows)} row(s)",
                file=sys.stderr,
            )
            sys.exit(1)

        # The route answers in request order, so each result pairs with its TSV row.
        output_rows = [
            (
                tsv_row[owner_biosample_id_column].strip(),
                *(str(getattr(result, f)) for f in _BULK_CREATE_OUTPUT_FIELDS),
            )
            for tsv_row, result in zip(tsv_rows, created.results, strict=True)
        ]
        return output_rows

    def _render(output_rows: list[tuple[str, ...]]) -> None:
        output_columns = (owner_biosample_id_column, *_BULK_CREATE_OUTPUT_FIELDS)
        try:
            # DuckDB truncates stdout even when it is a file opened for appending,
            # so the TSV goes to a temporary file that is then copied onto stdout.
            with tempfile.TemporaryDirectory() as output_dir:
                output_path = Path(output_dir) / _OUTPUT_TSV_NAME
                write_tsv(output_path, output_columns, output_rows)
                sys.stdout.flush()
                with output_path.open("rb") as output_tsv:
                    shutil.copyfileobj(output_tsv, sys.stdout.buffer)
                sys.stdout.flush()
        except (OSError, duckdb.Error) as exc:
            # The biosamples exist by now, so their identifiers must not be lost.
            created_rows = [dict(zip(output_columns, row, strict=True)) for row in output_rows]
            print(f"error: writing the created biosamples' TSV failed: {exc}", file=sys.stderr)
            print("The biosamples were created; their identifiers are:", file=sys.stderr)
            print(json.dumps(created_rows, indent=2), file=sys.stderr)
            sys.exit(1)

    return _common.run_http_subcommand(_run, render=_render)
