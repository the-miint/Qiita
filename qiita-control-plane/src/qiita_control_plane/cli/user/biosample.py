"""qiita user CLI — biosample subcommand handlers with biosample-specific logic."""

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import get_args

import duckdb
from pydantic import BaseModel, ValidationError
from qiita_common.api_paths import (
    PATH_BIOSAMPLE_BULK_BY_STUDY,
    PATH_BIOSAMPLE_BULK_LINK_BY_STUDY,
    PATH_BIOSAMPLE_BY_STUDY,
    PATH_BIOSAMPLE_LOOKUP_BY_STUDY,
    PATH_STUDY_PREFIX,
)
from qiita_common.models import (
    BIOSAMPLE_ATTRIBUTE_FIELDS,
    EXPORT_ENTITY_ID,
    IDX_KEY,
    UNIQUE_FIELD_KEY,
    BiosampleBulkImportRequest,
    BiosampleBulkImportResponse,
    BiosampleBulkLinkRequest,
    BiosampleImportRequest,
    BiosampleLinkKey,
    BiosampleStudyLookupRequest,
)

from ...delimited_text import count_data_lines, read_headed_tsv
from .. import _common
from ._helpers import _build_body

# The flag the TSV file arrives under, which every error about the file names.
_TSV_FILE_FLAG = "--tsv-file"
_OUTPUT_FLAG = "--output"
_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG = "--owner-biosample-id-field-name"
_KEY_FLAG = "--key"
_UNIQUE_FIELD_NAME_FLAG = "--unique-field-name"
_RETURN_KEY_FLAG = "--return-key"
# The CLI's spelling of a biosample's idx.
_BIOSAMPLE_IDX = "biosample_idx"
# Each `--key` choice to the request key that it sends: every identifier kind that
# names a biosample whatever its study. The request's idx key is offered as
# biosample_idx.
_BIOSAMPLE_KEY_BY_CHOICE = {
    (_BIOSAMPLE_IDX if key == IDX_KEY else key): key for key in get_args(BiosampleLinkKey)
}
# export_entity_id is listed first, as the default identifier kind.
_BIOSAMPLE_KEY_CHOICES = (
    EXPORT_ENTITY_ID,
    *(choice for choice in _BIOSAMPLE_KEY_BY_CHOICE if choice != EXPORT_ENTITY_ID),
)
# Each `lookup --return-key` choice, which also heads the output's second column,
# to the request's return_key.
_LOOKUP_RETURN_KEY_BY_CHOICE = {EXPORT_ENTITY_ID: EXPORT_ENTITY_ID, _BIOSAMPLE_IDX: IDX_KEY}
# Where DuckDB's read error turns to advice for whoever wrote the reading code.
_DUCKDB_ADVICE_MARKER = "Possible fixes"
# The fields of each created biosample that the output TSV carries, after the
# owner_biosample_id column.
_BULK_CREATE_OUTPUT_FIELDS = (_BIOSAMPLE_IDX, EXPORT_ENTITY_ID)
# Names that the owner_biosample_id column cannot take: the output columns and
# the attribute names. One rule ignores case for both, although routing a
# column to an attribute matches its exact name.
_REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES = (
    *_BULK_CREATE_OUTPUT_FIELDS,
    *BIOSAMPLE_ATTRIBUTE_FIELDS,
)


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


def _read_tsv_with_column(
    tsv_file: Path, required_column: str, parser: argparse.ArgumentParser, *, column_flag: str
) -> list[dict[str, str]]:
    """Read the TSV file at `tsv_file` into one dict per row, keyed on its
    stripped column headers, leaving out rows at the end of the file whose cells
    are all blank. A file that cannot be read, that lacks the `required_column`
    column, or that holds no rows exits through `parser.error`, naming the file
    and the fault; `column_flag` is the flag that named the column."""
    try:
        tsv_rows = read_headed_tsv(tsv_file, source_name=_TSV_FILE_FLAG)
    except duckdb.Error as exc:
        detail = str(exc).split(_DUCKDB_ADVICE_MARKER)[0]
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file}: {' '.join(detail.split())}")
    except OSError as exc:
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file}: {exc}")
    except ValueError as exc:
        parser.error(str(exc))

    # A blank row at the end of the file, read from a blank last line, holds no data.
    while tsv_rows and not any(cell.strip() for cell in tsv_rows[-1].values()):
        tsv_rows = tsv_rows[:-1]
    if not tsv_rows:
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file} holds no rows")
    if required_column not in tsv_rows[0]:
        parser.error(
            f"{_TSV_FILE_FLAG} {tsv_file} has no {required_column!r} column, which"
            f" {column_flag} names"
        )
    return tsv_rows


def _refuse_blank_cells(
    tsv_rows: list[dict[str, str]], column: str, tsv_file: Path, parser: argparse.ArgumentParser
) -> None:
    """Exit through `parser.error` if any row's `column` cell is blank, naming
    each such row by its line as DuckDB's read errors do, the header being line 1."""
    blank_lines = [
        str(line) for line, tsv_row in enumerate(tsv_rows, start=2) if not tsv_row[column].strip()
    ]
    if blank_lines:
        parser.error(
            f"{_TSV_FILE_FLAG} {tsv_file} has a blank {column!r} cell on line(s)"
            f" {', '.join(blank_lines)}, counting the header as line 1"
        )


def _build_values_request[RequestT: BaseModel](
    build_request: Callable[[list[str]], RequestT],
    tsv_rows: list[dict[str, str]],
    column: str,
    tsv_file: Path,
    parser: argparse.ArgumentParser,
) -> RequestT:
    """The request that `build_request` makes from every row's `column` value. A
    request that the model refuses exits through `parser.error`, naming the file,
    the column and each refusal."""
    try:
        values_request = build_request([tsv_row[column] for tsv_row in tsv_rows])
    except ValidationError as exc:
        messages = "; ".join(error["msg"] for error in exc.errors())
        parser.error(f"{_TSV_FILE_FLAG} {tsv_file} column {column!r}: {messages}")
    return values_request


def _require_new_output(output: Path, parser: argparse.ArgumentParser) -> None:
    """Exit through `parser.error` unless a new file can be written at `output`."""
    try:
        _common.require_new_output_file(output, flag=_OUTPUT_FLAG)
    except (FileNotFoundError, PermissionError, FileExistsError) as exc:
        parser.error(str(exc))


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
    all-or-nothing request, then write each created biosample's identifiers to
    the TSV file named by --output."""
    owner_biosample_id_column = args.owner_biosample_id_field_name.strip()
    folded_refused_names = {name.casefold() for name in _REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES}
    if owner_biosample_id_column.casefold() in folded_refused_names:
        parser.error(
            f"{_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG} {owner_biosample_id_column!r} cannot be"
            " used: ignoring case, it matches one of"
            f" {', '.join(_REFUSED_OWNER_BIOSAMPLE_ID_COLUMN_NAMES)};"
            " `qiita biosample create` accepts it."
        )

    # Check the output path and read the input file first, so an unusable output path
    # or an unreadable input file stops the command before any biosample is created.
    _require_new_output(args.output, parser)
    tsv_rows = _read_tsv_with_column(
        args.tsv_file,
        owner_biosample_id_column,
        parser,
        column_flag=_OWNER_BIOSAMPLE_ID_FIELD_NAME_FLAG,
    )

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
            _common.write_tsv_output(args.output, output_columns, output_rows)
        except (OSError, duckdb.Error) as exc:
            # The biosamples exist by now, so their identifiers must not be lost.
            created_rows = [dict(zip(output_columns, row, strict=True)) for row in output_rows]
            print(f"error: writing the created biosamples' TSV failed: {exc}", file=sys.stderr)
            print("The biosamples were created; their identifiers are:", file=sys.stderr)
            print(json.dumps(created_rows, indent=2), file=sys.stderr)
            sys.exit(1)

    return _common.run_http_subcommand(_run, render=_render)


def _lookup_column_to_tsv(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
    *,
    column: str,
    column_flag: str,
    returned_column: str,
    build_request: Callable[[list[str]], BaseModel],
    lookup_path: Callable[[], str],
) -> int:
    """Look up every value of `column` in the --tsv-file, and write to --output
    each row's value and the identifier that it resolved to, headed `column` and
    `returned_column`, one row per input row in input order.

    `build_request` makes the lookup request from the column's values and
    `lookup_path` names the route once the study is resolved; any value that does
    not resolve makes the command exit 1 listing them all, with no file written.
    """
    # The output's two headers must differ ignoring case, or the write refuses them.
    if column.casefold() == returned_column.casefold():
        parser.error(
            f"{column_flag} {column} and {_RETURN_KEY_FLAG} {returned_column} would give both"
            f" output columns the same header; choose another {_RETURN_KEY_FLAG}"
        )

    # Check the output, read the file, and build the request before any request is sent.
    _require_new_output(args.output, parser)
    tsv_rows = _read_tsv_with_column(args.tsv_file, column, parser, column_flag=column_flag)

    # The output pastes beside the file only when each data line is one row; the
    # reader skips an empty line in a file of several columns and joins a quoted cell
    # that spans lines.
    data_line_count = count_data_lines(args.tsv_file)
    if data_line_count != len(tsv_rows):
        parser.error(
            f"{_TSV_FILE_FLAG} {args.tsv_file} has {data_line_count} data line(s) but"
            f" {len(tsv_rows)} row(s), so the output would not line up with the file:"
            " remove any empty line between rows and any line break inside a cell"
        )
    _refuse_blank_cells(tsv_rows, column, args.tsv_file, parser)
    lookup_request = _build_values_request(build_request, tsv_rows, column, args.tsv_file, parser)

    def _run(token: str) -> list[tuple[str, str]]:
        _common.resolve_study_idx(args, parser, args.base_url, token)
        lookup = _common.lookup_natural_keys(args.base_url, token, lookup_path(), lookup_request)
        if lookup.missing:
            print(
                f"error: {len(lookup.missing)} value(s) of column {column!r} did not resolve:"
                f" {', '.join(lookup.missing)}",
                file=sys.stderr,
            )
            sys.exit(1)

        # The request holds each row's value as sent, so each output row pairs with its input row.
        output_rows = [(value, str(lookup.resolved[value])) for value in lookup_request.values]
        return output_rows

    def _render(output_rows: list[tuple[str, str]]) -> None:
        try:
            _common.write_tsv_output(args.output, (column, returned_column), output_rows)
        except (OSError, duckdb.Error) as exc:
            print(f"error: writing {args.output} failed: {exc}", file=sys.stderr)
            sys.exit(1)

    return _common.run_http_subcommand(_run, render=_render)


def _handle_biosample_lookup(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Resolve a TSV file's column of biosample identifiers, among one study's
    biosamples, and write each row's value and the identifier that it resolved to."""
    # The column is named by the unique field when one is given, else by the kind.
    if args.unique_field_name is not None:
        column = args.unique_field_name.strip()
        if not column:
            parser.error(f"{_UNIQUE_FIELD_NAME_FLAG}: must not be blank")
        key, column_flag, field_name = UNIQUE_FIELD_KEY, _UNIQUE_FIELD_NAME_FLAG, column
    else:
        column = args.key
        key, column_flag, field_name = _BIOSAMPLE_KEY_BY_CHOICE[args.key], _KEY_FLAG, None

    # The request model and the route, chosen together for a study-scoped lookup.
    def _build_request(values: list[str]) -> BiosampleStudyLookupRequest:
        lookup_request = BiosampleStudyLookupRequest(
            key=key,
            values=values,
            return_key=_LOOKUP_RETURN_KEY_BY_CHOICE[args.return_key],
            unique_field_display_name=field_name,
        )
        return lookup_request

    def _lookup_path() -> str:
        return f"{PATH_STUDY_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_STUDY}".format(
            study_idx=args.study_idx
        )

    return _lookup_column_to_tsv(
        args,
        parser,
        column=column,
        column_flag=column_flag,
        returned_column=args.return_key,
        build_request=_build_request,
        lookup_path=_lookup_path,
    )


def _handle_biosample_bulk_link(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Link into a study every biosample that a TSV file's column of identifiers
    names, in one all-or-nothing request, and print the route's answer."""
    # Read the file and build the request before any request is sent.
    column = args.key
    tsv_rows = _read_tsv_with_column(args.tsv_file, column, parser, column_flag=_KEY_FLAG)
    _refuse_blank_cells(tsv_rows, column, args.tsv_file, parser)

    def _build_request(values: list[str]) -> BiosampleBulkLinkRequest:
        link_request = BiosampleBulkLinkRequest(key=_BIOSAMPLE_KEY_BY_CHOICE[column], values=values)
        return link_request

    link_request = _build_values_request(_build_request, tsv_rows, column, args.tsv_file, parser)

    def _run(token: str) -> dict | list:
        _common.resolve_study_idx(args, parser, args.base_url, token)
        bulk_link_path = f"{PATH_STUDY_PREFIX}{PATH_BIOSAMPLE_BULK_LINK_BY_STUDY}".format(
            study_idx=args.study_idx
        )
        # Each refused value is labelled as the file's column holds it.
        link_response = _common.call_all_or_nothing(
            "POST",
            args.base_url,
            token,
            bulk_link_path,
            json=link_request.model_dump(mode="json"),
            label_for=lambda value: f"{column}={value}",
            advice="nothing was linked; fix these values and re-run.",
        )
        return link_response

    return _common.run_http_subcommand(_run)
