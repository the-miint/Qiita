"""qiita user CLI — `qiita feature-table select`, a feature table of stored values.

The control plane's `/feature-table` route answers the cohort's columns, its values as
sparse COO `(sample, feature, value)` and its features' sequences, and this writes them
as a bundle with `feature-table build`'s writer. The route is the one implementation;
this writes what it answers.

The table is written straight from its COO rows — BIOM and Parquet both take
`(feature_id, sample_id, value)` — so it is never made dense on the way to disk.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path
from typing import Any, get_args

from qiita_common import analytic as ft
from qiita_common.api_paths import PATH_FEATURE_TABLE_PREFIX, PATH_FEATURE_TABLE_ROOT
from qiita_common.duckdb_relation import registered
from qiita_common.models import EXPORT_ENTITY_ID
from qiita_common.models.biosample import StudyLookupKey
from qiita_common.models.feature_table import FeatureTableRequest

from ...stored_feature_table import FEATURE_TABLE_MEDIA_TYPE, SOURCES, unpack_feature_table
from .. import _common
from .biosample import _BIOSAMPLE_KEY_BY_CHOICE, _BIOSAMPLE_KEY_CHOICES
from .feature_table import (
    _bundle_targets,
    _companions,
    _tool_versions,
    _write_bundle,
)

MANIFEST_NOTE = (
    "This file records what produced the table beside it: the method, the settings its"
    " runs share, and the samples it was built over. A feature table is a selection"
    " over the lake, so the same method over a different cohort yields a different"
    " table — which is why this record is part of the bundle."
)
# Each `--method` choice: the methods whose stored values make a table.
SELECT_METHOD_CHOICES = tuple(sorted(SOURCES))
# Each `--key` choice: a study's identifier kinds, and a biosample's as the biosample
# commands spell them, with export_entity_id (the default) first.
SELECT_KEY_CHOICES = tuple(
    dict.fromkeys((EXPORT_ENTITY_ID, *get_args(StudyLookupKey), *_BIOSAMPLE_KEY_CHOICES))
)

MAP_NOTE = (
    "This file maps each column of the table beside it, named by its QM (export_id), to"
    " its biosample's public name (biosample), the key to join your sample metadata."
)


def _parameter(text: str) -> tuple[str, Any]:
    """`KEY=VALUE`, with VALUE read as JSON where it parses (150, false) and as a string
    otherwise, since a parameter matches a run's setting by type."""
    key, sep, value = text.partition("=")
    if not sep or not key:
        raise ValueError(f"--parameter takes KEY=VALUE, not {text!r}")
    try:
        return key, json.loads(value)
    except json.JSONDecodeError:
        return key, value


def _request(args: argparse.Namespace) -> FeatureTableRequest:
    named = {} if args.key is None else {"key": _BIOSAMPLE_KEY_BY_CHOICE.get(args.key, args.key)}
    cohort = (
        {"study": {**named, "value": args.study}}
        if args.study is not None
        else {"biosample": {**named, "values": args.biosample}}
    )
    taxonomy = None if args.taxonomy is None else dict(zip(("name", "version"), args.taxonomy))
    return FeatureTableRequest(
        method=args.method,
        parameters=dict(_parameter(p) for p in args.parameter or ()),
        taxonomy=taxonomy,
        **cohort,
    )


def _decoded(table):
    """`table` with its dictionary columns decoded to their values."""
    import pyarrow as pa  # noqa: PLC0415

    return pa.table(
        {
            name: (col.cast(col.type.value_type) if pa.types.is_dictionary(col.type) else col)
            for name, col in zip(table.column_names, table.columns, strict=True)
        }
    )


def _build(args: argparse.Namespace, token: str, con) -> tuple[list[Path], dict[str, int]]:
    """Fetch the cohort's table and write the bundle."""
    body, content_type = _common.fetch_binary(
        args.base_url,
        token,
        f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_ROOT}",
        accept=FEATURE_TABLE_MEDIA_TYPE,
        json=_request(args).model_dump(exclude_none=True, exclude_defaults=True),
    )
    if not content_type.startswith(FEATURE_TABLE_MEDIA_TYPE):
        raise RuntimeError(f"/feature-table answered {content_type!r}, not a feature table")
    answer = unpack_feature_table(body)
    columns, values, features = answer.samples, _decoded(answer.values), _decoded(answer.features)
    with registered(con, "stored_values", values):
        con.execute(
            f"CREATE TEMP TABLE {ft.LABELLED_RELATION} AS"
            " SELECT feature AS feature_id, sample AS sample_id, value"
            " FROM stored_values"
        )
    with registered(con, "stored_features", features):
        con.execute(
            f"CREATE TEMP TABLE {ft.FEATURE_SEQUENCE_TABLE} AS SELECT * FROM stored_features"
        )
    clearances: dict[str, Any] = {}
    if answer.taxonomy is not None:
        with registered(con, "stored_taxonomy", answer.taxonomy):
            # Scoped to the published features, so the sidecar is as wide as the table
            # by construction, which is the clearance it carries.
            con.execute(
                f"CREATE TEMP TABLE {ft.TAXONOMY_SIDECAR_RELATION} AS"
                " SELECT t.feature AS feature_id, t.* EXCLUDE (feature) FROM stored_taxonomy t"
                f" SEMI JOIN {ft.LABELLED_RELATION} l ON l.feature_id = t.feature"
            )
        (rows,) = con.execute(f"SELECT count(*) FROM {ft.TAXONOMY_SIDECAR_RELATION}").fetchone()
        clearances["taxonomy"] = ft.TaxonomyClearance(rows=rows)
    cur = con.execute(ft.sequences_diagnostics_sql())
    clearances["sequences"] = ft.check_sequences_diagnostics(
        **dict(zip((c[0] for c in cur.description), cur.fetchone(), strict=True))
    )
    sizes = dict(
        zip(
            ("observations", "samples", "features"),
            con.execute(
                "SELECT count(*), count(DISTINCT sample_id), count(DISTINCT feature_id)"
                f" FROM {ft.LABELLED_RELATION}"
            ).fetchone(),
            strict=True,
        )
    )

    identifiers = [s.model_dump() for s in columns.samples]

    def manifest(targets: list[Path]) -> dict[str, Any]:
        return {
            "note": MANIFEST_NOTE,
            "kind": columns.method,
            "value_kind": columns.value_kind,
            "parameters": columns.parameters,
            # The reference that classified the lineage beside the table, when one did:
            # the features are the same whichever reference names them, so this is what
            # makes the lineage reproducible.
            "taxonomy": None if columns.taxonomy is None else columns.taxonomy.model_dump(),
            "feature_id": SOURCES[columns.method].feature_id_note,
            # Each column's QM, not the request's idxs: a published manifest carries
            # only handles a reader can resolve. Sorted so the record does not depend
            # on the order the samples came back in.
            "cohort": sorted(s["export_id"] for s in identifiers),
            **sizes,
            "files": [p.name for p in targets],
            "versions": _tool_versions(con),
        }

    written = _write_bundle(
        con,
        table_path=args.output,
        fmt=args.format,
        identifiers=identifiers,
        manifest=manifest,
        clearances=clearances,
        map_note=MAP_NOTE,
    )
    return written, sizes


def _handle_feature_table_select(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita feature-table select`. Fails loudly (exit 1) on the
    first refusal; a failure part-way leaves no bundle file behind."""
    import duckdb  # noqa: PLC0415
    from pydantic import ValidationError  # noqa: PLC0415

    from ...miint import connect_with_miint  # noqa: PLC0415

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        # The refusals that need only the flags, before a connection or a read; the
        # write checks the whole bundle again.
        _request(args)
        companions = ("sequences", "taxonomy") if args.taxonomy else ("sequences",)
        _bundle_targets(args.output, args.format, companions=_companions(*companions))
        with contextlib.closing(connect_with_miint()) as con:
            written, sizes = _build(args, token, con)
    except ValidationError as exc:
        _common.print_validation_error(exc)
        return 1
    except _common.httpx.HTTPStatusError as exc:
        _common.print_http_status_error(exc.response)
        return 1
    except _common.httpx.RequestError as exc:
        _common.print_request_error(exc)
        return 1
    except duckdb.Error as exc:
        print(f"error: building the table failed on this machine: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    table_path, map_path, manifest_path, *companions = written
    print(
        f"wrote {sizes['samples']} column(s) x {sizes['features']} feature(s),"
        f" {sizes['observations']} observation(s), to {table_path} ({args.format})"
    )
    print(f"and the map needed to read it: {map_path}")
    print(f"and the record of what produced it: {manifest_path}")
    for path in companions:
        what = "taxonomic lineage" if path.name.endswith(".taxonomy.parquet") else "sequence"
        print(f"and each feature's {what}: {path}")
    return 0
