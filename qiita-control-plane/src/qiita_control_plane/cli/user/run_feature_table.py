"""qiita user CLI — the run feature table.

`qiita feature-table runs` writes a feature table of amplicon runs: the control
plane's `/feature-table` routes answer the cohort's columns, its values as sparse
COO `(sample, feature, value)`, and its features, and this writes them as a bundle.
The routes are the one implementation of a run table, which the web UI reads too,
so the two cannot disagree about one.

A run's features are ASVs (counts, named by the md5 of the ASV sequence). The table
is written straight from its COO rows — BIOM and Parquet both take
`(feature_id, sample_id, value)` — so it is never made dense on the way to disk.
The bundle also carries each ASV's sequence.
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path
from typing import Any

from qiita_common import analytic as ft
from qiita_common.actions import AMPLICON_ACTION_ID
from qiita_common.api_paths import (
    PATH_FEATURE_TABLE_FEATURE,
    PATH_FEATURE_TABLE_PREFIX,
    PATH_FEATURE_TABLE_SAMPLE,
    PATH_FEATURE_TABLE_VALUE,
)
from qiita_common.duckdb_relation import registered
from qiita_common.models.run_feature_table import FeatureTableRequest

from ...run_feature_table import ARROW_STREAM_MEDIA_TYPE, FEATURE_RELATION, SOURCES
from .. import _common
from .feature_table import (
    IDENTIFIER_MAP_NOTE,
    SequencesClearance,
    _bundle_targets,
    _tool_versions,
    _write_bundle,
)

MANIFEST_NOTE = (
    "This file records what produced the table beside it: the runs and the samples it"
    " was built over. A feature table is a selection over the lake, so the same runs"
    " over a different cohort yield a different table — which is why this record is"
    " part of the bundle."
)

# What each method's feature ids are, for the manifest.
_FEATURE_ID = {
    AMPLICON_ACTION_ID: "md5 of the ASV sequence",
}


def _request(args: argparse.Namespace) -> FeatureTableRequest:
    return FeatureTableRequest(
        processing_idx=args.processing_idx,
        study_idx=args.study_idx,
        prep_sample_idx=args.prep_sample_idx,
    )


def _arrow(args: argparse.Namespace, token: str, path: str, request: dict[str, Any]):
    """One of the cohort's Arrow IPC answers, with dictionary columns decoded."""
    import pyarrow as pa  # noqa: PLC0415

    body, content_type = _common.fetch_binary(
        args.base_url,
        token,
        f"{PATH_FEATURE_TABLE_PREFIX}{path}",
        accept=ARROW_STREAM_MEDIA_TYPE,
        json=request,
    )
    if not content_type.startswith(ARROW_STREAM_MEDIA_TYPE):
        raise RuntimeError(f"{path} answered {content_type!r}, not an Arrow stream")
    table = pa.ipc.open_stream(body).read_all()
    return pa.table(
        {
            name: (col.cast(col.type.value_type) if pa.types.is_dictionary(col.type) else col)
            for name, col in zip(table.column_names, table.columns, strict=True)
        }
    )


def _build(args: argparse.Namespace, token: str, con) -> tuple[list[Path], dict[str, int]]:
    """Fetch the cohort's columns, values and features, and write the bundle."""
    request = _request(args).model_dump(exclude_none=True)
    columns = _common.call(
        "POST",
        args.base_url,
        token,
        f"{PATH_FEATURE_TABLE_PREFIX}{PATH_FEATURE_TABLE_SAMPLE}",
        json=request,
    )
    source = SOURCES[columns["method"]]
    values = _arrow(args, token, PATH_FEATURE_TABLE_VALUE, request)
    features = _arrow(args, token, PATH_FEATURE_TABLE_FEATURE, request)
    with registered(con, "run_values", values):
        con.execute(
            f"CREATE TEMP TABLE {ft.LABELLED_RELATION} AS"
            " SELECT feature AS feature_id, sample AS sample_id, value"
            " FROM run_values ORDER BY sample_id, feature_id"
        )
    with registered(con, "run_features", features):
        con.execute(f"CREATE TEMP TABLE {FEATURE_RELATION} AS SELECT * FROM run_features")
    unnamed = con.execute(
        f"SELECT count(DISTINCT feature_id) FROM {ft.LABELLED_RELATION}"
        f" WHERE feature_id NOT IN (SELECT feature FROM {FEATURE_RELATION})"
    ).fetchone()[0]
    if unnamed:
        raise RuntimeError(f"{unnamed} feature(s) of the table are missing from its features")
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
    clearances: dict[str, Any] = {"sequences": SequencesClearance(features=sizes["features"])}

    def manifest(targets: list[Path]) -> dict[str, Any]:
        return {
            "note": MANIFEST_NOTE,
            "kind": source.method,
            "value_kind": source.value_kind,
            "request": request,
            "feature_id": _FEATURE_ID[source.method],
            **sizes,
            "files": [p.name for p in targets],
            "versions": _tool_versions(con),
        }

    written = _write_bundle(
        con,
        table_path=args.output,
        fmt=args.format,
        identifiers=columns["samples"],
        manifest=manifest,
        clearances=clearances,
    )
    return written, sizes


def _handle_run_feature_table(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita feature-table runs`. Fails loudly (exit 1) on the
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
        # The refusals that need only the flags, before a connection or a read.
        # The method is known only from the answer; the write checks the whole
        # bundle again.
        _request(args)
        _bundle_targets(args.output, args.format)
        with contextlib.closing(connect_with_miint()) as con:
            written, sizes = _build(args, token, con)
    except ValidationError as exc:
        print(f"error: {exc.errors()[0]['msg']}", file=sys.stderr)
        return 1
    except _common.httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    except _common.httpx.RequestError as exc:
        print(
            f"error: could not reach the control plane: {exc!r}. Check --base-url /"
            " $QIITA_CONTROL_PLANE_URL.",
            file=sys.stderr,
        )
        return 1
    except _common.SlowResponseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except duckdb.Error as exc:
        print(f"error: building the table failed on this machine: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    table_path, map_path, manifest_path, *companions = written
    print(
        f"wrote {sizes['samples']} sample(s) x {sizes['features']} feature(s),"
        f" {sizes['observations']} observation(s), to {table_path} ({args.format})"
    )
    print(f"and the map needed to read it: {map_path}")
    print(f"and the record of what produced it: {manifest_path}")
    for path in companions:
        print(f"and each feature's sequence: {path}")
    print(IDENTIFIER_MAP_NOTE)
    return 0
