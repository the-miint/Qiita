"""qiita user CLI — read-only mask-definition subcommands.

`qiita mask list` / `show` / `samples` are the client-side answer to "which mask
is this pool filtered under, and which of its samples are ready to act on?" — the
three questions that otherwise need a psql shell on the deploy host and
DATABASE_URL. A `mask_idx` is a required input to `long-read-assembly`, whose
audience includes a plain `user`, so the reads sit in this CLI rather than
`qiita-admin` (which keeps the destructive `mask delete` / `purge-failed`).

Thin clients: each of those verbs is one GET, printed verbatim, so a new
server-side field reaches the user without a CLI change.

`qiita mask syndna-read-count` is the one that writes a file: the per-insert SynDNA
read counts of a selection of prep_samples, as BIOM (default) or Parquet, named by
public identifiers only.
"""

import argparse
import contextlib
import sys
from collections.abc import Sequence
from pathlib import Path

from qiita_common.analytic import LABELLED_RELATION, biom_copy_sql, parquet_copy_sql
from qiita_common.api_paths import (
    PATH_MASK_DEFINITION_PREFIX,
    PATH_MASK_DEFINITION_SYNDNA_READ_COUNT,
)
from qiita_common.models import SyndnaReadCountResponse
from qiita_common.taxonomy import TAXONOMY_SOURCE_TABLE

from .. import _common
from .feature_table import TABLE_FORMATS, create_reference_doget_ticket, staged_stream


def _list_mask_definitions(
    base_url: str,
    token: str,
    *,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
) -> dict:
    """GET /api/v1/mask-definition. Returns each mask's config plus its
    completed / pending sample tally under the same filters."""
    return _common.call(
        "GET",
        base_url,
        token,
        PATH_MASK_DEFINITION_PREFIX,
        params=_common.filter_params(
            sequenced_pool_idx=sequenced_pool_idx, prep_sample_idx=prep_sample_idx
        ),
    )


def _get_mask_definition(base_url: str, token: str, mask_idx: int) -> dict:
    """GET /api/v1/mask-definition/{mask_idx}. Returns the mask's config blob —
    host/spike-in reference idxs and the resolved QC constants."""
    return _common.call("GET", base_url, token, f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}")


def _list_mask_prep_samples(
    base_url: str,
    token: str,
    mask_idx: int,
    *,
    sequenced_pool_idx: int | None,
) -> dict:
    """GET /api/v1/mask-definition/{mask_idx}/prep-sample. Returns one row per
    sample masked under this mask, with its state and which masking path
    resolved it."""
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_MASK_DEFINITION_PREFIX}/{mask_idx}/prep-sample",
        params=_common.filter_params(sequenced_pool_idx=sequenced_pool_idx),
    )


def _handle_mask_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List masks with their sample tallies. Filter by --sequenced-pool-idx to
    separate the masks one pool carries: `params` distinguishes them by config
    (a non-null host_rype_reference_idx is the human-filtered one) and the tally
    says which is usable."""
    return _common.run_http_subcommand(
        lambda t: _list_mask_definitions(
            args.base_url,
            t,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
        )
    )


def _handle_mask_show(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Print one mask's config, so what a filter ran with is quotable rather than
    read out of the orchestrator source."""
    return _common.run_http_subcommand(
        lambda t: _get_mask_definition(args.base_url, t, args.mask_idx)
    )


def _handle_mask_samples(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List the samples masked under one mask. The `completed` rows are the set a
    masked-read pull or an assembly submission can act on."""
    return _common.run_http_subcommand(
        lambda t: _list_mask_prep_samples(
            args.base_url,
            t,
            args.mask_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        )
    )


# The feature table's two formats, with BIOM the default here: classic Qiita publishes
# this table as syndna.biom. The same relation is written either way; BIOM drops zero
# cells (it is sparse), Parquet keeps them.
SYNDNA_TABLE_FORMATS = TABLE_FORMATS
DEFAULT_SYNDNA_TABLE_FORMAT = "biom"

# Where an insert's public name comes from: the FASTA header the reference load
# recorded (Postgres, in the route's response), or the `species` rank of the
# reference's taxonomy (DuckLake, over a reference DoGet).
FEATURE_NAME_SOURCES = ("accession", "species")
DEFAULT_FEATURE_NAME_SOURCE = "accession"


def _get_syndna_read_count(
    base_url: str,
    token: str,
    mask_idx: int,
    *,
    study_idx: int | None,
    sequenced_pool_idx: int | None,
    prep_sample_idx: Sequence[int] | None,
) -> SyndnaReadCountResponse:
    """GET /api/v1/mask-definition/{mask_idx}/syndna-read-count."""
    params: dict = _common.filter_params(study_idx=study_idx, sequenced_pool_idx=sequenced_pool_idx)
    if prep_sample_idx:
        params["prep_sample_idx"] = [str(i) for i in prep_sample_idx]
    body = _common.call(
        "GET",
        base_url,
        token,
        PATH_MASK_DEFINITION_PREFIX
        + PATH_MASK_DEFINITION_SYNDNA_READ_COUNT.format(mask_idx=mask_idx),
        params=params,
    )
    return SyndnaReadCountResponse.model_validate(body)


def syndna_sample_names(response: SyndnaReadCountResponse, *, prefix_pool: bool) -> list[str]:
    """Each prep_sample's `sample_id` in the file, in `response.samples` order: its
    biosample accession, or `<sequenced_pool_idx>_<accession>` under `prefix_pool`.

    Raises when a prep_sample has no accession (or, under `prefix_pool`, no pool), or
    when two would share a name — the BIOM writer sums duplicate cells without a
    trace, so a collision would silently merge two prep_samples.
    """
    names: list[str] = []
    owners: dict[str, list[int]] = {}
    for sample in response.samples:
        if not sample.biosample_accession:
            raise ValueError(
                f"prep_sample {sample.prep_sample_idx} has no biosample accession to name it by"
            )
        name = sample.biosample_accession
        if prefix_pool:
            if sample.sequenced_pool_idx is None:
                raise ValueError(
                    f"prep_sample {sample.prep_sample_idx} is on no sequenced_pool;"
                    " --prefix-pool cannot name it"
                )
            name = f"{sample.sequenced_pool_idx}_{name}"
        names.append(name)
        owners.setdefault(name, []).append(sample.prep_sample_idx)
    shared = {name: idxs for name, idxs in owners.items() if len(idxs) > 1}
    if shared:
        name, idxs = next(iter(sorted(shared.items())))
        hint = (
            "; narrow with --prep-sample-idx"
            if prefix_pool
            else "; pass --prefix-pool, or narrow with --prep-sample-idx"
        )
        raise ValueError(
            f"{len(shared)} sample_id(s) would be shared, e.g. {name!r} by prep_samples"
            f" {idxs}{hint}"
        )
    return names


def syndna_feature_names(
    response: SyndnaReadCountResponse, species: dict[int, str | None] | None
) -> list[str]:
    """Each insert's name in the file, in `response.inserts` order: its recorded
    accession, or its taxonomy `species` when `species` is given. Raises on a missing
    or repeated name, for the reason `syndna_sample_names` does."""
    names: list[str] = []
    for insert in response.inserts:
        name = insert.accession if species is None else species.get(insert.feature_idx)
        if not name:
            source = "accession" if species is None else "taxonomy species"
            raise ValueError(
                f"an insert of SynDNA reference {response.reference_idx} has no {source};"
                " try the other --feature-names source"
            )
        names.append(name)
    repeated = sorted({n for n in names if names.count(n) > 1})
    if repeated:
        raise ValueError(
            f"inserts of SynDNA reference {response.reference_idx} share the name(s)"
            f" {repeated[:5]}; try the other --feature-names source"
        )
    return names


def _fetch_species(
    base_url: str, token: str, con, data_plane_url: str, reference_idx: int
) -> dict[int, str | None]:
    """feature_idx → `species` over the reference's exclusion-aware taxonomy."""
    import pyarrow.flight as flight  # noqa: PLC0415

    ticket = create_reference_doget_ticket(
        base_url, token, reference_idx=reference_idx, table=TAXONOMY_SOURCE_TABLE
    )
    with (
        flight.FlightClient(data_plane_url) as client,
        staged_stream(con, client, ticket, relation="syndna_taxonomy") as source,
    ):
        rows = con.execute(f"SELECT feature_idx, species FROM {source}").fetchall()
    return {int(feature_idx): species for feature_idx, species in rows}


def write_syndna_table(
    con,
    response: SyndnaReadCountResponse,
    *,
    sample_names: list[str],
    feature_names: list[str],
    output: Path,
    fmt: str,
) -> int:
    """Write the `(sample_id, feature_id, value)` table to `output`; return the rows
    staged. Written to a `.partial` sibling and renamed, so a failure leaves no
    table behind. Refuses an existing `output`: the Parquet COPY would replace it
    silently."""
    if fmt not in SYNDNA_TABLE_FORMATS:
        raise ValueError(f"unsupported format {fmt!r} (expected {SYNDNA_TABLE_FORMATS})")
    if output.exists():
        raise ValueError(f"{output} already exists; remove it or choose another --output")
    rows = [
        (sample_names[i], feature_names[j], float(count))
        for i, sample in enumerate(response.samples)
        for j, count in enumerate(sample.read_counts)
    ]
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {LABELLED_RELATION}"
        " (sample_id VARCHAR, feature_id VARCHAR, value DOUBLE)"
    )
    if rows:
        con.executemany(f"INSERT INTO {LABELLED_RELATION} VALUES (?, ?, ?)", rows)
    partial = output.with_name(output.name + ".partial")
    partial.unlink(missing_ok=True)
    # Required by the Parquet COPY's options; see qiita_common.parquet.
    con.execute("SET preserve_insertion_order=false")
    try:
        con.execute(parquet_copy_sql(partial) if fmt == "parquet" else biom_copy_sql(partial))
        partial.replace(output)
    finally:
        partial.unlink(missing_ok=True)
    return len(rows)


def _handle_mask_syndna_read_count(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Entry point for `qiita mask syndna-read-count`. Exits 1 on any refusal, having
    written nothing."""
    import duckdb  # noqa: PLC0415
    import pyarrow.flight as flight  # noqa: PLC0415

    from ...miint import connect_with_miint  # noqa: PLC0415

    if args.study_idx is None and args.sequenced_pool_idx is None and not args.prep_sample_idx:
        parser.error("name at least one of --study-idx, --sequenced-pool-idx, --prep-sample-idx")
    if args.feature_names == "species" and not args.data_plane_url:
        parser.error("--feature-names species reads the reference taxonomy; pass --data-plane-url")
    try:
        token = _common.read_token()
        response = _get_syndna_read_count(
            args.base_url,
            token,
            args.mask_idx,
            study_idx=args.study_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
        )
        samples = syndna_sample_names(response, prefix_pool=args.prefix_pool)
        with contextlib.closing(connect_with_miint()) as con:
            species = (
                _fetch_species(
                    args.base_url, token, con, args.data_plane_url, response.reference_idx
                )
                if args.feature_names == "species"
                else None
            )
            features = syndna_feature_names(response, species)
            write_syndna_table(
                con,
                response,
                sample_names=samples,
                feature_names=features,
                output=args.output,
                fmt=args.format,
            )
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
    except flight.FlightError as exc:
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except duckdb.Error as exc:
        print(f"error: writing the table failed on this machine: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"wrote {len(samples)} sample(s) x {len(features)} insert(s) to {args.output}"
        f" ({args.format})"
    )
    return 0
