"""qiita user CLI — `qiita assembly export`: one assembly run's genomes as FASTA.

Composes the per-run reads a VIEWER holds into files on the caller's machine: the
export roster names the samples, and per sample the membership read (Postgres) says
which contig belongs to which subject, while three run-scoped DoGet streams carry
the contig lengths, the CheckM rows and the contig bytes. Nothing is computed
server-side.

Output, under `--output-dir`:

* one `<biosample accession>_<bin_id>.fasta.gz` per selected genome, its records
  named `<genome>_<n>`, longest first;
* `genomes.tsv` — one row per genome: length, contig count, the count of contigs the
  assembler called circular (`circularity = yes`), GC over the A/C/G/T bases,
  length-weighted depth, and the CheckM columns;
* `contigs.tsv` — one row per record: its header, genome, length and the assembler's
  report, including `raw_name`, the assembler's own contig name.

No internal identifier is written into any of them. The set is committed
all-or-nothing, and nothing is overwritten.
"""

import argparse
import base64
import contextlib
import re
import sys
from collections.abc import Iterator
from pathlib import Path

from qiita_common.api_paths import (
    PATH_ASSEMBLY_MEMBERSHIP_PARQUET,
    PATH_ASSEMBLY_PREFIX,
    PATH_ASSEMBLY_PREP_SAMPLE,
    PATH_ASSEMBLY_RUN_DOGET,
)
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
    BIN_QUALITY_TABLE,
    KIND_LCG,
    KIND_MAG,
    KIND_UNBINNED,
)
from qiita_common.chunking import reassemble_chunks_expr
from qiita_common.parquet import PARQUET_MEDIA_TYPE

from .. import _common

EXPORT_KINDS = (KIND_LCG, KIND_MAG, KIND_UNBINNED)
# An UNBINNED contig is a subject of its own, so exporting that kind writes one file per
# residue contig; it is left for the caller to ask for.
DEFAULT_EXPORT_KINDS = (KIND_LCG, KIND_MAG)

GENOMES_TSV = "genomes.tsv"
CONTIGS_TSV = "contigs.tsv"

# What a genome name may contain. The name becomes a filename and a FASTA header, and
# is inlined into the COPY that writes it, so anything outside this set is refused
# rather than escaped.
_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class ExportRefused(ValueError):
    """A state the export will not write through; the message says which and why."""


# ---------------------------------------------------------------------------
# HTTP and Flight reads
# ---------------------------------------------------------------------------


def _fetch_roster(
    base_url: str,
    token: str,
    *,
    processing_idx: int,
    prep_sample_idx: int | None,
    sequenced_pool_idx: int | None,
    study_idx: int | None,
) -> list[dict]:
    """GET the samples under the run that the caller may read, narrowed by the one
    filter given."""
    sub_path = PATH_ASSEMBLY_PREP_SAMPLE.format(processing_idx=processing_idx)
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_ASSEMBLY_PREFIX}{sub_path}",
        params=_common.filter_params(
            prep_sample_idx=prep_sample_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            study_idx=study_idx,
        ),
    )["samples"]


def _fetch_membership(base_url: str, token: str, *, prep_sample_idx: int, processing_idx: int):
    """GET one run's membership rows as an Arrow table (the uncapped Parquet form)."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    sub_path = PATH_ASSEMBLY_MEMBERSHIP_PARQUET.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    path = f"{PATH_ASSEMBLY_PREFIX}{sub_path}"
    body, content_type = _common.fetch_binary(base_url, token, path, accept=PARQUET_MEDIA_TYPE)
    if content_type.split(";")[0].strip().lower() != PARQUET_MEDIA_TYPE:
        raise RuntimeError(f"{path}: expected {PARQUET_MEDIA_TYPE}, server sent {content_type!r}")
    return pq.read_table(pa.BufferReader(body))


def _mint_run_ticket(
    base_url: str, token: str, *, prep_sample_idx: int, processing_idx: int, table: str
) -> bytes:
    sub_path = PATH_ASSEMBLY_RUN_DOGET.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    resp = _common.call(
        "POST", base_url, token, f"{PATH_ASSEMBLY_PREFIX}{sub_path}", json={"table": table}
    )
    return base64.b64decode(resp["ticket"])


@contextlib.contextmanager
def _registered(con, relation: str, obj) -> Iterator[str]:
    con.register(relation, obj)
    try:
        yield relation
    finally:
        con.unregister(relation)


def _stage_run_table(con, flight_client, ticket: bytes, *, relation: str, table_sql) -> None:
    """Drain one run-scoped DoGet into the table `table_sql(source)` builds.

    Realigned on receive for the reason `cli/user/reference.py` gives on its FASTA path.
    """
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.ipc as ipc  # noqa: PLC0415

    options = flight.FlightCallOptions(
        read_options=ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
    )
    reader = flight_client.do_get(flight.Ticket(ticket), options).to_reader()
    with _registered(con, relation, reader) as source:
        con.execute(table_sql(source))


# ---------------------------------------------------------------------------
# The export
# ---------------------------------------------------------------------------


def _check_roster(samples: list[dict], *, processing_idx: int) -> tuple[list[dict], list[dict]]:
    """Split the roster into the samples to export and the ones that assembled
    nothing; refuse a roster the export cannot name or cannot trust.

    A `pending` or `invalidated` sample is refused rather than skipped: its contigs
    are not to be consumed, and leaving it out would make a pool or study export
    short with nothing in the files to say so.
    """
    if not samples:
        raise ExportRefused(
            f"no sample under processing {processing_idx} that you can read matches these filters"
        )
    unusable = [s for s in samples if s["assembly_state"] in ("pending", "invalidated")]
    if unusable:
        listed = ", ".join(f"{s['prep_sample_idx']} ({s['assembly_state']})" for s in unusable)
        raise ExportRefused(
            f"{len(unusable)} sample(s) under processing {processing_idx} are not completed:"
            f" {listed}. Narrow the export with --prep-sample-idx, or wait for them."
        )
    done = [s for s in samples if s["assembly_state"] == "completed"]
    empty = [s for s in samples if s["assembly_state"] == "no_data"]
    unnamed = [s["prep_sample_idx"] for s in done if not s["biosample_accession"]]
    if unnamed:
        raise ExportRefused(
            f"prep_sample(s) {unnamed} have no biosample accession, which names their"
            " genomes in the export"
        )
    bad = [s["biosample_accession"] for s in done if not _NAME_RE.match(s["biosample_accession"])]
    if bad:
        raise ExportRefused(f"biosample accession(s) {bad} cannot be used in a file name")
    return done, empty


def _create_output_tables(con) -> None:
    con.execute(
        "CREATE TEMP TABLE genome_out (genome VARCHAR, biosample_accession VARCHAR,"
        " kind VARCHAR, bin_id VARCHAR, length_bp BIGINT, n_contigs BIGINT,"
        " n_circular BIGINT, gc DOUBLE, depth DOUBLE, completeness DOUBLE,"
        " contamination DOUBLE, strain_heterogeneity DOUBLE, marker_lineage VARCHAR,"
        " fasta VARCHAR)"
    )
    con.execute(
        "CREATE TEMP TABLE contig_out (contig VARCHAR, genome VARCHAR, length_bp BIGINT,"
        " circularity VARCHAR, depth DOUBLE, mult DOUBLE, raw_name VARCHAR)"
    )


def _drop_sample_tables(con) -> None:
    for table in ("membership", "seqlen", "quality", "genome_sel", "member_sel", "contig"):
        con.execute(f"DROP TABLE IF EXISTS {table}")


def _select_genomes(con, args: argparse.Namespace, *, accession: str) -> None:
    """Build `genome_sel` — the sample's subjects that pass the filters, one row per
    genome with its roll-up and CheckM row — and `member_sel`, their contigs.

    The completeness and contamination bounds exclude a subject CheckM did not score:
    a bound is a claim about a score, and an absent score does not meet it.
    """
    kinds = ", ".join(f"'{k}'" for k in args.kind)
    where = [f"g.kind IN ({kinds})"]
    params: list = []
    for column, op, value in (
        ("g.length_bp", ">=", args.min_bp),
        ("g.length_bp", "<=", args.max_bp),
        ("q.completeness", ">=", args.min_completeness),
        ("q.contamination", "<=", args.max_contamination),
    ):
        if value is not None:
            where.append(f"{column} {op} ?")
            params.append(value)
    con.execute(
        "CREATE TEMP TABLE genome_sel AS"
        " WITH g AS ("
        "   SELECT m.kind, m.bin_id, sum(s.sequence_length_bp) AS length_bp,"
        "          count(*) AS n_contigs,"
        "          count(*) FILTER (WHERE m.circularity = 'yes') AS n_circular,"
        "          sum(m.depth * s.sequence_length_bp) FILTER (WHERE m.depth IS NOT NULL)"
        "            / sum(s.sequence_length_bp) FILTER (WHERE m.depth IS NOT NULL) AS depth"
        "     FROM membership m JOIN seqlen s USING (feature_idx)"
        "    GROUP BY m.kind, m.bin_id)"
        " SELECT ? || '_' || g.bin_id AS genome, g.*, q.completeness, q.contamination,"
        "        q.strain_heterogeneity, q.marker_lineage"
        "   FROM g LEFT JOIN quality q USING (kind, bin_id)"
        "  WHERE " + " AND ".join(where),
        [accession, *params],
    )
    con.execute(
        "CREATE TEMP TABLE member_sel AS"
        " SELECT g.genome, m.feature_idx, s.sequence_length_bp, m.circularity, m.depth,"
        "        m.mult, m.raw_name,"
        "        g.genome || '_' || row_number() OVER ("
        "          PARTITION BY g.genome ORDER BY s.sequence_length_bp DESC, m.feature_idx"
        "        ) AS contig"
        "   FROM genome_sel g"
        "   JOIN membership m USING (kind, bin_id)"
        "   JOIN seqlen s USING (feature_idx)"
    )


def _check_streams_match_membership(con, *, prep_sample_idx: int) -> None:
    """Refuse when the run's streamed contigs and its Postgres membership disagree.

    Postgres keeps a superseded row where the lake replaced it, so after a re-run of
    the same run identity the two can name different contigs. Neither side is the one
    to trust alone: a listed contig with no bytes would drop from its genome, and a
    streamed contig with no membership would belong to no genome.
    """
    (listed_only, streamed_only) = con.execute(
        "SELECT"
        " (SELECT count(*) FROM (SELECT feature_idx FROM membership"
        "   EXCEPT SELECT feature_idx FROM seqlen)),"
        " (SELECT count(*) FROM (SELECT feature_idx FROM seqlen"
        "   EXCEPT SELECT feature_idx FROM membership))"
    ).fetchone()
    if listed_only or streamed_only:
        raise ExportRefused(
            f"prep_sample {prep_sample_idx}: the run's membership lists {listed_only}"
            f" contig(s) the data plane does not serve, and the data plane serves"
            f" {streamed_only} contig(s) the membership does not list. The two copies of"
            " the run's membership disagree; an operator has to reconcile them."
        )
    (repeated,) = con.execute(
        "SELECT count(*) FROM (SELECT kind, bin_id FROM quality"
        " GROUP BY kind, bin_id HAVING count(*) > 1)"
    ).fetchone()
    if repeated:
        raise ExportRefused(
            f"prep_sample {prep_sample_idx}: {repeated} subject(s) carry more than one"
            " CheckM row, so their scores are ambiguous"
        )


def _check_contigs(con, *, prep_sample_idx: int) -> None:
    """Refuse unless every selected contig reassembled to exactly its registered
    length. A chunk registered twice reassembles to a doubled record under one header,
    and a missing chunk to a short one; both would otherwise be written."""
    (missing, wrong_length) = con.execute(
        "SELECT count(*) FILTER (WHERE c.feature_idx IS NULL),"
        "       count(*) FILTER (WHERE c.feature_idx IS NOT NULL"
        "                        AND length(c.sequence) <> ms.sequence_length_bp)"
        "  FROM (SELECT DISTINCT feature_idx, sequence_length_bp FROM member_sel) ms"
        "  LEFT JOIN contig c USING (feature_idx)"
    ).fetchone()
    if missing or wrong_length:
        raise ExportRefused(
            f"prep_sample {prep_sample_idx}: {missing} selected contig(s) came back with no"
            f" bytes and {wrong_length} reassembled to a length other than the registered"
            " one; nothing was written"
        )


def _sql_str(value: str) -> str:
    return value.replace("'", "''")


def _write_sample(
    con, *, accession: str, output_dir: Path, pairs: list[tuple[Path, Path]], names: set[str]
) -> int:
    """Write each selected genome of the staged sample to its FASTA partial and add
    its rows to the two output tables. Returns the number of genomes written."""
    genomes = con.execute(
        "SELECT genome, n_contigs FROM genome_sel ORDER BY kind, bin_id"
    ).fetchall()
    for genome, n_contigs in genomes:
        if not _NAME_RE.match(genome):
            raise ExportRefused(f"genome name {genome!r} cannot be used in a file name")
        if genome in names:
            raise ExportRefused(
                f"two genomes in this export are both named {genome!r}: the name is"
                " <biosample accession>_<bin_id>, and it repeats across the selected"
                " samples or kinds. Export them separately."
            )
        names.add(genome)
        final = output_dir / f"{genome}.fasta.gz"
        if final.exists():
            raise FileExistsError(f"{final} already exists; nothing is overwritten")
        partial = final.with_name(final.name + ".partial")
        pairs.append((partial, final))
        (written,) = con.execute(
            "COPY (SELECT ms.contig AS read_id, c.sequence AS sequence1"
            "        FROM member_sel ms JOIN contig c USING (feature_idx)"
            f"      WHERE ms.genome = '{_sql_str(genome)}'"
            "      ORDER BY ms.sequence_length_bp DESC, ms.feature_idx)"
            f" TO '{_sql_str(str(partial))}' (FORMAT FASTA, COMPRESSION 'gzip')"
        ).fetchone()
        if written != n_contigs:
            raise ExportRefused(f"{genome}: wrote {written} of {n_contigs} record(s)")
    con.execute(
        "INSERT INTO genome_out"
        " SELECT g.genome, ? , g.kind, g.bin_id, g.length_bp, g.n_contigs, g.n_circular,"
        "        gc.gc, g.depth, g.completeness, g.contamination, g.strain_heterogeneity,"
        "        g.marker_lineage, g.genome || '.fasta.gz'"
        "   FROM genome_sel g"
        "   JOIN (SELECT ms.genome,"
        "                sum(length(c.sequence) - length(regexp_replace(c.sequence,"
        "                    '[GCgc]', '', 'g')))"
        "                / nullif(sum(length(c.sequence) - length(regexp_replace(c.sequence,"
        "                    '[ACGTacgt]', '', 'g'))), 0) AS gc"
        "           FROM member_sel ms JOIN contig c USING (feature_idx)"
        "          GROUP BY ms.genome) gc USING (genome)",
        [accession],
    )
    con.execute(
        "INSERT INTO contig_out"
        " SELECT contig, genome, sequence_length_bp, circularity, depth, mult, raw_name"
        "   FROM member_sel ORDER BY genome, sequence_length_bp DESC, feature_idx"
    )
    return len(genomes)


def _export_sample(
    con,
    flight_client,
    args: argparse.Namespace,
    token: str,
    *,
    sample: dict,
    pairs: list[tuple[Path, Path]],
    names: set[str],
) -> int:
    """Stage one sample's run, select its genomes, and write them."""
    ps, run = sample["prep_sample_idx"], args.processing_idx
    _drop_sample_tables(con)

    membership = _fetch_membership(args.base_url, token, prep_sample_idx=ps, processing_idx=run)
    with _registered(con, "membership_arrow", membership):
        con.execute("CREATE TEMP TABLE membership AS SELECT * FROM membership_arrow")

    def _ticket(table: str) -> bytes:
        return _mint_run_ticket(
            args.base_url, token, prep_sample_idx=ps, processing_idx=run, table=table
        )

    _stage_run_table(
        con,
        flight_client,
        _ticket(ASSEMBLED_SEQUENCE_TABLE),
        relation="seqlen_stream",
        table_sql=lambda src: (
            f"CREATE TEMP TABLE seqlen AS SELECT feature_idx, sequence_length_bp FROM {src}"
        ),
    )
    _stage_run_table(
        con,
        flight_client,
        _ticket(BIN_QUALITY_TABLE),
        relation="quality_stream",
        table_sql=lambda src: (
            "CREATE TEMP TABLE quality AS SELECT kind, bin_id, completeness, contamination,"
            f" strain_heterogeneity, marker_lineage FROM {src}"
        ),
    )
    _check_streams_match_membership(con, prep_sample_idx=ps)
    _select_genomes(con, args, accession=sample["biosample_accession"])
    (selected,) = con.execute("SELECT count(*) FROM genome_sel").fetchone()
    if not selected:
        return 0

    _stage_run_table(
        con,
        flight_client,
        _ticket(ASSEMBLED_SEQUENCE_CHUNKS_TABLE),
        relation="chunk_stream",
        table_sql=lambda src: (
            f"CREATE TEMP TABLE contig AS SELECT feature_idx, {reassemble_chunks_expr()}"
            f" AS sequence FROM {src}"
            " WHERE feature_idx IN (SELECT feature_idx FROM member_sel)"
            " GROUP BY feature_idx"
        ),
    )
    _check_contigs(con, prep_sample_idx=ps)
    return _write_sample(
        con,
        accession=sample["biosample_accession"],
        output_dir=args.output_dir,
        pairs=pairs,
        names=names,
    )


def _write_tables(con, *, output_dir: Path, pairs: list[tuple[Path, Path]]) -> None:
    for table, name, order in (
        ("genome_out", GENOMES_TSV, "genome"),
        ("contig_out", CONTIGS_TSV, "genome, length_bp DESC, contig"),
    ):
        final = output_dir / name
        partial = final.with_name(final.name + ".partial")
        pairs.append((partial, final))
        con.execute(
            f"COPY (SELECT * FROM {table} ORDER BY {order})"
            f" TO '{_sql_str(str(partial))}' (FORMAT CSV, DELIMITER '\t', HEADER)"
        )


def run_export(args: argparse.Namespace, token: str, con, flight_client) -> tuple[int, int, list]:
    """The export, on an open miint connection and Flight client. Returns
    `(genomes written, samples exported, samples that assembled nothing)`."""
    output_dir: Path = args.output_dir
    if not output_dir.is_dir():
        raise FileNotFoundError(f"--output-dir {output_dir} is not a directory")
    for name in (GENOMES_TSV, CONTIGS_TSV):
        if (output_dir / name).exists():
            raise FileExistsError(f"{output_dir / name} already exists; nothing is overwritten")

    samples = _fetch_roster(
        args.base_url,
        token,
        processing_idx=args.processing_idx,
        prep_sample_idx=args.prep_sample_idx,
        sequenced_pool_idx=args.sequenced_pool_idx,
        study_idx=args.study_idx,
    )
    done, empty = _check_roster(samples, processing_idx=args.processing_idx)

    pairs: list[tuple[Path, Path]] = []
    counts = {"genomes": 0}

    def _write() -> None:
        _create_output_tables(con)
        names: set[str] = set()
        for sample in done:
            counts["genomes"] += _export_sample(
                con, flight_client, args, token, sample=sample, pairs=pairs, names=names
            )
        _write_tables(con, output_dir=output_dir, pairs=pairs)

    _common.commit_partials(_write, pairs)
    return counts["genomes"], len(done), empty


def _handle_assembly_export(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita assembly export`. Exits 1 on the first refusal, having
    written nothing."""
    import duckdb  # noqa: PLC0415
    import pyarrow.flight as flight  # noqa: PLC0415

    from ...miint import connect_with_miint  # noqa: PLC0415

    if (args.min_bp is not None and args.max_bp is not None) and args.min_bp > args.max_bp:
        parser.error("--min-bp is greater than --max-bp")
    args.kind = tuple(dict.fromkeys(args.kind or DEFAULT_EXPORT_KINDS))
    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        with (
            contextlib.closing(connect_with_miint()) as con,
            flight.FlightClient(args.data_plane_url) as flight_client,
        ):
            genomes, exported, empty = run_export(args, token, con, flight_client)
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
        print(f"error: writing the export failed on this machine: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"wrote {genomes} genome(s) from {exported} sample(s) to {args.output_dir}")
    print(f"per-genome metadata: {args.output_dir / GENOMES_TSV}")
    print(f"per-contig metadata: {args.output_dir / CONTIGS_TSV}")
    if empty:
        listed = ", ".join(str(s["prep_sample_idx"]) for s in empty)
        print(f"assembled nothing, so not exported: prep_sample {listed}", file=sys.stderr)
    return 0
