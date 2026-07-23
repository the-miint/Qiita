"""Native job: align masked HiFi reads back to the noLCG contigs for binning coverage.

metaWRAP computes binning coverage by self-aligning the reads with **bwa**, a
short-read aligner, and `metawrap binning` has no aligner-selection flag. Its one
documented-by-behaviour seam is the alignment cache: it guards its own `bwa mem`
behind `if [[ ! -f <out>/work_files/<sample>.bam ]]` and skips it when that file
already exists, then derives depth from `work_files/*.bam`. qp-pacbio uses exactly
that seam — it pre-maps with `minimap2 -x map-hifi` and drops the sorted BAM into
work_files/. This step is our version of that pre-map, done with miint's embedded
minimap2 instead of a container-local binary.

It has to be a NATIVE step: miint is deliberately not exposed to containers
(`slurm/payload.py` forwards only the container contract vars, never the miint
dirs), so the aligner cannot run inside the binning image. The BAM therefore
crosses a step boundary as a file path. That is the intermediate-materialisation
the repo's streaming rule tells us to avoid, and it is called out rather than
hidden: metaWRAP's interface *is* "a BAM at this path", so the handoff is forced
by the third-party tool, not chosen. It is the thing to design away if metaWRAP is
ever replaced, not a pattern to copy.

Three behaviours of miint's BAM writer this step depends on. The parameters are
documented upstream (<https://the-miint.github.io/duckdb-miint/writing/>); the
notes below are what a probe against the shipped build adds on top:

  * `COPY … (FORMAT BAM)` REQUIRES `REFERENCE_LENGTHS`, and emits an @SQ line for
    every contig in that table — including zero-coverage ones, which is what makes
    jgi report them at depth 0 instead of dropping them.
  * @SQ comes out **reversed** from the reference-lengths table's physical order,
    and jgi's sortedness check is on the @SQ index (tid), not the contig name. So
    the table is built DESC to land @SQ ascending, after which `ORDER BY reference,
    position` is a genuine coordinate sort. Feeding the table ascending produces a
    BAM jgi rejects outright. That reversal is undocumented, so `_assert_sq_order`
    below re-checks it on every run and the contract test pins it; a miint version
    bump that changes it fails here, loudly, at the producer.
  * `SEQUENCE_DATA` RAISES on a lookup miss (`Invalid Input Error: Read '<id>' not
    found in SEQUENCE_DATA table`) rather than falling back to `*` for that record
    — so a partial lookup cannot silently reintroduce the depth bias below. Probed
    against hard-clipped supplementary, reverse-strand, AND secondary records: the
    writer trims SEQ to the clipped CIGAR, reverse-complements as SAM requires, and
    fills SEQ on secondaries too (by read_id, so they are NOT left `*` the way a
    conventional aligner writes them). `samtools quickcheck` clean, zero SEQ/CIGAR
    length mismatches, jgi accepts with no warnings. This is why keeping secondaries
    (no `max_secondary := 0`, unlike syndna/host_filter) is safe here.

WHY `SEQUENCE_DATA` IS NOT OPTIONAL. By default `FORMAT BAM` writes SEQ as `*`,
and that silently corrupts the depth jgi reports. Coverage ramps DOWN at both
contig ends (a read cannot extend past the end), so jgi excludes up to
`--maxEdgeBases` (default 75) at each end from the mean — and it sizes that
window from the READ LENGTH, which it takes from SEQ. With SEQ absent `seqlen`
is 0, the window collapses, the two low-coverage ramps are averaged in, and
reported depth falls by roughly `2 * 75 / contig_length`.

That is not a harmless constant: it is INVERSELY PROPORTIONAL TO CONTIG LENGTH,
so it distorts exactly the differential-coverage signal metabat2 clusters on, and
it inflates the depth variance metabat2 also consumes. metaWRAP invokes jgi
itself with no edge options, so it cannot be corrected downstream.

Measured on a 20 kb contig: 6.2506 without SEQ vs 6.29783 with — a 0.750% drop
against 150/20000 = 0.750% predicted, and confirmed by forcing
`--maxEdgeBases 0` on a SEQ-bearing BAM, which reproduces the no-SEQ number
exactly while the no-SEQ BAM ignores that flag entirely. Passing `SEQUENCE_DATA`
restores depth AND variance to the values a real `minimap2 | samtools sort` BAM
produces, to every printed digit, and silences jgi's per-record warnings. If you
are tempted to drop it because "the aligner already knows the sequences": it does
not put them in the file, and the resulting error is silent.

TODO(sizing): the SEQUENCE_DATA lookup is unspillable and holds ~1.5-1.7x the raw
read-sequence bytes (probed). `baseline_resources` in the workflow YAML has not
been validated against a real per-sample masked HiFi read volume — do that against
a real ticket's MaxRSS (`sacct`) and adjust, or the largest samples OOM in a way
escalation cannot fix. See the memory-split note at `_DUCKDB_CAP_GB`.
"""

from __future__ import annotations

import gzip
import struct
from pathlib import Path

from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)

# YAML step name this module implements.
YAML_STEP_NAME = "assembly_coverage"

# Output basename. metaWRAP derives its `sample` from the READS filename
# (`tmp=${reads##*/}; sample=${tmp%.*}`), so the name that matters is the one
# binning.sh copies this to inside work_files/ — not this one.
_BAM_NAME = "coverage.bam"

# The assemble step's non-circular contigs; the thing being binned.
_NOLCG_NAME = "noLCG.fa"

# PacBio HiFi. `map-hifi` is accepted by align_minimap2 (probed; an unknown preset
# raises `Unknown minimap2 preset`, so acceptance is not a silent no-op). This is
# the whole point of the step: metaWRAP's built-in path would use bwa here.
_MM2_PRESET = "map-hifi"

# Memory split. Two consumers share this step's cgroup, and both scale with the
# read set — so the split is not "DuckDB gets most, reserve a bit," it is the
# inverse (the shape build_rype_index / build_routing_index use for an in-process
# co-consumer):
#
#   * DuckDB holds the alignment table and sorts it for the COPY. It CAN spill to
#     temp_directory, so it is CAPPED at a modest limit and left to spill beyond.
#   * minimap2's index AND the writer's SEQUENCE_DATA lookup run inside the miint
#     call, on a separate connection, OUTSIDE DuckDB's memory_limit — and they
#     CANNOT spill. SEQUENCE_DATA holds ~1.5-1.7x the raw read-sequence bytes
#     (probed: +1.07 GB for 32k x ~10 kb reads over the no-SEQ baseline), growing
#     with the read set.
#
# So DuckDB is capped (cap_gb) and the non-spillable extension work gets the cgroup
# REMAINDER, which grows when OOM-escalation raises the allocation. The earlier
# shape (fixed reserve_gb, DuckDB gets the rest) pinned the extension at a constant
# and sent every escalated GB to the side that can already spill — an SEQUENCE_DATA
# OOM could never be escalated out of.
#
# CEILING, NOT YET SETTLED: for a read set whose sequence bytes * ~1.6 exceed the
# cgroup remainder, no escalation helps (the lookup is unspillable). Whether one
# sample's masked HiFi read set fits `baseline_resources` is a sizing question for
# a real sample — see the module TODO.
_DUCKDB_THREADS = 8
# DuckDB's cap under SLURM — modest on purpose: it spills beyond this, and the
# memory that matters is the extension's. Off SLURM (local/dev), the resolver
# returns the fallback instead of the cap, so the two are separate constants
# (mirroring build_rype_index): the fallback is dev-box-sized, the cap is what
# actually bounds DuckDB against the cgroup on the cluster.
_DUCKDB_CAP_GB = 16
_DUCKDB_FALLBACK_GB = 4

# In-DuckDB relation names.
_CONTIGS = "coverage_contigs"
_READS = "coverage_reads"
_ALIGNMENT = "coverage_alignment"
_REFLEN = "coverage_reflen"
_SEQDATA = "coverage_seqdata"


class Inputs(BaseModel):
    """Typed input contract.

    `genomes_dir` is the assemble step's output (we read `noLCG.fa` from it);
    `masked_reads_fastq` is the runner-streamed read set the binning step also
    consumes. `prep_sample_idx` / `work_ticket_idx` are framework-injected scope
    scalars (part of the native contract).
    """

    genomes_dir: Path
    masked_reads_fastq: Path
    prep_sample_idx: int
    work_ticket_idx: int


def _read_bam_reference_names(path: Path) -> list[str]:
    """Return the @SQ reference names, in header order, from a BAM.

    Parsed directly rather than shelled out to samtools: the orchestrator's native
    env has miint, not samtools, and this must run in the same process that wrote
    the file. BAM is BGZF, which is gzip-compatible, so the stdlib reads it.

    Layout after decompression: magic `BAM\\1`, int32 l_text, l_text bytes of
    header text, int32 n_ref, then per reference int32 l_name, l_name bytes of
    NUL-terminated name, int32 l_ref.
    """
    with gzip.open(path, "rb") as fh:
        magic = fh.read(4)
        if magic != b"BAM\x01":
            raise ValueError(f"{path} is not a BAM file (magic={magic!r})")
        (l_text,) = struct.unpack("<i", fh.read(4))
        fh.read(l_text)
        (n_ref,) = struct.unpack("<i", fh.read(4))
        names: list[str] = []
        for _ in range(n_ref):
            (l_name,) = struct.unpack("<i", fh.read(4))
            names.append(fh.read(l_name).rstrip(b"\x00").decode())
            fh.read(4)  # l_ref, unused
        return names


def _assert_sq_order(bam: Path, expected: list[str]) -> None:
    """Fail the step if @SQ is not in the order the coordinate sort assumed.

    This is the guard on the undocumented reversal described in the module
    docstring. Without it, a miint change to @SQ ordering would produce a BAM
    whose records are sorted by NAME while its tids run the other way — which jgi
    rejects three steps later with "the bam file is not sorted!", pointing at the
    binning container rather than at the writer that actually caused it.
    """
    actual = _read_bam_reference_names(bam)
    if actual != expected:
        raise RuntimeError(
            "miint's FORMAT BAM emitted @SQ in an unexpected order, so the "
            "coordinate sort in this step is no longer valid.\n"
            f"  expected (ascending): {expected}\n"
            f"  actual:               {actual}\n"
            "This step builds the REFERENCE_LENGTHS table in DESCENDING name "
            "order because miint reverses it when writing @SQ. If that reversal "
            "changed, drop the ORDER BY ... DESC on the reflen table and re-pin "
            "tests/jobs/test_assembly_coverage.py."
        )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    nolcg = inputs.genomes_dir / _NOLCG_NAME
    if not inputs.genomes_dir.is_dir():
        raise FileNotFoundError(f"genomes_dir not found: {inputs.genomes_dir}")
    if not inputs.masked_reads_fastq.exists():
        raise FileNotFoundError(f"masked_reads_fastq not found: {inputs.masked_reads_fastq}")

    workspace.mkdir(parents=True, exist_ok=True)
    bam = workspace / _BAM_NAME
    # A COPY target is a SQL string literal and cannot be bound, so it is
    # sanitised instead — same helper every other COPY in `jobs/` uses. Named for
    # Parquet, but format-agnostic: it rejects quotes/backslashes/control chars.
    bam_sql = validate_parquet_path(bam)
    reads_sql = validate_parquet_path(inputs.masked_reads_fastq)

    # No contigs to bin. The binning step short-circuits on the same condition
    # (empty noLCG.fa -> empty bins_dir, exit 0) and never reaches the BAM, so
    # emit an empty marker rather than raising: a no-contig assembly is a valid
    # outcome of this pipeline, not a failure of this step.
    if not nolcg.exists() or nolcg.stat().st_size == 0:
        bam.write_bytes(b"")
        return {"coverage_bam": bam}

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(
                    _DUCKDB_FALLBACK_GB, threads=_DUCKDB_THREADS, cap_gb=_DUCKDB_CAP_GB
                ),
                threads=_DUCKDB_THREADS,
            )
            # apply_duckdb_settings sets preserve_insertion_order=false, which
            # exists for the chunked-Parquet flush path (it lets the vectorized
            # engine reorder row groups). This job writes a BAM, not Parquet, and
            # its correctness depends on the COPY's ORDER BY reaching the writer
            # intact — so remove the assumption rather than rely on it: with
            # preservation ON, the explicit ORDER BY is authoritative. (Probed
            # fine either way at 9600 rows / 4 threads, but "fine when I tested it"
            # is exactly the assumption worth deleting.) `_assert_sq_order` still
            # backstops the header side.
            conn.execute("SET preserve_insertion_order=true")

            # Persistent relations, not TEMP/CTE: miint's table functions resolve
            # relation names on a SEPARATE connection, which sees neither.
            #
            # The reads are VIEWs and the contigs a TABLE, deliberately. A HiFi
            # read set is tens of GB; materialising it would spend the whole
            # memory_limit — the failure `bam_to_parquet` documents hitting on the
            # first real PacBio run. It is read TWICE (once by the aligner, once by
            # the writer's SEQUENCE_DATA lookup), so the view re-decodes the FASTQ
            # rather than holding it; that trade is deliberate, and the second pass
            # is the price of a SEQ-bearing BAM (see the docstring — without it the
            # depth is wrong). The contigs are small and read three times (aligner
            # subject, reflen, expected @SQ), so they are worth holding.
            conn.execute(
                f"CREATE OR REPLACE TABLE {_CONTIGS} AS "
                "SELECT read_id, sequence1 FROM read_fastx(?)",
                [str(nolcg)],
            )
            # Interpolated, not bound: DuckDB refuses a prepared parameter in a
            # VIEW body ("Unexpected prepared parameter"). Sanitised with the same
            # helper the COPY target uses.
            # The writer's SEQUENCE_DATA lookup wants read_id / sequence1 / qual1
            # (`read_fastx` emits qual1 as the UTINYINT[] it expects).
            conn.execute(
                f"CREATE OR REPLACE VIEW {_SEQDATA} AS "
                f"SELECT read_id, sequence1, qual1 FROM read_fastx('{reads_sql}')"
            )
            # Projected off _SEQDATA so the source path is stated once. Kept a
            # separate relation rather than pointing the aligner at _SEQDATA: the
            # query must carry exactly the columns align_minimap2 expects, and in
            # particular no `sequence2`, which would put it in paired-end mode.
            conn.execute(
                f"CREATE OR REPLACE VIEW {_READS} AS SELECT read_id, sequence1 FROM {_SEQDATA}"
            )
            # No `max_secondary := 0`, unlike syndna/host_filter — and NOT an
            # oversight. Those two ask "did this read come from X?", where a
            # secondary alignment is a false positive. This is a DEPTH input, and
            # the depth we are matching is qp-pacbio's, whose pre-map is
            # `minimap2 -x map-hifi -a --MD --eqx` with no `-N`/`--secondary=no`
            # — so its BAM carries secondaries too. Suppressing them here would
            # make our coverage quietly differ from the pipeline this was ported
            # from, which is the opposite of the point.
            conn.execute(
                f"CREATE OR REPLACE TABLE {_ALIGNMENT} AS "
                "SELECT * FROM align_minimap2(?, subject_table := ?, preset := ?)",
                [_READS, _CONTIGS, _MM2_PRESET],
            )

            # DESC on purpose — see the module docstring. This lands @SQ ascending,
            # which is what makes the ORDER BY below a real coordinate sort.
            conn.execute(
                f"CREATE OR REPLACE TABLE {_REFLEN} AS "
                "SELECT read_id AS reference, length(sequence1) AS length "
                f"FROM {_CONTIGS} ORDER BY read_id DESC"
            )
            expected_sq = [
                r[0]
                for r in conn.execute(
                    f"SELECT read_id FROM {_CONTIGS} ORDER BY read_id ASC"
                ).fetchall()
            ]

            # Contigs exist, so alignments must too. Zero here is NOT the
            # no-contig case handled above: it would write a header-only BAM,
            # which is non-empty and so passes binning.sh's `-s` guard, after
            # which metaWRAP skips bwa, jgi reports every contig at depth 0, and
            # the ticket COMPLETES with an empty bins_dir — indistinguishable
            # from "nothing was binnable". Fail loudly instead.
            aligned = conn.execute(f"SELECT count(*) FROM {_ALIGNMENT}").fetchone()[0]
            if aligned == 0:
                raise RuntimeError(
                    f"align_minimap2 produced no alignments for {inputs.masked_reads_fastq} "
                    f"against {nolcg} ({len(expected_sq)} contigs). Emitting a "
                    "header-only BAM would let binning complete with zero depth and "
                    "no bins, reported as success."
                )

            # SEQUENCE_DATA is REQUIRED for correctness here, not a nicety —
            # see the module docstring. Without it SEQ is written as `*`, and
            # jgi silently reports a length-dependent under-estimate of depth.
            conn.execute(
                f"COPY (SELECT * FROM {_ALIGNMENT} "
                "      ORDER BY reference ASC, position ASC) "
                f"TO '{bam_sql}' (FORMAT BAM, REFERENCE_LENGTHS '{_REFLEN}', "
                f"SEQUENCE_DATA '{_SEQDATA}')"
            )
        _assert_sq_order(bam, expected_sq)
        success = True
    finally:
        if not success:
            bam.unlink(missing_ok=True)

    return {"coverage_bam": bam}
