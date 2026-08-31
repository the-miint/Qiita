"""Split myloasm's `assembly_primary.fa` into circular (LCG) and linear (noLCG)
multi-FASTAs, using miint's `read_fastx` reader and `FORMAT FASTA` writer.

Run by assemble.sh's myloasm branch:

    python3 /opt/qiita/myloasm_split.py <assembly_primary.fa> <circular.fa> <noLCG.fa>

WHY THE HEADER AND NOT THE GFA
------------------------------
hifiasm-meta encodes circularity in its GFA segment NAME (`…tg……c` circular /
`…l` linear), which is what assemble.sh's hifiasm arm splits on. myloasm does NOT:
it states circularity directly in the assembly_primary.fa header, and its GFA
segment names carry no such marker. Applying the hifiasm rule to myloasm output
would match nothing and silently route every closed genome to binning, losing the
LCG class outright with a green exit — which is why this reads the header, and why
an unrecognised header fails the step instead of falling through.

Probed on myloasm 0.6.0 (bioconda) against one real genome — M. genitalium G37
(NC_000908.2), a complete circular chromosome — sampled twice into read sets
identical in every respect EXCEPT wrap-around, then assembled separately:

    with wrap:    >u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00
    without wrap: >u278ctg_len-577882_circular-no_depth-33-33-33_duplicated-no mult=1.00

Topology was the only variable, so the `circular-` field does track it.

WHAT MIINT DOES AND DOES NOT ABSORB
-----------------------------------
FASTA framing is `read_fastx`'s job and FASTA emission is `COPY … (FORMAT FASTA)`'s
— neither is re-implemented here, and sequence bytes are never touched by this
file. What miint cannot absorb is the header FIELD grammar: `read_fastx` yields
`read_id` = the header's FIRST TOKEN, and for myloasm that token is the whole
`u713ctg_len-580076_circular-yes_depth-…_duplicated-no` blob (the space-separated
`mult=1.00` tail lands in a separate `comment` column). So the field parsing below
is real work, expressed in SQL rather than a hand-written scanner.

WHICH miint — THE DEPLOY-STAGED ONE
-----------------------------------
This LOADs the extension the deploy already staged, bind-mounted into the
container read-only via the step's `derived_inputs`. It is the SAME build the
control plane, the compute orchestrator and the data plane run, which is the
point: a copy baked into this image would be a fourth miint that `make
preflight`'s byte-identity check does not compare, free to drift from the other
three.

LOAD-only, never INSTALL — the standing service-side rule. A per-job INSTALL would
need the mirror reachable from every compute node and a writable `$HOME`.

DuckDB namespaces the staged directory by **engine version + platform**, so this
container's DuckDB must be the version the orchestrator staged with. assemble.def
pins it and asserts it at build time; a mismatch surfaces here as an explicit LOAD
failure rather than a wrong answer.

WHAT COUNTS AS CIRCULAR
-----------------------
ONLY `circular-yes`. `circular-possibly` (a self-loop failing myloasm's
depth/connectivity criteria) is accepted as a well-formed value and routed to
noLCG. It is not rare: a run over one sample's real masked reads produced six of
them, and the flip from `yes` sits at a mean depth of 5.5, so a normal-coverage
sample reaches it. Routing it to binning is the safe default — a mis-called linear
contig is still recovered through binning, whereas a mis-called circular one
bypasses binning entirely and is stored as a genome that was never closed — and it
is what the assay owner does by hand today.

The call is also STORED per contig now, not only routed on, so the decision is
recoverable at query time without re-running the assembler: `contig_attributes.tsv`
carries the three-state value beside the raw header it came from.

WHY THE ID IS TRUNCATED AT `_len-`
----------------------------------
Whatever is left becomes the LCG bin_id in assembly_hash, and so the subject id in
`qiita.assembly_membership`. What the probe established:

  * myloasm is DETERMINISTIC — two runs over the same reads gave byte-identical
    headers. A literal re-run of a sample moves nothing.
  * Across two read samplings of the SAME circular genome, `depth-` moved
    (32-32-32 -> 31-31-31) while `_len-` was IDENTICAL (580076 both), and the
    unitig id itself moved (u713ctg -> u932ctg).

So truncation is worth doing because the discarded fields demonstrably vary with
the read set and a bin_id must not carry coverage statistics. It does NOT make
bin_id stable across re-assemblies from different reads — nothing in this header
would, since the unitig number moves too.

ATTRIBUTED, NOT REPRODUCED (Lucas Patel, on the issue that requested this): that
`_len-<N>` drifts by a few bp between re-assemblies as a circular contig's
rotational start moves. Our probe saw an identical length across two different read
samplings, so it neither confirms nor refutes it. Truncating costs nothing either
way.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

# 64 = EX_USAGE, the exit code every entrypoint in this workflow uses for a
# contract violation the step cannot proceed past.
EXIT_CONTRACT_VIOLATION = 64

MIINT_EXTENSION_DIRECTORY_VAR = "MIINT_EXTENSION_DIRECTORY"

# myloasm's header grammar, defined ONCE. The circularity VALUE is extracted
# rather than matched against a list of alternatives, which is what makes the two
# output classes exhaustive by construction: every record gets exactly one
# `circularity`, `yes` goes to circular.fa and everything else to noLCG.fa, so the
# two counts always sum to the input. Matching `_circular-yes` and its negation
# separately cannot promise that, and would route a (contrived) header carrying
# two `_circular-` fields into the UNRECOVERABLE direction.
_LEN_FIELD = r"_len-[0-9]+"
_CIRC_FIELD = "_circular-"
# The `_depth-A-B-C_` triple. Rust prints these f64s with `{}`, so an integral
# value has no decimal point; both shapes are accepted.
_NUM = r"[0-9]+(?:\.[0-9]+)?"
_DEPTH_RE = rf"_depth-({_NUM})-({_NUM})-({_NUM})_"
# `mult=<f>` is written AFTER the space, so it is read_fastx's `comment`, never
# part of `read_id` — verified against the extension.
_MULT_RE = rf"^mult=({_NUM})$"
# myloasm reports `mult` as 0.00 below this length (kmer_multiplicity returns
# early), which is absence of signal rather than a measured zero, so it is stored
# NULL. Keyed on the sequence actually written rather than on the value being 0,
# so the column never has to mean two things.
_MULT_MIN_LENGTH_BP = 1000
# Capture group 1 is the circularity value. A header that doesn't match at all
# yields '' — which `_validate` rejects, so an unparseable header stops the step.
_CIRCULARITY_RE = rf"{_LEN_FIELD}{_CIRC_FIELD}([a-z]+)_"
# Everything from the length field onward, cut to leave the bare contig id.
_DECORATION_RE = rf"{_LEN_FIELD}{_CIRC_FIELD}.*$"
# The values myloasm 0.6.0 documents. An unknown fourth stops the step rather
# than being guessed at.
_KNOWN_CIRCULARITY = ("yes", "no", "possibly")
_CIRCULAR = "yes"


def _die(msg: str) -> None:
    print(f"myloasm_split: {msg}", file=sys.stderr)
    raise SystemExit(EXIT_CONTRACT_VIOLATION)


def _sql_path(path: str) -> str:
    """A path safe to interpolate into a DuckDB SQL string literal.

    `COPY … TO`'s target is a string literal and cannot be parameter-bound, so it
    is interpolated — and REJECTED rather than escaped when it holds a quote,
    backslash or control character, matching `qiita_common.parquet`'s
    `validate_parquet_path`. Fail-fast beats a clever escape: no workspace path
    the entrypoint builds contains these, so one that does means something is
    already wrong.
    """
    if "'" in path or "\\" in path or any(ord(c) < 0x20 for c in path):
        _die(f"path contains characters unsafe to interpolate into SQL: {path!r}")
    return f"'{path}'"


def _connect() -> duckdb.DuckDBPyConnection:
    """A miint connection over the DEPLOY-STAGED extension directory, bounded by
    the step's own allocation.

    The connect config (LOAD-only, `allow_unsigned_extensions`, the staged
    `extension_directory`) mirrors what `qiita_common.duckdb_miint` produces for
    the Python services. It is duplicated rather than imported because
    `qiita-common` is not installed in this container; the Rust data plane carries
    the same duplication for the same reason. A change to the canonical config
    belongs here too.

    `memory_limit` / `threads` / `temp_directory` come from the step's resolved
    allocation, which `_shared/_lib.sh` exports as MEM_MB and THREADS (TMPDIR is
    pointed at the workspace by the SLURM payload). Without them DuckDB sizes
    itself from what it detects on the NODE, not from this step's cgroup, and a
    deep metagenome gets OOM-killed instead of spilling — the posture every native
    sibling takes via `apply_duckdb_settings`.
    """
    ext_dir = os.environ.get(MIINT_EXTENSION_DIRECTORY_VAR)
    if not ext_dir:
        _die(
            f"{MIINT_EXTENSION_DIRECTORY_VAR} is not set. The assemble step must "
            "declare it in the workflow YAML's `derived_inputs` so the deploy-staged "
            "miint extension is bind-mounted into this container."
        )
    if not Path(ext_dir).is_dir():
        _die(f"{MIINT_EXTENSION_DIRECTORY_VAR}={ext_dir!r} is not a directory")

    config = {"allow_unsigned_extensions": "true", "extension_directory": ext_dir}
    # Leave headroom under the cgroup ceiling: DuckDB's limit governs its own
    # buffers, not the whole process, and the reader holds contig bytes outside it.
    mem_mb = os.environ.get("MEM_MB")
    if mem_mb and mem_mb.isdigit() and int(mem_mb) > 0:
        config["memory_limit"] = f"{max(1, (int(mem_mb) * 3) // 4)}MB"
    threads = os.environ.get("THREADS")
    if threads and threads.isdigit() and int(threads) > 0:
        config["threads"] = threads
    tmpdir = os.environ.get("TMPDIR")
    if tmpdir and Path(tmpdir).is_dir():
        config["temp_directory"] = str(Path(tmpdir) / "duckdb-myloasm-split")

    con = duckdb.connect(config=config)
    try:
        # LOAD-only, never INSTALL — see the module docstring.
        con.execute("LOAD miint")
    except duckdb.Error as exc:
        _die(
            f"LOAD miint failed from {ext_dir!r}: {exc}. DuckDB namespaces that "
            f"directory by engine version + platform (this container runs DuckDB "
            f"{duckdb.__version__}), so the usual cause is a DuckDB version skew "
            "between this image and the orchestrator that staged the extension."
        )
    return con


def _load(con: duckdb.DuckDBPyConnection, src: str) -> None:
    """Parse the primary FASTA ONCE into a temp table.

    Everything downstream reads this table, so the reader runs a single time and
    the regexes are applied a single time — the alternative re-parses every contig
    per query. The output column is `contig_id`, deliberately NOT `read_id`: an
    alias that shadowed `read_fastx`'s own `read_id` would leave the WHERE clause's
    meaning resting on DuckDB's alias-vs-column precedence.
    """
    con.execute(
        "CREATE TEMP TABLE contig AS SELECT "
        "  read_id AS header,"
        "  regexp_replace(read_id, ?, '') AS contig_id,"
        "  regexp_extract(read_id, ?, 1) AS circularity,"
        # The mean of the triple, which is the scalar myloasm itself derives from
        # it: `min_read_depth_multi` holds one minimum read depth per identity
        # threshold, and polishing_mod.rs averages the three into the `avg_cov`
        # its own circularity gate tests. An empty match yields NULL rather than
        # 0 — TRY_CAST over a missing group, not a coalesce to a fake reading.
        "  (TRY_CAST(regexp_extract(read_id, ?, 1) AS DOUBLE)"
        "   + TRY_CAST(regexp_extract(read_id, ?, 2) AS DOUBLE)"
        "   + TRY_CAST(regexp_extract(read_id, ?, 3) AS DOUBLE)) / 3.0 AS depth,"
        "  CASE WHEN length(sequence1) >= ?"
        "       THEN TRY_CAST(regexp_extract(comment, ?, 1) AS DOUBLE) END AS mult,"
        "  sequence1"
        f" FROM read_fastx({src})",
        [
            _DECORATION_RE,
            _CIRCULARITY_RE,
            _DEPTH_RE,
            _DEPTH_RE,
            _DEPTH_RE,
            _MULT_MIN_LENGTH_BP,
            _MULT_RE,
        ],
    )


def _validate(con: duckdb.DuckDBPyConnection) -> None:
    """Reject a header shape we have not probed, and duplicate contig ids.

    Fail-closed on purpose. An unrecognised header does not error on its own — it
    simply carries no circularity we recognise, so a lenient split would exit 0
    having classified every genome as linear and quietly demoted every closed
    genome to binning input.
    """
    bad = con.execute(
        "SELECT header FROM contig WHERE circularity NOT IN ? ORDER BY header LIMIT 3",
        [list(_KNOWN_CIRCULARITY)],
    ).fetchall()
    if bad:
        _die(
            f"header(s) do not match the probed myloasm shape "
            f"<id>_len-<N>_circular-<{'|'.join(_KNOWN_CIRCULARITY)}>_… "
            f"(e.g. {bad[0][0]!r}) — re-probe the header format against the myloasm "
            "version pinned in assemble.def before trusting this split"
        )

    # An LCG's bin_id IS its contig id (assembly_hash COALESCEs it from the
    # record), so a duplicate id puts two distinct genomes under one
    # `assembly_membership` subject downstream.
    dupes = con.execute(
        "SELECT contig_id, count(*) AS n FROM contig GROUP BY 1 HAVING n > 1"
        " ORDER BY 1 LIMIT 5"
    ).fetchall()
    if dupes:
        _die(f"duplicate contig id(s) after cutting the _len-… decoration: {dupes}")


def _write(con: duckdb.DuckDBPyConnection, out: str, *, circular: bool) -> int:
    """COPY one circularity class to a FASTA. Returns the record count.

    A zero-row COPY still creates the file (0 bytes) — verified against miint's
    FORMAT FASTA writer — so an empty CLASS is an empty file, never a missing one,
    with no separate pre-creation step. No ORDER BY: nothing downstream reads these
    in record order (binning re-emits the assembly in the coverage BAM's @SQ order
    regardless), and sorting would carry whole contig sequences through the sort.
    """
    op = "=" if circular else "<>"
    (count,) = con.execute(
        "COPY ("
        "  SELECT contig_id AS read_id, sequence1 FROM contig"
        f"  WHERE circularity {op} ?"
        f") TO {_sql_path(out)} (FORMAT FASTA)",
        [_CIRCULAR],
    ).fetchone()
    return count


def _write_attributes(con: duckdb.DuckDBPyConnection, out: str) -> int:
    """COPY the per-contig attributes to a TSV. Returns the record count.

    One row per contig across BOTH published classes, keyed on `contig_id` — the
    same cut id `_write` puts in the FASTA headers, and so the id that reaches
    `bin_map.contig_id`. Keying on the full header instead would not join: the
    FASTAs carry the cut id, and that is what read_fastx reads back downstream.

    TSV, not Parquet: the container emits its tool's values and `assembly_load`
    owns all parsing, which is the same split checkm.sh and bin_refine.sh use for
    CheckM and DAS_Tool output. `HEADER` because the reader selects by name.
    ORDER BY contig_id so a re-run over identical input produces an identical file.
    """
    (count,) = con.execute(
        "COPY ("
        "  SELECT contig_id, header AS raw_name, circularity, depth, mult"
        "  FROM contig ORDER BY contig_id"
        f") TO {_sql_path(out)} (FORMAT CSV, DELIMITER '\t', HEADER)"
    ).fetchone()
    return count


def main(argv: list[str]) -> int:
    if len(argv) != 5:
        _die(
            f"usage: {argv[0]} <assembly_primary.fa> <circular.fa> <noLCG.fa>"
            " <contig_attributes.tsv>"
        )
    primary, circ_out, nolcg_out, attrs_out = argv[1], argv[2], argv[3], argv[4]

    # `read_fastx` RAISES on a zero-record input ("Error Empty file: …") rather
    # than returning no rows — the same trap `qiita_common.duckdb_miint`'s
    # `is_empty_sequence_file` exists for on the native side. assemble.sh already
    # skips this script when the primary FASTA is empty; this is the second gate,
    # so a direct invocation gets our message instead of a DuckDB traceback.
    try:
        size = Path(primary).stat().st_size
    except OSError as exc:
        _die(f"cannot stat {primary!r}: {exc}")
    if size == 0:
        _die(f"{primary} is empty; read_fastx raises on a zero-record input")

    src = _sql_path(primary)
    con = _connect()
    try:
        _load(con, src)
        _validate(con)
        n_circ = _write(con, circ_out, circular=True)
        n_lin = _write(con, nolcg_out, circular=False)
        n_attrs = _write_attributes(con, attrs_out)
        # The two classes partition the input by construction (see
        # _CIRCULARITY_RE). Assert it anyway: a silent shortfall here is contigs
        # vanishing between the assembler and the lake.
        (total,) = con.execute("SELECT count(*) FROM contig").fetchone()
    finally:
        con.close()

    if n_circ + n_lin != total:
        _die(f"split lost records: {n_circ} circular + {n_lin} linear != {total} input")
    # The attributes cover BOTH classes, so this is the input count, not either
    # class. A shortfall means a contig reaches the lake with no stored call while
    # its FASTA record is loaded normally — invisible downstream, since the join
    # is a LEFT JOIN that renders a missing row as NULL.
    if n_attrs != total:
        _die(f"attributes lost records: {n_attrs} rows != {total} input contigs")

    print(
        f"myloasm_split: {n_circ} circular (LCG), {n_lin} linear (noLCG),"
        f" {n_attrs} attribute rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
