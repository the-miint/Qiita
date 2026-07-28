"""Split myloasm's `assembly_primary.fa` into circular (LCG) and linear (noLCG)
multi-FASTAs, using miint's `read_fastx` reader and `FORMAT FASTA` writer.

Run by assemble.sh's myloasm branch:

    python3 /opt/qiita/myloasm_split.py <assembly_primary.fa> <circular.fa> <noLCG.fa>

WHY miint AND NOT A HAND-ROLLED PARSER
--------------------------------------
Repo rule: reach for a miint reader/writer before hand-rolling one. FASTA framing
is `read_fastx`'s job and FASTA emission is `COPY … (FORMAT FASTA)`'s, so neither
is re-implemented here. What miint does NOT do for us is the header FIELD
grammar: `read_fastx` yields `read_id` = the header's FIRST TOKEN, and for myloasm
that token is the whole `u713ctg_len-580076_circular-yes_depth-…_duplicated-no`
blob (the space-separated `mult=1.00` tail lands in a separate `comment` column).
So the `_len-` / `_circular-` parsing below is real work miint cannot absorb — it
is just expressed in SQL instead of a hand-written scanner, and sequence bytes are
never touched by us at all.

WHICH miint — THE DEPLOY-STAGED ONE
-----------------------------------
This LOADs the extension the deploy already staged into
`MIINT_EXTENSION_DIRECTORY`, bind-mounted into the container read-only via the
step's `derived_inputs`. It is the SAME build the control plane, the compute
orchestrator and the data plane run, which is the point: a copy baked into this
image would be a fourth miint that `make preflight`'s byte-identity check does not
compare, free to drift from the other three.

LOAD-only, never INSTALL — the standing service-side rule. A per-job INSTALL would
need the mirror reachable from every compute node and a writable `$HOME`.

DuckDB namespaces the staged directory by **engine version + platform**
(`<dir>/v1.5.4/linux_amd64/miint.duckdb_extension`), so this container's DuckDB
must be the same version the orchestrator staged with. assemble.def pins it and
asserts it at build time; a mismatch surfaces here as a clear LOAD failure rather
than a wrong answer.

WHY THE HEADER AND NOT THE GFA
------------------------------
hifiasm-meta encodes circularity in its GFA segment NAME (`…tg……c` circular /
`…l` linear), which is what the hifiasm branch splits on. myloasm does NOT: it
states circularity directly in the assembly_primary.fa header, and its GFA
segment names carry no such marker. Applying the hifiasm `tg[0-9]+c$` regex to
myloasm output would match nothing, silently sending every circular genome to
binning and losing the LCG class outright.

Probed on myloasm 0.6.0 (bioconda) against one real genome — M. genitalium G37
(NC_000908.2), a complete circular chromosome — sampled twice into read sets
identical in every respect EXCEPT wrap-around, then assembled separately:

    with wrap:    >u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00
    without wrap: >u278ctg_len-577882_circular-no_depth-33-33-33_duplicated-no mult=1.00

Topology was the only variable, so the `circular-` field does track it.

WHAT COUNTS AS CIRCULAR
-----------------------
ONLY `circular-yes`. myloasm also documents `circular-possibly` (a self-loop that
fails its depth/connectivity criteria); that value was NOT reproduced by the probe
— low-depth runs (1-3x) all reported `circular-no` — so it is accepted as a
well-formed value but routed to noLCG. That is both the safe default (a mis-called
linear contig is still recovered through binning, whereas a mis-called circular one
bypasses binning entirely) and what the assay owner does by hand today.

WHY THE ID IS TRUNCATED AT `_len-`
----------------------------------
Whatever is left becomes the LCG bin_id in assembly_hash, which keys read_id as
`kind:bin_id:contig_id`. What the probe established:

  * myloasm is DETERMINISTIC — two runs over the same reads gave byte-identical
    headers. A literal re-run of a sample moves nothing.
  * Across two read samplings of the SAME circular genome, `depth-` moved
    (32-32-32 -> 31-31-31) while `_len-` was IDENTICAL (580076 both), and the
    unitig id itself moved (u713ctg -> u932ctg).

So truncation is worth doing because the discarded fields demonstrably vary with
the read set and a bin_id must not carry coverage statistics. It does NOT make
bin_id stable across re-assemblies from different reads — nothing in this header
would, since the unitig number moves too.

ATTRIBUTED, NOT REPRODUCED (Lucas Patel, issue #259): that `_len-<N>` drifts by a
few bp between re-assemblies as a circular contig's rotational start moves. Our
probe saw an identical length across two different read samplings, so it neither
confirms nor refutes it. Truncating costs nothing either way.
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

# myloasm's header grammar, defined ONCE and composed into every expression
# below. Written out three times (validate / classify / truncate) it would be
# possible to update two and miss the third — and the miss that matters is
# CLASSIFY: every header would still validate, every id would still truncate
# correctly, and every contig would silently classify as linear.
_LEN_FIELD = r"_len-[0-9]+"
_CIRC_FIELD = "_circular-"
# A header we are willing to interpret at all. Pinned to the three documented
# values so an unknown fourth stops the step instead of being guessed at.
_HEADER_RE = rf"{_LEN_FIELD}{_CIRC_FIELD}(yes|no|possibly)_"
# A header that means "closed circular molecule".
_CIRCULAR_RE = rf"{_CIRC_FIELD}yes_"
# Everything from the length field onward, cut to leave the bare contig id.
_DECORATION_RE = rf"{_LEN_FIELD}{_CIRC_FIELD}.*$"


def _die(msg: str) -> None:
    print(f"myloasm_split: {msg}", file=sys.stderr)
    raise SystemExit(EXIT_CONTRACT_VIOLATION)


def _sql_str(value: str) -> str:
    """A single-quoted SQL string literal.

    `COPY … TO` and `read_fastx(…)` targets are string LITERALS — they cannot be
    parameter-bound — so the path is escaped rather than passed as a parameter.
    Control characters are rejected outright instead of escaped: no legitimate
    workspace path contains one, and the entrypoint builds these paths itself.
    """
    if "\x00" in value or any(ord(c) < 32 for c in value):
        _die(f"path contains a control character: {value!r}")
    return "'" + value.replace("'", "''") + "'"


def _connect() -> duckdb.DuckDBPyConnection:
    """A miint connection over the DEPLOY-STAGED extension directory.

    Mirrors `qiita_common.duckdb_miint.miint_connect_config()` deliberately rather
    than importing it: `qiita-common` is not installed in this container (it is a
    path dep of the two Python services), and the Rust data plane already carries
    the same duplication for the same reason. Keep the copies in sync.
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

    con = duckdb.connect(
        config={"allow_unsigned_extensions": "true", "extension_directory": ext_dir}
    )
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


def _validate(con: duckdb.DuckDBPyConnection, src: str) -> None:
    """Reject a header shape we have not probed, and duplicate contig ids.

    Both are fail-closed on purpose. An unrecognised header does not error on its
    own — it simply fails to match the circular pattern, so the step would exit 0
    having classified every genome as linear and quietly demoted every closed
    genome to binning input. That is the silent failure this whole branch exists
    to avoid.
    """
    (bad,) = con.execute(
        f"SELECT count(*) FROM read_fastx({src}) WHERE NOT regexp_matches(read_id, ?)",
        [_HEADER_RE],
    ).fetchone()
    if bad:
        (example,) = con.execute(
            f"SELECT read_id FROM read_fastx({src}) "
            "WHERE NOT regexp_matches(read_id, ?) LIMIT 1",
            [_HEADER_RE],
        ).fetchone()
        _die(
            f"{bad} record(s) do not match the probed myloasm header shape "
            f"<id>_len-<N>_circular-<yes|no|possibly>_… (e.g. {example!r}) — re-probe "
            "the header format against the myloasm version pinned in assemble.def "
            "before trusting this split"
        )

    # An LCG's bin_id IS its contig id (assembly_hash COALESCEs it from the
    # record), and read_id is `kind:bin_id:contig_id` — so a duplicate id would
    # collapse two distinct genomes onto one identity downstream.
    dupes = con.execute(
        f"SELECT regexp_replace(read_id, ?, '') AS id, count(*) AS n "
        f"FROM read_fastx({src}) GROUP BY 1 HAVING n > 1 ORDER BY 1 LIMIT 5",
        [_DECORATION_RE],
    ).fetchall()
    if dupes:
        _die(f"duplicate contig id(s) after cutting the _len-… decoration: {dupes}")


def _split(
    con: duckdb.DuckDBPyConnection, src: str, out: str, *, circular: bool
) -> int:
    """COPY one circularity class to a FASTA. Returns the record count.

    A zero-row COPY still creates the file (0 bytes) — verified against miint's
    FORMAT FASTA writer — so an empty CLASS is an empty file, never a missing one,
    with no separate pre-creation step.
    """
    negate = "" if circular else "NOT "
    (count,) = con.execute(
        "COPY ("
        f"  SELECT regexp_replace(read_id, {_sql_str(_DECORATION_RE)}, '') AS read_id,"
        "         sequence1"
        f"    FROM read_fastx({src})"
        f"   WHERE {negate}regexp_matches(read_id, {_sql_str(_CIRCULAR_RE)})"
        "   ORDER BY read_id"
        f") TO {_sql_str(out)} (FORMAT FASTA)"
    ).fetchone()
    return count


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        _die(f"usage: {argv[0]} <assembly_primary.fa> <circular.fa> <noLCG.fa>")
    primary, circ_out, nolcg_out = argv[1], argv[2], argv[3]

    # `read_fastx` RAISES on a zero-record input ("Error Empty file: …") rather
    # than returning no rows — the same trap `qiita_common.duckdb_miint`'s
    # `is_empty_sequence_file` exists for on the native side. assemble.sh already
    # skips this script when the primary FASTA is empty; this is the second gate,
    # so a direct invocation cannot hit the raise either.
    if Path(primary).stat().st_size == 0:
        _die(f"{primary} is empty; read_fastx raises on a zero-record input")

    src = _sql_str(primary)
    con = _connect()
    try:
        _validate(con, src)
        n_circ = _split(con, src, circ_out, circular=True)
        n_lin = _split(con, src, nolcg_out, circular=False)
    finally:
        con.close()

    print(f"myloasm_split: {n_circ} circular (LCG), {n_lin} linear (noLCG)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
