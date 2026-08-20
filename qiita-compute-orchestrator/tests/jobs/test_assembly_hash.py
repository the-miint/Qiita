"""Isolated unit tests for `assembly_hash.execute` — the container-FASTA ->
manifest / hash-keyed-chunks / bin_map head of the assembly-storage tail.

Runs against the team-mirror miint build (conftest stages it): the job reads FASTA
with miint `read_fastx` and chunks with `sequence_split`, and the `_hash` oracle
below takes its reverse complement from the same miint scalar the production
expression uses. Calls execute() directly.
Covers: happy path (LCG + MAG, synthetic read_ids, the contig_id beside them,
hash-keyed chunks, dedup of identical contigs), synthetic-id disambiguation of a
contig id reused across bins and repeated within one file, soft-masked (lowercase)
contigs folding onto their upper-case twin and which record's bytes that fold then
stores, the unbinned noLCG residue (its exclusion key, its bin_id, and what it does
to a hash-collision group), a bin_id carrying the id's own separator, the one
identity collision the step refuses (two bin files stemming to one bin_id), and
empty -> StepNoData.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from functools import cache
from uuid import UUID

import duckdb
import pytest
from qiita_common.backend_failure import StepNoData

from qiita_compute_orchestrator.jobs.assembly_hash import Inputs, execute
from qiita_compute_orchestrator.miint import open_miint_conn


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


def _fasta(path, records: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f">{cid}\n{seq}\n" for cid, seq in records.items()))


def _layout(tmp_path):
    genomes = tmp_path / "genomes"
    refined = tmp_path / "refined"
    genomes.mkdir(parents=True)
    refined.mkdir(parents=True)
    return genomes, refined


@cache
def _rc(seq: str) -> str:
    """miint's `sequence_dna_reverse_complement` over the upper-cased sequence —
    the same call `canonical_sequence_hash_expr` makes for the second strand.

    `sequence_dna_reverse_complement` preserves case
    (https://the-miint.github.io/duckdb-miint/utilities/), so `upper()` wraps the
    argument, not the result. Cached per distinct sequence — this is called from
    assertion sites rather than from a fixture."""
    with open_miint_conn() as conn:
        return conn.execute("SELECT sequence_dna_reverse_complement(upper(?))", [seq]).fetchone()[0]


def _hash(seq: str) -> UUID:
    """Mirror `canonical_sequence_hash_expr`: md5 BOTH strands, keep the smaller
    UUID. The LEAST is over the two HASHES, not over the two sequences, so it does
    not in general return the hash of the lex-smaller strand.

    Only the reverse complement comes from miint; the composition around it is
    re-derived here, so a change to how the two hashes combine still fails."""
    return min(
        UUID(hashlib.md5(seq.upper().encode()).hexdigest()),
        UUID(hashlib.md5(_rc(seq).encode()).hexdigest()),
    )


def _rows(parquet, cols: str, order: str):
    with duckdb.connect(":memory:") as con:
        return con.execute(
            f"SELECT {cols} FROM read_parquet('{parquet}') ORDER BY {order}"
        ).fetchall()


def _reassembled(chunks_dir) -> dict[str, str]:
    """sequence_hash -> the bytes stored under it, chunk_index ordered."""
    with duckdb.connect(":memory:") as con:
        return dict(
            con.execute(
                "SELECT CAST(sequence_hash AS VARCHAR), "
                "string_agg(chunk_data, '' ORDER BY chunk_index) "
                "FROM read_parquet(?) GROUP BY sequence_hash",
                [str(chunks_dir / "part_*.parquet")],
            ).fetchall()
        )


def test_happy_path_manifest_bin_map_and_chunks(tmp_path):
    genomes, refined = _layout(tmp_path)
    # circular.fa is a single multi-FASTA of circular contigs; each record is its
    # OWN LCG genome, so its bin_id IS the contig id (from the read, not a filename)
    # — no per-contig split step.
    # c2 is not a reverse-complement palindrome and its RC hashes lower than it
    # does, so its `_hash` comparison exercises the strand fold; the other three
    # fixtures are palindromes, where the fold is the identity.
    _fasta(genomes / "circular.fa", {"c1": "AAAACCCCGGGGTTTT", "c2": "GGGGAAAACCCCTTTT"})
    _fasta(refined / "bin.1.fa", {"x1": "ACGTACGTACGTACGT", "x2": "TTTTGGGGCCCCAAAA"})

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=7
        ),
        tmp_path / "ws",
    )

    # manifest: synthetic read_id kind:bin_id:sequence_index, canonical hash, length.
    manifest = _rows(
        out["manifest"],
        "read_id, CAST(sequence_hash AS VARCHAR), sequence_length_bp",
        "read_id",
    )
    assert manifest == sorted(
        [
            ("LCG:c1:1", str(_hash("AAAACCCCGGGGTTTT")), 16),
            ("LCG:c2:2", str(_hash("GGGGAAAACCCCTTTT")), 16),
            ("MAG:bin.1:1", str(_hash("ACGTACGTACGTACGT")), 16),
            ("MAG:bin.1:2", str(_hash("TTTTGGGGCCCCAAAA")), 16),
        ]
    )

    # bin_map: kind + bin_id per synthetic read_id, and the contig id the id itself
    # no longer carries. Each LCG contig is its own bin (bin_id == contig id); the
    # MAG's contigs share the file's bin_id, so `x1`/`x2` survive only here.
    bin_map = _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id")
    assert bin_map == sorted(
        [
            ("LCG:c1:1", "LCG", "c1", "c1"),
            ("LCG:c2:2", "LCG", "c2", "c2"),
            ("MAG:bin.1:1", "MAG", "bin.1", "x1"),
            ("MAG:bin.1:2", "MAG", "bin.1", "x2"),
        ]
    )

    # chunks: a directory of part_*.parquet keyed by sequence_hash; reassembled
    # chunk_data equals the canonical bytes.
    chunks_dir = out["assembly_chunks"]
    assert chunks_dir.is_dir()
    parts = sorted(chunks_dir.glob("part_*.parquet"))
    assert parts
    glob = str(chunks_dir / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        cols = {
            c[0]: c[1]
            for c in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [glob]).fetchall()
        }
    assert cols == {"sequence_hash": "UUID", "chunk_index": "INTEGER", "chunk_data": "VARCHAR"}
    reassembled = _reassembled(chunks_dir)
    assert reassembled[str(_hash("AAAACCCCGGGGTTTT"))] == "AAAACCCCGGGGTTTT"
    assert reassembled[str(_hash("ACGTACGTACGTACGT"))] == "ACGTACGTACGTACGT"


def test_identical_contigs_dedup_to_one_chunk_set(tmp_path):
    """Two contigs (different bins) with identical bytes collapse to ONE
    sequence_hash in the chunks, but BOTH keep their manifest + bin_map rows (so
    write-assembly-membership records both bins for the shared feature)."""
    genomes, refined = _layout(tmp_path)
    _fasta(refined / "bin.1.fa", {"ctg": "ACGTACGTACGTACGT"})
    _fasta(refined / "bin.2.fa", {"ctg": "ACGTACGTACGTACGT"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    # Same raw contig id "ctg" in two bins — synthetic read_ids disambiguate.
    manifest = _rows(out["manifest"], "read_id", "read_id")
    assert manifest == [("MAG:bin.1:1",), ("MAG:bin.2:1",)]

    glob = str(out["assembly_chunks"] / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        distinct_hashes = con.execute(
            f"SELECT count(DISTINCT sequence_hash) FROM read_parquet('{glob}')"
        ).fetchone()[0]
    assert distinct_hashes == 1


def test_lcg_only_no_mag(tmp_path):
    """LCG-only: a circular genome but NO refined MAGs (empty refined_bins_dir)
    stores successfully — the single-file circular.fa LCG path + COALESCE bin_id
    run without any MAG row alongside them (bin_id resolves to the contig id)."""
    genomes, refined = _layout(tmp_path)
    _fasta(genomes / "circular.fa", {"c1": "AAAACCCCGGGGTTTT"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=5, work_ticket_idx=9),
        tmp_path / "ws",
    )
    bin_map = _rows(out["bin_map"], "read_id, kind, bin_id", "read_id")
    assert bin_map == [("LCG:c1:1", "LCG", "c1")]


def test_binned_contig_is_excluded_from_the_residue_despite_a_different_header(tmp_path):
    """A contig in BOTH noLCG.fa and a refined bin produces ONE membership row.

    The fixture gives the shared contig a DIFFERENT header in each file — the case
    an id-keyed exclusion gets wrong silently.
    """
    genomes, refined = _layout(tmp_path)
    binned = "AAAAAAAACCCCGGGG"
    unbinned = "TTTTAAAACCCCGGGG"
    _fasta(genomes / "noLCG.fa", {"ctgA": binned, "ctgB": unbinned})
    _fasta(refined / "bin.1.fa", {"renamed_by_dastool": binned})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=3, work_ticket_idx=4),
        tmp_path / "ws",
    )

    # `binned` appears once, as the MAG. `unbinned` is the residue, and its bin_id
    # is its own contig id — the same shape LCG uses, so (kind, bin_id) is uniform.
    assert _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id") == sorted(
        [
            ("MAG:bin.1:1", "MAG", "bin.1", "renamed_by_dastool"),
            ("UNBINNED:ctgB:2", "UNBINNED", "ctgB", "ctgB"),
        ]
    )
    manifest = _rows(out["manifest"], "CAST(sequence_hash AS VARCHAR)", "1")
    assert manifest == sorted([(str(_hash(binned)),), (str(_hash(unbinned)),)])


def test_a_reverse_complemented_bin_contig_still_excludes_its_nolcg_record(tmp_path):
    """The exclusion matches on the canonical hash, which folds both strands.

    A bin that stores a contig on the opposite strand from its noLCG record still
    excludes that record, so the run records the sequence once, as the MAG.
    """
    genomes, refined = _layout(tmp_path)
    seq = "AAAAAAAACCCCGGGG"
    assert _rc(seq) != seq, "fixture sequence is a palindrome — the test would be vacuous"
    _fasta(genomes / "noLCG.fa", {"ctgA": seq})
    _fasta(refined / "bin.1.fa", {"ctgA": _rc(seq)})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=3, work_ticket_idx=4),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id", "read_id") == [
        ("MAG:bin.1:1", "MAG", "bin.1")
    ]


def test_soft_masked_contigs_hash_as_their_upper_case_twin(tmp_path):
    """A soft-masked (lowercase) contig mints the same feature as its uppercase
    twin, on either strand: the canonical hash upper-cases BEFORE the strand fold,
    so case never splits one sequence into two feature_idx.

    The fixture sequence discriminates both halves of that expression — it is not
    a palindrome (so the fold is not the identity) and `md5(min(strand))` differs
    from `min(md5(strand))` on it (so a LEAST moved back over the sequences fails
    here rather than passing by coincidence).

    Which of the three records' bytes land in the chunks is asserted too. The
    representative is `DISTINCT ON (sequence_hash) ... ORDER BY sequence_hash,
    read_id`, which picks `LCG:c1:1` here, and its bytes are chunked as read —
    so the stored strand and casing follow that tie-break. The lake replaces
    these chunks by `feature_idx`, so the bytes a load stores are the ones a
    reader reassembles.
    """
    genomes, refined = _layout(tmp_path)
    seq = "GCTAAAGACAATTACA"
    assert _rc(seq) != seq, "fixture sequence is a palindrome — the test would be vacuous"
    _fasta(genomes / "circular.fa", {"c1": seq, "c2": seq.lower(), "c3": _rc(seq).lower()})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=8, work_ticket_idx=3),
        tmp_path / "ws",
    )

    manifest = _rows(out["manifest"], "read_id, CAST(sequence_hash AS VARCHAR)", "read_id")
    assert manifest == [
        ("LCG:c1:1", str(_hash(seq))),
        ("LCG:c2:2", str(_hash(seq.lower()))),
        ("LCG:c3:3", str(_hash(_rc(seq).lower()))),
    ]
    # Three records, one canonical sequence -> one feature, so one chunk set, and
    # the reassembled bytes are the winning record's, byte for byte.
    assert len({h for _, h in manifest}) == 1
    glob = str(out["assembly_chunks"] / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        assert con.execute(
            "SELECT CAST(sequence_hash AS VARCHAR), "
            "string_agg(chunk_data, '' ORDER BY chunk_index) "
            f"FROM read_parquet('{glob}') GROUP BY sequence_hash"
        ).fetchall() == [(str(_hash(seq)), seq)]


def test_hash_equal_nolcg_records_share_the_residue_verdict(tmp_path):
    """Two noLCG records with one canonical sequence both leave when either is binned.

    The exclusion is keyed on content, not on records, so a canonical hash a bin
    claims removes EVERY noLCG record carrying it — the unbinned twin included.
    Nothing here treats a canonical hash as identifying one record.
    """
    genomes, refined = _layout(tmp_path)
    shared = "AAAAAAAACCCCGGGG"
    _fasta(genomes / "noLCG.fa", {"ctgA": shared, "ctgB": _rc(shared), "ctgC": "TTTTAAAACCCCGGGG"})
    _fasta(refined / "bin.1.fa", {"ctgA": shared})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=3, work_ticket_idx=4),
        tmp_path / "ws",
    )
    # ctgB carries `shared`'s canonical hash too, so it leaves with ctgA.
    assert _rows(out["bin_map"], "read_id, kind, bin_id", "read_id") == sorted(
        [("MAG:bin.1:1", "MAG", "bin.1"), ("UNBINNED:ctgC:3", "UNBINNED", "ctgC")]
    )


def test_hash_equal_records_in_one_file_each_keep_a_bin_map_row(tmp_path):
    """Distinct headers over one canonical sequence give one feature and N bin_ids.

    Holds for LCG and for the unbinned residue alike: `contig` (and so the manifest
    and bin_map) is per RECORD, while the chunk set is per canonical hash. Downstream
    that is N assembly_membership rows sharing a feature_idx and differing only in
    bin_id — the contig id each record carried.
    """
    genomes, refined = _layout(tmp_path)
    seq = "AAAAAAAACCCCGGGG"
    _fasta(genomes / "circular.fa", {"c1": seq, "c2": _rc(seq)})
    _fasta(genomes / "noLCG.fa", {"n1": "TTTTAAAACCCCGGGG", "n2": "TTTTAAAACCCCGGGG"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=3, work_ticket_idx=4),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id", "read_id") == sorted(
        [
            ("LCG:c1:1", "LCG", "c1"),
            ("LCG:c2:2", "LCG", "c2"),
            ("UNBINNED:n1:1", "UNBINNED", "n1"),
            ("UNBINNED:n2:2", "UNBINNED", "n2"),
        ]
    )
    # Four records, two canonical sequences -> two chunk sets.
    glob = str(out["assembly_chunks"] / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        assert (
            con.execute(
                f"SELECT count(DISTINCT sequence_hash) FROM read_parquet('{glob}')"
            ).fetchone()[0]
            == 2
        )


def test_unbinned_only_sample_is_not_no_data(tmp_path):
    """Contigs that assembled but binned nowhere are stored, not discarded.

    StepNoData is reserved for an assembler that produced no contig at all: with a
    non-empty noLCG.fa there is something to hash, whether or not anything binned.
    """
    genomes, refined = _layout(tmp_path)
    _fasta(genomes / "noLCG.fa", {"ctgA": "AAAAAAAACCCCGGGG"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=6, work_ticket_idx=2),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id", "read_id") == [
        ("UNBINNED:ctgA:1", "UNBINNED", "ctgA")
    ]


@pytest.mark.skipif(
    os.environ.get("QIITA_ASSEMBLY_STRESS") != "1",
    reason="heavy (~800 MB fixture); opt in with QIITA_ASSEMBLY_STRESS=1",
)
def test_pass2_stays_bounded_at_scale(tmp_path, monkeypatch):
    """Regression for the pass-2 memory blow-up: the dedup must NOT carry the
    sequence payload through a sort. The old `DISTINCT ON (sequence_hash) …,
    sequence ORDER BY …` sorted every row's full contig bytes (~6x amplification)
    and OOM'd at ~1 GB of assembled input under a few-GB cap; the narrow-dedup +
    streaming-chunk rewrite completes with ~constant memory regardless of input.

    Here: ~800 MB of distinct random contigs under a 3 GB DuckDB cap — the old
    query OOMs (0.8 GB x ~6 > 3 GB), the new one stays ~1.8 GB. Opt-in because the
    fixture is large (the orchestrator suite has no slow tier to exclude it from).
    """
    import qiita_compute_orchestrator.jobs.assembly_hash as ahmod

    genomes, refined = _layout(tmp_path)
    # 400 x 2 MB distinct random contigs (random bytes → no accidental dedup).
    lut = bytes(b"ACGT"[i & 3] for i in range(256))
    with open(genomes / "circular.fa", "wb") as f:
        for i in range(400):
            f.write(b">ctg%06d\n" % i)
            f.write(os.urandom(2_000_000).translate(lut))
            f.write(b"\n")

    # Constrain DuckDB to a cap the old payload-carrying sort would exceed.
    monkeypatch.setattr(ahmod, "_DUCKDB_MEMORY_GB", 3)

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    # Completed under the cap; every contig is distinct → 400 hashes, none deduped.
    n_contigs = _rows(out["manifest"], "count(*)", "1")[0][0]
    chunks_glob = str(out["assembly_chunks"] / "part_*.parquet")
    n_hashes = _rows(chunks_glob, "count(DISTINCT sequence_hash)", "1")[0][0]
    assert n_contigs == 400
    assert n_hashes == 400


@pytest.mark.parametrize("write_empty_files", [False, True])
def test_no_contigs_is_no_data(tmp_path, write_empty_files):
    """No contig of any kind -> StepNoData.

    Both shapes `assemble.sh` can leave behind: an empty genomes_dir (the assembler
    produced nothing), and present-but-zero-record circular.fa + noLCG.fa (its
    writers truncate both files into existence). `read_fastx` raises on a 0-record
    input, so the empty ones must be dropped before the scan, not scanned and found
    empty.
    """
    genomes, refined = _layout(tmp_path)
    if write_empty_files:
        (genomes / "circular.fa").write_text("")
        (genomes / "noLCG.fa").write_text("")
    with pytest.raises(StepNoData):
        _run(
            Inputs(
                genomes_dir=genomes,
                refined_bins_dir=refined,
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )


def test_repeated_contig_id_stores_each_contigs_own_bytes(tmp_path):
    """Two LCG records whose headers share a first token keep separate identities.

    `read_fastx` returns the shared first token as both records' id, so the pair
    shares a bin_id; the synthetic ids differ in `sequence_index`, so each contig's
    chunks land under its own `sequence_hash`.

    Neither sequence equals its own reverse complement, and one is soft-masked, so
    the stored-bytes assertion discriminates on both axes the canonical hash folds.
    """
    seq_a, seq_b = "ACGTTGCAAGGGTTCA", "ggatccTTAACCggat"
    genomes, refined = _layout(tmp_path)
    (genomes / "circular.fa").write_text(f">ctg1 first\n{seq_a}\n>ctg1 second\n{seq_b}\n")

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
        ),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id") == [
        ("LCG:ctg1:1", "LCG", "ctg1", "ctg1"),
        ("LCG:ctg1:2", "LCG", "ctg1", "ctg1"),
    ]
    assert _reassembled(out["assembly_chunks"]) == {
        str(_hash(seq_a)): seq_a,
        str(_hash(seq_b)): seq_b,
    }


def test_repeated_contig_id_within_one_bin_keeps_both_rows(tmp_path):
    """The MAG arm of the same repeat.

    A MAG's bin_id is its FILE stem, not a contig id, so the two records shared a
    bin_id before the repeat as well and `sequence_index` is the whole of what
    separates their ids.
    """
    seq_a, seq_b = "ACGTTGCAAGGGTTCA", "ggatccTTAACCggat"
    genomes, refined = _layout(tmp_path)
    (refined / "bin.1.fa").write_text(f">ctg1 first\n{seq_a}\n>ctg1 second\n{seq_b}\n")

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
        ),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id") == [
        ("MAG:bin.1:1", "MAG", "bin.1", "ctg1"),
        ("MAG:bin.1:2", "MAG", "bin.1", "ctg1"),
    ]
    assert _reassembled(out["assembly_chunks"]) == {
        str(_hash(seq_a)): seq_a,
        str(_hash(seq_b)): seq_b,
    }


def test_the_residue_delete_leaves_the_survivors_ordinal_where_it_was(tmp_path):
    """A dropped UNBINNED row does not renumber the record after it.

    Both noLCG records are named `ctg1`; a refined bin claims the bytes of the
    first, so the residue DELETE removes its row. The survivor keeps ordinal 2 —
    pass 2 re-derives ids from the FILE, so a survivor renumbered to 1 would find no
    `winner` and lose its chunks.
    """
    genomes, refined = _layout(tmp_path)
    binned, residue = "AAAAAAAACCCCGGGG", "TTTTAAAACCCCGGGG"
    (genomes / "noLCG.fa").write_text(f">ctg1 binned\n{binned}\n>ctg1 residue\n{residue}\n")
    _fasta(refined / "bin.1.fa", {"renamed": binned})

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
        ),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id") == [
        ("MAG:bin.1:1", "MAG", "bin.1", "renamed"),
        ("UNBINNED:ctg1:2", "UNBINNED", "ctg1", "ctg1"),
    ]
    assert _reassembled(out["assembly_chunks"]) == {
        str(_hash(binned)): binned,
        str(_hash(residue)): residue,
    }


def test_a_colon_in_a_bin_id_composes_no_other_bins_id(tmp_path):
    """A bin_id holding the id's own separator still composes a distinct read_id.

    `:` is not escaped. With a contig id as the last component, bin `a:b` / contig
    `c` and bin `a` / contig `b:c` both compose `MAG:a:b:c`; with `sequence_index`
    there the last `:` is always the one before the digits, so these two bins get
    `MAG:a:b:1` and `MAG:a:1` and each keeps its own bytes.
    """
    seq_a, seq_b = "ACGTTGCAAGGGTTCA", "ggatccTTAACCggat"
    genomes, refined = _layout(tmp_path)
    _fasta(refined / "a:b.fa", {"c": seq_a})
    _fasta(refined / "a.fa", {"b:c": seq_b})

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
        ),
        tmp_path / "ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id, contig_id", "read_id") == [
        ("MAG:a:1", "MAG", "a", "b:c"),
        ("MAG:a:b:1", "MAG", "a:b", "c"),
    ]
    assert _reassembled(out["assembly_chunks"]) == {
        str(_hash(seq_a)): seq_a,
        str(_hash(seq_b)): seq_b,
    }


def test_two_bin_files_stemming_to_one_bin_id_fail(tmp_path):
    """`bin.1.fa` + `bin.1.fna` are two bins the tail would key as one.

    This raise is what makes `(kind, bin_id)` name a single MAG file, which is what
    makes the synthetic read_id unique: each file's ordinals start at 1, so a shared
    bin_id would compose `MAG:bin.1:1` twice. The control renames the second file
    and both bins survive with their own bin_id.
    """
    genomes, refined = _layout(tmp_path)
    _fasta(refined / "bin.1.fa", {"c1": "ACGTTGCAAGGGTTCA"})
    _fasta(refined / "bin.1.fna", {"c2": "ggatccTTAACCggat"})

    with pytest.raises(ValueError, match=r"stem to bin_id 'bin\.1'"):
        _run(
            Inputs(
                genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
            ),
            tmp_path / "ws",
        )

    (refined / "bin.1.fna").rename(refined / "bin.2.fna")
    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=3
        ),
        tmp_path / "control-ws",
    )
    assert _rows(out["bin_map"], "read_id, kind, bin_id", "read_id") == [
        ("MAG:bin.1:1", "MAG", "bin.1"),
        ("MAG:bin.2:1", "MAG", "bin.2"),
    ]
