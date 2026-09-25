"""`qiita assembly export` end to end: a VIEWER, the real control plane (roster,
membership, the run mint) and the real data plane (the run-scoped `assembled_sequence`,
`assembled_sequence_chunks` and `bin_quality` streams).

One contig is shared across the sample's two runs — a MAG member under run P and an
UNBINNED contig under run Q — and its bytes are stored once, as the lake stores every
content-deduped contig. Each run's export must write it exactly once, at its registered
length, under that run's subject.
"""

import csv
import random
import secrets
import uuid

import pytest
from conftest import ducklake_connect
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models.reference import Tier

from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
)

_CHUNK_BP = 4_096

# name -> (length, [(run, kind, bin_id, circularity)])
_CONTIGS = {
    "lcg": (9_000, [("p", "LCG", "u7ctg", "yes")]),
    "mag": (5_000, [("p", "MAG", "bin.1", "no")]),
    "shared": (4_500, [("p", "MAG", "bin.1", "no"), ("q", "UNBINNED", "u3ctg", "no")]),
}


def _chunk_values(feature_idx: int, sequence: str) -> str:
    return ", ".join(
        f"({feature_idx}, {i}, '{sequence[i * _CHUNK_BP : (i + 1) * _CHUNK_BP]}')"
        for i in range((len(sequence) + _CHUNK_BP - 1) // _CHUNK_BP)
    )


@pytest.fixture
async def seeded(postgres_pool, human_admin_session, regular_user_session, data_plane):
    db = postgres_pool
    owner = human_admin_session["principal_idx"]
    reader = regular_user_session["principal_idx"]
    tag = secrets.token_hex(4)
    rng = random.Random(594)
    sequences = {
        n: "".join(rng.choice("ACGT") for _ in range(ln))
        for n, (ln, _) in _CONTIGS.items()
    }

    study_idx = await db.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        f"assembly-export-e2e-{tag}",
    )
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        reader,
        Tier.VIEWER,
        owner,
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        db, owner_idx=owner
    )
    accession = f"SAMEA{tag}"
    await db.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2",
        accession,
        biosample_idx,
    )
    await seed_biosample_to_study_link(
        db, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
    )
    await seed_prep_sample_to_study_link(
        db, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner
    )

    runs = {}
    async with db.acquire() as conn:
        for name in ("p", "q"):
            version = f"v-{uuid.uuid4()}"
            row = await mint_processing(
                conn,
                workflow="long-read-assembly",
                version=version,
                params={
                    "workflow": "long-read-assembly",
                    "version": version,
                    "run": name,
                },
            )
            runs[name] = row["processing_idx"]
    await db.executemany(
        "INSERT INTO qiita.assembly_sample (processing_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        [(runs["p"], prep_sample_idx), (runs["q"], prep_sample_idx)],
    )

    features = {
        name: await db.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        for name in _CONTIGS
    }
    memberships = [
        (runs[run], kind, bin_id, features[name], f"{name}_raw", circ)
        for name, (_, subjects) in _CONTIGS.items()
        for run, kind, bin_id, circ in subjects
    ]
    await db.executemany(
        "INSERT INTO qiita.assembly_membership"
        " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, raw_name, circularity)"
        " VALUES ($1, $2, $3, $4, $5, $6, $7)",
        [(prep_sample_idx, *m) for m in memberships],
    )

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.execute(
            "INSERT INTO qiita_lake.assembled_sequence VALUES "
            + ", ".join(
                f"({features[n]}, gen_random_uuid(), {len(sequences[n])})"
                for n in _CONTIGS
            )
        )
        lake.execute(
            "INSERT INTO qiita_lake.assembled_sequence_chunks VALUES "
            + ", ".join(_chunk_values(features[n], sequences[n]) for n in _CONTIGS)
        )
        lake.execute(
            "INSERT INTO qiita_lake.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx) VALUES "
            + ", ".join(
                f"({prep_sample_idx}, {run}, '{kind}', '{bin_id}', {feature})"
                for run, kind, bin_id, feature, _, _ in memberships
            )
        )
        lake.execute(
            "INSERT INTO qiita_lake.bin_quality"
            " (prep_sample_idx, processing_idx, kind, bin_id, completeness, contamination)"
            f" VALUES ({prep_sample_idx}, {runs['p']}, 'MAG', 'bin.1', 91.5, 2.5),"
            f" ({prep_sample_idx}, {runs['p']}, 'LCG', 'u7ctg', 99.0, 0.0)"
        )
    finally:
        lake.close()

    yield {
        "prep_sample_idx": prep_sample_idx,
        "runs": runs,
        "accession": accession,
        "sequences": sequences,
    }

    await db.execute(
        "DELETE FROM qiita.assembly_membership WHERE processing_idx = ANY($1::bigint[])",
        list(runs.values()),
    )
    await db.execute(
        "DELETE FROM qiita.assembly_sample WHERE processing_idx = ANY($1::bigint[])",
        list(runs.values()),
    )
    await db.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])",
        list(runs.values()),
    )
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
    )
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await db.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute("DELETE FROM qiita.study_access WHERE study_idx = $1", study_idx)
    await db.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


def _fasta(path) -> list[tuple[str, str]]:
    from qiita_control_plane.miint import connect_with_miint

    with connect_with_miint() as conn:
        return conn.execute(
            f"SELECT read_id, sequence1 FROM read_fastx('{path}') ORDER BY read_id"
        ).fetchall()


def _tsv(path) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


async def test_a_viewer_exports_each_runs_genomes_once(
    cp_server, data_plane, regular_user_session, seeded, tmp_path, monkeypatch, capsys
):
    from qiita_control_plane.cli import user as cli

    monkeypatch.setenv("QIITA_TOKEN", regular_user_session["token"])
    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    acc, seqs, runs = seeded["accession"], seeded["sequences"], seeded["runs"]

    def _export(run: str, out, *extra: str) -> int:
        out.mkdir()
        return cli.main(
            [
                "--base-url",
                cp_server,
                "assembly",
                "export",
                "--processing-idx",
                str(runs[run]),
                "--prep-sample-idx",
                str(seeded["prep_sample_idx"]),
                "--output-dir",
                str(out),
                "--data-plane-url",
                dp_url,
                *extra,
            ]
        )

    p_out = tmp_path / "p"
    assert _export("p", p_out) == 0, capsys.readouterr().err
    assert sorted(f.name for f in p_out.iterdir()) == [
        f"{acc}_bin.1.fasta.gz",
        f"{acc}_u7ctg.fasta.gz",
        "contigs.tsv",
        "genomes.tsv",
    ]
    # Longest first: the MAG member (5,000 bp), then the shared contig (4,500 bp).
    assert _fasta(p_out / f"{acc}_bin.1.fasta.gz") == [
        (f"{acc}_bin.1_1", seqs["mag"]),
        (f"{acc}_bin.1_2", seqs["shared"]),
    ]
    assert _fasta(p_out / f"{acc}_u7ctg.fasta.gz") == [(f"{acc}_u7ctg_1", seqs["lcg"])]
    genomes = {g["genome"]: g for g in _tsv(p_out / "genomes.tsv")}
    assert float(genomes[f"{acc}_bin.1"]["completeness"]) == 91.5
    assert int(genomes[f"{acc}_bin.1"]["length_bp"]) == len(seqs["mag"]) + len(
        seqs["shared"]
    )
    assert int(genomes[f"{acc}_u7ctg"]["n_circular"]) == 1

    # Run Q holds the same contig as UNBINNED, which the default kinds leave out.
    q_default = tmp_path / "q-default"
    assert _export("q", q_default) == 0
    assert _tsv(q_default / "genomes.tsv") == []

    q_out = tmp_path / "q"
    assert _export("q", q_out, "--kind", "UNBINNED") == 0
    assert _fasta(q_out / f"{acc}_u3ctg.fasta.gz") == [
        (f"{acc}_u3ctg_1", seqs["shared"])
    ]
    # UNBINNED under the length cut has no CheckM row, so its scores are empty.
    (unbinned,) = _tsv(q_out / "genomes.tsv")
    assert unbinned["completeness"] == ""
