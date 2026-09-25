"""`qiita mask syndna-read-count` end to end: a study VIEWER, the real control plane
(the route and its gate) and, for `--feature-names species`, the real data plane
(a reference DoGet of the taxonomy).

The counts are seeded as the read-mask action writes them; the action itself is
covered in the control plane's DB tier.
"""

import json
import secrets
import uuid

import pytest
from conftest import ducklake_connect
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models import Tier

from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
)

_INSERTS = {
    "synDNA_16SrRNA_seq_1_gc=0.26": ("syn one", 12),
    "synDNA_16SrRNA_seq_2_gc=0.36": ("syn two", 0),
}


@pytest.fixture
async def seeded(postgres_pool, human_admin_session, regular_user_session, data_plane):
    db = postgres_pool
    owner = human_admin_session["principal_idx"]
    tag = secrets.token_hex(4)
    study_idx = await db.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        f"syndna-e2e-{tag}",
    )
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        regular_user_session["principal_idx"],
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

    reference_idx = await db.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"syndna-e2e-{tag}",
        owner,
    )
    features = {}
    for header in _INSERTS:
        features[header] = await db.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        await db.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, accession)"
            " VALUES ($1, $2, $3)",
            reference_idx,
            features[header],
            header,
        )
    mask_idx = await db.fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', $2::jsonb, $3) RETURNING mask_idx",
        uuid.uuid4().bytes + uuid.uuid4().bytes,
        json.dumps({"resolved_syndna": {"reference_idx": reference_idx}}),
        owner,
    )
    await db.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        mask_idx,
        prep_sample_idx,
    )
    await db.executemany(
        "INSERT INTO qiita.syndna_read_count (mask_idx, prep_sample_idx, feature_idx, read_count)"
        " VALUES ($1, $2, $3, $4)",
        [(mask_idx, prep_sample_idx, features[h], n) for h, (_, n) in _INSERTS.items()],
    )

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.execute(
            "INSERT INTO qiita_lake.reference_taxonomy (reference_idx, feature_idx, species)"
            " VALUES "
            + ", ".join(
                f"({reference_idx}, {features[h]}, '{species}')"
                for h, (species, _) in _INSERTS.items()
            )
        )
    finally:
        lake.close()

    yield {"mask_idx": mask_idx, "study_idx": study_idx, "accession": accession}

    await db.execute(
        "DELETE FROM qiita.syndna_read_count WHERE mask_idx = $1", mask_idx
    )
    await db.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await db.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await db.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await db.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
    )
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await db.execute("DELETE FROM qiita.study_access WHERE study_idx = $1", study_idx)
    await db.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


def _cells(path, source: str) -> list[tuple]:
    from qiita_control_plane.miint import connect_with_miint

    with connect_with_miint() as conn:
        return sorted(
            conn.execute(
                f"SELECT sample_id, feature_id, value FROM {source}('{path}')"
            ).fetchall()
        )


async def test_a_viewer_exports_the_table_in_both_formats_and_namings(
    cp_server, data_plane, regular_user_session, seeded, tmp_path, monkeypatch, capsys
):
    from qiita_control_plane.cli import user as cli

    monkeypatch.setenv("QIITA_TOKEN", regular_user_session["token"])
    acc = seeded["accession"]

    def _export(out, *extra: str) -> int:
        return cli.main(
            [
                "--base-url",
                cp_server,
                "mask",
                "syndna-read-count",
                "--mask-idx",
                str(seeded["mask_idx"]),
                "--study-idx",
                str(seeded["study_idx"]),
                "--output",
                str(out),
                *extra,
            ]
        )

    biom = tmp_path / "syndna.biom"
    assert _export(biom) == 0, capsys.readouterr().err
    # BIOM is sparse: the zero cell is absent.
    assert _cells(biom, "read_biom") == [(acc, "synDNA_16SrRNA_seq_1_gc=0.26", 12.0)]

    parquet = tmp_path / "syndna.parquet"
    rc = _export(
        parquet,
        "--format",
        "parquet",
        "--feature-names",
        "species",
        "--data-plane-url",
        f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
    )
    assert rc == 0, capsys.readouterr().err
    assert _cells(parquet, "read_parquet") == [
        (acc, "syn one", 12.0),
        (acc, "syn two", 0.0),
    ]
