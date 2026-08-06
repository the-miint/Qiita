"""End-to-end: a plain USER discovers a cohort, mints a ticket for it, and
streams the alignment rows from a live data plane.

The only test that exercises discovery → mint → DP projection together, and the
one that matters most, because the signed cohort IS the authorization boundary:
the data plane serves exactly the prep_sample_idx list the ticket carries and
knows nothing about studies or users. Every control-plane check is therefore the
only check there is, and this test is what proves the ticket the CP actually
signs is the one the DP actually honours.

The shape is one pool spanning two studies — `ps_readable` in a study the user
holds Tier.VIEWER on, `ps_hidden` in one they do not. Both have alignment rows
in DuckLake under the same alignment_idx. The user must receive the first
sample's rows and never the second's, having named neither: the cohort comes
from the discovery route and the filter from the ticket.

Also pins the M2 projection end to end for a human ticket: the requested column
list is narrow (no `cigar`, which is ~96% of an alignment row), so the returned
schema proves the projection rode the signature rather than being a DP default.
"""

import base64
import uuid

import httpx
import pyarrow.flight as flight
import pytest
from conftest import ducklake_connect
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    URL_ALIGNMENT_COHORT_DOGET,
    URL_SEQUENCED_POOL_ALIGNMENT,
    URL_SEQUENCED_POOL_ALIGNMENT_COHORT,
)
from qiita_common.models.reference import Tier

from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
)

# Narrow on purpose: `cigar` is the wide column M2 exists to keep off the wire
# unless asked for, so its absence from the result schema is the assertion.
_COLUMNS = ["prep_sample_idx", "feature_idx", "mapq"]

# Out-of-band feature_idx values, one per sample, so a leaked row is
# identifiable by content and not only by count.
_FEATURE_READABLE = 810001
_FEATURE_HIDDEN = 810002


@pytest.fixture
async def two_study_pool(postgres_pool, human_admin_session, regular_user_session):
    """One pool, two studies, one alignment over both — and a reader who holds
    Tier.VIEWER on only the first study.

    Yields the identifiers the test drives; tears everything down in FK-reverse
    order.
    """
    db = postgres_pool
    owner = human_admin_session["principal_idx"]
    reader = regular_user_session["principal_idx"]

    studies = [
        await db.fetchval(
            "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
            " VALUES ($1, $2, $1) RETURNING idx",
            owner,
            f"cohort-mint-e2e-{tag}-{uuid.uuid4()}",
        )
        for tag in ("readable", "hidden")
    ]
    study_readable, study_hidden = studies
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_readable,
        reader,
        Tier.VIEWER,
        owner,
    )

    samples = []
    run_idx = pool_idx = None
    for i in range(2):
        biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
            db, owner_idx=owner
        )
        run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
            db,
            prep_sample_idx=prep_sample_idx,
            owner_idx=owner,
            sequenced_pool_item_id=f"cohort-mint-{i}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        samples.append((biosample_idx, prep_sample_idx, ss_idx))

    for (biosample_idx, prep_sample_idx, _), study_idx in zip(samples, studies, strict=True):
        await seed_biosample_to_study_link(
            db, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
        )
        await seed_prep_sample_to_study_link(
            db, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner
        )

    ps_readable, ps_hidden = samples[0][1], samples[1][1]
    params = {"reference_idx": 1, "aligner": "minimap2", "shard_ids": [0], "t": str(uuid.uuid4())}
    async with db.acquire() as conn:
        alignment_idx = (await mint_alignment_definition(conn, params=params, principal_idx=owner))[
            "alignment_idx"
        ]
    for prep_sample_idx in (ps_readable, ps_hidden):
        await db.execute(
            "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
            " VALUES ($1, $2, 'completed')",
            alignment_idx,
            prep_sample_idx,
        )

    yield {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "alignment_idx": alignment_idx,
        "ps_readable": ps_readable,
        "ps_hidden": ps_hidden,
        "study_hidden": study_hidden,
    }

    prep_idxs = [ps for _, ps, _ in samples]
    bio_idxs = [bs for bs, _, _ in samples]
    ss_idxs = [ss for _, _, ss in samples]
    await db.execute("DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", alignment_idx)
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_idxs,
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])", bio_idxs
    )
    await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])", ss_idxs)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_idxs)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bio_idxs)
    await db.execute(
        "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
        study_readable,
        reader,
    )
    await db.execute("DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", studies)


@pytest.fixture
def seeded_alignment_rows(data_plane, two_study_pool):
    """Two alignment rows in DuckLake, one per sample, under the same alignment."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        for prep_sample_idx, feature_idx in (
            (two_study_pool["ps_readable"], _FEATURE_READABLE),
            (two_study_pool["ps_hidden"], _FEATURE_HIDDEN),
        ):
            conn.execute(
                "INSERT INTO qiita_lake.alignment"
                "  (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, mapq, cigar)"
                " VALUES (?, ?, 1, ?, 60, '150M')",
                [two_study_pool["alignment_idx"], prep_sample_idx, feature_idx],
            )
        yield two_study_pool
        conn.execute(
            "DELETE FROM qiita_lake.alignment WHERE alignment_idx = ?",
            [two_study_pool["alignment_idx"]],
        )
    finally:
        conn.close()


async def test_user_discovers_mints_and_streams_only_their_slice(
    cp_server, data_plane, regular_user_session, seeded_alignment_rows
):
    """The whole milestone in one flow, from the client's side.

    Nothing here names a prep_sample by hand after the seed: the cohort comes
    from the discovery route and the row filter from the signed ticket, so a
    narrowing bug on either side shows up as the hidden study's rows arriving.
    """
    seed = seeded_alignment_rows
    auth = {"Authorization": f"Bearer {regular_user_session['token']}"}

    async with httpx.AsyncClient(base_url=cp_server, headers=auth, timeout=30.0) as client:
        listed = await client.get(
            URL_SEQUENCED_POOL_ALIGNMENT.format(
                sequencing_run_idx=seed["run_idx"], sequenced_pool_idx=seed["pool_idx"]
            )
        )
        assert listed.status_code == 200, listed.text
        summary = next(
            a for a in listed.json()["alignments"] if a["alignment_idx"] == seed["alignment_idx"]
        )
        # Caller-scoped counts: the alignment really covers two samples.
        assert (summary["samples_completed"], summary["samples_total"]) == (1, 1)

        discovered = await client.get(
            URL_SEQUENCED_POOL_ALIGNMENT_COHORT.format(
                sequencing_run_idx=seed["run_idx"],
                sequenced_pool_idx=seed["pool_idx"],
                alignment_idx=seed["alignment_idx"],
            )
        )
        assert discovered.status_code == 200, discovered.text
        cohort = discovered.json()["prep_sample_idx"]
        assert cohort == [seed["ps_readable"]]

        minted = await client.post(
            URL_ALIGNMENT_COHORT_DOGET.format(alignment_idx=seed["alignment_idx"]),
            json={"prep_sample_idx": cohort, "columns": _COLUMNS},
        )
        assert minted.status_code == 201, minted.text

        # And the boundary itself: asking for the hidden sample is refused
        # here, which is the only place it can be refused at all.
        denied = await client.post(
            URL_ALIGNMENT_COHORT_DOGET.format(alignment_idx=seed["alignment_idx"]),
            json={"prep_sample_idx": [*cohort, seed["ps_hidden"]], "columns": _COLUMNS},
        )
        assert denied.status_code == 403, denied.text
        assert str(seed["study_hidden"]) in denied.json()["detail"]

    ticket = base64.b64decode(minted.json()["ticket"])
    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    try:
        table = client.do_get(flight.Ticket(ticket)).read_all()
    finally:
        client.close()

    assert table.column_names == _COLUMNS, "the projection must ride the signed ticket"
    assert table.num_rows == 1
    assert table.column("prep_sample_idx").to_pylist() == [seed["ps_readable"]]
    assert table.column("feature_idx").to_pylist() == [_FEATURE_READABLE]
