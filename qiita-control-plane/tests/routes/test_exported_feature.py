"""Route tests for POST /exported-feature — the public label for a published row.

The schema tests (`tests/test_exported_feature_schema.py`) cover what the database
guarantees. These cover what the route promises on top of it:

* an accession is published as-is wherever one exists, because a reader can resolve
  `GCF_…` and cannot resolve anything we mint;
* a **collision is not an error** — the loser gets a `QF` handle and the response
  says which accession it wanted, so the fallback is visible rather than silent, for
  both kinds and whatever order the caller listed them in;
* both kinds in one request, ordered genomes-then-features;
* the mint is idempotent, which is the contract for anything published;
* an entity that cannot be named at all is a 422, all-or-nothing.
"""

import uuid

import pytest
from qiita_common.api_paths import URL_EXPORTED_FEATURE

from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_genome,
    seed_reference_membership,
)

pytestmark = pytest.mark.db


async def _drop_identifiers(pool, *, genome_idxs=(), feature_idxs=()):
    await pool.execute(
        "DELETE FROM qiita.exported_feature"
        " WHERE genome_idx = ANY($1::bigint[]) OR feature_idx = ANY($2::bigint[])",
        list(genome_idxs),
        list(feature_idxs),
    )


async def test_a_genome_is_labelled_with_its_accession(role_keyed_clients):
    """Not with a minted handle. A reader can resolve `GCF_…`; `QF7` means nothing
    outside this system, which is why the accession wins whenever there is one."""
    db = role_keyed_clients["pool"]
    genome_idx, source_id = await seed_genome(db)
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE, json={"genome_idx": [genome_idx]}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["count"] == 1
        entry = body["identifiers"][0]
        assert entry["genome_idx"] == genome_idx
        assert entry["export_feature_id"] == source_id
        assert entry["accession"] == source_id
        assert entry["accession_published"] is True
    finally:
        await _drop_identifiers(db, genome_idxs=[genome_idx])
        await db.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)


async def test_a_collided_genome_accession_falls_back_instead_of_failing(role_keyed_clients):
    """Two genomes can share a `source_id` under different `source`s, because
    `qiita.genome`'s uniqueness is the composite. The second one to be published
    cannot have the string, and the request must still succeed — the response says
    what it wanted and that it lost, so nobody has to guess why the label changed
    shape.

    The request lists the entities in DESCENDING idx order. The winner is
    the lower idx either way, which is only true because the mint's INSERT orders by
    it; listing them ascending would pass against an unordered INSERT too, and pin
    nothing.
    """
    db = role_keyed_clients["pool"]
    shared = f"GCF_{uuid.uuid4().hex[:12]}"
    first = await db.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1) RETURNING genome_idx",
        shared,
    )
    second = await db.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('genbank', $1) RETURNING genome_idx",
        shared,
    )
    assert first < second
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE, json={"genome_idx": [second, first]}
        )
        assert resp.status_code == 201, resp.text
        entries = {e["genome_idx"]: e for e in resp.json()["identifiers"]}
        assert entries[first]["export_feature_id"] == shared
        assert entries[first]["accession_published"] is True

        loser = entries[second]
        assert loser["export_feature_id"].startswith("QF")
        assert loser["export_feature_id"][2:].isdigit()
        assert loser["accession"] == shared, "the wanted accession is still reported"
        assert loser["accession_published"] is False
    finally:
        await _drop_identifiers(db, genome_idxs=[first, second])
        await db.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", [first, second]
        )


async def test_a_collided_feature_accession_falls_back_instead_of_failing(role_keyed_clients):
    """The likelier collision of the two, and the one with no constraint behind it at
    all: `reference_membership.accession` is a FASTA header, and real assemblies
    routinely emit `contig_1` / `NODE_1` / `scaffold_1`, so one reference can name two
    features the same thing.

    Listed in descending idx order, for the reason the genome twin above gives.
    """
    db = role_keyed_clients["pool"]
    reference_idx = await seed_bare_reference(db, label="expfeat-route-dup-header")
    first = await seed_bare_feature(db)
    second = await seed_bare_feature(db)
    header = f"contig_{uuid.uuid4().hex[:8]}"
    for feature_idx in (first, second):
        await seed_reference_membership(
            db, reference_idx=reference_idx, feature_idx=feature_idx, accession=header
        )
    assert first < second
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE,
            json={"reference_idx": reference_idx, "feature_idx": [second, first]},
        )
        assert resp.status_code == 201, resp.text
        entries = {e["feature_idx"]: e for e in resp.json()["identifiers"]}
        assert entries[first]["export_feature_id"] == header
        assert entries[first]["accession_published"] is True

        loser = entries[second]
        assert loser["export_feature_id"].startswith("QF")
        assert loser["accession"] == header
        assert loser["accession_published"] is False
    finally:
        await _drop_identifiers(db, feature_idxs=[first, second])
        await cleanup_reference_graph(db, reference_idx=reference_idx, feature_idxs=[first, second])


async def test_a_feature_is_labelled_with_the_accession_of_its_reference(role_keyed_clients):
    db = role_keyed_clients["pool"]
    reference_idx = await seed_bare_reference(db, label="expfeat-route-feat")
    feature_idx = await seed_bare_feature(db)
    accession = f"G{uuid.uuid4().hex[:10]}"
    await seed_reference_membership(
        db, reference_idx=reference_idx, feature_idx=feature_idx, accession=accession
    )
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE,
            json={"reference_idx": reference_idx, "feature_idx": [feature_idx]},
        )
        assert resp.status_code == 201, resp.text
        entry = resp.json()["identifiers"][0]
        assert entry["feature_idx"] == feature_idx
        assert entry["reference_idx"] == reference_idx
        assert entry["genome_idx"] is None
        assert entry["export_feature_id"] == accession
    finally:
        await _drop_identifiers(db, feature_idxs=[feature_idx])
        await cleanup_reference_graph(db, reference_idx=reference_idx, feature_idxs=[feature_idx])


async def test_a_feature_with_no_accession_gets_a_minted_handle(role_keyed_clients):
    """A reference loaded before `reference_membership.accession` existed, or through
    a non-FASTA path, has nothing to publish — and still has to be nameable."""
    db = role_keyed_clients["pool"]
    reference_idx = await seed_bare_reference(db, label="expfeat-route-noacc")
    feature_idx = await seed_bare_feature(db)
    await seed_reference_membership(
        db, reference_idx=reference_idx, feature_idx=feature_idx, accession=None
    )
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE,
            json={"reference_idx": reference_idx, "feature_idx": [feature_idx]},
        )
        assert resp.status_code == 201, resp.text
        entry = resp.json()["identifiers"][0]
        assert entry["export_feature_id"].startswith("QF")
        assert entry["accession"] is None
        assert entry["accession_published"] is False
    finally:
        await _drop_identifiers(db, feature_idxs=[feature_idx])
        await cleanup_reference_graph(db, reference_idx=reference_idx, feature_idxs=[feature_idx])


async def test_both_kinds_in_one_request_are_ordered_genomes_then_features(role_keyed_clients):
    db = role_keyed_clients["pool"]
    genome_idx, source_id = await seed_genome(db)
    reference_idx = await seed_bare_reference(db, label="expfeat-route-both")
    feature_idx = await seed_bare_feature(db)
    await seed_reference_membership(
        db, reference_idx=reference_idx, feature_idx=feature_idx, accession=None
    )
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE,
            json={
                "genome_idx": [genome_idx],
                "reference_idx": reference_idx,
                "feature_idx": [feature_idx],
            },
        )
        assert resp.status_code == 201, resp.text
        entries = resp.json()["identifiers"]
        assert [e["genome_idx"] for e in entries] == [genome_idx, None]
        assert entries[0]["export_feature_id"] == source_id
        assert entries[1]["feature_idx"] == feature_idx
    finally:
        await _drop_identifiers(db, genome_idxs=[genome_idx], feature_idxs=[feature_idx])
        await cleanup_reference_graph(
            db, reference_idx=reference_idx, feature_idxs=[feature_idx], genome_idxs=[genome_idx]
        )


async def test_the_label_is_stable_across_requests(role_keyed_clients):
    """The mint is idempotent, and that is the contract rather than an optimization:
    the label is published, so the same entity must resolve the same way forever."""
    db = role_keyed_clients["pool"]
    genome_idx, _ = await seed_genome(db)
    try:
        body = {"genome_idx": [genome_idx]}
        first = await role_keyed_clients["user"].post(URL_EXPORTED_FEATURE, json=body)
        second = await role_keyed_clients["user"].post(URL_EXPORTED_FEATURE, json=body)
        assert first.status_code == second.status_code == 201
        assert (
            first.json()["identifiers"][0]["export_feature_id"]
            == second.json()["identifiers"][0]["export_feature_id"]
        )
        assert (
            await db.fetchval(
                "SELECT count(*) FROM qiita.exported_feature WHERE genome_idx = $1", genome_idx
            )
            == 1
        )
    finally:
        await _drop_identifiers(db, genome_idxs=[genome_idx])
        await db.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)


async def test_an_unknown_genome_is_refused_and_nothing_partial_is_returned(role_keyed_clients):
    db = role_keyed_clients["pool"]
    genome_idx, _ = await seed_genome(db)
    absent = await db.fetchval("SELECT max(genome_idx) + 1000 FROM qiita.genome")
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE, json={"genome_idx": [genome_idx, absent]}
        )
        assert resp.status_code == 422, resp.text
        assert "unknown genome_idx" in resp.json()["detail"]
    finally:
        await _drop_identifiers(db, genome_idxs=[genome_idx])
        await db.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)


async def test_a_feature_outside_the_named_reference_is_refused(role_keyed_clients):
    """It has no accession from that reference, so there is nothing to publish for
    it — and labelling it anyway would put a row in the artifact that the reference
    does not contain."""
    db = role_keyed_clients["pool"]
    reference_idx = await seed_bare_reference(db, label="expfeat-route-outside")
    member = await seed_bare_feature(db)
    stranger = await seed_bare_feature(db)
    await seed_reference_membership(db, reference_idx=reference_idx, feature_idx=member)
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE,
            json={"reference_idx": reference_idx, "feature_idx": [member, stranger]},
        )
        assert resp.status_code == 422, resp.text
        assert f"not in reference {reference_idx}" in resp.json()["detail"]
    finally:
        await _drop_identifiers(db, feature_idxs=[member, stranger])
        await cleanup_reference_graph(
            db, reference_idx=reference_idx, feature_idxs=[member, stranger]
        )


async def test_an_unknown_reference_is_a_404_not_a_membership_complaint(role_keyed_clients):
    """ "N feature_idx not in reference 99" would send the caller to audit a membership
    table when the reference itself is the typo."""
    db = role_keyed_clients["pool"]
    absent = await db.fetchval("SELECT coalesce(max(reference_idx), 0) + 1000 FROM qiita.reference")
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_FEATURE, json={"reference_idx": absent, "feature_idx": [1]}
    )
    assert resp.status_code == 404, resp.text


async def test_a_repeated_entity_is_one_entity(role_keyed_clients):
    """Deduped in the request, so the cap counts entities rather than list entries."""
    db = role_keyed_clients["pool"]
    genome_idx, source_id = await seed_genome(db)
    try:
        resp = await role_keyed_clients["user"].post(
            URL_EXPORTED_FEATURE, json={"genome_idx": [genome_idx, genome_idx, genome_idx]}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["count"] == 1
        assert body["identifiers"][0]["export_feature_id"] == source_id
    finally:
        await _drop_identifiers(db, genome_idxs=[genome_idx])
        await db.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)


async def test_an_empty_request_is_refused(role_keyed_clients):
    resp = await role_keyed_clients["user"].post(URL_EXPORTED_FEATURE, json={})
    assert resp.status_code == 422, resp.text


async def test_a_feature_without_its_reference_is_refused(role_keyed_clients):
    """An accession belongs to a (reference, feature) pair, so a feature_idx alone
    cannot be resolved to one."""
    resp = await role_keyed_clients["user"].post(URL_EXPORTED_FEATURE, json={"feature_idx": [1]})
    assert resp.status_code == 422, resp.text


async def test_a_reference_naming_no_feature_is_refused(role_keyed_clients):
    resp = await role_keyed_clients["user"].post(URL_EXPORTED_FEATURE, json={"reference_idx": 1})
    assert resp.status_code == 422, resp.text


async def test_an_unauthenticated_caller_is_refused(role_keyed_clients):
    resp = await role_keyed_clients["user"].post(
        URL_EXPORTED_FEATURE, json={"genome_idx": [1]}, headers={"Authorization": "Bearer nope"}
    )
    assert resp.status_code == 401, resp.text
