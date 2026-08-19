"""Schema-level invariants for `qiita.exported_feature`.

The route tests cover who may mint and what comes back. These cover the
guarantees that live in the database itself, because they are the ones a caller
cannot reach and a future migration could quietly drop:

* `export_feature_id` is authored by Postgres and unwritable, so a public
  identifier cannot be forged and cannot be edited after publication.
* It resolves to the **accession** when one won, and to a minted `QF<idx>` handle
  otherwise — the two halves of one namespace.
* That namespace is **UNIQUE across both kinds**. This is the constraint the mint's
  fallback exists to satisfy: a client cannot see another caller's rows, so only
  the database can tell an accession it is already using.
* Exactly one kind on a live row, and a feature-kind row names its reference —
  a feature's accession belongs to the `(reference, feature)` membership, so a
  feature identifier without a reference would be naming nothing in particular.
* An identifier OUTLIVES the entity it names: deleting a reference detaches and
  retires it rather than deleting it, which is what makes the FKs' `ON DELETE SET
  NULL` satisfy the checks at all.
* Retirement releases a **genome's** accession and reserves a **feature's** forever.
  That asymmetry is the subtlest thing in the table and the only reason
  `entity_kind` is stored, so both directions are tested — a released string that
  should have been reserved means a published label can come to name a different
  sequence.
"""

import uuid

import asyncpg
import pytest

from qiita_control_plane.testing.db_seeds import (
    cleanup_reference_graph,
    seed_bare_feature,
    seed_bare_reference,
    seed_genome,
    seed_reference_membership,
    seed_user_principal,
)

pytestmark = pytest.mark.db


async def _principal(postgres_pool):
    return await seed_user_principal(
        postgres_pool, prefix="expfeat-schema", suffix=str(uuid.uuid4())[:8]
    )


async def _insert_genome_row(postgres_pool, *, genome_idx, accession, published, created_by_idx):
    return await postgres_pool.fetchrow(
        "INSERT INTO qiita.exported_feature"
        "       (entity_kind, genome_idx, accession, accession_published, created_by_idx)"
        " VALUES ('genome', $1, $2, $3, $4) RETURNING idx, export_feature_id",
        genome_idx,
        accession,
        published,
        created_by_idx,
    )


async def _insert_feature_row(
    postgres_pool, *, reference_idx, feature_idx, accession, published, created_by_idx
):
    return await postgres_pool.fetchrow(
        "INSERT INTO qiita.exported_feature"
        "       (entity_kind, reference_idx, feature_idx, accession, accession_published,"
        "        created_by_idx)"
        " VALUES ('feature', $1, $2, $3, $4, $5) RETURNING idx, export_feature_id",
        reference_idx,
        feature_idx,
        accession,
        published,
        created_by_idx,
    )


async def _cleanup(postgres_pool, *, reference_idx=None, feature_idxs=(), genome_idxs=()):
    await postgres_pool.execute(
        "DELETE FROM qiita.exported_feature"
        " WHERE genome_idx = ANY($1::bigint[]) OR feature_idx = ANY($2::bigint[])",
        list(genome_idxs),
        list(feature_idxs),
    )
    if reference_idx is not None:
        await cleanup_reference_graph(
            postgres_pool,
            reference_idx=reference_idx,
            feature_idxs=feature_idxs,
            genome_idxs=genome_idxs,
        )


async def test_the_constraint_and_index_set_is_exactly_what_a_new_kind_must_update(postgres_pool):
    """The FORWARD PLAN comment in the migration says a further entity kind has to
    update five things. This is the mechanism behind that sentence: every other test in
    this module is behavioural, so each one exercises a constraint it happens to trip
    and none of them notices a constraint that was never created — or one added later
    without the reasoning. Adding a kind fails here first, which is where the list is.

    Follows the `pg_constraint` / `pg_index` pattern the other schema tests in this
    suite use. Names only: the definitions are asserted behaviourally below, and pinning
    their text would break on a Postgres that renders an equivalent expression
    differently."""
    checks = {
        r["conname"]
        for r in await postgres_pool.fetch(
            "SELECT c.conname FROM pg_constraint c"
            "  JOIN pg_class ct ON ct.oid = c.conrelid"
            "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
            " WHERE c.contype = 'c' AND cn.nspname = 'qiita'"
            "   AND ct.relname = 'exported_feature'"
        )
    }
    assert checks == {
        "exported_feature_one_kind",
        "exported_feature_entity_kind_known",
        "exported_feature_kind_agrees_with_columns",
        "exported_feature_reference_pairs_with_feature",
        "exported_feature_published_accession_exists",
        "exported_feature_retirement_consistent",
    }, checks

    indexes = {
        r["indexname"]
        for r in await postgres_pool.fetch(
            "SELECT indexname FROM pg_indexes"
            " WHERE schemaname = 'qiita' AND tablename = 'exported_feature'"
        )
    }
    assert indexes == {
        "exported_feature_pkey",
        "exported_feature_live_genome",
        "exported_feature_live_reference_feature",
        "exported_feature_export_feature_id_unique",
    }, indexes

    # The one index whose PREDICATE is the asymmetry the namespace depends on: a
    # retired genome releases its accession, a retired feature keeps it reserved.
    # Behaviourally pinned by the two retirement tests below; pinned textually here
    # because those two would both still pass if the predicate lost `NOT retired`.
    (namespace,) = [
        r["indexdef"]
        for r in await postgres_pool.fetch(
            "SELECT indexdef FROM pg_indexes"
            " WHERE schemaname = 'qiita' AND tablename = 'exported_feature'"
            "   AND indexname = 'exported_feature_export_feature_id_unique'"
        )
    ]
    assert "NOT retired" in namespace and "'feature'" in namespace, namespace


async def test_a_genome_publishes_its_accession(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        row = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        assert row["export_feature_id"] == source_id
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_an_entity_with_no_accession_falls_back_to_a_minted_handle(postgres_pool):
    """The `QF` half of the namespace. A feature loaded before
    `reference_membership.accession` existed, or through a non-FASTA path, has no
    accession at all — and still has to be nameable.
    """
    principal_idx = await _principal(postgres_pool)
    reference_idx = await seed_bare_reference(postgres_pool, label="expfeat-noacc")
    feature_idx = await seed_bare_feature(postgres_pool)
    await seed_reference_membership(
        postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx, accession=None
    )
    try:
        row = await _insert_feature_row(
            postgres_pool,
            reference_idx=reference_idx,
            feature_idx=feature_idx,
            accession=None,
            published=False,
            created_by_idx=principal_idx,
        )
        assert row["export_feature_id"] == f"QF{row['idx']}"
    finally:
        await _cleanup(postgres_pool, reference_idx=reference_idx, feature_idxs=[feature_idx])


async def test_a_collided_accession_is_kept_as_provenance_while_the_handle_is_minted(
    postgres_pool,
):
    """The fallback must stay distinguishable from "there was no accession".

    Someone will ask why their table says `QF77` instead of `GCF_…`; the row has to
    be able to answer.
    """
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        row = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=False,
            created_by_idx=principal_idx,
        )
        assert row["export_feature_id"] == f"QF{row['idx']}"
        stored = await postgres_pool.fetchrow(
            "SELECT accession, accession_published FROM qiita.exported_feature WHERE idx = $1",
            row["idx"],
        )
        assert stored["accession"] == source_id
        assert stored["accession_published"] is False
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_export_feature_id_cannot_be_supplied_by_a_caller(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        with pytest.raises(asyncpg.PostgresError) as exc:
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, genome_idx, accession, accession_published,"
                "        created_by_idx, export_feature_id)"
                " VALUES ('genome', $1, $2, true, $3, 'FORGED')",
                genome_idx,
                source_id,
                principal_idx,
            )
        assert "export_feature_id" in str(exc.value)
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_two_entities_cannot_publish_the_same_identifier(postgres_pool):
    """`UNIQUE (export_feature_id)` across BOTH kinds, which is the whole reason the
    mint needs a fallback: a genome and a feature can carry the same accession
    string, and `UNIQUE (source, source_id)` on `qiita.genome` is composite so two
    genomes can too.
    """
    principal_idx = await _principal(postgres_pool)
    shared = f"GCF_{uuid.uuid4().hex[:12]}"
    first_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1) RETURNING genome_idx",
        shared,
    )
    second_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('genbank', $1) RETURNING genome_idx",
        shared,
    )
    try:
        await _insert_genome_row(
            postgres_pool,
            genome_idx=first_idx,
            accession=shared,
            published=True,
            created_by_idx=principal_idx,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_genome_row(
                postgres_pool,
                genome_idx=second_idx,
                accession=shared,
                published=True,
                created_by_idx=principal_idx,
            )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[first_idx, second_idx])


async def test_exactly_one_kind_on_a_live_row(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    reference_idx = await seed_bare_reference(postgres_pool, label="expfeat-onekind")
    feature_idx = await seed_bare_feature(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, genome_idx, reference_idx, feature_idx,"
                "        accession_published, created_by_idx)"
                " VALUES ('genome', $1, $2, $3, false, $4)",
                genome_idx,
                reference_idx,
                feature_idx,
                principal_idx,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, accession_published, created_by_idx)"
                " VALUES ('genome', false, $1)",
                principal_idx,
            )
    finally:
        await _cleanup(
            postgres_pool,
            reference_idx=reference_idx,
            feature_idxs=[feature_idx],
            genome_idxs=[genome_idx],
        )


async def test_a_feature_row_must_name_the_reference_that_accessioned_it(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    reference_idx = await seed_bare_reference(postgres_pool, label="expfeat-pair")
    feature_idx = await seed_bare_feature(postgres_pool)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, feature_idx, accession_published, created_by_idx)"
                " VALUES ('feature', $1, false, $2)",
                feature_idx,
                principal_idx,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, reference_idx, accession_published, created_by_idx)"
                " VALUES ('feature', $1, false, $2)",
                reference_idx,
                principal_idx,
            )
    finally:
        await _cleanup(postgres_pool, reference_idx=reference_idx, feature_idxs=[feature_idx])


async def test_publishing_an_accession_requires_having_one(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    genome_idx, _ = await seed_genome(postgres_pool)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert_genome_row(
                postgres_pool,
                genome_idx=genome_idx,
                accession=None,
                published=True,
                created_by_idx=principal_idx,
            )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_one_live_identifier_per_genome(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_genome_row(
                postgres_pool,
                genome_idx=genome_idx,
                accession=source_id,
                published=False,
                created_by_idx=principal_idx,
            )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_one_live_identifier_per_reference_feature(postgres_pool):
    """Per KIND, and scoped to the reference: the same feature in two references is
    two published entities, because the accession that names it is the membership's.
    """
    principal_idx = await _principal(postgres_pool)
    first_ref = await seed_bare_reference(postgres_pool, label="expfeat-live-a")
    second_ref = await seed_bare_reference(postgres_pool, label="expfeat-live-b")
    feature_idx = await seed_bare_feature(postgres_pool)
    for reference_idx in (first_ref, second_ref):
        await seed_reference_membership(
            postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx, accession=None
        )
    try:
        await _insert_feature_row(
            postgres_pool,
            reference_idx=first_ref,
            feature_idx=feature_idx,
            accession=None,
            published=False,
            created_by_idx=principal_idx,
        )
        await _insert_feature_row(
            postgres_pool,
            reference_idx=second_ref,
            feature_idx=feature_idx,
            accession=None,
            published=False,
            created_by_idx=principal_idx,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_feature_row(
                postgres_pool,
                reference_idx=first_ref,
                feature_idx=feature_idx,
                accession=None,
                published=False,
                created_by_idx=principal_idx,
            )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE feature_idx = $1", feature_idx
        )
        await cleanup_reference_graph(
            postgres_pool, reference_idx=first_ref, feature_idxs=[feature_idx]
        )
        await cleanup_reference_graph(postgres_pool, reference_idx=second_ref)


async def test_a_retired_genome_row_does_not_block_a_fresh_identifier(postgres_pool):
    """And the fresh one gets the ACCESSION back, not a minted handle.

    The common retirement here is automatic — deleting a reference detaches every
    identifier it accessioned — so holding a genome's accession after retirement
    would hand a re-loaded genome a `QF<n>` purely because a reference was once
    deleted, which is the outcome the hybrid exists to avoid. A GENOME accession is
    the source's name for an organism, so handing it back names the same thing; the
    feature kind is the opposite and is tested below.
    """
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        first = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute(
            "UPDATE qiita.exported_feature"
            "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
            " WHERE idx = $1",
            first["idx"],
        )
        second = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        assert second["idx"] != first["idx"]
        assert second["export_feature_id"] == source_id
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_a_retired_feature_keeps_its_accession_reserved(postgres_pool):
    """The other half of the asymmetry above, and the reason `entity_kind` exists.

    A feature's accession is a FASTA header from one load: `contig_5` names nothing
    outside the reference that emitted it. Releasing it on retirement would let an
    unrelated sequence publish `contig_5` later, so one published label would have
    named two different sequences.
    """
    principal_idx = await _principal(postgres_pool)
    first_ref = await seed_bare_reference(postgres_pool, label="expfeat-reserved-a")
    second_ref = await seed_bare_reference(postgres_pool, label="expfeat-reserved-b")
    first_feature = await seed_bare_feature(postgres_pool)
    second_feature = await seed_bare_feature(postgres_pool)
    header = f"contig_{uuid.uuid4().hex[:8]}"
    await seed_reference_membership(
        postgres_pool, reference_idx=first_ref, feature_idx=first_feature, accession=header
    )
    await seed_reference_membership(
        postgres_pool, reference_idx=second_ref, feature_idx=second_feature, accession=header
    )
    try:
        published = await _insert_feature_row(
            postgres_pool,
            reference_idx=first_ref,
            feature_idx=first_feature,
            accession=header,
            published=True,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute(
            "UPDATE qiita.exported_feature"
            "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
            " WHERE idx = $1",
            published["idx"],
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_feature_row(
                postgres_pool,
                reference_idx=second_ref,
                feature_idx=second_feature,
                accession=header,
                published=True,
                created_by_idx=principal_idx,
            )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE feature_idx = ANY($1::bigint[])"
            "    OR accession = $2",
            [first_feature, second_feature],
            header,
        )
        await cleanup_reference_graph(
            postgres_pool, reference_idx=first_ref, feature_idxs=[first_feature]
        )
        await cleanup_reference_graph(
            postgres_pool, reference_idx=second_ref, feature_idxs=[second_feature]
        )


async def test_a_retired_feature_cannot_be_edited_out_of_the_reserved_namespace(postgres_pool):
    """The reservation above is only as strong as the columns it is computed from.

    Every CHECK on this table is written `retired OR ...`, because a detached row has
    lost the columns they test — so a retired row is exactly where they stop helping,
    and it is also where the namespace index is still reading `entity_kind`. Three
    edits would each hand a reserved accession to the next caller: flipping
    `entity_kind` to 'genome' drops the row out of the index predicate, and clearing
    either `accession_published` or `accession` regenerates `export_feature_id` to
    'QF<idx>'. The trigger rejects all three above its retired early-return.
    """
    principal_idx = await _principal(postgres_pool)
    reference_idx = await seed_bare_reference(postgres_pool, label="expfeat-immutable")
    feature_idx = await seed_bare_feature(postgres_pool)
    header = f"contig_{uuid.uuid4().hex[:8]}"
    await seed_reference_membership(
        postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx, accession=header
    )
    try:
        row = await _insert_feature_row(
            postgres_pool,
            reference_idx=reference_idx,
            feature_idx=feature_idx,
            accession=header,
            published=True,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute(
            "UPDATE qiita.exported_feature"
            "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
            " WHERE idx = $1",
            row["idx"],
        )
        for column, value in (
            ("entity_kind", "genome"),
            ("accession_published", False),
            ("accession", "something-else"),
        ):
            with pytest.raises(asyncpg.RaiseError, match="immutable"):
                await postgres_pool.execute(
                    f"UPDATE qiita.exported_feature SET {column} = $1 WHERE idx = $2",
                    value,
                    row["idx"],
                )
        # Still reserved, which is the property all three edits were reaching for.
        assert (
            await postgres_pool.fetchval(
                "SELECT export_feature_id FROM qiita.exported_feature WHERE idx = $1", row["idx"]
            )
            == header
        )
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE feature_idx = $1", feature_idx
        )
        await cleanup_reference_graph(
            postgres_pool, reference_idx=reference_idx, feature_idxs=[feature_idx]
        )


async def test_retirement_itself_is_still_allowed(postgres_pool):
    """The control for the test above: the guard sits over three columns, not over
    the UPDATE that retires a row. Without this, making the trigger reject every
    UPDATE would pass the immutability test and break the FK detach paths."""
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        row = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute(
            "UPDATE qiita.exported_feature"
            "   SET retired = true, retired_at = now(), retire_reason = 'withdrawn'"
            " WHERE idx = $1",
            row["idx"],
        )
        assert await postgres_pool.fetchval(
            "SELECT retired FROM qiita.exported_feature WHERE idx = $1", row["idx"]
        )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_entity_kind_must_name_the_columns_the_row_holds(postgres_pool):
    """Otherwise a genome could be inserted as a feature and would keep its accession
    reserved forever — the namespace index reads this column, not the id columns."""
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, genome_idx, accession, accession_published,"
                "        created_by_idx)"
                " VALUES ('feature', $1, $2, true, $3)",
                genome_idx,
                source_id,
                principal_idx,
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "INSERT INTO qiita.exported_feature"
                "       (entity_kind, genome_idx, accession_published, created_by_idx)"
                " VALUES ('plasmid', $1, false, $2)",
                genome_idx,
                principal_idx,
            )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])


async def test_an_identifier_outlives_the_genome_it_names(postgres_pool):
    """A published identifier is never deleted. The reference-delete path hard-DELETEs
    `qiita.genome` rows, so without the detach-and-retire trigger either that delete
    fails or a citation stops resolving.
    """
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        row = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        await postgres_pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
        after = await postgres_pool.fetchrow(
            "SELECT genome_idx, retired, retired_at, retire_reason, export_feature_id"
            "  FROM qiita.exported_feature WHERE idx = $1",
            row["idx"],
        )
        assert after is not None, "the identifier was deleted with the genome"
        assert after["genome_idx"] is None
        assert after["retired"] is True
        assert after["retired_at"] is not None
        assert str(genome_idx) in after["retire_reason"]
        assert after["export_feature_id"] == source_id
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE accession = $1", source_id
        )


async def test_an_identifier_outlives_the_reference_that_accessioned_it(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    reference_idx = await seed_bare_reference(postgres_pool, label="expfeat-outlive")
    feature_idx = await seed_bare_feature(postgres_pool)
    accession = f"G{uuid.uuid4().hex[:10]}"
    await seed_reference_membership(
        postgres_pool, reference_idx=reference_idx, feature_idx=feature_idx, accession=accession
    )
    try:
        row = await _insert_feature_row(
            postgres_pool,
            reference_idx=reference_idx,
            feature_idx=feature_idx,
            accession=accession,
            published=True,
            created_by_idx=principal_idx,
        )
        await cleanup_reference_graph(
            postgres_pool, reference_idx=reference_idx, feature_idxs=[feature_idx]
        )
        after = await postgres_pool.fetchrow(
            "SELECT reference_idx, feature_idx, retired, export_feature_id"
            "  FROM qiita.exported_feature WHERE idx = $1",
            row["idx"],
        )
        assert after is not None, "the identifier was deleted with the reference"
        assert after["retired"] is True
        assert after["feature_idx"] is None
        assert after["export_feature_id"] == accession
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE accession = $1", accession
        )


async def test_retirement_columns_cannot_disagree(postgres_pool):
    principal_idx = await _principal(postgres_pool)
    genome_idx, source_id = await seed_genome(postgres_pool)
    try:
        row = await _insert_genome_row(
            postgres_pool,
            genome_idx=genome_idx,
            accession=source_id,
            published=True,
            created_by_idx=principal_idx,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "UPDATE qiita.exported_feature SET retired = true WHERE idx = $1", row["idx"]
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await postgres_pool.execute(
                "UPDATE qiita.exported_feature SET retire_reason = 'no' WHERE idx = $1",
                row["idx"],
            )
    finally:
        await _cleanup(postgres_pool, genome_idxs=[genome_idx])
