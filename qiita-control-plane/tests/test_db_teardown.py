"""The ordered sweep that tears down a study / biosample / prep_sample graph.

These pin what the sweep promises: everything hanging off the entities goes,
including rows the caller never recorded; what belongs to nobody else is left
alone; and a table missing from the sweep list is named rather than surfacing
later as a foreign-key violation.
"""

import secrets

import pytest
from qiita_common.models import GenomeSource

from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_bare_feature,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_feature_genome,
    seed_genome,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
    seed_study,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    _ENTITY_TABLES,
    _GENOME_KEY_COLUMN,
    BIOSAMPLE,
    PREP_SAMPLE,
    STUDY,
    SWEEP_TIERS,
    UNSWEPT_ENTITY_TABLES,
    EntityGraphNotSweptError,
    _as_idx_list,
    _entity_keyed_candidates,
    _sweep_table,
    assert_entity_graph_swept,
    delete_idxs,
    delete_principal,
    resolve_ena_study_idxs,
    teardown_ena_study_graph,
    teardown_entity_graph,
)
from qiita_control_plane.testing.unique_names import unique_ena_accession

# What the graph fixtures seed, and what their tests leave for the teardown under
# test to remove, in reverse FK order. Each entry is a table, the predicate that
# matches the seeded rows, and the seeded idx bound to $1.
_MANUAL_TEARDOWN_ROWS = (
    (
        "feature_genome",
        "genome_idx IN (SELECT genome_idx FROM qiita.genome WHERE prep_sample_idx = $1)",
        "prep_sample_idx",
    ),
    ("genome", "prep_sample_idx = $1", "prep_sample_idx"),
    ("sequenced_sample", "prep_sample_idx = $1", "prep_sample_idx"),
    ("prep_sample_to_study", "prep_sample_idx = $1", "prep_sample_idx"),
    ("biosample_to_study", "biosample_idx = $1", "biosample_idx"),
    ("biosample_to_study", "study_idx = $1", "study_idx"),
    ("exported_entity", "biosample_idx = $1", "biosample_idx"),
    ("exported_entity", "study_idx = $1", "study_idx"),
    ("prep_sample", "idx = $1", "prep_sample_idx"),
    ("biosample", "idx = $1", "biosample_idx"),
    ("study", "idx = $1", "study_idx"),
    ("sequenced_pool", "idx = $1", "pool_idx"),
    ("sequencing_run", "idx = $1", "run_idx"),
    ("user", "principal_idx = $1", "principal_idx"),
    ("principal", "idx = $1", "principal_idx"),
)


def _sweep_drift_message(drift: dict[str, list[str]]) -> str:
    return (
        f"SWEEP_TIERS has drifted from the qiita schema: {drift}."
        " A table keyed on an entity or on a genome must be added to the tier"
        " that deletes it before its parents, or named in UNSWEPT_ENTITY_TABLES"
        " with the reason it is not swept; a swept table must name every such"
        " key it carries, since a row is matched on whichever one is in range;"
        " a table no longer in the schema must go."
    )


async def _count(pool, table, column, idxs) -> int:
    sql = f"SELECT count(*) FROM qiita.{table} WHERE {column} = ANY($1::bigint[])"
    return await pool.fetchval(sql, idxs)


async def _manual_teardown_graph(
    pool,
    *,
    study_idx: int | None = None,
    biosample_idx: int | None = None,
    prep_sample_idx: int | None = None,
    pool_idx: int | None = None,
    run_idx: int | None = None,
    principal_idx: int | None = None,
) -> None:
    """Delete a seeded graph, its run and pool, and its principal; an entry whose
    idx is not given is skipped.

    A hand-written list rather than SWEEP_TIERS or
    teardown_entity_graph, so this cleanup is independent of the sweep under test.
    The cost is that _MANUAL_TEARDOWN_ROWS must be kept in step with what
    the fixtures seed: a table that a seed or trigger newly populates loudly fails
    the entity delete if it references under RESTRICT, and is left behind silently
    if under SET NULL or CASCADE.
    """
    seeded = {
        "study_idx": study_idx,
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
        "pool_idx": pool_idx,
        "run_idx": run_idx,
        "principal_idx": principal_idx,
    }
    for table, predicate, key in _MANUAL_TEARDOWN_ROWS:
        if seeded[key] is None:
            continue
        await pool.execute(f"DELETE FROM qiita.{table} WHERE {predicate}", seeded[key])


@pytest.fixture
async def graph(postgres_pool):
    """A principal owning a study, a biosample, and a sequenced prep_sample."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="teardown", suffix="probe")
    study_idx = await seed_study(postgres_pool, owner_idx=principal_idx, title="teardown probe")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    # The subtype chain gives the prep_sample a sequenced_sample the caller never
    # names, and a run and pool that belong to the caller rather than the sweep.
    run_idx, pool_idx, _ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id="A1",
    )
    yield {
        "principal_idx": principal_idx,
        "study_idx": study_idx,
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
    }

    await _manual_teardown_graph(
        postgres_pool,
        study_idx=study_idx,
        biosample_idx=biosample_idx,
        prep_sample_idx=prep_sample_idx,
        pool_idx=pool_idx,
        run_idx=run_idx,
        principal_idx=principal_idx,
    )


@pytest.mark.db
async def test_teardown_entity_graph_sweeps_the_whole_graph(postgres_pool, graph):
    """Tests the case where entities carrying children are torn down.

    The fixture's subtype chain leaves a sequenced_sample that the teardown call
    below never names, which is the kind of row the sweep exists to catch.
    """
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[graph["study_idx"]],
        biosample_idxs=[graph["biosample_idx"]],
        prep_sample_idxs=[graph["prep_sample_idx"]],
    )

    survivors = {
        "sequenced_sample": await _count(
            postgres_pool, "sequenced_sample", "prep_sample_idx", [graph["prep_sample_idx"]]
        ),
        "prep_sample": await _count(postgres_pool, PREP_SAMPLE, "idx", [graph["prep_sample_idx"]]),
        "biosample": await _count(postgres_pool, BIOSAMPLE, "idx", [graph["biosample_idx"]]),
        "study": await _count(postgres_pool, STUDY, "idx", [graph["study_idx"]]),
    }
    assert survivors == {"sequenced_sample": 0, "prep_sample": 0, "biosample": 0, "study": 0}


@pytest.mark.db
async def test_teardown_entity_graph_sweeps_an_assembly_derived_genome(postgres_pool, graph):
    """Tests the case where a prep_sample has produced a genome.

    The genome references the prep_sample under RESTRICT, so leaving it would
    block the prep_sample delete; its feature_genome rows have to go first.
    """
    genome_idx, _source_id = await seed_genome(
        postgres_pool, source=GenomeSource.QIITA, prep_sample_idx=graph["prep_sample_idx"]
    )
    feature_idx = await seed_bare_feature(postgres_pool)
    await seed_feature_genome(postgres_pool, feature_idx=feature_idx, genome_idx=genome_idx)

    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[graph["study_idx"]],
        biosample_idxs=[graph["biosample_idx"]],
        prep_sample_idxs=[graph["prep_sample_idx"]],
    )

    survivors = {
        "feature_genome": await _count(postgres_pool, "feature_genome", "genome_idx", [genome_idx]),
        "genome": await _count(postgres_pool, "genome", "genome_idx", [genome_idx]),
    }
    assert survivors == {"feature_genome": 0, "genome": 0}
    await postgres_pool.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feature_idx)


@pytest.mark.db
async def test_teardown_entity_graph_leaves_an_external_genome(postgres_pool, graph):
    """Tests the case where an unrelated reference genome shares the database.

    An external genome carries no prep_sample_idx, so nothing about it is in
    range and the sweep must not widen to it.
    """
    external_idx, _source_id = await seed_genome(postgres_pool, source=GenomeSource.REFSEQ)

    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[graph["study_idx"]],
        biosample_idxs=[graph["biosample_idx"]],
        prep_sample_idxs=[graph["prep_sample_idx"]],
    )

    assert await _count(postgres_pool, "genome", "genome_idx", [external_idx]) == 1
    await postgres_pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", external_idx)


@pytest.mark.db
async def test_assert_entity_graph_swept_names_the_table_the_sweep_missed(postgres_pool, graph):
    """Tests the case where a table carrying an entity idx is not in the sweep list.

    This is what a schema gaining a table nobody added here looks like, and it
    has to fail naming that table rather than as an FK violation three
    statements later.
    """
    with pytest.raises(EntityGraphNotSweptError, match="sequenced_sample"):
        await assert_entity_graph_swept(
            postgres_pool,
            study_idxs=[],
            biosample_idxs=[],
            prep_sample_idxs=[graph["prep_sample_idx"]],
        )


@pytest.mark.db
async def test_teardown_entity_graph_accepts_empty_lists(postgres_pool):
    """Tests the case where a caller created none of some entity kind."""
    await teardown_entity_graph(
        postgres_pool, study_idxs=[], biosample_idxs=[], prep_sample_idxs=[]
    )


@pytest.mark.db
async def test_delete_principal_removes_the_user_row_first(postgres_pool):
    """Tests the case where a principal with a user row is deleted.

    qiita.user references qiita.principal under RESTRICT, so the user row has to
    go first.
    """
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="teardown-principal", suffix="probe"
    )

    await delete_principal(postgres_pool, principal_idx)

    survivors = {
        "user": await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.user WHERE principal_idx = $1", principal_idx
        ),
        "principal": await _count(postgres_pool, "principal", "idx", [principal_idx]),
    }
    assert survivors == {"user": 0, "principal": 0}


@pytest.mark.db
async def test_sweep_tiers_matches_the_live_schema(postgres_pool):
    """Tests the case where the schema and the sweep list have moved apart.

    `assert_entity_graph_swept` only names a forgotten table once some test
    seeds a row into it. This fails on the schema alone, so a table added to a
    migration is caught whether or not anything exercises it yet. It walks the
    assertion's own candidate query, which matches on the four key column names
    and so sees only tables that follow that convention; it checks both halves
    of an entry: that the table is swept, and that it names every key it
    carries.
    """
    entity_keyed = await _entity_keyed_candidates(postgres_pool)
    # Every drift bucket empties together if the candidate query returns
    # nothing, which would read as a pass.
    assert entity_keyed
    all_tables = await postgres_pool.fetch(
        "SELECT table_name FROM information_schema.tables"
        " WHERE table_schema = 'qiita' AND table_type = 'BASE TABLE'"
    )

    swept = {table for tier in SWEEP_TIERS for table, _keys in tier}
    accounted_for = swept | _ENTITY_TABLES | UNSWEPT_ENTITY_TABLES
    # A swept table must name every entity key it carries, not just one of them:
    # a row whose other key is out of range is matched on the key that is in it.
    declared_keys = {
        (table, column) for tier in SWEEP_TIERS for table, keys in tier for column, _key in keys
    }
    # qiita.genome carries genome_idx as its own primary key rather than as a
    # reference to a parent, so it is keyed on the prep_sample that made it.
    carried_keys = {
        (row["table_name"], row["column_name"])
        for row in entity_keyed
        if row["table_name"] in swept
        and (row["table_name"], row["column_name"]) != ("genome", _GENOME_KEY_COLUMN)
    }
    drift = {
        "missing_from_sweep": sorted({row["table_name"] for row in entity_keyed} - accounted_for),
        "absent_from_schema": sorted(swept - {row["table_name"] for row in all_tables}),
        "keys_not_declared": sorted(carried_keys - declared_keys),
    }

    expected = {"missing_from_sweep": [], "absent_from_schema": [], "keys_not_declared": []}
    assert drift == expected, _sweep_drift_message(drift)


@pytest.mark.db
async def test_assert_entity_graph_swept_names_a_missed_genome_table(postgres_pool, graph):
    """Tests the case where a table hanging off a genome survives the sweep.

    `exported_feature` clears its genome_idx on delete rather than refusing, so
    a row left behind here is never surfaced by a foreign-key violation later.
    The genomes have to be named explicitly, because a real teardown deletes
    qiita.genome before the check runs and they could not be found by walking
    back from the prep_sample. No sweep runs here, so the genome stands too, and
    the candidates are ordered by name, which is what settles that
    `feature_genome` is the survivor named first.
    """
    genome_idx, _source_id = await seed_genome(
        postgres_pool, source=GenomeSource.QIITA, prep_sample_idx=graph["prep_sample_idx"]
    )
    feature_idx = await seed_bare_feature(postgres_pool)
    await seed_feature_genome(postgres_pool, feature_idx=feature_idx, genome_idx=genome_idx)

    with pytest.raises(EntityGraphNotSweptError, match="feature_genome"):
        await assert_entity_graph_swept(
            postgres_pool,
            study_idxs=[],
            biosample_idxs=[],
            prep_sample_idxs=[],
            genome_idxs=[genome_idx],
        )

    await postgres_pool.execute(
        "DELETE FROM qiita.feature_genome WHERE genome_idx = $1", genome_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
    await postgres_pool.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feature_idx)


@pytest.mark.db
async def test_teardown_entity_graph_sweeps_a_link_row_by_either_side(postgres_pool, graph):
    """Tests the case where a link row names one entity in range and one outside it.

    biosample_to_study is keyed on both its sides, and a caller can hand over a
    biosample without the second study it is linked to. Matching any one key is
    what carries that row out; requiring every key would strand it, and the
    assertion would then name biosample_to_study rather than let the teardown
    reach its entity delete.
    """
    other_study_idx = await seed_study(
        postgres_pool, owner_idx=graph["principal_idx"], title="teardown probe unswept study"
    )
    try:
        await postgres_pool.execute(
            "INSERT INTO qiita.biosample_to_study (biosample_idx, study_idx, created_by_idx)"
            " VALUES ($1, $2, $3)",
            graph["biosample_idx"],
            other_study_idx,
            graph["principal_idx"],
        )

        await teardown_entity_graph(
            postgres_pool,
            study_idxs=[graph["study_idx"]],
            biosample_idxs=[graph["biosample_idx"]],
            prep_sample_idxs=[graph["prep_sample_idx"]],
        )

        survivors = {
            "biosample_to_study": await _count(
                postgres_pool, "biosample_to_study", "study_idx", [other_study_idx]
            ),
            "study": await _count(postgres_pool, STUDY, "idx", [other_study_idx]),
        }
        assert survivors == {"biosample_to_study": 0, "study": 1}
    finally:
        # The graph fixture's teardown does not delete the second study,
        # which it wasn't told about, so that must be manually torn down here.
        await _manual_teardown_graph(postgres_pool, study_idx=other_study_idx)


@pytest.mark.db
async def test_teardown_entity_graph_leaves_a_reference_exclusion(postgres_pool, graph):
    """Tests the case where an exclusion names a genome the teardown deletes.

    An exclusion carries its genome as a bare BIGINT and is meant to outlive it,
    so the assertion has to pass over the table rather than report the row as a
    survivor and refuse the teardown.
    """
    genome_idx, _source_id = await seed_genome(
        postgres_pool, source=GenomeSource.QIITA, prep_sample_idx=graph["prep_sample_idx"]
    )
    exclusion_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference_exclusion (genome_idx, reason, excluded_by_idx)"
        " VALUES ($1, $2, $3) RETURNING reference_exclusion_idx",
        genome_idx,
        "teardown probe",
        graph["principal_idx"],
    )

    try:
        await teardown_entity_graph(
            postgres_pool,
            study_idxs=[graph["study_idx"]],
            biosample_idxs=[graph["biosample_idx"]],
            prep_sample_idxs=[graph["prep_sample_idx"]],
        )

        survivors = {
            "reference_exclusion": await _count(
                postgres_pool, "reference_exclusion", "genome_idx", [genome_idx]
            ),
            "genome": await _count(postgres_pool, "genome", "genome_idx", [genome_idx]),
        }
        assert survivors == {"reference_exclusion": 1, "genome": 0}
    finally:
        # The exclusion is not swept and references the principal under
        # RESTRICT, so leaving it would fail the fixture's own teardown.
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_exclusion WHERE reference_exclusion_idx = $1",
            exclusion_idx,
        )


def test__as_idx_list():
    """Tests the case where the idxs arrive as a scalar and as an iterator: both
    come back as a list, so a caller can tell an empty run from a full one."""
    assert (_as_idx_list(7), _as_idx_list(iter([1, 2])), _as_idx_list(iter([]))) == (
        [7],
        [1, 2],
        [],
    )


async def test_delete_idxs_empty_iterator():
    """Tests the case where the idxs arrive as an exhausted iterator rather than
    an empty list: the call is a no-op, so the pool is never reached."""
    await delete_idxs(None, STUDY, iter([]))


async def test_delete_idxs_rejects_a_non_identifier_table():
    """Tests the case where the interpolated table name is not a bare identifier."""
    with pytest.raises(ValueError, match="non-identifier name"):
        await delete_idxs(None, "study; SELECT 1 --", [1])


async def test__sweep_table_rejects_a_non_identifier_table():
    """Tests the case where the interpolated table name is not a bare identifier."""
    with pytest.raises(ValueError, match="non-identifier name"):
        await _sweep_table(
            None,
            "study_access; SELECT 1 --",
            (("study_idx", STUDY),),
            {STUDY: [1]},
        )


async def test__sweep_table_rejects_a_non_identifier_column():
    """Tests the case where an interpolated column name is not a bare identifier.

    The keys are interpolated alongside the table, so a bad one has to be
    refused on the same terms rather than ride in behind a clean table name.
    """
    with pytest.raises(ValueError, match="non-identifier name"):
        await _sweep_table(
            None,
            "study_access",
            (("study_idx; SELECT 1 --", STUDY),),
            {STUDY: [1]},
        )


# ---------------------------------------------------------------------------
# The ENA-shaped teardown composed on top of the sweep
# ---------------------------------------------------------------------------


@pytest.fixture
async def ena_graph(postgres_pool):
    """A study registered under an accession, with its linked entities and a run.

    Shaped the way an ENA import leaves things: the study carries both accession
    columns, the biosample and prep_sample reach it only through their link
    rows, and the sequencing run is named after the accession.
    """
    accession = unique_ena_accession("PRJNA")
    ena_accession = unique_ena_accession("ERP")
    principal_idx = await seed_user_principal(postgres_pool, prefix="ena-td", suffix="probe")
    study_idx = await seed_study(postgres_pool, owner_idx=principal_idx, title=accession)
    await postgres_pool.execute(
        "UPDATE qiita.study SET bioproject_accession = $2, ena_study_accession = $3 WHERE idx = $1",
        study_idx,
        accession,
        ena_accession,
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    await seed_biosample_to_study_link(
        postgres_pool,
        biosample_idx=biosample_idx,
        study_idx=study_idx,
        created_by_idx=principal_idx,
    )
    await seed_prep_sample_to_study_link(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        study_idx=study_idx,
        created_by_idx=principal_idx,
    )
    run_idx, pool_idx, _ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id="E1",
    )
    # An import names the run after the accession; the teardown matches that.
    await postgres_pool.execute(
        "UPDATE qiita.sequencing_run SET instrument_run_id = $2 WHERE idx = $1",
        run_idx,
        f"{accession}:illumina",
    )
    yield {
        "accession": accession,
        "ena_accession": ena_accession,
        "principal_idx": principal_idx,
        "study_idx": study_idx,
        "biosample_idx": biosample_idx,
        "prep_sample_idx": prep_sample_idx,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
    }

    await _manual_teardown_graph(
        postgres_pool,
        study_idx=study_idx,
        biosample_idx=biosample_idx,
        prep_sample_idx=prep_sample_idx,
        pool_idx=pool_idx,
        run_idx=run_idx,
        principal_idx=principal_idx,
    )


@pytest.mark.db
async def test_resolve_ena_study_idxs_matches_the_bioproject_accession(postgres_pool, ena_graph):
    """Tests the case where a study is looked up by the accession it was registered under."""
    resolved = await resolve_ena_study_idxs(postgres_pool, [ena_graph["accession"]])

    assert resolved == [ena_graph["study_idx"]]


@pytest.mark.db
async def test_resolve_ena_study_idxs_by_ena_accession(postgres_pool, ena_graph):
    """Tests the case where the caller holds only the accession the import echoed back.

    The ena_study_accession column is searched only when asked for, so the same
    value finds nothing by default.
    """
    found = await resolve_ena_study_idxs(
        postgres_pool, [ena_graph["ena_accession"]], by_ena_accession=True
    )
    not_found = await resolve_ena_study_idxs(postgres_pool, [ena_graph["ena_accession"]])

    assert (found, not_found) == ([ena_graph["study_idx"]], [])


@pytest.mark.db
async def test_resolve_ena_study_idxs_unknown_accession(postgres_pool):
    """Tests the case where no study carries the accession, and where none is given."""
    unknown = await resolve_ena_study_idxs(postgres_pool, [unique_ena_accession("PRJNA")])
    empty = await resolve_ena_study_idxs(postgres_pool, [])

    assert (unknown, empty) == ([], [])


@pytest.mark.db
async def test_teardown_ena_study_graph_drops_the_study_its_samples_and_its_runs(
    postgres_pool, ena_graph
):
    """Tests the case where an import's whole footprint is torn down from the accession.

    The biosample and prep_sample are named nowhere in the call: they are
    reached through the link rows, which the sweep then deletes.
    """
    await teardown_ena_study_graph(
        postgres_pool,
        study_idxs=[ena_graph["study_idx"]],
        run_accessions=[ena_graph["accession"]],
    )

    survivors = {
        "study": await _count(postgres_pool, STUDY, "idx", [ena_graph["study_idx"]]),
        "biosample": await _count(postgres_pool, BIOSAMPLE, "idx", [ena_graph["biosample_idx"]]),
        "prep_sample": await _count(
            postgres_pool, PREP_SAMPLE, "idx", [ena_graph["prep_sample_idx"]]
        ),
        "sequenced_sample": await _count(
            postgres_pool, "sequenced_sample", "prep_sample_idx", [ena_graph["prep_sample_idx"]]
        ),
        "sequenced_pool": await _count(
            postgres_pool, "sequenced_pool", "idx", [ena_graph["pool_idx"]]
        ),
        "sequencing_run": await _count(
            postgres_pool, "sequencing_run", "idx", [ena_graph["run_idx"]]
        ),
    }
    assert survivors == {
        "study": 0,
        "biosample": 0,
        "prep_sample": 0,
        "sequenced_sample": 0,
        "sequenced_pool": 0,
        "sequencing_run": 0,
    }


@pytest.mark.db
async def test_teardown_ena_study_graph_without_run_accessions(postgres_pool, ena_graph):
    """Tests the case where the caller keeps the runs its study registered.

    Naming no accession leaves the run and its pool standing, which is what
    lets a caller that owns them tear the entity graph down first.
    """
    await teardown_ena_study_graph(
        postgres_pool, study_idxs=[ena_graph["study_idx"]], run_accessions=[]
    )

    survivors = {
        "study": await _count(postgres_pool, STUDY, "idx", [ena_graph["study_idx"]]),
        "prep_sample": await _count(
            postgres_pool, PREP_SAMPLE, "idx", [ena_graph["prep_sample_idx"]]
        ),
        "sequenced_pool": await _count(
            postgres_pool, "sequenced_pool", "idx", [ena_graph["pool_idx"]]
        ),
        "sequencing_run": await _count(
            postgres_pool, "sequencing_run", "idx", [ena_graph["run_idx"]]
        ),
    }
    assert survivors == {
        "study": 0,
        "prep_sample": 0,
        "sequenced_pool": 1,
        "sequencing_run": 1,
    }


@pytest.mark.db
async def test_teardown_ena_study_graph_drops_a_biosample_named_directly(postgres_pool, ena_graph):
    """Tests the case where a biosample must go although its link is already gone.

    This is the shape two studies sharing one biosample leave behind: the
    caller names the biosample, and the sweep takes it even though nothing
    reaches it from the study.
    """
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = $1",
        ena_graph["biosample_idx"],
    )

    await teardown_ena_study_graph(
        postgres_pool,
        study_idxs=[ena_graph["study_idx"]],
        run_accessions=[ena_graph["accession"]],
        extra_biosample_idxs=[ena_graph["biosample_idx"]],
    )

    survivors = {
        "biosample": await _count(postgres_pool, BIOSAMPLE, "idx", [ena_graph["biosample_idx"]]),
        "study": await _count(postgres_pool, STUDY, "idx", [ena_graph["study_idx"]]),
    }
    assert survivors == {"biosample": 0, "study": 0}


@pytest.mark.db
async def test_teardown_ena_study_graph_no_study(postgres_pool, ena_graph):
    """Tests the case where no study resolves but the accession's runs still exist.

    With no entity in range the sweep clears no sequenced_sample, so the pools
    those rows reference cannot go either; the runs are left standing rather
    than failing the teardown on a foreign key.
    """
    await teardown_ena_study_graph(
        postgres_pool, study_idxs=[], run_accessions=[ena_graph["accession"]]
    )

    survivors = {
        "sequencing_run": await _count(
            postgres_pool, "sequencing_run", "idx", [ena_graph["run_idx"]]
        ),
        "sequenced_pool": await _count(
            postgres_pool, "sequenced_pool", "idx", [ena_graph["pool_idx"]]
        ),
        "study": await _count(postgres_pool, STUDY, "idx", [ena_graph["study_idx"]]),
    }
    assert survivors == {"sequencing_run": 1, "sequenced_pool": 1, "study": 1}


@pytest.mark.db
async def test_teardown_ena_study_graph_leaves_another_accessions_run(postgres_pool, ena_graph):
    """Tests the case where a second run is named for a different accession.

    The runs are the one thing this matches by string rather than by foreign
    key, so a neighbouring accession sharing the prefix must stay out of range.
    """
    other_accession = f"{ena_graph['accession']}-other"
    other_run_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.sequencing_run (instrument_run_id, platform, created_by_idx)"
        " VALUES ($1, 'illumina'::qiita.platform, $2) RETURNING idx",
        f"{other_accession}:illumina",
        ena_graph["principal_idx"],
    )

    try:
        await teardown_ena_study_graph(
            postgres_pool,
            study_idxs=[ena_graph["study_idx"]],
            run_accessions=[ena_graph["accession"]],
        )

        survivors = {
            "seeded_run": await _count(
                postgres_pool, "sequencing_run", "idx", [ena_graph["run_idx"]]
            ),
            "other_run": await _count(postgres_pool, "sequencing_run", "idx", [other_run_idx]),
        }
        assert survivors == {"seeded_run": 0, "other_run": 1}
    finally:
        await delete_idxs(postgres_pool, "sequencing_run", other_run_idx)


@pytest.mark.db
async def test_teardown_ena_study_graph_clears_a_pool_scoped_ticket(postgres_pool, ena_graph):
    """Tests the case where a work ticket references the pool being dropped.

    A ticket is never swept and references its pool under RESTRICT, so one left
    standing would fail the pool delete rather than go with it.
    """
    action_id = "ena-teardown-probe-action"
    version = f"v-{secrets.token_hex(4)}"
    created = await seed_action_if_absent(
        postgres_pool, action_id=action_id, version=version, target_kind="sequenced_pool"
    )
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  sequenced_pool_idx, action_context, state)"
        " VALUES ($1, $2, $3, 'sequenced_pool'::qiita.scope_target_kind, $4,"
        "         '{}'::jsonb, 'completed'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ena_graph["principal_idx"],
        ena_graph["pool_idx"],
    )

    try:
        await teardown_ena_study_graph(
            postgres_pool,
            study_idxs=[ena_graph["study_idx"]],
            run_accessions=[ena_graph["accession"]],
        )

        survivors = {
            "work_ticket": await _count(
                postgres_pool, "work_ticket", "work_ticket_idx", [ticket_idx]
            ),
            "sequenced_pool": await _count(
                postgres_pool, "sequenced_pool", "idx", [ena_graph["pool_idx"]]
            ),
        }
        assert survivors == {"work_ticket": 0, "sequenced_pool": 0}
    finally:
        # work_ticket's key is work_ticket_idx, which delete_idxs cannot address.
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
        )
        await delete_action_if_created(
            postgres_pool, action_id=action_id, version=version, created=created
        )
