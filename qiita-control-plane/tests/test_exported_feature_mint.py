"""The genome kind's source predicate: whose `qiita.genome.source_id` gets published.

`_SOURCE_ID_IS_EXTERNAL_ACCESSION` in `repositories/exported_feature.py` is the
single copy of why the predicate is written the way it is. Two things are covered
here:

* every `GenomeSource` member is classified external or not. Needs no database, so
  it runs in the pure-unit tier.
* the behaviour that follows, per member: an external source publishes its
  source_id as `export_feature_id`, an internal one gets `QF<idx>`, a NULL
  `accession` and `accession_published` false.

The route tests (`tests/routes/test_exported_feature.py`) cover the rest of the
mint's contract; the schema tests cover what the database guarantees under it.
"""

import uuid

import pytest
from qiita_common.models import GenomeSource

from qiita_control_plane.repositories.exported_feature import (
    _SOURCE_ID_IS_EXTERNAL_ACCESSION,
    mint_exported_features,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_genome,
    seed_user_principal,
)


def test_every_genome_source_is_classified_external_or_not():
    """Nothing else in the suite reports a member left unclassified, and the cost is
    asymmetric: the omission is right for an internal source and wrong for the next
    external one, either way visible only once a published artifact carries the
    label."""
    classified = set(_SOURCE_ID_IS_EXTERNAL_ACCESSION)
    members = set(GenomeSource)
    assert classified == members, (
        f"unclassified={sorted(members - classified)},"
        f" not a GenomeSource={sorted(classified - members)}"
    )


@pytest.mark.db
@pytest.mark.parametrize("source", list(GenomeSource), ids=lambda s: s.value)
async def test_the_source_decides_whether_source_id_is_published(postgres_pool, source):
    """Parametrized over the whole vocabulary rather than over one external source
    and one internal one, so a new member has to be given a behaviour along with its
    classification — `_SOURCE_ID_IS_EXTERNAL_ACCESSION[source]` raises otherwise.

    The seeded source_id spells `GCF_…` for every source, so an internal genome that
    is nonetheless offered its source_id would produce an accession-shaped label and
    the assertion would still catch it.
    """
    external = _SOURCE_ID_IS_EXTERNAL_ACCESSION[source]
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="expfeat-mint", suffix=str(uuid.uuid4())[:8]
    )
    # qiita.biosample.owner_idx is guarded by a role-typed FK trigger that rejects a
    # service account, so the principal above is user-kind.
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    genome_idx, source_id = await seed_genome(
        postgres_pool,
        source=source.value,
        prep_sample_idx=prep_sample_idx if source is GenomeSource.QIITA else None,
    )
    try:
        rows = await mint_exported_features(
            postgres_pool,
            genome_idx=[genome_idx],
            reference_idx=None,
            feature_idx=[],
            created_by_idx=principal_idx,
        )
        assert len(rows) == 1, rows
        row = rows[0]
        assert row["genome_idx"] == genome_idx
        if external:
            assert row["accession"] == source_id
            assert row["accession_published"] is True
            assert row["export_feature_id"] == source_id
        else:
            assert row["accession"] is None
            assert row["accession_published"] is False
            assert row["export_feature_id"].startswith("QF")
            assert row["export_feature_id"][2:].isdigit()
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.exported_feature WHERE genome_idx = $1", genome_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)
