"""System test: `MiintEnaResolver` against a real, live ENA study — the one
network-touching `ena_import` test (every other is fixture-driven), a deliberate
exception that guards against `read_ena` / `read_ena_attributes` column drift. If
ENA renames/removes a field the resolver relies on, this catches it; the
network-free suite can't, by design.

Accession PRJNA48739: a tiny (2 runs, 1 sample), long-finished deposit — small
and stable is what a column-drift guard needs, not a growing crowdsourced project
(unlike PRJEB11419/American Gut). Runs manually via `make test-system`; the
`@pytest.mark.system` marker keeps it out of `make test` / `make test-integration`.
"""

import pytest

from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

_STUDY_ACCESSION = "PRJNA48739"


@pytest.mark.system
def test_miint_resolver_resolves_a_real_small_stable_study():
    resolver = MiintEnaResolver()

    header = resolver.resolve_study_header(_STUDY_ACCESSION)
    assert header.study_accession == _STUDY_ACCESSION
    assert header.secondary_study_accession
    assert header.study_title

    runs = resolver.resolve_runs(_STUDY_ACCESSION)
    assert len(runs) >= 2
    for run in runs:
        assert run.study_accession == _STUDY_ACCESSION
        assert run.run_accession
        assert run.experiment_accession
        assert run.sample_accession
        assert run.library_layout in ("SINGLE", "PAIRED")
        assert run.fastq_ftp
        assert run.fastq_md5
        assert run.read_count is not None
        assert run.base_count is not None

    attrs = resolver.resolve_sample_attributes(_STUDY_ACCESSION)
    assert len(attrs) >= 1
    sample_accessions = {run.sample_accession for run in runs}
    assert {a.sample_accession for a in attrs} == sample_accessions
    for sample in attrs:
        assert sample.attributes


@pytest.mark.system
def test_miint_resolver_unresolvable_accession_fails_loud():
    from qiita_control_plane.ena_import.resolver import EnaAccessionNotFoundError

    resolver = MiintEnaResolver()
    # A well-formed but non-existent BioProject accession.
    with pytest.raises(EnaAccessionNotFoundError):
        resolver.resolve_study_header("PRJNA00000000")
