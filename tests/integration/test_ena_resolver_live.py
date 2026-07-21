"""System test: `MiintEnaResolver` against a real, live ENA study —
guards against `read_ena` / `read_ena_attributes` column drift (T01-2).

Every other `ena_import` test is network-free (monkeypatched query
functions / `httpx.MockTransport` against recorded fixtures — see
`qiita-control-plane/tests/ena_import/`). This is the one exception,
approved as part of the reconciled TASK-01 plan: it drives the real
resolver against the real ENA Portal/Browser APIs and asserts the columns
this resolver depends on are still present with the expected shape. If ENA
renames/removes a field `read_ena`'s `DefaultFields` (or our explicit
`_RUN_FIELDS`) relies on, this is the test that catches it — the
network-free suite can't, by design.

Accession choice — `PRJNA48739` ("Streptococcus pneumoniae GA17570 genome
sequencing project", first_public 2013-05-31): a genuinely tiny (2 runs, 1
sample), long-finished genome-sequencing deposit, not a growing/curated
crowdsourced project (unlike e.g. PRJEB11419/American Gut, which has grown
past 44,000 runs and keeps changing) — small and stable is exactly what a
column-drift guard needs, not a resolver-scale/perf test. Recorded via a
live `curl` against the ENA Portal/Browser APIs on 2026-07-21; the same
rows are recorded as fixtures for the network-free suite
(`qiita-control-plane/tests/ena_import/fixtures/`).

Runs manually via ``make test-system`` (`pytest -m system`); the
`@pytest.mark.system` marker keeps it out of ``make test`` and
``make test-integration`` (`pytest -m 'not system'`) alike.
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
