"""Tests for `ena_import.harmonization.build_biosample_metadata`."""

from pathlib import Path

from qiita_control_plane import ena_import
from qiita_control_plane.ena_import.harmonization import build_biosample_metadata


def test_build_biosample_metadata_empty_input_marks_host_taxon_id_unknown():
    global_metadata, local_metadata, result = build_biosample_metadata({})

    assert global_metadata == {"host taxon id": "not provided"}
    assert local_metadata == {}
    assert result.mapped_count == 0


def test_ena_import_source_has_no_bare_host_taxon_id_literal():
    """The display name is written through its constant, never a literal."""
    package_dir = Path(ena_import.__file__).parent
    offenders = [p.name for p in package_dir.glob("*.py") if '"host taxon id"' in p.read_text()]
    assert not offenders
