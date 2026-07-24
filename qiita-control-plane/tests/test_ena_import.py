import qiita_control_plane.ena_import


def test_ena_import_package_importable():
    """The ena_import package (reserved for ENA-study ingestion) is importable."""
    assert qiita_control_plane.ena_import is not None
