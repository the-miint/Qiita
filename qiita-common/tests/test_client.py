"""Tests for ControlPlaneClient."""


def test_client_importable():
    """ControlPlaneClient must be importable."""
    from qiita_common.client import ControlPlaneClient

    assert ControlPlaneClient is not None


def test_client_has_required_methods():
    """ControlPlaneClient must have create_reference, mint_features, update_reference_status."""
    from qiita_common.client import ControlPlaneClient

    client = ControlPlaneClient(base_url="http://localhost:8080")
    assert hasattr(client, "create_reference")
    assert hasattr(client, "mint_features")
    assert hasattr(client, "update_reference_status")
    assert callable(client.create_reference)
    assert callable(client.mint_features)
    assert callable(client.update_reference_status)
