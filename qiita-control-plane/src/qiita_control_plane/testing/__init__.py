"""Pytest fixtures for control-plane database, sessions, and OIDC.

Imported by the conftest.py of any test suite that needs to talk to a real
control-plane database. Fixtures are organized by concern:
postgres (URL/pool/migrations), sessions (PAT-authenticated principals),
jwks (OIDC harness).

Pytest is required at import time — these modules are only used in test
environments.
"""
