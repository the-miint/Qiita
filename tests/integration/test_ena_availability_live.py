"""System test: `EnaAvailabilityClient` against the live ENA Browser API, pinning
the facts the client depends on: status 4 is public, 5 is suppressed, runs batch
in one request, and an accession ENA can't serve returns HTTP 500. Runs manually
via `make test-system`, like `test_ena_resolver_live.py`.
"""

import httpx
import pytest

from qiita_control_plane.ena_import.availability import EnaAvailabilityClient

_PUBLIC_RUN = "SRR096342"
_SUPPRESSED_RUN = "ERR000130"
_NONEXISTENT_RUN = "SRR000000001"


@pytest.mark.system
async def test_availability_client_reads_live_run_statuses():
    async with EnaAvailabilityClient() as client:
        statuses = await client.check_runs([_PUBLIC_RUN, _SUPPRESSED_RUN])
    assert statuses == {_PUBLIC_RUN: None, _SUPPRESSED_RUN: "suppressed"}


@pytest.mark.system
async def test_availability_client_raises_for_a_run_ena_cannot_serve():
    async with EnaAvailabilityClient() as client:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.check_runs([_PUBLIC_RUN, _NONEXISTENT_RUN])
    assert exc_info.value.response.status_code == 500
