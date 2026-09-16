"""The ENA egress check the compute-readiness probe job runs.

Never touches the network: `urlopen` is replaced per case. What is pinned is the
verdict for each shape a blocked host produces — including an HTTP status, which
means the host answered and so counts as reachable.
"""

from __future__ import annotations

import urllib.error
import urllib.request

from qiita_compute_orchestrator import ena_reachability_check as erc

_HOSTS = ("https://www.ebi.ac.uk", "https://ftp.sra.ebi.ac.uk")


def _urlopen(verdicts: dict[str, Exception | None]):
    def fake(request, timeout=None):
        exc = verdicts[request.full_url]
        if exc is not None:
            raise exc

        class _Response:
            def close(self):
                return None

        return _Response()

    return fake


def test_every_host_reachable_exits_zero(monkeypatch, capsys):
    monkeypatch.setattr(urllib.request, "urlopen", _urlopen(dict.fromkeys(_HOSTS)))

    assert erc.main(_HOSTS) == 0
    assert capsys.readouterr().out.strip() == "ok (2 hosts)"


def test_http_status_counts_as_reachable(monkeypatch):
    """ENA's archive roots owe a HEAD no 200 — an answer is the whole question."""
    http_error = urllib.error.HTTPError(_HOSTS[1], 403, "Forbidden", {}, None)
    monkeypatch.setattr(
        urllib.request, "urlopen", _urlopen({_HOSTS[0]: None, _HOSTS[1]: http_error})
    )

    assert erc.main(_HOSTS) == 0


def test_blocked_host_exits_one_and_names_it(monkeypatch, capsys):
    """The failure must not depend on `assert`: a probe run under `python -O`
    would report the blocked host as reachable."""
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        _urlopen({_HOSTS[0]: None, _HOSTS[1]: urllib.error.URLError("no route to host")}),
    )

    assert erc.main(_HOSTS) == 1
    out = capsys.readouterr().out.strip()
    assert _HOSTS[1] in out
    assert "URLError" in out
    assert _HOSTS[0] not in out


def test_failure_detail_is_one_line(monkeypatch, capsys):
    """One check per line is the probe log's contract (`_parse_probe_log`)."""
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        _urlopen(dict.fromkeys(_HOSTS, urllib.error.URLError("dns\nfailed\ttwice"))),
    )

    assert erc.main(_HOSTS) == 1
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert len(out.strip()) <= erc.MAX_DETAIL
