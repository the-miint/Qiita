"""The ENA egress check the compute-readiness probe job runs.

Never touches the network: `urlopen` is replaced per case. What is pinned is the
host list the deploy actually asks about and the verdict for each shape a
blocked host produces — including an HTTP status, which a blocking gateway
serves and which therefore must not read as reachable.
"""

from __future__ import annotations

import urllib.error
import urllib.request

from qiita_compute_orchestrator import ena_reachability_check as erc


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


def test_checks_both_ena_archives_by_default():
    """The two hosts an ENA import reaches: metadata resolve on the control
    plane, read download on the compute nodes (docs/runbooks/ena-import.md).
    Dropping one here would silently shrink what the deploy proves, so the
    cases below drive `main()` through this default rather than their own list.
    """
    assert erc.ENA_HOSTS == ("https://www.ebi.ac.uk", "https://ftp.sra.ebi.ac.uk")


def test_every_host_reachable_exits_zero(monkeypatch, capsys):
    monkeypatch.setattr(urllib.request, "urlopen", _urlopen(dict.fromkeys(erc.ENA_HOSTS)))

    assert erc.main() == 0
    assert capsys.readouterr().out.strip() == "ok (2 hosts)"


def test_http_error_status_is_unreachable(monkeypatch, capsys):
    """A gateway that blocks egress still answers on the socket — its 403 block
    page is the failure this check exists for, not evidence of reachability."""
    blocked = urllib.error.HTTPError(erc.ENA_HOSTS[1], 403, "Forbidden", {}, None)
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        _urlopen({erc.ENA_HOSTS[0]: None, erc.ENA_HOSTS[1]: blocked}),
    )

    assert erc.main() == 1
    out = capsys.readouterr().out
    assert erc.ENA_HOSTS[1] in out
    assert "403" in out


def test_blocked_host_exits_one_and_names_it(monkeypatch, capsys):
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        _urlopen(
            {
                erc.ENA_HOSTS[0]: None,
                erc.ENA_HOSTS[1]: urllib.error.URLError("no route to host"),
            }
        ),
    )

    assert erc.main() == 1
    out = capsys.readouterr().out.strip()
    assert erc.ENA_HOSTS[1] in out
    assert "URLError" in out
    assert erc.ENA_HOSTS[0] not in out


def test_failure_detail_is_one_truncated_line(monkeypatch, capsys):
    """One check per line is the probe log's contract (`_parse_probe_log`), and
    the line is bounded so a chained error can't swamp the log. Both hosts fail
    with a message long enough that the pair exceeds MAX_DETAIL."""
    long_error = urllib.error.URLError("dns\nfailed\ttwice " + "x" * erc.MAX_DETAIL)
    monkeypatch.setattr(
        urllib.request, "urlopen", _urlopen(dict.fromkeys(erc.ENA_HOSTS, long_error))
    )

    assert erc.main() == 1
    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert len(out.strip()) == erc.MAX_DETAIL
