"""Does this host have outbound HTTPS to the ENA archives?

Run as a module, from the interpreter under test:

    <SLURM_NATIVE_PYTHON> -P -m qiita_compute_orchestrator.ena_reachability_check

Exit 0, `ok (N hosts)` on stdout. Exit 1, a single line naming each host that
could not be reached and why. The caller names the check
(`ena-from-compute=fail err=...`), as it does for the miint probes, so this
prints the payload only.

**Why this exists.** ENA import resolves study/sample metadata from
`www.ebi.ac.uk` on the control plane and downloads reads from both archives on
the compute nodes. A firewall, NAT or proxy that blocks outbound HTTPS passes
every other deploy check and then fails *every* import at runtime, so both
halves of the deploy probe egress here: `deploy/verify.sh` (`ena-reachability`)
for the control-plane host, and the `compute-readiness` probe job
(`ena-from-compute`) for a compute node.

**What a green row proves, and what it does not.** stdlib `urllib` answers "does
this host have outbound HTTPS to ENA", not "can miint fetch from it". The real
work goes through DuckDB httpfs (`qiita_common.duckdb_miint`), whose own TLS,
CA and proxy configuration neither this module nor `curl` can see, so a green
row rules out the blocked-egress case and nothing more. A problem confined to
httpfs still surfaces first at resolve or download.

**Why a module and not `curl`.** Not availability — the same probe script runs
`curl` on the compute node for `cp-from-compute`. It is that the host list, the
reachable/unreachable verdict and the one-line detail are then one unit with
unit tests (`tests/test_ena_reachability_check.py`); the same loop inlined in
the generated bash script would be covered by `bash -n` and nothing else. The
module form also shares `native_import_check`'s `err=` contract, so the caller
captures both the same way. Deliberately no miint LOAD: a red row then means
egress, never a broken extension.
"""

import urllib.request

from .native_import_check import MAX_DETAIL

ENA_HOSTS = ("https://www.ebi.ac.uk", "https://ftp.sra.ebi.ac.uk")

_TIMEOUT_S = 10


def main(hosts: tuple[str, ...] = ENA_HOSTS) -> int:
    """HEAD each host; return 0 when every one answered 2xx, 1 otherwise.

    A non-2xx status is unreachable, not reachable: a blocking gateway answers
    on the socket, and its 403 block page is exactly the failure this check is
    for. `urlopen` follows redirects, so a 3xx is judged on where it lands.
    Both archive roots answer a HEAD with 200 and no redirect.
    """
    unreachable = []
    for host in hosts:
        try:
            urllib.request.urlopen(  # noqa: S310 — fixed https:// constants
                urllib.request.Request(host, method="HEAD"), timeout=_TIMEOUT_S
            ).close()
        except Exception as exc:
            # `.split()` folds the newlines, carriage returns and tabs that
            # would break the caller's one-check-per-line log; MAX_DETAIL keeps
            # a long chained error from swamping it.
            unreachable.append(f"{host} ({type(exc).__name__}: {exc})")
    if unreachable:
        print(" ".join("; ".join(unreachable).split())[:MAX_DETAIL])
        return 1
    print(f"ok ({len(hosts)} hosts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
