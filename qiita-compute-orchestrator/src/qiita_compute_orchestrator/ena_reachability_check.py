"""Does this host have outbound HTTPS to the ENA archives?

Run as a module, from the interpreter under test:

    <SLURM_NATIVE_PYTHON> -P -m qiita_compute_orchestrator.ena_reachability_check

Exit 0, `ok (N hosts)` on stdout. Exit 1, a single line naming each host that
could not be reached and why. The caller names the check
(`ena-from-compute=fail err=...`), as it does for the miint probes, so this
prints the payload only.

Scope: stdlib `urllib` answers "does this host have outbound HTTPS to ENA", not
"can miint fetch from it". The jobs reach ENA through DuckDB httpfs
(`qiita_common.duckdb_miint`), whose own TLS and proxy configuration this cannot
see, so a green result rules out the blocked-egress case and nothing more. That
independence is the point on a compute node: no miint LOAD, no curl on the image.
"""

import urllib.error
import urllib.request

from .native_import_check import MAX_DETAIL

ENA_HOSTS = ("https://www.ebi.ac.uk", "https://ftp.sra.ebi.ac.uk")

_TIMEOUT_S = 10


def main(hosts: tuple[str, ...] = ENA_HOSTS) -> int:
    """HEAD each host; return 0 when every one answered, 1 otherwise."""
    unreachable = []
    for host in hosts:
        try:
            urllib.request.urlopen(  # noqa: S310 — fixed https:// constants
                urllib.request.Request(host, method="HEAD"), timeout=_TIMEOUT_S
            ).close()
        except urllib.error.HTTPError:
            # An HTTP status means the host answered, which is the whole question;
            # ENA's archive roots are under no obligation to serve 200 to a HEAD.
            continue
        except Exception as exc:
            unreachable.append(f"{host} ({type(exc).__name__}: {exc})")
    if unreachable:
        print(" ".join("; ".join(unreachable).split())[:MAX_DETAIL])
        return 1
    print(f"ok ({len(hosts)} hosts)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
