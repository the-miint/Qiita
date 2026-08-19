"""`python -m qiita_control_plane.cli.user` entry point.

Package execution runs `__main__`, never `__init__`, so the `-m` module form
(used by the integration smoke test) dispatches to `main` from here. The
`qiita` console script goes through `cli._bootstrap` instead.
"""

import sys

from . import main

if __name__ == "__main__":
    sys.exit(main())
