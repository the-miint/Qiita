"""`python -m qiita_control_plane.cli.admin` entry point.

Package execution runs `__main__`, never `__init__`, so the `-m` module form
dispatches to `main` from here. The `qiita-admin` console script goes through
`cli._bootstrap` instead.
"""

import sys

from . import main

if __name__ == "__main__":
    sys.exit(main())
