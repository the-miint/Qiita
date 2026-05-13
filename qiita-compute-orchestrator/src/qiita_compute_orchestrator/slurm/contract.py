"""Shared container/native-output contract.

These constants are the surface where the producer (the container's
own entrypoint, or `jobs/__main__.py` for native steps) and the
consumer (`slurm/verify.py`) meet. Keeping them in one module means a
change here forces both sides to update together.
"""

from __future__ import annotations

# Mode the data plane requires before it'll register a Parquet file.
# Owner-and-group read, no write, no other. Both verifier and launcher
# read this value rather than re-typing 0o440 — drift between them
# would make some valid outputs look like contract violations.
EXPECTED_FILE_MODE: int = 0o440

# Filename the producer writes inside $QIITA_OUTPUT_PATH (final act
# before chmod; its presence is the completion marker). The verifier
# reads it; the launcher writes it. Constant here so a rename touches
# both sites at once.
MANIFEST_FILENAME: str = "manifest.json"
