"""The jsonb round-trip check for a definition's params: the hash stored beside a
mask's, alignment's or processing's params must be the hash of what the row reads back.
"""

import json

import asyncpg
from qiita_common.hashing import canonical_params_hash

__all__ = ["ParamsDoNotSurviveStorageError", "assert_params_survive_storage"]


class ParamsDoNotSurviveStorageError(RuntimeError):
    """`params` contains a value whose jsonb round trip changes its canonical JSON.

    Postgres stores a JSON number as `numeric` and renders it back in plain decimal,
    so a float Python spells with an exponent can come back as an integer literal
    (1.5e30 → 1500000000000000000000000000000, which re-parses as an `int`), and -0.0
    comes back as 0.0. Either way `canonical_params_hash` of the row read back is not
    the digest stored beside it.

    The pool alignment listing publishes an alignment's `params_hash`, and the CLI
    recomputes it from the `params` beside it and refuses to build on a mismatch
    (`cli/user/alignment.py`), so a mismatch stored at a write surfaces as a refusal
    in a later build. A config needing a fractional value carries it as a string, or
    as a scaled integer.
    """


async def assert_params_survive_storage(
    conn: asyncpg.Connection, params: dict, params_hash: bytes
) -> None:
    """Refuse `params` whose canonical hash changes across a jsonb round trip.

    Asks Postgres to normalize the blob rather than predicting how it will: the rule is
    `numeric`'s output format. One extra statement per definition write.
    """
    stored = await conn.fetchval("SELECT ($1::jsonb)::text", json.dumps(params))
    if canonical_params_hash(json.loads(stored)) != params_hash:
        raise ParamsDoNotSurviveStorageError(
            f"these params do not survive jsonb storage unchanged: stored as "
            f"{stored!r}, which no longer hashes to the digest computed for them. See "
            f"ParamsDoNotSurviveStorageError."
        )
