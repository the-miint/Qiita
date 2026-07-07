"""qiita-admin CLI — shared direct-DB constants.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5
