"""The optimistic-concurrency header names that the control plane's routes and
its clients share."""

# The optimistic-concurrency header pair: every route that emits a version
# stamp writes ETAG_HEADER, and every write that gates on one reads
# IF_MATCH_HEADER. A caller round-trips the first into the second, so the two
# spellings are a contract rather than incidental strings.
ETAG_HEADER = "ETag"
IF_MATCH_HEADER = "If-Match"
