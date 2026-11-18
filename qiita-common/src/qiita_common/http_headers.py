"""The optimistic-concurrency header pair that the control plane's routes and
its clients share. Every route that emits a version stamp writes ETAG_HEADER,
and every write that gates on one reads IF_MATCH_HEADER, so a caller
round-trips the first into the second.
"""

ETAG_HEADER = "ETag"
IF_MATCH_HEADER = "If-Match"
