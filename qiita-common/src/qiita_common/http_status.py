"""HTTP status and transport-error classification shared across components."""

# Substrings marking an error as transport/network-shaped (vs. format/parse).
_TRANSIENT_ERROR_MARKERS = (
    "connection",
    "timed out",
    "timeout",
    "network",
    "reset",
    "refused",
    "unreachable",
    "temporarily",
    "curl",
    "could not connect",
)


def is_transient_http_status(status: int) -> bool:
    """True for a status a retry can self-heal: any 5xx, 408 or 429."""
    return status >= 500 or status in (408, 429)


def is_transient_error_text(text: str) -> bool:
    """True when an error's message names a transport failure a retry can self-heal."""
    text = text.lower()
    return any(marker in text for marker in _TRANSIENT_ERROR_MARKERS)
