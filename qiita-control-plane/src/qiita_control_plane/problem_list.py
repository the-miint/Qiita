"""Cap a list of problems for an error message."""


def capped(items: list[str], cap: int) -> list[str]:
    """`items`, or its first `cap` plus an "and N more" tail when longer."""
    if len(items) <= cap:
        return items
    return [*items[:cap], f"and {len(items) - cap} more"]
