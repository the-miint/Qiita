"""Comparing two phylogenies as a placement sees them.

Both tables have `read_newick`'s row shape (`node_index`, `parent_index`,
`edge_id`, `branch_length`, `is_tip`, `name`). A placement names an edge by its
number, so two trees are the same tree for placement when every node has the same
edge number, the same parent edge number, the same branch length and, at a tip, the
same name. Internal labels and the order of a node's children are not compared; a
placement's meaning does not depend on them.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

_SHAPE = (
    "SELECT c.edge_id, p.edge_id AS parent_edge_id, {length},"
    " CASE WHEN c.is_tip THEN c.name END AS tip"
    " FROM {t} c LEFT JOIN {t} p ON c.parent_index = p.node_index"
)


@dataclass(frozen=True)
class TreeDifference:
    """`missing`: how many of `expected`'s nodes have no node in `actual` with the
    same shape; `examples`: up to five of them, described; the two node counts."""

    missing: int
    examples: list[str]
    expected_nodes: int
    actual_nodes: int

    @property
    def same(self) -> bool:
        return self.missing == 0 and self.expected_nodes == self.actual_nodes


def tree_difference(
    conn: duckdb.DuckDBPyConnection,
    expected: str,
    actual: str,
    *,
    compare_lengths: bool = True,
) -> TreeDifference:
    """Compare the trees in tables `expected` and `actual`. With equal node counts, no
    missing node means the two multisets of node shapes are equal. `compare_lengths`
    False leaves branch lengths out, for a tree whose lengths are not the placement's
    (JEST's `labelled.nwk` is read for its edge numbers)."""
    length = "c.branch_length" if compare_lengths else "NULL::DOUBLE AS branch_length"
    diff = (
        f"{_SHAPE.format(t=expected, length=length)}"
        f" EXCEPT ALL {_SHAPE.format(t=actual, length=length)}"
    )
    describe = (
        "SELECT 'edge ' || coalesce(CAST(edge_id AS VARCHAR), 'none')"
        " || ' under edge ' || coalesce(CAST(parent_edge_id AS VARCHAR), 'none')"
        " || ', length ' || coalesce(CAST(branch_length AS VARCHAR), 'none')"
        " || coalesce(', tip ' || tip, '')"
        f" FROM ({diff}) ORDER BY edge_id NULLS FIRST, parent_edge_id NULLS FIRST, tip NULLS FIRST"
    )
    (missing,) = conn.execute(f"SELECT count(*) FROM ({diff})").fetchone()
    examples = [r[0] for r in conn.execute(f"{describe} LIMIT 5").fetchall()] if missing else []
    expected_nodes, actual_nodes = conn.execute(
        f"SELECT (SELECT count(*) FROM {expected}), (SELECT count(*) FROM {actual})"
    ).fetchone()
    return TreeDifference(missing, examples, expected_nodes, actual_nodes)
