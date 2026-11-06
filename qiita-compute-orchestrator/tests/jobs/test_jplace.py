"""Contract tests for `jobs._jplace.split_jplace`.

The split stands between a jplace and miint's `read_jplace`, so it has two jobs: carry
every placement through, byte for byte, into parts small enough to read; and refuse
every placement the macro would load as something other than what it says. Neither
needs miint, so these run without it; the read through `read_jplace` is tested where
`reference_load` makes it.
"""

from __future__ import annotations

import json

import pytest

from qiita_compute_orchestrator.jobs import _jplace
from qiita_compute_orchestrator.jobs._jplace import EXPECTED_FIELDS, split_jplace


def _placement(name: str, edge: int) -> dict:
    return {"p": [[edge, -1.5, 0.75, 0.001, 0.02]], "n": [name]}


_PLACEMENTS = [_placement(f"frag{i}", i * 3) for i in range(40)]
_TREE = "((frag0:0.1{0},frag1:0.2{1}):0.3{2});"


def _write(tmp_path, doc, *, raw: str | None = None):
    path = tmp_path / "in.jplace"
    path.write_text(raw if raw is not None else json.dumps(doc))
    return path


def _doc(placements=None, **overrides):
    doc = {
        "tree": _TREE,
        "placements": _PLACEMENTS if placements is None else placements,
        "metadata": {"invocation": "test"},
        "version": 3,
        "fields": list(EXPECTED_FIELDS),
    }
    doc.update(overrides)
    return doc


def _split(source, out_dir, cap=1 << 20):
    """Run the split, reading each part back as it is yielded (the split's caller
    deletes each part before taking the next, so a test must read it then too)."""
    parts = []
    tree_out = out_dir.with_name("tree.nwk")
    for path, count in split_jplace(source, out_dir, part_max_bytes=cap, tree_out=tree_out):
        data = path.read_bytes()
        parts.append((len(data), count, json.loads(data)))
    return parts


def test_one_part_carries_every_placement_when_the_file_fits(tmp_path):
    parts = _split(_write(tmp_path, _doc()), tmp_path / "parts")
    assert len(parts) == 1
    _, count, part = parts[0]
    assert part == {"placements": _PLACEMENTS}
    assert count == len(_PLACEMENTS)


def test_a_small_cap_splits_without_losing_or_reordering_a_placement(tmp_path):
    cap = 300
    parts = _split(_write(tmp_path, _doc()), tmp_path / "parts", cap=cap)
    assert len(parts) > 5
    assert all(size <= cap for size, _, _ in parts)
    assert all(count == len(part["placements"]) for _, count, part in parts)
    assert [p for _, _, part in parts for p in part["placements"]] == _PLACEMENTS


def test_placement_text_is_copied_through_unchanged(tmp_path):
    """The split frames placements; it does not re-serialize them, so a float's
    spelling reaches miint as the producer wrote it."""
    raw = (
        '{"fields": ["edge_num", "likelihood", "like_weight_ratio", "distal_length",'
        ' "pendant_length"], "placements": [{"n": ["a"], "p": [[7, -1.50e+02, 1.0, 0.0, 1e-5]]}],'
        ' "tree": "(a:1{7});", "version": 3}'
    )
    path = _write(tmp_path, None, raw=raw)
    [(part_path, _)] = list(
        split_jplace(
            path, tmp_path / "parts", part_max_bytes=1 << 20, tree_out=tmp_path / "tree.nwk"
        )
    )
    assert "[[7, -1.50e+02, 1.0, 0.0, 1e-5]]" in part_path.read_text()


@pytest.mark.parametrize(
    "order",
    [
        ("fields", "metadata", "placements", "tree", "version"),
        # pplacer's order: `fields` after `placements`.
        ("tree", "placements", "metadata", "version", "fields"),
    ],
)
def test_keys_in_either_order_carry_placements_and_tree(tmp_path, order):
    doc = _doc()
    parts = _split(_write(tmp_path, {k: doc[k] for k in order}), tmp_path / "parts")
    assert [p for _, _, part in parts for p in part["placements"]] == _PLACEMENTS
    assert (tmp_path / "tree.nwk").read_text() == _TREE


def test_the_tree_is_the_top_level_key_decoded(tmp_path):
    """A `"tree"` key in `metadata` and a fragment named `tree`, both ahead of the tree,
    and a tip name `json.dumps` writes as a `\\u` escape: the cases duckdb-miint#413
    gets wrong."""
    tree = "((Ésp:0.1{0},tree:0.2{1}):0.3{2});"
    doc = {
        "metadata": {"tree": "(decoy:1{0});"},
        "placements": [_placement("tree", 1)],
        "fields": list(EXPECTED_FIELDS),
        "version": 3,
        "tree": tree,
    }
    path = _write(tmp_path, doc)
    assert "\\u00c9sp" in path.read_text()
    _split(path, tmp_path / "parts")
    assert (tmp_path / "tree.nwk").read_text(encoding="utf-8") == tree


def test_buffer_refills_mid_value(tmp_path, monkeypatch):
    """A refill can land inside any value — a placement, a key, the tree string.
    Reading a few characters at a time puts one in all of them."""
    monkeypatch.setattr(_jplace, "_READ_CHARS", 7)
    path = _write(tmp_path, None, raw=json.dumps(_doc(), indent=2))
    parts = _split(path, tmp_path / "parts")
    assert [p for _, _, part in parts for p in part["placements"]] == _PLACEMENTS
    assert (tmp_path / "tree.nwk").read_text() == _TREE


@pytest.mark.parametrize(
    ("number", "first_read"),
    [
        ("1234567", '{"version": 1234'),
        ("3.5", '{"version": 3.'),
        ("1e5", '{"version": 1e'),
        ("2E-3", '{"version": 2E-'),
    ],
)
def test_a_number_cut_by_the_buffer_end_is_read_whole(tmp_path, monkeypatch, number, first_read):
    """The first read ends partway through a top-level number: within its digits, or
    just after its "." or an unfinished exponent."""
    raw = '{"version": ' + number + ', "fields": ' + json.dumps(list(EXPECTED_FIELDS))
    raw += ', "placements": ' + json.dumps(_PLACEMENTS) + ', "tree": ' + json.dumps(_TREE) + "}"
    assert raw.startswith(first_read)
    monkeypatch.setattr(_jplace, "_READ_CHARS", len(first_read))
    parts = _split(_write(tmp_path, None, raw=raw), tmp_path / "parts")
    assert [p for _, _, part in parts for p in part["placements"]] == _PLACEMENTS


def test_nm_with_one_name_of_multiplicity_one_is_accepted(tmp_path):
    placement = {"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["seq1", 1]]}
    parts = _split(_write(tmp_path, _doc([placement])), tmp_path / "parts")
    assert parts[0][2]["placements"] == [placement]


def test_empty_placements_yield_no_part(tmp_path):
    assert _split(_write(tmp_path, _doc([])), tmp_path / "parts") == []


@pytest.mark.parametrize(
    ("placement", "match"),
    [
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "n": ["a", "b"]}, "exactly one fragment in 'n'"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["a", 1], ["b", 1]]}, "multiplicity 1"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["a", 3]]}, "multiplicity 1"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["a", True]]}, "multiplicity 1"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "nm": [["a", 1.0]]}, "multiplicity 1"),
        (
            {"p": [[5, -1.0, 0.6, 0.01, 0.02], [6, -2.0, 0.4, 0.01, 0.02]], "n": ["a"]},
            "exactly one location",
        ),
        ({"p": [], "n": ["a"]}, "exactly one location"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "n": ["a"], "nm": [["a", 1]]}, "exactly one of"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]]}, "exactly one of"),
        ({"p": [[5, -1.0, 0.9, 0.01, 0.02]], "n": ["a"], "m": 1}, "carries keys"),
        ({"p": [[5, -1.0, 0.9, 0.01]], "n": ["a"]}, "not 5 values"),
        ({"p": [[5.5, -1.0, 0.9, 0.01, 0.02]], "n": ["a"]}, "not an integer"),
        ({"p": [[None, -1.0, 0.9, 0.01, 0.02]], "n": ["a"]}, "not an integer"),
    ],
)
def test_a_placement_read_jplace_would_truncate_is_refused(tmp_path, placement, match):
    path = _write(tmp_path, _doc([_placement("ok", 1), placement]))
    with pytest.raises(ValueError, match=match) as exc:
        _split(path, tmp_path / "parts")
    assert "placement 1" in str(exc.value)


@pytest.mark.parametrize(
    "fields",
    [
        ["distal_length", "edge_num", "like_weight_ratio", "likelihood", "pendant_length"],
        ["edge_num", "likelihood", "like_weight_ratio", "distal_length"],
        [*EXPECTED_FIELDS, "marginal_like"],
    ],
)
@pytest.mark.parametrize("fields_last", [False, True])
def test_fields_read_jplace_would_misread_are_refused(tmp_path, fields, fields_last):
    doc = _doc(fields=fields)
    if fields_last:
        doc = {k: doc[k] for k in ("tree", "placements", "metadata", "version", "fields")}
    with pytest.raises(ValueError, match="jplace fields are"):
        _split(_write(tmp_path, doc), tmp_path / "parts")


@pytest.mark.parametrize("key", ["fields", "placements", "tree"])
def test_a_missing_required_key_is_refused(tmp_path, key):
    doc = _doc()
    del doc[key]
    with pytest.raises(ValueError, match=f"no '{key}'"):
        _split(_write(tmp_path, doc), tmp_path / "parts")


def test_a_tree_that_is_not_a_string_is_refused(tmp_path):
    with pytest.raises(ValueError, match="not a Newick string"):
        _split(_write(tmp_path, _doc(tree={"newick": _TREE})), tmp_path / "parts")


# A part's framing around its placements: `{"placements": [` and `]}`.
_FRAMING = len(b'{"placements": [') + len(b"]}")


def test_a_placement_that_exactly_fills_a_part_is_accepted(tmp_path):
    one = len(json.dumps(_PLACEMENTS[0]).encode())
    [(size, count, _)] = _split(
        _write(tmp_path, _doc(_PLACEMENTS[:1])), tmp_path / "parts", cap=_FRAMING + one
    )
    assert (size, count) == (_FRAMING + one, 1)


def test_a_placement_larger_than_a_part_is_refused(tmp_path):
    one = len(json.dumps(_PLACEMENTS[0]).encode())
    with pytest.raises(ValueError, match="more than a part may hold"):
        _split(_write(tmp_path, _doc(_PLACEMENTS[:1])), tmp_path / "parts", cap=_FRAMING + one - 1)


@pytest.mark.parametrize(("slack", "parts"), [(0, 1), (-1, 2)])
def test_a_part_closes_at_exactly_its_cap(tmp_path, slack, parts):
    """Two placements of equal length and the comma between them: a cap of exactly
    that holds both, one byte less needs a second part."""
    two = [_placement("a", 10), _placement("b", 11)]
    one = len(json.dumps(two[0]).encode())
    assert one == len(json.dumps(two[1]).encode())
    split = _split(
        _write(tmp_path, _doc(two)), tmp_path / "parts", cap=_FRAMING + 2 * one + 1 + slack
    )
    assert len(split) == parts
    assert [p for _, _, part in split for p in part["placements"]] == two


_FIELDS_JSON = json.dumps(list(EXPECTED_FIELDS))


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        (f'{{"fields": {_FIELDS_JSON}, "placements": [{{"n": ["a"], "p": [[1, 2', "malformed"),
        (f'{{"fields": {_FIELDS_JSON}, "placements": [{{"n": ["a"] "p": []}}]}}', "malformed"),
        (f'{{"fields": {_FIELDS_JSON}, "placements": []}} trailing', "after the jplace"),
        (f'{{"fields": {_FIELDS_JSON}, "placements": [], "placements": []}}', "repeats the key"),
        (f'[{{"fields": {_FIELDS_JSON}, "placements": []}}]', "expected '{'"),
    ],
)
def test_malformed_json_is_refused(tmp_path, raw, match):
    with pytest.raises(ValueError, match=match):
        _split(_write(tmp_path, None, raw=raw), tmp_path / "parts")
