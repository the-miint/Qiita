"""Unit tests for the action_context host-path gate (`ingest_path`)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from qiita_control_plane.ingest_path import (
    IngestPathError,
    host_path_keys,
    named_host_paths,
    resolve_ingest_path,
)

# ---------------------------------------------------------------------------
# host_path_keys — which context_schema properties the gate covers
# ---------------------------------------------------------------------------


def test_host_path_keys_reads_string_pattern_properties():
    """A property is a host path iff it is `type: string` with `pattern: "^/"`.
    An integer, a differently-patterned string, and an array are all skipped."""
    keys = host_path_keys(
        {
            "properties": {
                "bcl_input_dir": {"type": "string", "pattern": "^/"},
                "fastq_path": {"type": "string", "minLength": 1, "pattern": "^/"},
                "instrument_model": {"type": "string", "minLength": 1},
                "host_rype_reference_idx": {"type": "integer"},
                "sample_map": {"type": "array"},
                "prefix": {"type": "string", "pattern": "^[a-z]+$"},
            }
        }
    )
    assert keys.paths == ("bcl_input_dir", "fastq_path")


def test_host_path_keys_reads_upload_handles():
    """Upload handles are keyed on the `_upload_idx` suffix, independent of the
    declared type — the runner rewrites them to `*_path` bindings."""
    keys = host_path_keys(
        {
            "properties": {
                "fasta_upload_idx": {"type": "integer", "minimum": 1},
                "tree_upload_idx": {"type": "integer", "minimum": 1},
                "shard_index": {"type": "boolean"},
            }
        }
    )
    assert keys.uploads == ("fasta_upload_idx", "tree_upload_idx")
    assert keys.paths == ()


@pytest.mark.parametrize("schema", [{}, {"type": "object"}, {"properties": None}])
def test_host_path_keys_tolerates_permissive_schema(schema):
    """The default `{}` context_schema (accept any object) declares neither
    kind of key, so the gate has nothing to enforce and must not raise."""
    keys = host_path_keys(schema)
    assert keys.paths == ()
    assert keys.uploads == ()


# ---------------------------------------------------------------------------
# named_host_paths — what the route actually checks
# ---------------------------------------------------------------------------


def test_named_host_paths_takes_schema_declared_keys():
    schema = {"properties": {"bcl_input_dir": {"type": "string", "pattern": "^/"}}}
    assert named_host_paths(schema, {"bcl_input_dir": "/sequencing/run", "n": 3}) == {
        "bcl_input_dir": "/sequencing/run"
    }


def test_named_host_paths_takes_suffix_named_keys_under_a_permissive_schema():
    """The `{}` schema declares nothing, so the naming convention is the only
    thing standing between a `*_path` value and an ungated submission."""
    assert named_host_paths({}, {"fastq_path": "/sequencing/a.fastq"}) == {
        "fastq_path": "/sequencing/a.fastq"
    }


def test_named_host_paths_skips_non_string_values():
    """A non-string is not a path. `validate_context` has already rejected one
    under a schema that pins the type; under a permissive schema it is simply
    not this gate's business."""
    assert named_host_paths({}, {"fastq_path": 123, "bam_path": None}) == {}


def test_named_host_paths_ignores_unrelated_keys():
    """A key that is neither schema-declared nor `*_path` / `*_dir` is left
    alone, however path-like its value looks — `instrument_model`, an
    `*_upload_idx`, a free-text description."""
    context = {
        "instrument_model": "Illumina NovaSeq X",
        "fasta_upload_idx": 7,
        "description": "/looks/like/a/path",
    }
    assert named_host_paths({}, context) == {}


def test_named_host_paths_unions_both_sources():
    """A schema-declared key whose name does not end in `_path` / `_dir`, and a
    suffix-named key the schema is silent about, are both returned."""
    schema = {"properties": {"manifest": {"type": "string", "pattern": "^/"}}}
    context = {"manifest": "/sequencing/m.tsv", "bam_path": "/sequencing/a.bam"}
    assert named_host_paths(schema, context) == context


# ---------------------------------------------------------------------------
# resolve_ingest_path — containment
# ---------------------------------------------------------------------------


def test_accepts_path_under_root(tmp_path):
    run = tmp_path / "runs" / "240101_M00001_0001_000000000-ABCDE"
    run.mkdir(parents=True)
    assert resolve_ingest_path(str(run), roots=(tmp_path,)) == run


def test_accepts_file_under_root(tmp_path):
    """A host-path key can name a file (`fastq_path`, `bam_path`), not only a
    run-folder directory."""
    fastq = tmp_path / "ABC_R1.fastq.gz"
    fastq.write_bytes(b"")
    assert resolve_ingest_path(str(fastq), roots=(tmp_path,)) == fastq


def test_accepts_path_under_any_of_several_roots(tmp_path):
    first = tmp_path / "sequencing"
    second = tmp_path / "other"
    (second / "run").mkdir(parents=True)
    first.mkdir()
    assert resolve_ingest_path(str(second / "run"), roots=(first, second)) == second / "run"


def test_normalizes_interior_dot_dot(tmp_path):
    """`os.path.normpath` collapses the value before the containment test, so a
    path that reaches its target through a parent hop is accepted as its
    normalized self rather than compared verbatim."""
    (tmp_path / "runs").mkdir()
    raw = str(tmp_path / "other" / ".." / "runs")
    assert resolve_ingest_path(raw, roots=(tmp_path,)) == tmp_path / "runs"


def test_rejects_relative_path(tmp_path):
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path("runs/240101", roots=(tmp_path,))
    assert "absolute" in exc.value.reason


def test_rejects_empty_path(tmp_path):
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path("", roots=(tmp_path,))
    assert "absolute" in exc.value.reason


def test_rejects_path_outside_every_root(tmp_path):
    outside = tmp_path.parent / "not-a-root"
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path(str(outside), roots=(tmp_path,))
    assert "outside every configured ingest root" in exc.value.reason


def test_rejects_dot_dot_escape(tmp_path):
    """The normalization runs BEFORE the containment test, so `<root>/../etc`
    is compared as `/etc` and refused — it does not sneak through on the
    strength of its `<root>/` prefix."""
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path(str(tmp_path / ".." / "etc"), roots=(tmp_path,))
    assert "outside every configured ingest root" in exc.value.reason


def test_rejects_empty_roots(tmp_path):
    """No configured root admits nothing. Settings.from_env refuses to boot
    without PATH_INGEST_ROOTS, so this shape only arises in a directly-
    constructed Settings — it must fail closed, not open."""
    run = tmp_path / "run"
    run.mkdir()
    with pytest.raises(IngestPathError):
        resolve_ingest_path(str(run), roots=())


def test_error_carries_path_and_roots(tmp_path):
    """The 422 body names the offending value and the roots, so the submitter
    can see what was expected without reading the deploy's env file."""
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path("/somewhere/else", roots=(tmp_path,))
    assert exc.value.path == "/somewhere/else"
    assert exc.value.roots == (tmp_path,)


# ---------------------------------------------------------------------------
# resolve_ingest_path — symlinks
# ---------------------------------------------------------------------------


def test_rejects_symlink_escaping_the_root(tmp_path):
    """A symlink under the root pointing out of it is refused: the lexical test
    passes, the resolved test catches it."""
    outside = tmp_path.parent / "outside-target"
    outside.mkdir(exist_ok=True)
    link = tmp_path / "escape"
    link.symlink_to(outside)
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path(str(link), roots=(tmp_path,))
    assert "resolves to" in exc.value.reason


def test_accepts_symlink_staying_inside_the_root(tmp_path):
    """A symlink that lands back inside the root is fine — the rule bounds
    where the bytes are, not how the path spells its way there."""
    (tmp_path / "real").mkdir()
    link = tmp_path / "alias"
    link.symlink_to(tmp_path / "real")
    assert resolve_ingest_path(str(link), roots=(tmp_path,)) == link


def test_accepts_path_when_the_root_itself_is_behind_a_symlink(tmp_path):
    """The resolved path is compared against the RESOLVED roots. A root reached
    through a symlink (`/var` -> `/private/var` on macOS, a linked mount on
    Linux) otherwise rejects every path beneath it."""
    real_root = tmp_path / "real-root"
    (real_root / "run").mkdir(parents=True)
    linked_root = tmp_path / "linked-root"
    linked_root.symlink_to(real_root)
    assert resolve_ingest_path(str(linked_root / "run"), roots=(linked_root,)) == (
        linked_root / "run"
    )


# ---------------------------------------------------------------------------
# resolve_ingest_path — existence
# ---------------------------------------------------------------------------


def test_rejects_missing_path(tmp_path):
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path(str(tmp_path / "typo"), roots=(tmp_path,))
    assert exc.value.reason == "host path does not exist"


def test_rejects_path_through_a_regular_file(tmp_path):
    """ENOTDIR has the same standing as ENOENT: a component of the path is a
    file, so the entry cannot exist."""
    (tmp_path / "a.fastq").write_bytes(b"")
    with pytest.raises(IngestPathError) as exc:
        resolve_ingest_path(str(tmp_path / "a.fastq" / "nested"), roots=(tmp_path,))
    assert exc.value.reason == "host path does not exist"


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses regardless of mode bits")
def test_admits_path_it_cannot_traverse(tmp_path):
    """The control plane (`qiita-api`) and SLURM steps (`qiita-job`) are
    different accounts with different group membership, so a directory the job
    can read may be one the control plane cannot stat. EACCES is 'cannot tell',
    not 'absent' — the submission is admitted and the step reports the truth.
    """
    closed = tmp_path / "closed"
    (closed / "run").mkdir(parents=True)
    closed.chmod(0o000)
    try:
        assert resolve_ingest_path(str(closed / "run"), roots=(tmp_path,)) == closed / "run"
    finally:
        closed.chmod(0o755)


def test_missing_beats_permission_when_the_parent_is_readable(tmp_path):
    """Sanity check on the pairing above: when the parent IS traversable, an
    absent entry is still definitively absent."""
    (tmp_path / "open").mkdir()
    with pytest.raises(IngestPathError):
        resolve_ingest_path(str(tmp_path / "open" / "nope"), roots=(tmp_path,))


# ---------------------------------------------------------------------------
# PATH_INGEST_ROOTS parsing
# ---------------------------------------------------------------------------


def _roots(raw: str) -> tuple[Path, ...]:
    from qiita_control_plane.config import _parse_ingest_roots

    return _parse_ingest_roots(raw)


def test_parse_roots_splits_on_colon():
    assert _roots("/sequencing:/data/runs") == (Path("/data/runs"), Path("/sequencing"))


def test_parse_roots_normalizes_and_dedupes():
    """A trailing slash and an interior `.` name the same root; both normalize
    to the form the containment test compares against."""
    assert _roots("/sequencing/:/sequencing:/sequencing/./") == (Path("/sequencing"),)


def test_parse_roots_ignores_empty_entries():
    assert _roots("/sequencing::") == (Path("/sequencing"),)


@pytest.mark.parametrize("raw", ["", ":", "::"])
def test_parse_roots_rejects_empty_list(raw):
    with pytest.raises(RuntimeError, match="at least one directory"):
        _roots(raw)


def test_parse_roots_rejects_relative_entry():
    with pytest.raises(RuntimeError, match="must be absolute"):
        _roots("/sequencing:relative/dir")


def test_parse_roots_rejects_filesystem_root():
    """`/` as a root admits every absolute path, which is the state the
    variable exists to end."""
    with pytest.raises(RuntimeError, match="may not contain"):
        _roots("/")
