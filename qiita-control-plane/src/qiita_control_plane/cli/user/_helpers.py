"""qiita user CLI — shared body/arg helpers and generic read/patch handlers.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse

from pydantic import BaseModel, ValidationError

from .. import _common


def _build_body(
    model_cls: type[BaseModel],
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> dict:
    """Construct `model_cls` from the parsed-args fields that match its
    model_fields, then return the exclude_unset JSON dump.

    Filters None out of the namespace before construction so the only
    fields Pydantic treats as "set" are the ones the caller actually
    passed (matches the server's exclude_unset semantics on the PATCH
    side; honest with the schema on the POST side). Argparse's dest
    names line up with the on-the-wire key (snake_case from hyphenated
    flags) — a field's alias where it has one, else its field name — so
    the filter is a single comprehension, and the dump emits the same
    keys the server validates.

    On ValidationError (e.g. a too-long --title, malformed --orcid),
    flattens the errors into a single stderr line and exits 2 via
    parser.error — same code path as argparse's own validation
    failures, so callers don't see a Python traceback for invalid
    input.
    """
    wire_keys = [field.alias or name for name, field in model_cls.model_fields.items()]
    fields = {key: getattr(args, key) for key in wire_keys if getattr(args, key, None) is not None}
    try:
        return model_cls(**fields).model_dump(exclude_unset=True, mode="json", by_alias=True)
    except ValidationError as exc:
        msgs = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
        parser.error(f"invalid {model_cls.__name__}: {msgs}")


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _lane_arg(raw: str) -> int | None:
    """argparse `type` for a lane value: a positive integer, or one of
    'none'/'null'/'' for a NULL lane (a real, distinct value to update_lane).

    Returning None lets the caller pass an explicit NULL lane on the command
    line; the flag is still `required` so 'omitted' and 'NULL' never collide."""
    if raw.strip().lower() in ("none", "null", ""):
        return None
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"lane must be a positive integer or 'none', got {raw!r}")
    if value < 1:
        raise argparse.ArgumentTypeError(
            f"lane must be >= 1 (or 'none' for a NULL lane), got {value}"
        )
    return value


def _proportion_arg(raw: str) -> float:
    """argparse `type` for a proportion in [0, 1] — a coverage breadth, a sequence
    identity, a query coverage.

    Rejected at parse time (exit 2) rather than after the run has streamed a cohort's
    worth of alignment data, which is what a downstream check would cost.
    """
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"must be a proportion in [0, 1], got {raw!r}") from None
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError(f"must be a proportion in [0, 1], got {value}")
    return value


def _handle_read(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Fetch a resource by idx (GET) and print its JSON body.

    The per-command `set_defaults` supplies `read_path` (a subpath
    template) and `read_idx_arg` (the namespace attr whose value fills
    the template), so the path formats from exactly one identifier.
    """
    idx_arg = args.read_idx_arg
    path = args.read_path.format(**{idx_arg: getattr(args, idx_arg)})
    return _common.run_http_subcommand(lambda t: _common.call("GET", args.base_url, t, path))


def _handle_study_field_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a study-local field definition on one study (POST).

    The per-command `set_defaults` supplies `study_field_model` (the request
    model, whose mode coupling is enforced at body construction so an invalid
    flag combination exits 2 without a request) and `study_field_path` (a
    subpath template filled from --study-idx).
    """

    def _run(token: str) -> dict:
        body = _build_body(args.study_field_model, args, parser)
        path = args.study_field_path.format(study_idx=args.study_idx)
        return _common.call("POST", args.base_url, token, path, json=body)

    return _common.run_http_subcommand(_run)


def _handle_patch(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Apply a partial update to a resource under optimistic concurrency.

    The per-command `set_defaults` supplies `patch_model` (the
    PatchRequestModel subclass the flags map to), `patch_path` (a subpath
    template), `patch_idx_arg` (the namespace attr that fills it), and
    `patch_json_fields` (flags parsed from JSON before validation). An
    empty update (no field flags) fails the model's at-least-one-field
    rule and exits 2.
    """
    for field in args.patch_json_fields:
        setattr(
            args,
            field,
            _common.parse_json_arg(
                getattr(args, field), parser, flag=f"--{field.replace('_', '-')}"
            ),
        )
    body = _build_body(args.patch_model, args, parser)
    idx_arg = args.patch_idx_arg
    path = args.patch_path.format(**{idx_arg: getattr(args, idx_arg)})
    return _common.run_http_subcommand(
        lambda t: _common.patch_with_if_match(args.base_url, t, path, body)
    )
