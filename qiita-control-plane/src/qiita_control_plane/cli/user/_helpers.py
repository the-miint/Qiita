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
    names line up with the Pydantic field names (snake_case from
    hyphenated flags), so the filter is a single comprehension.

    On ValidationError (e.g. a too-long --title, malformed --orcid),
    flattens the errors into a single stderr line and exits 2 via
    parser.error — same code path as argparse's own validation
    failures, so callers don't see a Python traceback for invalid
    input.
    """
    fields = {
        name: getattr(args, name)
        for name in model_cls.model_fields
        if getattr(args, name, None) is not None
    }
    try:
        return model_cls(**fields).model_dump(exclude_unset=True, mode="json")
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


def _handle_read(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Fetch a resource by idx (GET) and print its JSON body.

    The per-command `set_defaults` supplies `read_path` (a subpath
    template) and `read_idx_arg` (the namespace attr whose value fills
    the template), so the path formats from exactly one identifier.
    """
    idx_arg = args.read_idx_arg
    path = args.read_path.format(**{idx_arg: getattr(args, idx_arg)})
    return _common.run_http_subcommand(lambda t: _common.call("GET", args.base_url, t, path))


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
