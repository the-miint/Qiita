"""qiita user CLI — the field-exception subcommands of `biosample` and
`sequenced-sample`: set and clear the exception that restricts one sample's
value of one study field. `get-field-exception` is a plain study-scoped read.
"""

import argparse
import sys

import httpx
from qiita_common.http_headers import ETAG_HEADER, IF_MATCH_HEADER
from qiita_common.models import SampleFieldExceptionPutRequest

from .. import _common
from ._helpers import _build_body


class _AlreadySet(Exception):
    """`set-field-exception` found a field exception stored and was not told to
    replace it."""

    def __init__(self, stored: dict) -> None:
        super().__init__("a field exception is already set")
        self.stored = stored


def _handle_field_exception_set(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Set the field exception on one sample's value of one study field.

    The body is validated before any request goes out (exit 2). The route
    creates only without If-Match and replaces only with it, so this reads
    first. With nothing stored it sends a bare PUT. With one stored, under
    --replace it sends the ETag that the read returned, and otherwise it stops
    (exit 1) and shows what is stored, so an exception that another admin set
    is never overwritten unseen. A 404 on the read is taken as nothing stored;
    when it meant a field or sample that the path does not reach, the PUT
    answers that 404 itself.
    """
    body = _build_body(SampleFieldExceptionPutRequest, args, parser)
    path = args.field_exception_path.format(**vars(args))

    def _set(token: str) -> dict:
        headers = None
        try:
            current = _common._request("GET", args.base_url, token, path)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
        else:
            if not args.replace:
                raise _AlreadySet(current.json())
            headers = {IF_MATCH_HEADER: current.headers[ETAG_HEADER]}
        return _common._request(
            "PUT", args.base_url, token, path, json=body, extra_headers=headers
        ).json()

    try:
        return _common.run_http_subcommand(_set)
    except _AlreadySet as exc:
        print(
            "error: a field exception is already set here"
            f" (tier_override={exc.stored['tier_override']},"
            f" reason={exc.stored['reason']!r}); pass --replace to replace it",
            file=sys.stderr,
        )
        return 1


def _handle_field_exception_clear(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Clear the field exception on one sample's value of one study field, and
    print the exception that it cleared, as the read before the delete
    returned it."""
    path = args.field_exception_path.format(**vars(args))

    def _clear(token: str) -> dict:
        read, _deleted = _common.write_with_read_etag("DELETE", args.base_url, token, path)
        return {"cleared": read.json()}

    return _common.run_http_subcommand(_clear)
