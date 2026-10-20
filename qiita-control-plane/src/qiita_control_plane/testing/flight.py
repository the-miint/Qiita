"""Test support for the control plane's data-plane Flight calls: how a busy refusal
reaches pyarrow, and a Flight client stand-in that raises it."""

# How a busy data plane reaches this layer. The data plane refuses a request it has
# no free DuckLake connection for with gRPC RESOURCE_EXHAUSTED; this string is what
# a pyarrow client raised for that status in a probe (pyarrow 23.0.1, grpcio 1.80.0,
# a generic gRPC server answering RESOURCE_EXHAUSTED, with gRPC UNAVAILABLE as the
# control). pyarrow surfaces this status as `ArrowInvalid`, a ValueError rather than
# a FlightError.
REAL_BUSY_REFUSAL = (
    "gRPC returned resource exhausted error, with message: data plane busy: all 8 "
    "DuckLake connections are in use; retry. gRPC client debug context: "
    "UNKNOWN:Error received from peer ipv4:127.0.0.1:50051 {grpc_status:8, "
    'grpc_message:"data plane busy: all 8 DuckLake connections are in use; retry"}. '
    "Client context: IOError: Server never sent a data message."
)


def flight_client_raising(exc: BaseException) -> type:
    """A `pyarrow.flight.FlightClient` stand-in whose `do_action` raises `exc`, for
    patching below the action library so its own error handling runs."""

    class _Client:
        def __init__(self, url):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def do_action(self, action, options=None):
            raise exc

    return _Client
