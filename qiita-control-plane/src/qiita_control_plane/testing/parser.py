"""A stand-in for argparse.ArgumentParser in tests of code that reports through it."""


class RaisingParser:
    """Stand-in for argparse.ArgumentParser whose `error` raises instead of
    calling sys.exit, so tests can assert on the message."""

    class Error(Exception):
        pass

    def error(self, message: str):
        raise self.Error(message)
