"""
Test the mabel logger, this extends the Python logging logger.
We test that the trace method and decorators raise no errors.
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.logging.google_cloud_logger import GoogleLogger

os.environ["IGNORE_STACKDRIVER"] = "true"


def test_google_logging():
    """
    This doesn't save to StackDriver, instead we're testing that this is handled
    gracefully and doesn't error
    """
    GoogleLogger().warning("Noooooo")


if __name__ == "__main__":  # pragma: no cover
    test_google_logging()
