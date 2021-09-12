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
    This doesn't save to StackDriver - we're mainly ensuring that the redaction and suppression works
    """
    assert "Noooooo" in GoogleLogger().warning("Noooooo")
    assert "suppressed" == GoogleLogger().warning("Noooooo")
    assert "Yess!!" in GoogleLogger().warning("Yess!!")
    assert "suppressed" in GoogleLogger().warning("Yess!!")
    assert "suppressed" == GoogleLogger().warning("Noooooo")
    redacted = GoogleLogger().warning({"password": "sauce"})
    assert "password" in redacted
    assert "sauce" not in redacted


if __name__ == "__main__":  # pragma: no cover
    test_google_logging()