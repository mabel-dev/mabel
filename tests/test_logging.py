"""
Test the mabel logger, this extends the Python logging logger.
We test that the trace method and decorators raise no errors.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from orso.logging import LEVELS, get_logger, set_log_name
from rich import traceback

traceback.install()

os.environ["PROJECT_NAME"] = ""

LOG_NAME = "TEST_SCRIPTS"
set_log_name(LOG_NAME)


def test_new_log_levels(caplog=None):
    """
    caplog is a feature of pytest that allows logs to be captured and
    inspected.

    It is passed to a test function and has an attribute .record_tuples which
    is a stack of logging messages. To read the last item, pop it off the
    stack. It is in the form of:

    log name, log level, log message
    """
    if caplog is None:  # pragma: no cover
        print("unable to test logging interactively - use pytest")
        return

    logger = get_logger()

    logger.audit("this is a sample audit")
    res = caplog.record_tuples.pop()
    assert res == (LOG_NAME, LEVELS.AUDIT, "this is a sample audit"), res

    logger.alert("this is a sample alert")
    res = caplog.record_tuples.pop()
    assert res == (LOG_NAME, LEVELS.ALERT, "this is a sample alert"), res


def test_smoke_test():
    """
    This is just a smoke test, it exercises most of the logging functionality
    it should just work.
    """
    logger = get_logger()
    logger.setLevel(LEVELS.DEBUG)
    logger.debug("debug")
    logger.warning("warn")
    logger.warning("warn")
    logger.warning("warn")
    logger.warning("warn")
    logger.warning("warned")
    logger.warning("warn")
    logger.warning("warn")
    logger.warning("warned")
    logger.warning("warn")
    logger.warning("warn")
    logger.warning("warn")
    logger.error("error")
    logger.alert("alert")
    logger.audit("audit")


def test_log_sanitizer():
    """
    caplog records the message before the formatter so can't be used to
    test sanitization of logs automatically.

    These are another set of smoke tests, these should run without
    error.
    """
    logger = get_logger()
    logger.audit({"password": "top secret 1"})
    logger.audit('{"password": "top secret 2"}')
    logger.audit("password:topsecret3")
    logger.audit(["password", "top secret 4"])
    logger.debug({"alpha": "`1`", "beta": "`2`", "gamma": "'3'", "delta": "'4'"})
    logger.debug("{'action':'list','limit':'1000'}")


if __name__ == "__main__":  # pragma: no cover
    from tests.helpers.runner import run_tests

    run_tests()
