"""
Test the mabel logger, this extends the Python logging logger.
We test that the trace method and decorators raise no errors.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.logging import LEVELS, get_logger, set_log_name
from rich import traceback

traceback.install()

LOG_NAME = "TEST_SCRIPTS"
set_log_name(LOG_NAME)

def alert():
  pass

def test_new_log_levels(caplog):
    """
    caplog is a feature of pytest that allows logs to be captured and 
    inspected.

    It is passed to a test function and has an attribute .record_tuples which
    is a stack of logging messages. To read the last item, pop it off the 
    stack. It is in the form of:

    log name, log level, log message
    """
    if caplog is None:
        print('unable to test logging interactively - use pytest')
        return

    logger = get_logger()

    logger.audit("this is a sample audit")
    assert caplog.record_tuples.pop() == (LOG_NAME, LEVELS.AUDIT, "this is a sample audit")

    logger.alert("this is a sample alert")
    assert caplog.record_tuples.pop() == (LOG_NAME, LEVELS.ALERT, "this is a sample alert")

  
def test_smoke_test():
    """
    This is just a smoke test, it exercises most of the logging functionality
    it should just work.
    """
    logger = get_logger()
    logger.setLevel(LEVELS.DEBUG)
    logger.debug("debug")
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
    logger.audit({"password": "top secret"})
    logger.audit('{"password": "top secret"}')
    logger.audit('password:topsecret')
    logger.audit(['password', 'top secret'])

def test_adding_logging_levels_fails_if_the_level_already_exists():

    from mabel.logging import add_level

    add_level.add_logging_level('test', 25)

    failed = False
    try:
        add_level.add_logging_level('test', 25)
    except:
        failed = True
    assert failed

    failed = False
    try:
        add_level.add_logging_level('none', 25, alert)
    except:
        failed = True
    assert failed

if __name__ == "__main__":  # pragma: no cover
    test_smoke_test()
    test_log_sanitizer()
    test_adding_logging_levels_fails_if_the_level_already_exists()
    test_new_log_levels(None)
