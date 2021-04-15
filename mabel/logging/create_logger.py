import os
import logging
from functools import lru_cache
from .add_level import add_logging_level
from .sanitizing_log_formatter import SanitizingLogFormatter

LOG_NAME: str = "MABEL"
LOG_FORMAT: str = '%(name)s | %(levelname)-8s | %(asctime)s | %(filename)s:%(lineno)s:%(funcName)s() | %(message)s'

class LEVELS():
    """
    Proxy the Logging levels so we can just reference these without
    having to import Logging everywhere.

    DEBUG       | Information for engineers to observe inner state and flow
    INFO        | Information recording user and system events
    WARNING     | Undesirable but workable event has happened
    ERROR       | A problem has happened that stopped a minor part of the system
    AUDIT       | Information relating to proving activities happened
    ALERT       | Intervention is required
    """
    DEBUG = int(logging.DEBUG)          # 10
    INFO = int(logging.INFO)            # 20
    WARNING = int(logging.WARNING)      # 30
    ERROR = int(logging.ERROR)          # 40
    AUDIT = 80
    ALERT = 90

def set_log_name(log_name: str):
    global LOG_NAME
    LOG_NAME = log_name

@lru_cache(1)
def get_logger() -> logging.Logger:
    """
    Use Python's native logging - we created a named logger so we can make sure
    only the logs related to our jobs are included (other modules also use the
    Python's logging module).
    """
    logger = logging.getLogger(LOG_NAME)
    try:
        # default is to log WARNING and above
        logger.setLevel(int(os.environ.get('LOGGING_LEVEL', 25)))
    except:  # nosec
        pass # nosec

    # add the TRACE, AUDIT and ALERT levels to the logger
    add_logging_level("AUDIT", LEVELS.AUDIT)
    add_logging_level("ALERT", LEVELS.ALERT)

    # configure the logger
    mabel_logging_handler = logging.StreamHandler()    
    formatter = SanitizingLogFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z"))
    mabel_logging_handler.setFormatter(formatter)
    logger.addHandler(mabel_logging_handler)

    return logger
