import os
import logging
from functools import lru_cache
from .add_level import add_logging_level
from .log_formatter import LogFormatter

LOG_NAME: str = "MABEL"
LOG_FORMAT: str = '{BOLD_CYAN}%(name)s{OFF} | %(levelname)-8s | %(asctime)s | {GREEN}%(funcName)s(){OFF} | {YELLOW}%(filename)s{OFF}:{PURPLE}%(lineno)s{OFF} | %(message)s'

class LEVELS():

    DEBUG = int(logging.DEBUG)          # 10
    INFO = int(logging.INFO)            # 20
    WARNING = int(logging.WARNING)      # 30
    ERROR = int(logging.ERROR)          # 40
    AUDIT = 80
    ALERT = 90

    def __init__(self):
        """
        Proxy the Logging levels so we can just reference these without
        having to import Logging everywhere.

        LEVEL       | PURPOSE                                                        | Format
        ----------- | -------------------------------------------------------------- | ----------------------------------
        DEBUG       | Information for engineers to observe inner state and flow      | Format as desired
        INFO        | Information recording user and system events                   | Messages should be JSON formatted
        WARNING     | Undesirable but workable event has happened                    | Messages should be informative
        ERROR       | A problem has happened that stopped a minor part of the system | Messages should be instructive
        AUDIT       | Information relating to proving activities happened            | Messages should be JSON formatted
        ALERT       | Intervention is required                                       | Messages should be instructive
        """
        pass

def set_log_name(log_name: str):
    global LOG_NAME
    LOG_NAME = log_name
    get_logger.cache_clear()

@lru_cache(1)
def get_logger() -> logging.Logger:
    """
    Use Python's native logging - we created a named logger so we can make sure
    only the logs related to our jobs are included (other modules also use the
    Python's logging module).
    """
    logger = logging.getLogger(LOG_NAME)

    # default is to log WARNING and above
    logger.setLevel(int(os.environ.get('LOGGING_LEVEL', 25)))

    # add the TRACE, AUDIT and ALERT levels to the logger
    if not hasattr(logger, 'audit'):
        add_logging_level("AUDIT", LEVELS.AUDIT)
    if not hasattr(logging, 'alert'):
        add_logging_level("ALERT", LEVELS.ALERT)

    # override the existing handlers for these levels
    add_logging_level("DEBUG", LEVELS.DEBUG)
    add_logging_level("INFO", LEVELS.INFO)
    add_logging_level("WARNING", LEVELS.WARNING)
    add_logging_level("ERROR", LEVELS.ERROR)

    # configure the logger
    mabel_logging_handler = logging.StreamHandler()    
    formatter = LogFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%S%z"))
    mabel_logging_handler.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(mabel_logging_handler)

    return logger
