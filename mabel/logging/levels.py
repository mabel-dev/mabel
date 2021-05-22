from enum import Enum
import logging


class LEVELS(int, Enum):
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

    DEBUG = int(logging.DEBUG)  # 10
    INFO = int(logging.INFO)  # 20
    WARNING = int(logging.WARNING)  # 30
    ERROR = int(logging.ERROR)  # 40
    AUDIT = 80
    ALERT = 90


LEVELS_TO_STRING = {
    LEVELS.DEBUG: "DEBUG",
    LEVELS.INFO: "INFO",
    LEVELS.WARNING: "WARNING",
    LEVELS.ERROR: "ERROR",
    LEVELS.AUDIT: "AUDIT",
    LEVELS.ALERT: "ALERT",
}
