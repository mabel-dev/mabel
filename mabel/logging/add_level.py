"""
from: https://stackoverflow.com/a/35804945

#nodoc - don't add to the documentation wiki
"""

import logging
import atexit

logging_seen_warnings = []


def report_suppressions(message):
    """
    We suppress warnings from being repeatedly logged after they have been logged once.
    But, so users know we've been suppressing logs, before we end, tell the user how
    many times the log has been suppressed.
    """
    import mabel.logging

    mabel.logging.get_logger().warning(
        f"The following message was suppressed being logged {len(logging_seen_warnings)} additional time(s) - ({message})"
    )


def add_logging_level(level_name, level_num, method_name=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `level_name` becomes an attribute of the `logging` module with the value
    `level_num`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `method_name` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example:
        addLoggingLevel('TRACE', logging.DEBUG - 5)
        logging.getLogger(__name__).setLevel("TRACE")
        logging.getLogger(__name__).trace('that worked')
        logging.trace('so did this')
        logging.TRACE

    """
    if not method_name:
        method_name = level_name.lower()

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def log_for_level(self, message, *args, **kwargs):
        # if we've added the level,it doesn't format the message as JSON
        if isinstance(message, dict):
            import orjson as json

            message = json.dumps(message).decode("UTF8")
        if self.isEnabledFor(level_num):
            # supress duplicate warnings
            if level_num == 30:  # warnings
                if message in logging_seen_warnings:
                    return
                logging_seen_warnings.append(message)
                atexit.register(report_suppressions, message)
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, log_for_level)
    setattr(logging, method_name, log_to_root)
