# google-cloud-logging
# pydantic
import os
import logging
import datetime
from typing import Union, Optional
from .levels import LEVELS, LEVELS_TO_STRING
from ..utils import is_running_from_ipython

try:
    from google.cloud import logging as stackdriver  # type:ignore
    from google.cloud.logging import DESCENDING  # type:ignore
except ImportError:
    stackdriver = None  # type:ignore


def extract_caller():
    import traceback
    import os.path
    frames = traceback.extract_stack()
    #if len(frames) < 3:
    #    return "", ""
    #frame = frames[len(frames) - 3]
    for i, frame in enumerate(frames):
        head, tail = os.path.split(frame.filename)
        print("GCP LOG STACK", i, tail, frame.name, frame.lineno)
    return frame.name, f"{tail}():{frame.lineno}"

class GoogleLogger:

    @staticmethod
    def safe_field_name(field_name):
        """strip all the non-alphanums from a field name"""
        import re

        pattern = re.compile("[^a-zA-Z0-9]+")
        return pattern.sub("", field_name)

    @staticmethod
    def supported():
        use_logger = []
        use_logger.append(stackdriver is not None)
        use_logger.append(not is_running_from_ipython())
        use_logger.append(not os.environ.get("IGNORE_STACKDRIVER", False))
        return all(use_logger)

    @staticmethod
    def write_event(
        message: Union[str, dict],
        system: Optional[str] = None,
        severity: Optional[int] = logging.DEBUG,
    ):

        from .create_logger import LOG_NAME

        client = stackdriver.Client()
        logger = client.logger(GoogleLogger.safe_field_name(LOG_NAME))

        labels = {}
        if system:
            labels["system"] = system
        method, location = extract_caller()
        labels["method"] = method
        labels["location"] = location

        if os.environ.get("DUAL_LOG", False):
            print(
                f"{LOG_NAME} | {LEVELS_TO_STRING.get(severity, 'UNKNOWN')} | {datetime.datetime.now().isoformat()} | {method} | {location} | {message}"    # type:ignore
            )

        if isinstance(message, dict):
            logger.log_struct(
                info=message,
                severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                labels=labels,
            )
        else:
            logger.log_text(
                text=f"{message}",
                severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                labels=labels,
            )

    def __init__(self):
        # rewrite one of the levels
        LEVELS_TO_STRING[LEVELS.AUDIT] = "NOTICE"

        self.level = int(os.environ.get("LOGGING_LEVEL", 25))
        self.debug = self.create_logger(LEVELS.DEBUG)
        self.info = self.create_logger(LEVELS.INFO)
        self.warning = self.create_logger(LEVELS.WARNING)
        self.error = self.create_logger(LEVELS.ERROR)
        self.audit = self.create_logger(LEVELS.AUDIT)
        self.alert = self.create_logger(LEVELS.ALERT)

    def create_logger(self, level):
        from .create_logger import LOG_NAME

        def base_logger(message):
            GoogleLogger.write_event(message=message, system=LOG_NAME, severity=level)

        def do_nothing(message):
            pass

        if level > self.level:
            return base_logger
        else:
            return do_nothing

    def setLevel(self, level):
        self.level = level
        self.debug = self.create_logger(LEVELS.DEBUG)
        self.info = self.create_logger(LEVELS.INFO)
        self.warning = self.create_logger(LEVELS.WARNING)
        self.error = self.create_logger(LEVELS.ERROR)
        self.audit = self.create_logger(LEVELS.AUDIT)
        self.alert = self.create_logger(LEVELS.ALERT)
