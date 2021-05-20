
# google-cloud-logging
# pydantic
import os
import logging
import datetime
from typing import Union, Optional
from ..utils import is_running_from_ipython
try:
    from google.cloud import logging as stackdriver  # type:ignore
    from google.cloud.logging import DESCENDING  # type:ignore
except ImportError:
    stackdriver = None  # type:ignore


class GoogleLogger():

    @staticmethod
    def supported():
        use_logger = []
        use_logger.append(stackdriver is not None)
        use_logger.append(not is_running_from_ipython())
        use_logger.append(not os.environ.get('IGNORE_STACKDRIVER', False))
        return all(use_logger)

    @staticmethod
    def write_event(
            message: Union[str,dict],
            system: Optional[str] = None,
            severity: Optional[int] = logging.DEBUG):

        from .create_logger import LOG_NAME
        from .levels import LEVELS_TO_STRING

        print(f"{LOG_NAME}| {LEVELS_TO_STRING.get(severity, 'UNKNOWN')} | {datetime.datetime.now().isoformat()} | {message}")  # type:ignore

        client = stackdriver.Client()
        logger = client.logger(LOG_NAME)
        
        labels = {}
        if system:
            labels['system'] = system

        if isinstance(message, dict):
            logger.log_struct(
                    info=message,
                    severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                    labels=labels)
        else:
            logger.log_text(
                    text=F"{message}",
                    severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                    labels=labels)

    def __init__(self):
        from .levels import LEVELS

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
            GoogleLogger.write_event(
                message=message,
                system=LOG_NAME,
                severity=level)

        def do_nothing(message):
            pass

        if level > self.level:
            return base_logger
        else:
            return do_nothing

    def setLevel(self, level):
        self.level = level
        self.debug = self.create_logger(LEVELS.INFO)
        self.info = self.create_logger(LEVELS.INFO)
        self.warning = self.create_logger(LEVELS.WARNING)
        self.error = self.create_logger(LEVELS.ERROR)
        self.audit = self.create_logger(LEVELS.AUDIT)
        self.alert = self.create_logger(LEVELS.ALERT)

