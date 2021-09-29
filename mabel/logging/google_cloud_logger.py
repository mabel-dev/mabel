# google-cloud-logging
# pydantic

import os
import atexit
import logging
import datetime
import orjson as json
from typing import Union, Optional, Dict
from mabel.logging.levels import LEVELS, LEVELS_TO_STRING
from mabel.utils import is_running_from_ipython, safe_field_name
from mabel.logging.log_formatter import LogFormatter

try:
    from google.cloud import logging as stackdriver  # type:ignore
    from google.cloud.logging import DESCENDING  # type:ignore
except ImportError:
    stackdriver = None  # type:ignore


LOG_SINK = "MABEL"
logging_seen_warnings: Dict[int, int] = {}


def report_suppressions(message):
    import mabel.logging

    record = logging_seen_warnings.get(hash(message))
    if record:
        mabel.logging.get_logger().warning(
            f'The following message was suppressed {record} time(s) - "{message}"'
        )


def extract_caller():
    import traceback
    import os.path

    frames = traceback.extract_stack()
    if len(frames) < 4:
        return "<unknown>", "<unknown>", -1
    frame = frames[len(frames) - 4]
    head, tail = os.path.split(frame.filename)
    return frame.name, tail, frame.lineno


def fix_dict(obj: dict) -> dict:
    def fix_fields(dt):
        if isinstance(dt, (datetime.date, datetime.datetime)):
            return dt.isoformat()
        if isinstance(dt, dict):
            return {k: fix_fields(v) for k, v in dt.items()}
        return dt

    if not isinstance(obj, dict):
        return obj  # type:ignore
    return {k: fix_fields(v) for k, v in obj.items()}


class GoogleLogger(object):
    @staticmethod
    def supported():
        if not stackdriver:
            return False
        if os.environ.get("IGNORE_STACKDRIVER", False):
            return False
        if is_running_from_ipython():
            return False
        global LOG_SINK
        LOG_SINK = os.environ.get("LOG_SINK", LOG_SINK)
        return True

    @staticmethod
    def write_event(
        message: Union[str, dict],
        system: Optional[str] = None,
        severity: Optional[int] = logging.DEBUG,
    ):

        from .create_logger import LOG_NAME

        labels = {}
        if system:
            labels["system"] = system
        method, module, line = extract_caller()
        labels["method"] = method
        labels["module"] = module
        labels["line"] = str(line)

        payload = message
        if isinstance(message, dict):
            payload = json.dumps(message).decode("UTF8")

        # supress duplicate warnings
        if severity == LEVELS.WARNING:  # warnings
            hashed = hash(payload)
            if hashed in logging_seen_warnings:
                logging_seen_warnings[hashed] += 1
                return "suppressed"
            logging_seen_warnings[hashed] = 0
            atexit.register(report_suppressions, payload)

        log = f"{LOG_NAME} | {LEVELS_TO_STRING.get(severity, 'UNKNOWN')} | {datetime.datetime.now().isoformat()} | {method}() | {module}:{line} | {payload}"  # type:ignore
        formatter = LogFormatter(None, suppress_color=True)
        log = formatter.format(log)

        if os.environ.get("DUAL_LOG", False) or os.environ.get("IGNORE_STACKDRIVER"):
            print(log)

        if os.environ.get("IGNORE_STACKDRIVER"):
            return log

        client = stackdriver.Client()
        logger = client.logger(safe_field_name(LOG_SINK))

        if isinstance(message, dict):
            logger.log_struct(
                info=fix_dict(message),
                severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                labels=labels,
            )
        else:
            logger.log_text(
                text=f"{message}",
                severity=LEVELS_TO_STRING.get(severity),  # type:ignore
                labels=labels,
            )

        return message

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
            return GoogleLogger.write_event(
                message=message, system=LOG_NAME, severity=level
            )

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

    def __call__(self, message):
        self.debug(message)
