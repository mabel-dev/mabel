# google-cloud-logging
# pydantic

import os
import orjson as json
import logging
from typing import Union, Optional
from .levels import LEVELS, LEVELS_TO_STRING
from ..utils import is_running_from_ipython
from .log_formatter import LogFormatter


def log_it(payload):
    print(payload)
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    if isinstance(payload, bytes):
        payload = payload.decode()
    print(payload, flush=True)
    return payload


def extract_caller():
    import traceback
    import os.path

    frames = traceback.extract_stack()
    if len(frames) < 4:
        return "<unknown>", "<unknown>", -1
    frame = frames[len(frames) - 4]
    head, tail = os.path.split(frame.filename)
    return frame.name, tail, frame.lineno


class GoogleLogger(object):
    @staticmethod
    def supported():
        if is_running_from_ipython():
            return False
        if os.environ.get("PROJECT_NAME") or "" == "":
            return False
        return True

    @staticmethod
    def write_event(
        message: Union[str, dict],
        system: Optional[str] = None,
        severity: Optional[int] = logging.DEBUG,
        spanId: Optional[str] = None,
    ):

        from .create_logger import LOG_NAME

        structured_log = {
            "logName": f'projects/{os.environ.get("PROJECT_NAME")}/logs/{os.environ.get("LOG_SINK")}',
            "severity": str(severity).split(".")[1],
        }
        structured_log["logging.googleapis.com/labels"] = {
            "system": system,
            "log_name": LOG_NAME,
        }

        method, module, line = extract_caller()
        structured_log["logging.googleapis.com/sourceLocation"] = {
            "function": method,
            "file": module,
            "line": line,
        }

        if spanId:
            structured_log["logging.googleapis.com/spanId"] = spanId

        if isinstance(message, dict):
            formatter = LogFormatter(None)
            message = formatter.clean_record(message, False)
            structured_log["jsonPayload"] = message
            return log_it(structured_log)
        else:
            structured_log["textPayload"] = message
            return log_it(structured_log)

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
