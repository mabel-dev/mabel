import os
import re
import hashlib
import logging
import orjson as json
from functools import lru_cache
from ..utils.colors import COLORS, colorize
from ..utils.ipython import is_running_from_ipython


# if we find a key which matches these strings, we hash the contents
KEYS_TO_SANITIZE = [r"password$", r"pwd$", r".*_secret$", r".*_key$", r"_token$"]
COLOR_EXCHANGES = {
    " ALERT    ": "{BOLD_RED} ALERT    {OFF}",
    " ERROR    ": "{RED} ERROR    {OFF}",
    " DEBUG    ": "{GREEN} DEBUG    {OFF}",
    " AUDIT    ": "{YELLOW} AUDIT    {OFF}",
    " WARNING  ": "{CYAN} WARNING  {OFF}",
    " INFO     ": "{BOLD_WHITE} INFO     {OFF}",
}


class LogFormatter(logging.Formatter):
    def __init__(self, orig_formatter, suppress_color: bool = False):
        """
        Remove sensitive data from records before saving to external logs. Note that
        the value is hashed using (SHA256) and only the first 8 characters of the
        hex-encoded hash are presented. This information allows values to be traced
        without disclosing the actual value.

        The Sanitizer can only sanitize dictionaries, it doesn't sanitize strings,
        which could contain sensitive information We use the message id as a salt to
        further protect sensitive information.

        Based On: https://github.com/joocer/cronicl/blob/main/cronicl/utils/sanitizer.py
        """
        self.orig_formatter = orig_formatter
        self.suppress_color = suppress_color

    def format(self, record):
        try:
            msg = self.orig_formatter.format(record)
        except:
            msg = record
        msg = self.sanitize_record(msg)
        if "://" in msg:
            msg = re.sub(r":\/\/(.*?)\@", r"://{BOLD_PURPLE}<redacted>{OFF}", msg)
        return msg

    @lru_cache(1)
    def _can_colorize(self):

        if self.suppress_color:
            return False
        if is_running_from_ipython():
            return True

        colorterm = os.environ.get("COLORTERM", "").lower()
        term = os.environ.get("TERM", "").lower()
        return "yes" in colorterm or "true" in colorterm or "256" in term

    def color_code(self, record):
        if self._can_colorize():
            for k, v in COLOR_EXCHANGES.items():
                if k in record:
                    return record.replace(k, v)
        return record

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

    def hash_it(self, value_to_hash):
        return hashlib.sha256(value_to_hash.encode()).hexdigest()[:8]

    def clean_record(self, dirty_record, colorize: bool = True):
        if colorize:
            BLUE = "{BLUE}"
            OFF = "{OFF}"
            PURPLE = "{PURPLE}"
            YELLOW = "{YELLOW}"
            GREEN = "{GREEN}"
        else:
            BLUE = ""
            OFF = ""
            PURPLE = ""
            YELLOW = ""
            GREEN = ""

        clean_record = {}
        for key, value in dirty_record.items():
            if isinstance(value, dict):
                value = self.clean_record(value, colorize)
            elif any(
                [
                    True
                    for expression in KEYS_TO_SANITIZE
                    if re.match(expression, key, re.IGNORECASE)
                ]
            ):
                clean_record[BLUE + key + OFF] = (
                    PURPLE + "<redacted:" + self.hash_it(str(value)) + ">" + OFF
                )
            else:
                value = re.sub(
                    r"`([^`]*)`", r"`" + YELLOW + "\1" + GREEN + "`", f"{value}"
                )
                value = re.sub(
                    r"'([^']*)'", r"'" + YELLOW + "\1" + GREEN + "'", f"{value}"
                )
                clean_record[BLUE + key + OFF] = GREEN + f"{value}" + OFF
        return clean_record

    def sanitize_record(self, record):

        record = self.color_code(record)
        parts = record.split("|")
        json_part = parts.pop()

        try:
            dirty_record = json.loads(json_part.encode("UTF8"))
            clean_record = self.clean_record(dirty_record)
            parts.append(" " + json.dumps(clean_record).decode("UTF8"))

        except ValueError:
            json_part = re.sub(r"`([^`]*)`", r"`{YELLOW}\1{OFF}`", f"{json_part}")
            json_part = re.sub(r"'([^']*)'", r"'{YELLOW}\1{OFF}'", f"{json_part}")
            json_part = re.sub(r'"([^"]*)"', r"'{YELLOW}\1{OFF}'", f"{json_part}")
            parts.append(" " + json_part.strip() + " *")

        record = "|".join(parts)
        return colorize(record, self._can_colorize())
