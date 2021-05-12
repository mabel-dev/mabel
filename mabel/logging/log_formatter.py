import os
import re
import hashlib
import logging
import ujson as json  # use ujson rather than orjson for compatibility
from functools import lru_cache

COLORS = {
    "{OFF}": "\033[0m",             # Text Reset

    # Regular Colors
    "{BLACK}": "\033[0;30m",        # Black
    "{RED}": "\033[0;31m",          # Red
    "{GREEN}": "\033[0;32m",        # Green
    "{YELLOW}": "\033[0;33m",       # Yellow
    "{BLUE}": "\033[0;34m",         # Blue
    "{PURPLE}": "\033[0;35m",       # Purple
    "{CYAN}": "\033[0;36m",         # Cyan
    "{WHITE}": "\033[0;37m",        # White

    # Bold
    "{BOLD_BLACK}": "\033[1;30m",       # Black
    "{BOLD_RED}": "\033[1;31m",         # Red
    "{BOLD_GREEN}": "\033[1;32m",       # Green
    "{BOLD_YELLOW}": "\033[1;33m",      # Yellow
    "{BOLD_BLUE}": "\033[1;34m",        # Blue
    "{BOLD_PURPLE}": "\033[1;35m",      # Purple
    "{BOLD_CYAN}": "\033[1;36m",        # Cyan
    "{BOLD_WHITE}": "\033[1;37m",       # White

    # Underline
    "{UNDERLINE_BLACK}": "\033[4;30m",       # Black
    "{UNDERLINE_RED}": "\033[4;31m",         # Red
    "{UNDERLINE_GREEN}": "\033[4;32m",       # Green
    "{UNDERLINE_YELLOW}": "\033[4;33m",      # Yellow
    "{UNDERLINE_BLUE}": "\033[4;34m",        # Blue
    "{UNDERLINE_PURPLE}": "\033[4;35m",      # Purple
    "{UNDERLINE_CYAN}": "\033[4;36m",        # Cyan
    "{UNDERLINE_WHITE}": "\033[4;37m"       # White
}



# if we find a key which matches these strings, we hash the contents
KEYS_TO_SANITIZE = ["password$", "pwd$", ".*_secret$", ".*_key$"]
COLOR_EXCHANGES = {
    ' ALERT    ': '{BOLD_RED} ALERT    {OFF}',
    ' ERROR    ': '{RED} ERROR    {OFF}', 
    ' DEBUG    ': '{GREEN} DEBUG    {OFF}',
    ' AUDIT    ': '{YELLOW} AUDIT    {OFF}',
    ' WARNING  ': '{CYAN} WARNING  {OFF}',
    ' INFO     ': '{BOLD_WHITE} INFO     {OFF}'
}

class LogFormatter(logging.Formatter):

    def __init__(self, orig_formatter):
        """
        Remove sensitive data from records before saving to external logs.
        Note that the value is hashed using (SHA256) and only the first 8 
        characters of the hex-encoded hash are presented. This information
        allows values to be traced without disclosing the actual value. 

        The Sanitizer can only sanitize dictionaries, it doesn't
        sanitize strings, which could contain sensitive information
        We use the message id as a salt to further protect sensitive 
        information.

        Based On: https://github.com/joocer/cronicl/blob/main/cronicl/utils/sanitizer.py
        """
        self.orig_formatter = orig_formatter

    def format(self, record):
        msg = self.orig_formatter.format(record)
        msg = self.sanitize_record(msg)
        if '://' in msg:
            msg = re.sub(r':\/\/(.*?)\@', r'://{BOLD_PURPLE}<redacted>{OFF}', msg)
        return msg
    
    @lru_cache(1)
    def _can_colorize(self):

        def is_running_from_ipython():
            """
            True when running in Jupyter
            """
            try:
                from IPython import get_ipython  # type:ignore
                return get_ipython() is not None
            except Exception:
                return False

        if is_running_from_ipython():
            return True

        colorterm = os.environ.get('COLORTERM', '').lower()
        term = os.environ.get('TERM', '').lower()
        return 'yes' in colorterm or 'true' in colorterm or '256' in term        

    def color_code(self, record):
        if self._can_colorize():
            for k, v in COLOR_EXCHANGES.items():
                if k in record:
                    return record.replace(k, v)
        return record

    def colorize(self, record):
        if self._can_colorize():
            for k, v in COLORS.items():
                record = record.replace(k, v)
        else:
            for k, v in COLORS.items():
                record = record.replace(k, '')
        return record

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

    def hash_it(self, value_to_hash):
        return hashlib.sha256(value_to_hash.encode()).hexdigest()[:8]

    def sanitize_record(self, record):

        record = self.color_code(record)
        parts = record.split('|')
        json_part = parts.pop()

        try:
            dirty_record = json.loads(json_part)
        except ValueError:
            dirty_record = {"message": json_part.strip()}

        if isinstance(dirty_record, dict):
            clean_record = {}
            for key, value in dirty_record.items():
                if any([True for expression in KEYS_TO_SANITIZE if re.match(expression, key, re.IGNORECASE)]):
                    clean_record["{BLUE}" + key + "{OFF}"] = '{PURPLE}<redacted:' + self.hash_it(str(value)) + '>{OFF}'
                else:
                    value = re.sub("`([^`]*)`", r"`{YELLOW}\1{GREEN}`", F"{value}")
                    value = re.sub("'([^']*)'", r"'{YELLOW}\1{GREEN}'", F"{value}")
                    clean_record["{BLUE}" + key + "{OFF}"] = "{GREEN}" + F"{value}" + "{OFF}" 

            parts.append(' ' + json.dumps(clean_record))
        
        record = '|'.join(parts)
        return self.colorize(record)
