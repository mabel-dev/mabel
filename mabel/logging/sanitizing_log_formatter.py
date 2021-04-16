import re
import hashlib
import logging
import ujson as json

# import ujson because importing the mabel json module creates circular
# references, we use ujson rather than orjson for greatest compatibility


# if we find a key which matches these strings, we hash the contents
KEYS_TO_SANITIZE = ["password$", "pwd$", ".*_secret$", ".*_key$"]
COLOR_EXCHANGES = {
    ' ALERT    ': '\033[1;31m' + ' ALERT    ' + '\033[0m',  # BOLD RED
    ' ERROR    ': '\033[31m' + ' ERROR    ' + '\033[0m',    # RED 
    ' DEBUG    ': '\033[32m' + ' DEBUG    ' + '\033[0m',    # GREEN
    ' AUDIT    ': '\033[33m' + ' AUDIT    ' + '\033[0m',    # YELLOW
    ' WARNING  ': '\033[36m' + ' WARNING  ' + '\033[0m',    # CYAN
}

class SanitizingLogFormatter(logging.Formatter):

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
            msg = re.sub(r':\/\/(.*?)\@', r'://<redacted>', msg)
        return msg

    def color_code(self, record):
        for k, v in COLOR_EXCHANGES.items():
            if k in record:
                return record.replace(k, v)
        return record

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

    def hash_it(self, value_to_hash):
        return hashlib.sha256(value_to_hash.encode()).hexdigest()[:8]

    def sanitize_record(self, record):

        record = self.color_code(record)
        parts = record.split('|')
        json_part = parts.pop()
        done_anything = False

        try:
            dirty_record = json.loads(json_part)
        except:
            return record

        if isinstance(dirty_record, dict):
            for key, value in dirty_record.items():
                if any([True for expression in KEYS_TO_SANITIZE if re.match(expression, key, re.IGNORECASE)]):
                    dirty_record[key] = '<redacted:' + self.hash_it(str(value)) + '>'
                    done_anything = True

        if done_anything:
            parts.append(' ' + json.dumps(dirty_record))
            return '|'.join(parts)

        return record
