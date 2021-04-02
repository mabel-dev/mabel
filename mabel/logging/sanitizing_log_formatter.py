import re
import hashlib
import logging
import ujson as json


KEYS_TO_SANITIZE = ["password$", "pwd$", ".*_secret$", ".*_key$"]


class SanitizingLogFormatter(logging.Formatter):

    def __init__(self, orig_formatter):
        """
        Remove sensitive data from records before saving to external logs.
        Note that the value is hashed using (SHA256) and the first 16 
        characters of the hexencoded hash are presented. This information
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
        msg = re.sub(r':\/\/(.*?)\@', r'://[REDACTED]', msg)
        return msg

    def __getattr__(self, attr):
        return getattr(self.orig_formatter, attr)

    def hash_it(self, value_to_hash):
        return hashlib.sha256(value_to_hash.encode()).hexdigest()[:8]

    def sanitize_record(self, record):

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
            parts.append(json.dumps(dirty_record))
            return '|'.join(parts)
        return record
