"""
We can't test the log sanitizer through logging so we test by calling it 
directly
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.logging.sanitizing_log_formatter import SanitizingLogFormatter
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_sanitizing_log_formatter_pass_thru():
    # test we can just pass-thru a basic formatted message
    sanitizer = SanitizingLogFormatter(None)
    assert sanitizer.sanitize_record("abc") == 'abc'

def test_sanitizing_log_formatter_redact_simple_case():
    # test that the password entry is removed from the log
    sanitizer = SanitizingLogFormatter(None)
    redacted_record = sanitizer.sanitize_record('|{"password":"secret"}')
    assert 'secret' not in redacted_record
    assert 'redacted' in redacted_record

def test_sanitizing_log_formatter_mixed_redact_and_keep():
    # test that data is passed through when redacting
    sanitizer = SanitizingLogFormatter(None)
    redacted_record = sanitizer.sanitize_record('|{"username":"chunkylover53@aol.com","password":"secret"}')
    assert 'chunkylover53@aol.com' in redacted_record
    assert 'secret' not in redacted_record
    assert 'redacted' in redacted_record

def test_sanitizing_log_formatter_predefined_redaction_keys():
    # test all the predefined markers for redaction work

    sanitizer = SanitizingLogFormatter(None)
    
    redacted_record_password = sanitizer.sanitize_record('|{"password":"private"}')
    assert 'private' not in redacted_record_password
    redacted_record_pwd = sanitizer.sanitize_record('|{"pwd":"private"}')
    assert 'private' not in redacted_record_pwd
    redacted_record_secret = sanitizer.sanitize_record('|{"shared_secret":"private"}')
    assert 'private' not in redacted_record_secret
    redacted_record_key = sanitizer.sanitize_record('|{"user_key":"private"}')
    assert 'private' not in redacted_record_key


if __name__ == "__main__":
    test_sanitizing_log_formatter_pass_thru()
    test_sanitizing_log_formatter_redact_simple_case()
    test_sanitizing_log_formatter_mixed_redact_and_keep()
    test_sanitizing_log_formatter_predefined_redaction_keys()

    print('okay')
