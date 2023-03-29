"""
We can't test the log sanitizer through logging so we test by calling it 
directly
"""
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
from mabel.logging.log_formatter import LogFormatter
from rich import traceback

traceback.install()


def test_sanitizing_log_formatter_pass_thru():
    # test we can just pass-thru a basic formatted message
    sanitizer = LogFormatter(None)
    sanitized = sanitizer.sanitize_record("log name | log level | date | location | message")
    assert "log name" in sanitized
    assert "log level" in sanitized
    assert "date" in sanitized
    assert "location" in sanitized
    assert "message" in sanitized, sanitized


def test_sanitizing_log_formatter_redact_simple_case():
    # test that the password entry is removed from the log
    sanitizer = LogFormatter(None)
    redacted_record = sanitizer.sanitize_record(
        'log name | log level | date | location | {"password":"secret"}'
    )
    assert "secret" not in redacted_record
    assert "redacted" in redacted_record


def test_sanitizing_log_formatter_mixed_redact_and_keep():
    # test that data is passed through when redacting
    sanitizer = LogFormatter(None)
    redacted_record = sanitizer.sanitize_record(
        'log name | log level | date | location | {"username":"chunkylover53@aol.com","password":"secret"}'
    )
    assert "chunkylover53@aol.com" in redacted_record
    assert "secret" not in redacted_record
    assert "redacted" in redacted_record


def test_sanitizing_log_formatter_predefined_redaction_keys():
    # test all the predefined markers for redaction work

    sanitizer = LogFormatter(None)

    redacted_record_password = sanitizer.sanitize_record(
        'log name | log level | date | location | {"password":"private"}'
    )
    assert "private" not in redacted_record_password
    redacted_record_pwd = sanitizer.sanitize_record(
        'log name | log level | date | location | {"pwd":"private"}'
    )
    assert "private" not in redacted_record_pwd
    redacted_record_secret = sanitizer.sanitize_record(
        'log name | log level | date | location | {"shared_secret":"private"}'
    )
    assert "private" not in redacted_record_secret
    redacted_record_key = sanitizer.sanitize_record(
        'log name | log level | date | location | {"user_key":"private"}'
    )
    assert "private" not in redacted_record_key


if __name__ == "__main__":  # pragma: no cover
    test_sanitizing_log_formatter_pass_thru()
    test_sanitizing_log_formatter_redact_simple_case()
    test_sanitizing_log_formatter_mixed_redact_and_keep()
    test_sanitizing_log_formatter_predefined_redaction_keys()

    print("okay")
