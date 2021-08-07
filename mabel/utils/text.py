import re
import string
from functools import lru_cache

VALID_CHARACTERS = string.ascii_letters + string.digits + string.whitespace
REGEX_CHARACTERS = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


def tokenize(text):
    text = text.lower()
    text = "".join([c for c in text if c in VALID_CHARACTERS])
    return text.split()


def sanitize(text, safe_characters: str = VALID_CHARACTERS):
    return "".join([c for c in text if c in safe_characters])


def wrap_text(text, line_len):
    from textwrap import fill

    def _inner(text):
        for line in text.splitlines():
            yield fill(line, line_len)

    return "\n".join(list(_inner(text)))


# https://codereview.stackexchange.com/a/248421


@lru_cache(4)
def _sql_like_fragment_to_regex(fragment):
    """
    Allows us to accepts LIKE statements to search data
    """
    # https://codereview.stackexchange.com/a/36864/229677
    safe_fragment = "".join([REGEX_CHARACTERS.get(ch, ch) for ch in fragment])
    return re.compile("^" + safe_fragment.replace("%", ".*?").replace("_", ".") + "$")


def like(x, y):
    return _sql_like_fragment_to_regex(y.lower()).match(str(x).lower())


def not_like(x, y):
    return not like(x, y)
