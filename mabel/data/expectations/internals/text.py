import re
from functools import lru_cache

SPECIAL_REGEX_CHARS = {ch: "\\" + ch for ch in ".^$*+?{}[]|()\\"}


@lru_cache(4)
def sql_like_to_regex(fragment):
    safe_fragment = "".join([SPECIAL_REGEX_CHARS.get(ch, ch) for ch in fragment])
    return re.compile("^" + safe_fragment.replace("%", ".*?").replace("_", ".") + "$")


@lru_cache(4)
def build_regex(fragment):
    return re.compile(fragment)
