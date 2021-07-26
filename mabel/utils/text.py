import string

VALID_CHARACTERS = string.ascii_letters + string.digits + string.whitespace


def tokenize(text):
    text = text.lower()
    text = "".join([c for c in text if c in VALID_CHARACTERS])
    return text.split()


def sanitize(text, safe_characters: str = string.ascii_letters + string.digits + " "):
    return "".join([c for c in text if c in VALID_CHARACTERS])


def wrap_text(text, line_len):
    from textwrap import fill

    def _inner(text):
        for line in text.splitlines():
            yield fill(line, line_len)

    return "\n".join(list(_inner(text)))
