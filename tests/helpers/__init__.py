try:
    import orjson as json
except ImportError:
    import ujson as json
from .mock import Mock



def read_jsonl(filename, limit=-1, chunk_size=32*1024*1024, delimiter="\n"):
    """"""
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader, None)
    while line:
        yield json.loads(line)
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except StopIteration:
            return


def read_file(filename, chunk_size=32*1024*1024, delimiter="\n"):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, "r", encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward
