pass_thru = lambda x: x

import simdjson
from mabel.logging import get_logger

json_parser = simdjson.Parser()

def json_not_used(line):
    """
    Parse each line in the file to a dictionary.

    We do some juggling so we can delete the object which is faster than creating a
    new Parser for each record.
    """
    dic = json_parser.parse(line)
    keys = dic.keys()
    values = dic.values()
    del dic
    return dict(zip(keys, values))

def json(ds):
    """parse each line in the file to a dictionary"""
    json_parser = simdjson.Parser()
    return json_parser.parse(ds)


def pass_thru_block(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    return "\n".join([r for r in ds])  # pragma: no cover


def xml(ds):
    from ...internals import xmler

    return xmler.parse(ds)
