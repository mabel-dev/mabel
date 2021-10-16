from ...internals import xmler
import simdjson


def json(ds):
    """parse each line in the file to a dictionary"""

    try:
        json_parser = simdjson.Parser()
        return json_parser.parse(ds)
    except Exception as e:
        print(ds, e)
        raise e

def pass_thru(ds):
    """just pass it through"""
    return ds


def pass_thru_block(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    return "\n".join([r for r in ds])  # pragma: no cover


def xml(ds):
    return xmler.parse(ds)
