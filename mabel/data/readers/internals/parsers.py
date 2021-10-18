import orjson
from ...internals import xmler


def json(ds):
    """parse each line in the file to a dictionary"""
    return orjson.loads(ds)


pass_thru = lambda x: x


def pass_thru_block(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    return "\n".join([r for r in ds])  # pragma: no cover


def xml(ds):
    return xmler.parse(ds)
