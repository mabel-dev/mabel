import orjson

pass_thru = lambda x: x

json = orjson.loads


def pass_thru_block(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    return "\n".join([r for r in ds])  # pragma: no cover


def xml(ds):
    from ...internals import xmler

    return xmler.parse(ds)
