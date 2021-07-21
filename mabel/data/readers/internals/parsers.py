from ...formats.json import parse
from ....errors import MissingDependencyError
from ....utils import xml_parse


def json_parser(ds):
    """parse each line in the file to a dictionary"""
    yield from map(parse, ds)


def pass_thru_parser(ds):
    """just pass it through"""
    yield from ds


def block_parser(ds):
    """each blob is read as a block"""
    if isinstance(ds, str):
        return ds
    yield "\n".join([r for r in ds])  # pragma: no cover


def xml_parser(ds):
    yield from map(xml_parse.parse, ds)
