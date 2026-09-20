from ....errors import MissingDependencyError


def zstd(stream):
    """
    Read zstandard compressed files
    """
    import zstandard  # type:ignore

    with zstandard.open(stream, "rb") as file:  # type:ignore
        yield from file.read().split(b"\n")[:-1]


def lzma(stream):
    """
    Read LZMA compressed files
    """
    # lzma should always be present
    import lzma

    with lzma.open(stream, "rb") as file:  # type:ignore
        yield from file


def unzip(stream):
    """
    Read ZIP compressed files
    """
    # zipfile should always be present
    import io
    import zipfile

    from .parallel_reader import KNOWN_EXTENSIONS

    with zipfile.ZipFile(stream, "r") as zip:
        for file_name in zipfile.ZipFile.namelist(zip):
            file = zip.read(file_name)
            # get the extention of the file(s) in the ZIP and put them
            # through a secondary decompressor and parser
            ext = "." + file_name.split(".")[-1]
            if ext in KNOWN_EXTENSIONS:
                decompressor, parser, file_type = KNOWN_EXTENSIONS[ext]
                for line in decompressor(io.BytesIO(file)):
                    yield parser(line)


def parquet(stream):
    """
    Read parquet formatted files
    """
    try:
        from rugo.parquet import read_parquet  # type:ignore
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`rugo` is missing, please install or include in requirements.txt"
        )

    # rugo reads a buffer rather than a file-like object
    if hasattr(stream, "read"):
        stream = stream.read()

    import datetime

    def naive_utc(timestamp):
        # pyarrow handed timestamps back naive, and the rest of mabel compares them
        # against naive datetimes (utcnow), which raises TypeError against an aware
        # one - rugo tags them as UTC, so put them back the way callers expect
        if timestamp is None or timestamp.tzinfo is None:
            return timestamp
        return timestamp.astimezone(datetime.timezone.utc).replace(tzinfo=None)

    # rugo hands back a column-oriented morsel at a time, rather than the whole
    # file - transpose each one into the records the readers work with, so we
    # never hold more than a morsel of rows.
    for morsel in read_parquet(stream):
        names = [
            name.decode() if isinstance(name, bytes) else name
            for name in morsel.column_names
        ]
        columns = []
        for name in morsel.column_names:
            values = morsel.column(name).to_pylist()
            first = next((value for value in values if value is not None), None)
            if isinstance(first, datetime.datetime) and first.tzinfo is not None:
                values = [naive_utc(value) for value in values]
            columns.append(values)
        yield from (dict(zip(names, values)) for values in zip(*columns))


def lines(stream):
    """
    Default reader, assumes text format
    """
    text = stream.read()  # type:ignore
    yield from text.splitlines()


def block(stream):
    yield stream.read()


def csv(stream):
    import csv

    yield from csv.DictReader(stream.read().decode("utf8").splitlines())
