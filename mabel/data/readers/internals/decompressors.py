from mabel.errors import MissingDependencyError
import orjson


def zstd(stream):
    """
    Read zstandard compressed files
    """
    # zstandard should always be present
    import zstandard  # type:ignore

    with zstandard.open(stream, "r", encoding="utf8") as file:  # type:ignore
        yield from file


def lzma(stream):
    """
    Read LZMA compressed files
    """
    # lzma should always be present
    import lzma

    with lzma.open(stream, "rb") as file:  # type:ignore
        yield from file


def zip(stream):
    """
    Read ZIP compressed files
    """
    # zipfile should always be present
    import zipfile

    with zipfile.ZipFile(stream, "r") as zip:
        file = zip.read(zipfile.ZipFile.namelist(zip)[0])
        yield from file.split(b"\n")


def parquet(stream):
    """
    Read parquet formatted files
    """
    try:
        import pyarrow.parquet as pq  # type:ignore
    except ImportError:  # pragma: no cover
        raise MissingDependencyError(
            "`pyarrow` is missing, please install or include in requirements.txt"
        )
    table = pq.read_table(stream)
    for batch in table.to_batches():
        dict_batch = batch.to_pydict()
        for index in range(len(batch)):
            yield orjson.dumps(
                {k: v[index] for k, v in dict_batch.items()}
            ).decode()  # type:ignore


def lines(stream):
    """
    Default reader, assumes text format
    """
    text = stream.read().decode("utf8")  # type:ignore
    yield from text.splitlines()

def block(stream):
    yield stream.read().decode("utf8")
