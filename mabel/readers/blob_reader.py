try:
    from google.cloud import storage  # type:ignore
except ImportError:
    pass
import lzma
import datetime
from ..helpers.blob_paths import BlobPaths
from typing import Tuple, Union, Optional
import mabel.logging  # type:ignore


def blob_reader(
        path: str,
        project: str,
        date_range: Tuple[Optional[datetime.date], Optional[datetime.date]] = (None, None),
        chunk_size=16*1024*1024,
        **kwargs):

    """
    Blob reader, will iterate over as set of blobs in a path.
    """
    # validate request
    if not project:
        raise ValueError('Blob Reader requires Project to be set')
    if not path:
        raise ValueError('Blob Reader requires Path to be set')

    # if dates aren't provided, use today
    start_date, end_date = date_range
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    bucket, blob_path, name, extention = BlobPaths.get_parts(path)

    # cycle through the days, loading each days' file
    for cycle in range(int((end_date - start_date).days) + 1):
        cycle_date = start_date + datetime.timedelta(cycle)
        cycle_path = BlobPaths.build_path(path=blob_path, date=cycle_date)
        blobs_at_path = find_blobs_at_path(project=project, bucket=bucket, path=cycle_path, extention=extention)
        blobs_at_path = list(blobs_at_path)
        for blob in blobs_at_path:
            reader = _inner_blob_reader(blob_name=blob.name, project=project, bucket=bucket, chunk_size=chunk_size)
            yield from reader


def find_blobs_at_path(
        project: str,
        bucket: str,
        path: str,
        extention: str):

    client = storage.Client(project=project)
    gcs_bucket = client.get_bucket(bucket)
    blobs = client.list_blobs(bucket_or_name=gcs_bucket, prefix=path)
    if extention:
        blobs = [blob for blob in blobs if extention in blob.name]
    yield from blobs


def _inner_blob_reader(
        project: str,
        bucket: str,
        blob_name: str,
        chunk_size: int = 16*1024*1024,
        delimiter: str = '\n'):

    """
    Reads lines from an arbitrarily long blob, line by line.

    Automatically detecting if the blob is compressed.
    """
    blob = get_blob(project=project, bucket=bucket, blob_name=blob_name)
    if blob:
        blob_size = blob.size
    else:
        blob_size = 0

    carry_forward = ''
    cursor = 0
    while (cursor < blob_size):
        chunk = _download_chunk(blob=blob, start=cursor, end=min(blob_size, cursor+chunk_size-1))
        cursor += chunk_size   # was previously +len(chunk)
        chunk = chunk.decode('utf-8')
        # add the last line from the previous cycle
        chunk += carry_forward
        lines = chunk.split(delimiter)
        # the list line is likely to be incomplete, save it to carry forward
        carry_forward = lines.pop()
        yield from lines
    if len(carry_forward) > 0:
        yield carry_forward


def _download_chunk(
        blob: storage.blob,
        start: int,
        end: int):

    """
    Detects if a chunk is compressed by looking for a magic string
    """
    chunk = blob.download_as_string(start=start, end=end)
    if blob.name.endswith('.lzma'):
        try:
            return lzma.decompress(chunk)
        except lzma.LZMAError:
            # if we fail maybe we're not compressed
            pass
    return chunk


def get_blob(
        project: str,
        bucket: str,
        blob_name: str):

    client = storage.Client(project=project)
    gcs_bucket = client.get_bucket(bucket)
    blob = gcs_bucket.get_blob(blob_name)
    return blob
