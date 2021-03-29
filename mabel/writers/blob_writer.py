import datetime
from ..helpers import BlobPaths
try:
    from google.cloud import storage  # type:ignore
except ImportError:
    pass
from typing import Optional


def blob_writer(
        source_file_name: str,
        target_path: str,
        date: Optional[datetime.date] = None,
        add_extention: str = '',
        **kwargs):

    # default the date to today
    if date is None:
        date = datetime.datetime.today()

    # get the project name
    project = kwargs.get('project')
    if project is None:
        raise Exception('blob_writer must have project defined')

    # factorize the path
    bucket, gcs_path, filename, extention = BlobPaths.get_parts(target_path)

    # get a reference to the gcs bucket
    client = storage.Client(project=project)
    gcs_bucket = client.get_bucket(bucket)
    
    # avoid collisions
    collision_tests = 0
    maybe_colliding_filename = BlobPaths.build_path(f"{gcs_path}{filename}-{collision_tests:04d}{extention}{add_extention}", date)
    blob = gcs_bucket.blob(maybe_colliding_filename)

    while blob.exists():
        collision_tests += 1
        maybe_colliding_filename = BlobPaths.build_path(f"{gcs_path}{filename}-{collision_tests:04d}{extention}{add_extention}", date)
        blob = gcs_bucket.blob(maybe_colliding_filename)

    # save the blob
    blob.upload_from_filename(source_file_name)

    return maybe_colliding_filename
