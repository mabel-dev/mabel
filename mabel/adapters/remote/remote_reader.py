from ...data.readers.internals.base_inner_reader import BaseInnerReader
from mabel.logging import get_logger
from ...utils import paths
import requests


def get(url: str, parameters: dict = None, username: str = None, password: str = None) -> tuple:

    if not url.startswith("https://") and not url.startswith(
        "http://"
    ):
        raise ValueError(
            f"`{url}` is not a valid HTTP end-point, RemoteReader can only be used for HTTP end-points."
        )

    try:
        response = requests.get(
            url,
            params=parameters if parameters else None,
            auth=(username, password) if username else None,
            timeout=60,
        )
        return response.status_code, response.headers, response.content
    except Exception as e:  # pragma: no cover
        get_logger().error(f"GET request failed: {type(e).__name__} - {e}")
    return 500, {}, bytes()  # pragma: no cover



class RemoteReader(BaseInnerReader):
    def __init__(self, domain:str = None, **kwargs):
        """
        File System Reader
        """
        super().__init__(**kwargs)
        if domain:
           self.domain = domain
        else:
            import os
            self.domain = os.environ.get("REMOTE_MABLE", "//")
        if self.domain[-1] != "/":
            self.domain += "/"


    def get_blobs_at_path(self, path):
        """
        GET remote_server/v1/o/list/<bucket>?prefix=<prefix>
        """
        bucket, object_path, name, extension = paths.get_parts(path)

        url = self.domain + f"v1/o/list/{bucket}?prefix={object_path}"
        response = get(url)


    def get_blob_bytes(self, blob_name: str) -> bytes:
        """
        GET remote_server/v1/b/get/<bucket>/<path>/
        """

        bucket, object_path, name, extension = paths.get_parts(blob_name)
        remote_blob_name=object_path + name + extension

        url = self.domain + f"v1/b/get/{bucket}/{remote_blob_name}"
        response = get(url)
