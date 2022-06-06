from ...data.readers.internals.base_inner_reader import BaseInnerReader


class NullReader(BaseInnerReader):

    RULES = [{"name": "data", "required": True}]

    def __init__(self, **kwargs):
        """
        Null reader
        """
        super().__init__(**kwargs)
        self._data = kwargs.get("data", [])

    def get_blobs_at_path(self, path):
        return [f"{path}/null.jsonl"]

    def get_blob_bytes(self, blob_name: str) -> bytes:
        import orjson

        return b"\n".join(map(orjson.dumps, self._data))
