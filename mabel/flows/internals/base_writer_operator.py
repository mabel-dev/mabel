import datetime
from ..internals.base_operator import BaseOperator
from ...utils import paths
from ...data.formats import json


class BaseWriterOperator(BaseOperator):
    def __init__(self, *, writer, inner_writer, **kwargs):
        """
        Writers should extend this class
        """
        super().__init__(**kwargs)
        self.inner_writer = inner_writer(**kwargs)
        self.writer = writer(inner_writer=inner_writer, **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)
        return data, context

    def finalize(self, context: dict = None):

        if isinstance(context, dict):

            # if we have a profiler in the path, it will
            profile = context.get("mabel:profile")
            if profile:
                timestamp = datetime.datetime.now().strftime("_SYS.as_at_%Y%m%d-%H%M%S")
                profile_path = self.writer.dataset + timestamp + ".profile.json"

                fn_bucket, fn_path, fn_stem, fn_ext = paths.get_parts(
                    self.inner_writer.filename
                )
                bucket, path, stem, ext = paths.get_parts(profile_path)

                if fn_bucket != bucket:
                    profile_path = path + stem + ext

                self.inner_writer.commit(
                    json.serialize(profile, indent=True, as_bytes=True), profile_path
                )

        if not context:
            context = {}

        if self.writer.schema:
            context["schema_object"] = self.writer.schema

        self.writer.finalize()
        return context

    def __del__(self):
        self.writer.finalize()
