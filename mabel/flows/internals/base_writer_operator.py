from ..internals.base_operator import BaseOperator


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

        if not context:
            context = {}

        if self.writer.schema:
            context["schema_object"] = self.writer.schema

        self.writer.finalize(has_failure=context.get("mabel:errored", False))
        return context
