from orso import DataFrame
from orso.logging import get_logger


class BaseDatabaseWriter:
    def __init__(self, **kwargs):
        self.logger = get_logger()
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.logger.debug(", ".join(kwargs_passed))

        self.dataset = kwargs.get("dataset")
        self.engine = kwargs.get("engine")
        self.finalizer = kwargs.get("finalizer")
        self.finalized = False

    def commit(self, dataframe: DataFrame):
        raise NotImplementedError("Database")

    def finalize(self):
        # you can either supply your own finalizer via setting the finalizer paramter
        # or you can overide the finalizer in your database writer.
        if self.finalizer and not self.finalized:
            self.finalizer(self)
            self.logger.debug("Database Writer Finalizer")
        self.finalized = True
