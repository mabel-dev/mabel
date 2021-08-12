import time

import orjson
from ..flows.internals.base_operator import BaseOperator
from ..data import Reader
from ..errors.time_exceeded import TimeExceeded


class ReaderOperator(BaseOperator):
    def __init__(
        self,
        *args,
        time_out: float = 315569652.0,
        signal_format: str = "{cursor}",
        **kwargs
    ):
        """
        Read through a dataset and emit each row as a new record. Includes optional
        functionality to stop after a set amount of time.

        Parameters:
            time_out: float (optional)
                The number of seconds to run before bailing, default is 1 year
                (non-leap)
            signal_format: string (optional)
                The format of the detail in the exception. The default is '{cursor}'
                which is replaced with the reader cursor value at the time of the
                time_out.
            other values as per the inner_reader.
        Yields:
            tuple

        Raises:
            TimeExceeded
        """
        super().__init__(*args, **kwargs)
        self.time_out = time_out + time.time()
        self.signal_format = signal_format
        self.reader = Reader(**kwargs)

    def execute(self, data, context):
        for row in self.reader:
            if time.time() > self.time_out:
                signal = self.signal_format.replace(
                    "{cursor}", orjson.dumps(self.reader.cursor()).decode()
                )
                raise TimeExceeded(signal)
            yield row, context
