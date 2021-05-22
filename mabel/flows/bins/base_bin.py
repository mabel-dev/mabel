"""
Base Bin

Implements common functions for each of the bins.
"""
import datetime
import abc


class BaseBin(abc.ABC):
    def __init__(self, bin_name: str):
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    @abc.abstractmethod
    def __call__(self, record: str) -> str:
        raise NotImplementedError()

    def _date_part(self):
        return datetime.date.today().strftime("%Y-%m-%d")
