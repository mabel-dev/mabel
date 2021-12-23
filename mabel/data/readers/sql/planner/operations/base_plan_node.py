import abc
from typing import Union
from mabel.data.internals.relation import Relation


@abc.ABC
class BasePlanNode:
    def __init__(self, source:Union[str, Relation], configuration: dict = None):
        pass

    def execute(self):
        pass
