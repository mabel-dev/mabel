"""

** unique

string:
- string length

"""
from typing import Any
from pydantic import BaseModel  # type:ignore


class Measures(BaseModel):
    minimum: Any = None
    maximum: Any = None
    count: int = 0
    missing: int = 0
    cummulative_sum: float = 0
