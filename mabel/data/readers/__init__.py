from enum import Enum


class STORAGE_CLASS(int, Enum):
    NO_PERSISTANCE = 1
    MEMORY = 2
    DISK = 3
