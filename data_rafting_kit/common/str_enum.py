from enum import Enum

class StrEnum(str, Enum):
    """
    A backported version of StrEnum for Python 3.10.
    Ensures the enumeration values are also strings.
    """
    def __str__(self):
        return str(self.value)