from enum import Enum


class StrEnum(str, Enum):
    """A backported version of StrEnum for Python 3.10.

    Ensures the enumeration values are also strings.
    """

    def __str__(self) -> str:
        """Return the string representation of the enumeration value.

        Returns
        -------
            str: The string representation of the enumeration value.
        """
        return str(self.value)
