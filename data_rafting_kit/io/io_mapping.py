from data_rafting_kit.io.delta_table import DeltaTableIO
from data_rafting_kit.io.file import FileIO
from data_rafting_kit.io.io_base import IOEnum


class IOMapping:
    """Holds the mapping for the IO types."""

    @staticmethod
    def get_input_map(key: IOEnum) -> object:
        """Maps the input type to the corresponding class.

        Args:
        ----
            key (IOEnum): The key to map to a class and method.

        Returns:
        -------
        dict: The dictionary mapping the input type to the corresponding class.
        """
        map = {
            IOEnum.DELTA_TABLE: (DeltaTableIO, DeltaTableIO.read),
            IOEnum.FILE: (FileIO, FileIO.read),
        }

        if key not in map:
            raise NotImplementedError(f"Input Type {key} not implemented")

        return map[key]

    @staticmethod
    def get_output_map(key: IOEnum) -> object:
        """Maps the output type to the corresponding class.

        Args:
        ----
            key (IOEnum): The key to map to a class and method.

        Returns:
        -------
        dict: The dictionary mapping the output type to the corresponding class.
        """
        map = {
            IOEnum.DELTA_TABLE: (DeltaTableIO, DeltaTableIO.write),
            IOEnum.FILE: (FileIO, FileIO.write),
        }

        if key not in map:
            raise NotImplementedError(f"Output Type {key} not implemented")

        return map[key]
