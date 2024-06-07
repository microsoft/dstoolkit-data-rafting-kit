from enum import StrEnum
from typing import Literal

from pydantic import Field
from pyspark.sql import DataFrame

from data_rafting_kit.io.io_base import (
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


class FileModeEnum(StrEnum):
    """Enumeration class for File modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR = "error"
    IGNORE = "ignore"


# The following classes are used to define the input and output specifications for the File.
class FileFormatEnum(StrEnum):
    """Enumeration class for file formats."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"


class FileOutputParamSpec(OutputBaseParamSpec):
    """File output parameters."""

    format: Literal[FileFormatEnum.CSV, FileFormatEnum.JSON, FileFormatEnum.PARQUET]
    location: str
    mode: str | None = Field(default=FileModeEnum.APPEND)


class FileOutputSpec(OutputBaseSpec):
    """File output specification."""

    type: Literal[IOEnum.FILE]
    params: FileOutputParamSpec


class FileInputParamSpec(InputBaseParamSpec):
    """File input parameters."""

    format: Literal[FileFormatEnum.CSV, FileFormatEnum.JSON, FileFormatEnum.PARQUET]
    location: str


class FileInputSpec(InputBaseSpec):
    """File input specification."""

    type: Literal[IOEnum.FILE]
    params: FileInputParamSpec


class FileIO(IOBase):
    """Represents a file object for data pipelines."""

    def read(self, spec: FileInputSpec) -> DataFrame:
        """Reads from a file on the file system.

        Args:
        ----
            spec (FileInputSpec): The input parameter specification object.

        Returns:
        -------
            DataFrame: The DataFrame object.
        """
        self.logger.info("Reading from File...")

        return (
            self._spark.read.options(**spec.params.options)
            .format(spec.params.format)
            .load(spec.params.location)
        )

    def write(self, spec: FileOutputSpec, input_df: DataFrame):
        """Writes to a file on the file system.

        Args:
        ----
            spec (FileOutputSpec): The output parameter specification object.
            input_df (DataFrame): The DataFrame object to write.
        """
        self.logger.info("Writing to File...")

        writer = (
            input_df.write.options(**spec.params.options)
            .mode(spec.params.mode)
            .format(spec.params.format)
        )

        writer.save(spec.params.location)
