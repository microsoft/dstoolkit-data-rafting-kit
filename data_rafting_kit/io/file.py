from enum import StrEnum
from typing import Literal

from pydantic import model_validator
from pyspark.sql import DataFrame

from data_rafting_kit.io.io_base import (
    InputBaseParamSpec,
    InputBaseSpec,
    IOBase,
    IOEnum,
    OutputBaseParamSpec,
    OutputBaseSpec,
)


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

    @model_validator(mode="before")
    @classmethod
    def validate_delta_table_output_param_spec_before(cls, data: dict) -> dict:
        """Validates the Delta Table output param spec."""
        if data["streaming"] is not None:
            if isinstance(data["streaming"], bool):
                data["streaming"] = {}
            if "checkpoint_location" not in data["streaming"]:
                file_location = data["location"].split("/")[-1]
                data["streaming"][
                    "checkpoint_location"
                ] = f"/.checkpoints/file/{file_location}"

        return data


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
        self._logger.info("Reading from File...")

        reader = self._spark.readStream if spec.params.streaming else self._spark.read
        reader = reader.options(**spec.params.options)

        return reader.format(spec.params.format).load(spec.params.location)

    def write(self, spec: FileOutputSpec, input_df: DataFrame):
        """Writes to a file on the file system.

        Args:
        ----
            spec (FileOutputSpec): The output parameter specification object.
            input_df (DataFrame): The DataFrame object to write.
        """
        self._logger.info("Writing to File...")

        writer = (
            input_df.writeStream.outputMode(spec.params.mode)
            if spec.params.streaming
            else input_df.write.mode(spec.params.mode)
        )
        writer = writer.format(spec.params.format)

        writer = writer.option("path", spec.params.location)

        return writer
