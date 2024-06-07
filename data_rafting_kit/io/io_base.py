from enum import StrEnum
from logging import Logger
from typing import Literal

from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

from data_rafting_kit.common.base_spec import BaseSpec


class IOEnum(StrEnum):
    """Enumeration class for IO types."""

    DELTA_TABLE = "delta_table"
    FILE = "file"


class SchemaFieldSpec(BaseModel):
    """Schema field."""

    name: str
    type: Literal[
        "string",
        "binary",
        "boolean",
        "date",
        "timestamp",
        "decimal",
        "double",
        "float",
        "byte",
        "integer",
        "long",
        "short",
        "array",
        "map",
    ]
    nullable: bool | None = Field(default=True)


class InputBaseParamSpec(BaseModel):
    """Base input parameter specification."""

    expected_schema: list[SchemaFieldSpec] | None = Field(default=None)
    options: dict | None = Field(default_factory=dict)


class InputBaseSpec(BaseSpec):
    """Base input specification."""

    pass


class OutputBaseParamSpec(BaseModel):
    """Base output parameter specification."""

    expected_schema: list[SchemaFieldSpec] | None = Field(default=None)
    options: dict | None = Field(default_factory=dict)


class OutputBaseSpec(BaseSpec):
    """Base output specification."""

    input_df: str | None = Field(default=None)


class IOBase:
    """Represents an IO object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _spec (BaseSpec): The specification object or child specification derirved from it.
        _logger (Logger): The logger object.

    """

    def __init__(self, spark: SparkSession, logger: Logger):
        """Initializes an instance of the IO class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.

        """
        self._spark = spark
        self._logger = logger
