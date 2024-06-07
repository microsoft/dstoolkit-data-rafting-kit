from collections import OrderedDict
from enum import StrEnum
from logging import Logger

from pydantic import Field
from pyspark.sql import SparkSession

from data_rafting_kit.common.base_spec import BaseSpec


class DataQualityExpectationEnum(StrEnum):
    """Enumeration of different types of data quality expectations."""

    GREAT_EXPECTATIONS = "great_expectations"


class DataQualityBaseSpec(BaseSpec):
    """Base output specification."""

    input_df: str | None = Field(default=None)


class DataQualityResult:
    """Represents the result of a data quality check."""

    def __init__(self, success, message):
        """Initializes an instance of the DataQualityResult class."""
        self.success = success
        self.message = message


class DataQualityBase:
    """Represents a data quality object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _logger (Logger): The logger object.
        _dfs (OrderedDict): The ordered dictionary of DataFrames.

    """

    def __init__(self, spark: SparkSession, logger: Logger, dfs: OrderedDict):
        """Initializes an instance of the data quality class class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (OrderedDict): The ordered dictionary of DataFrames.

        """
        self._spark = spark
        self._logger = logger
        self._dfs = dfs
