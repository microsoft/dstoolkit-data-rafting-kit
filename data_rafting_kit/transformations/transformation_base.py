from collections import OrderedDict
from enum import StrEnum
from logging import Logger

from pydantic import Field
from pyspark.sql import SparkSession

from data_rafting_kit.common.base_spec import BaseSpec


class TransformationEnum(StrEnum):
    """Enumeration class for transformation types."""

    AGG = "agg"
    ANONYMIZE = "anonymize"
    DISTINCT = "distinct"
    DROP = "drop"
    DROP_DUPLICATES = "drop_duplicates"
    FILTER = "filter"
    GROUP_BY = "group_by"
    INTERSECT = "intersect"
    JOIN = "join"
    WITH_COLUMNS = "with_columns"
    WITH_COLUMNS_RENAMED = "with_columns_renamed"
    WINDOW = "window"
    SELECT = "select"


class TransformationBaseSpec(BaseSpec):
    """Base output specification."""

    input_df: str | None = Field(default=None)


class TransformationBase:
    """Represents a transformation object for data pipelines.

    Attributes
    ----------
        _spark (SparkSession): The SparkSession object.
        _logger (Logger): The logger object.
        _dfs (OrderedDict): The ordered dictionary of DataFrames.

    """

    def __init__(self, spark: SparkSession, logger: Logger, dfs: OrderedDict, env):
        """Initializes an instance of the Transformation class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (OrderedDict): The ordered dictionary of DataFrames.
            env (EnvSpec): The environment specification.

        """
        self._spark = spark
        self._logger = logger
        self._dfs = dfs
        self._env = env
