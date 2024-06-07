from collections import OrderedDict
from logging import Logger

from pyspark.sql import SparkSession


class BaseFactory:
    """Represents a base factory object for data pipelines. All factories should inherit from this class."""

    def __init__(self, spark: SparkSession, logger: Logger, dfs: OrderedDict):
        """Initializes an instance of the Factory class.

        Args:
        ----
            spark (SparkSession): The SparkSession object.
            logger (Logger): The logger object.
            dfs (OrderedDict): The ordered dictionary of DataFrames.
        """
        self._spark = spark
        self.logger = logger
        self._dfs = dfs
