from typing import Literal

import pyspark.sql.functions as f
from pydantic import BaseModel, Field
from pyspark.sql import DataFrame

from data_rafting_kit.transformations.transformation_base import (
    TransformationBase,
    TransformationBaseSpec,
    TransformationEnum,
)

# Store the transformations we can infer automatically from the PySpark API. Here we can avoid writing specs
PYSPARK_DYNAMIC_TRANSFORMATIONS = (
    TransformationEnum.AGG,
    TransformationEnum.DISTINCT,
    TransformationEnum.DROP,
    TransformationEnum.DROP_DUPLICATES,
    TransformationEnum.FILTER,
    TransformationEnum.WITH_COLUMNS_RENAMED,
)


class PysparkJoinTransformationParamSpec(BaseModel):
    """PySpark Join Transformation Parameters."""

    other_df: str
    join_on: list[str]
    how: str | None = Field(default="inner")


class PysparkJoinTransformationSpec(TransformationBaseSpec):
    """PySpark Join transformation specification."""

    type: Literal[TransformationEnum.JOIN]
    params: PysparkJoinTransformationParamSpec


class PySparkColumnExpressionSpec(BaseModel):
    """PySpark Column Expression Specification."""

    name: str
    expr: str


class PysparkWithColumnsTransformationParamSpec(BaseModel):
    """PySpark With Columns Transformation Parameters."""

    columns: list[PySparkColumnExpressionSpec]


class PysparkWithColumnsTransformationSpec(TransformationBaseSpec):
    """PySpark Join transformation specification."""

    type: Literal[TransformationEnum.WITH_COLUMNS]
    params: PysparkWithColumnsTransformationParamSpec


PYSPARK_TRANSFORMATION_SPECS = [
    PysparkJoinTransformationSpec,
    PysparkWithColumnsTransformationSpec,
]


class PysparkTransformation(TransformationBase):
    """Represents a PySpark transformation object for data pipelines."""

    def join(
        self, spec: PysparkJoinTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Joins two DataFrames according to the spec.

        Args:
        ----
            spec (PysparkJoinTransformationSpec): The PySpark Join Transformation parameter specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Joining DataFrames...")

        other_df = self._dfs[spec.params.other_df]

        return input_df.join(
            other=other_df, on=spec.params.join_on, how=spec.params.how
        )

    def with_columns(
        self, spec: PysparkWithColumnsTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Adds columns to a DataFrame according to the spec.

        Args:
        ----
            spec (PysparkWithColumnsTransformationSpec): The PySpark With Columns Transformation parameter specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Adding columns to DataFrame...")

        with_columns_map = {
            column.name: f.expr(column.expr) for column in spec.params.columns
        }

        return input_df.withColumns(with_columns_map)
