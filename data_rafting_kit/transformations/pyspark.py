# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Literal

import pyspark.sql.functions as f
from pydantic import Field, model_validator
from pyspark.sql import DataFrame, Window

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.transformations.transformation_base import (
    TransformationBase,
    TransformationBaseSpec,
    TransformationEnum,
)

# Store the transformations we can infer automatically from the PySpark API. Here we can avoid writing specs
PYSPARK_DYNAMIC_TRANSFORMATIONS = (
    TransformationEnum.DISTINCT,
    TransformationEnum.DROP_DUPLICATES,
    TransformationEnum.FILTER,
    TransformationEnum.WITH_COLUMNS_RENAMED,
    TransformationEnum.FILL_NA,
    TransformationEnum.LIMIT,
    TransformationEnum.OFFSET,
    TransformationEnum.DROP_NA,
)


class PysparkOrderByColumnSpec(BaseParamSpec):
    """PySpark Column Expression Specification."""

    name: str
    ascending: bool | None = Field(default=True)


class PysparkOrderByTransformationParamSpec(BaseParamSpec):
    """PySpark OrderBy Transformation Parameters."""

    columns: list[str] | list[PysparkOrderByColumnSpec]

    @model_validator(mode="after")
    def validate_order_by_transformation_param_spec(self):
        """Validate the Order By Transformation Parameters."""
        if isinstance(self.columns, list) and isinstance(self.columns[0], str):
            self.columns = [
                PysparkOrderByColumnSpec(name=column) for column in self.columns
            ]

        return self


class PysparkOrderByTransformationSpec(TransformationBaseSpec):
    """PySpark OrderBy transformation specification."""

    type: Literal[TransformationEnum.ORDER_BY]
    params: PysparkOrderByTransformationParamSpec


class PysparkDropColumnSpec(BaseParamSpec):
    """PySpark Column Expression Specification."""

    name: str


class PysparkDropTransformationParamSpec(BaseParamSpec):
    """PySpark Drop Transformation Parameters."""

    columns: list[PysparkDropColumnSpec] | list[str]

    @model_validator(mode="after")
    def validate_drop_transformation_param_spec(self):
        """Validate the Drop Select Transformation Parameters."""
        if isinstance(self.columns, list) and isinstance(self.columns[0], str):
            self.columns = [
                PysparkDropColumnSpec(name=column) for column in self.columns
            ]

        return self


class PysparkDropTransformationSpec(TransformationBaseSpec):
    """PySpark Drop transformation specification."""

    type: Literal[TransformationEnum.DROP]
    params: PysparkDropTransformationParamSpec


class PysparkJoinTransformationParamSpec(BaseParamSpec):
    """PySpark Join Transformation Parameters."""

    other_df: str
    join_on: list[str]
    how: str | None = Field(default="inner")


class PysparkJoinTransformationSpec(TransformationBaseSpec):
    """PySpark Join transformation specification."""

    type: Literal[TransformationEnum.JOIN]
    params: PysparkJoinTransformationParamSpec


class PysparkWithColumnExpressionSpec(BaseParamSpec):
    """PySpark Column Expression Specification."""

    name: str
    expr: str


class PysparkSelectExpressionSpec(BaseParamSpec):
    """PySpark Column Expression Specification."""

    name: str
    expr: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_pyspark_select_expression_spec(self):
        """Validate the PySpark Select Expression Specification."""
        if self.expr is None:
            self.expr = self.name

        return self


class PysparkWithColumnsTransformationParamSpec(BaseParamSpec):
    """PySpark With Columns Transformation Parameters."""

    columns: list[PysparkWithColumnExpressionSpec]


class PysparkWithColumnsTransformationSpec(TransformationBaseSpec):
    """PySpark With Columns transformation specification."""

    type: Literal[TransformationEnum.WITH_COLUMNS]
    params: PysparkWithColumnsTransformationParamSpec


class PysparkWindowFunctionParamSpec(BaseParamSpec):
    """Parameters for the custom window function transformation."""

    partition_by: list[str]
    order_by: list[str]
    window_function: str
    column: str | None = None
    result_column: str
    offset: int | None = None
    default_value: str | None = None

    @model_validator(mode="after")
    def check_column(self):
        """Validate the presence of the column field based on the window_function."""
        column_required_functions = [
            "avg",
            "sum",
            "min",
            "max",
            "count",
            "first",
            "last",
            "lead",
            "lag",
        ]
        if self.window_function in column_required_functions and self.column is None:
            raise ValueError(
                f"{self.window_function} requires a column to be specified."
            )
        return self


class PysparkWindowTransformationSpec(TransformationBaseSpec):
    """PySpark window function transformation specification."""

    type: Literal[TransformationEnum.WINDOW]
    params: PysparkWindowFunctionParamSpec


class PysparkSelectTransformationParamSpec(BaseParamSpec):
    """PySpark Select Transformation Parameters."""

    columns: list[PysparkSelectExpressionSpec] | str

    @model_validator(mode="after")
    def validate_select_transformation_param_spec(self):
        """Validate the PySpark Select Transformation Parameters."""
        if isinstance(self.columns, list) and isinstance(self.columns[0], str):
            self.columns = [
                PysparkSelectExpressionSpec(name=column) for column in self.columns
            ]

        return self


class PysparkSelectTransformationSpec(TransformationBaseSpec):
    """PySpark Select transformation specification."""

    type: Literal[TransformationEnum.SELECT]
    params: PysparkSelectTransformationParamSpec


PYSPARK_TRANSFORMATION_SPECS = [
    PysparkJoinTransformationSpec,
    PysparkWithColumnsTransformationSpec,
    PysparkWindowTransformationSpec,
    PysparkSelectTransformationSpec,
    PysparkDropTransformationSpec,
    PysparkOrderByTransformationSpec,
]


class PysparkTransformation(TransformationBase):
    """Represents a PySpark transformation object for data pipelines."""

    def order_by(
        self, spec: PysparkOrderByTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Orders the DataFrame according to the spec.

        Args:
        ----
            spec (PysparkOrderByTransformationSpec): The PySpark OrderBy Transformation parameter specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Ordering DataFrame...")

        columns = [
            f.col(column.name).asc() if column.ascending else f.col(column.name).desc()
            for column in spec.params.columns
        ]

        return input_df.orderBy(*columns)

    def drop(
        self, spec: PysparkDropTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Drops columns from a DataFrame according to the spec.

        Args:
        ----
            spec (PysparkDropTransformationSpec): The PySpark Drop Transformation parameter specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Dropping columns from DataFrame...")

        columns = [column.name for column in spec.params.columns]

        return input_df.drop(*columns)

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

    def window(
        self, spec: PysparkWindowTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Applies a custom window function to the input DataFrame.

        Args:
        ----
            spec (PysparkWindowTransformationSpec): The window function transformation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame with the applied window function.

        """
        self._logger.info("Applying window function...")

        window_spec = Window.partitionBy(*spec.params.partition_by).orderBy(
            *spec.params.order_by
        )
        window_function = getattr(f, spec.params.window_function)

        column_required_functions = [
            "avg",
            "sum",
            "min",
            "max",
            "count",
            "first",
            "last",
        ]
        offset_required_functions = ["lead", "lag"]

        if spec.params.window_function in column_required_functions:
            return input_df.withColumn(
                spec.params.result_column,
                window_function(spec.params.column).over(window_spec),
            )
        elif spec.params.window_function in offset_required_functions:
            offset = spec.params.offset or 1
            default_value = spec.params.default_value or None
            return input_df.withColumn(
                spec.params.result_column,
                window_function(spec.params.column, offset, default_value).over(
                    window_spec
                ),
            )
        else:
            return input_df.withColumn(
                spec.params.result_column, window_function().over(window_spec)
            )

    def select(
        self, spec: PysparkSelectTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Selects specific columns from a DataFrame according to the spec.

        Args:
        ----
            spec (PysparkSelectTransformationSpec): The PySpark Select Transformation parameter specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame.

        """
        self._logger.info("Selecting columns from DataFrame...")

        columns = [
            f.expr(column.expr).alias(column.name) for column in spec.params.columns
        ]

        return input_df.select(*columns)
