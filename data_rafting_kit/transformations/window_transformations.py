from typing import Literal

import pyspark.sql.functions as f
from pydantic import BaseModel
from pyspark.sql import DataFrame, Window

from data_rafting_kit.transformations.transformation_base import (
    TransformationBase,
    TransformationBaseSpec,
)


class WindowFunctionParamSpec(BaseModel):
    """Parameters for the custom window function transformation."""

    partition_by: list[str]
    order_by: list[str]
    window_function: str
    column: str


class WindowTransformationSpec(TransformationBaseSpec):
    """Window function transformation specification."""

    type: Literal["window_function"]
    params: WindowFunctionParamSpec


class WindowTransformation(TransformationBase):
    """Represents a window function transformation object for data pipelines."""

    def apply_window_function(
        self, spec: WindowTransformationSpec, input_df: DataFrame
    ) -> DataFrame:
        """Applies a custom window function to the input DataFrame.

        Args:
        ----
            spec (WindowTransformationSpec): The window function transformation specification.
            input_df (DataFrame): The input DataFrame.

        Returns:
        -------
            DataFrame: The resulting DataFrame with the applied window function.
        """
        window_spec = Window.partitionBy(*spec.params.partition_by).orderBy(
            *spec.params.order_by
        )
        window_function = getattr(f, spec.params.window_function)
        return input_df.withColumn(
            spec.params.column, window_function().over(window_spec)
        )
