from typing import Literal

from pydantic import Field

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBaseSpec,
    DataQualityEnum,
)


class TimelinessMetricsParamSpec(BaseParamSpec):
    """Timeliness metrics specification."""

    column: str = Field(
        ..., description="The name of the column to check for timeliness."
    )
    min_timestamp: str = Field(..., description="The minimum timestamp for the column.")


class DataQualityMetricsParamSpec(BaseParamSpec):
    """Data quality metrics specification."""

    timeliness: list[TimelinessMetricsParamSpec] | None = Field(..., default=None)


class DataQualityMetricsSpec(DataQualityBaseSpec):
    """Data quality metrics specification."""

    input_df: str | None = Field(
        ..., description="The name of the input DataFrame.", default=None
    )
    type: Literal[DataQualityEnum.METRICS] = Field(
        ..., description="The data quality expectation type."
    )
    params: DataQualityMetricsParamSpec = Field(
        ..., description="The data quality metrics parameters."
    )
