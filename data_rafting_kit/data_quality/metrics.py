from typing import Literal

from pydantic import Field

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityBase,
    DataQualityBaseSpec,
    DataQualityEnum,
)


class TimelinessMetricsDataQualityParamSpec(BaseParamSpec):
    """Timeliness metrics specification."""

    column: str = Field(
        ..., description="The name of the column to check for timeliness."
    )
    min_timestamp: str = Field(..., description="The minimum timestamp for the column.")


class MetricsDataQualityParamSpec(BaseParamSpec):
    """Data quality metrics specification."""

    timeliness: list[TimelinessMetricsDataQualityParamSpec] | None = Field(
        ..., default=None
    )


class MetricsDataQualitySpec(DataQualityBaseSpec):
    """Data quality metrics specification."""

    input_df: str | None = Field(
        ..., description="The name of the input DataFrame.", default=None
    )
    type: Literal[DataQualityEnum.METRICS] = Field(
        ..., description="The data quality expectation type."
    )
    params: MetricsDataQualityParamSpec = Field(
        ..., description="The data quality metrics parameters."
    )


class MetricsDataQuality(DataQualityBase):
    """Data quality metrics class."""

    pass
