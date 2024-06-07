from pydantic import BaseModel

from data_rafting_kit.data_quality_checks.data_quality_base import (
    DataQualityExpectationEnum,
)

# Store the data quality checks
DATA_QUALITY_CHECKS = (
    DataQualityExpectationEnum.expect_column_to_exist,
    DataQualityExpectationEnum.expect_column_values_to_be_in_set,
    DataQualityExpectationEnum.expect_column_values_to_be_unique,
    # Add more dynamic data quality checks as needed
)


class ColumnExistCheckParams(BaseModel):
    """Parameters for the 'column exists' data quality check."""

    column: str


class ColumnValuesInSetCheckParams(BaseModel):
    """Parameters for the 'column values in set' data quality check."""

    column: str
    value_set: list[str]


class ColumnValuesUniqueParams(BaseModel):
    """Parameters for the 'column values unique' data quality check."""

    column: str


class DataQualityBaseSpec(BaseModel):
    """Base data quality expectation specification."""

    type: DataQualityExpectationEnum
    params: (
        ColumnExistCheckParams | ColumnValuesInSetCheckParams | ColumnValuesUniqueParams
    )
