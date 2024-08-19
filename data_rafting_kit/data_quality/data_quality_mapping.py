# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from data_rafting_kit.data_quality.checks import (
    ChecksDataQuality,
)
from data_rafting_kit.data_quality.data_quality_base import (
    DataQualityEnum,
)


class DataQualityMapping:
    """Represents a mapping from data quality expectation types to their corresponding functions."""

    @staticmethod
    def get_data_quality_map(key: DataQualityEnum):
        """Returns the function for the given data quality expectation type.

        Args:
        ----
            key (DataQualityExpectationEnum): The data quality expectation type.

        Raises:
        ------
            ValueError: If df is None.
            NotImplementedError: If the given data quality expectation type is not implemented.
        """
        print(key)
        if key == DataQualityEnum.CHECKS:
            return (
                ChecksDataQuality,
                ChecksDataQuality.expectation,
            )
        else:
            raise NotImplementedError(
                f"Data Quality Expectation Type {key} not implemented"
            )
