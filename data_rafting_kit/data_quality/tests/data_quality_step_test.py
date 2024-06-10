import json
from collections import OrderedDict
from pathlib import Path

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from data_rafting_kit.common.test_utils import (
    extract_and_convert_model_name_to_file_name,
)
from data_rafting_kit.data_quality.data_quality_factory import DataQualityFactory
from data_rafting_kit.data_quality.data_quality_spec import (
    ALL_DATA_QUALITY_SPECS,
    DataQualityRootSpec,
)
from data_rafting_kit.data_quality.great_expectations import DataQualityModeEnum


@pytest.fixture(scope="session")
def spark_session():
    """Fixture to set up Spark session for all tests."""
    spark = (
        SparkSession.builder.appName("DataQualityTests")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def logger():
    """Fixture to set up a fake logger for all tests."""

    class FakeLogger:
        def __init__(self):
            self.logs = []

        def info(self, message):
            self.logs.append(f"INFO: {message}")

        def error(self, message):
            self.logs.append(f"ERROR: {message}")

        def get_logs(self):
            return self.logs

    logger = FakeLogger()
    yield logger


@pytest.mark.parametrize("data_quality_spec_model", ALL_DATA_QUALITY_SPECS)
def test_data_quality_check(data_quality_spec_model, spark_session, logger):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        data_quality_spec_model (Pydantic BaseModel): The data quality model to test.
        spark_session (SparkSession): The Spark session fixture.
        logger (FakeLogger): The fake logger fixture.
    """
    pattern = r"^(GreatExpectations)(.*)DataQualitySpec$"
    mock_directory, mock_data_file_name = extract_and_convert_model_name_to_file_name(
        data_quality_spec_model.__name__, pattern
    )

    # Check if the file exists
    try:
        with open(
            Path(
                f"./data_rafting_kit/data_quality/tests/mock_data/{mock_directory}/mock_{mock_data_file_name}.json"
            )
        ) as file:
            mock_data = json.load(file)
    except FileNotFoundError:
        pytest.fail(
            f"Mock data file not found for data quality {data_quality_spec_model.__name__}."
        )

    for mock_dataset in mock_data["mock_data"]:
        input_rows_df = spark_session.createDataFrame(mock_dataset["input_rows"])

        # Test the data quality spec
        mock_spec = {
            "name": "test_dq",
            "mode": DataQualityModeEnum.SEPARATE,
            "unique_column_identifiers": mock_data["unique_column_identifiers"],
            "checks": [{"type": mock_data_file_name, "params": mock_dataset["spec"]}],
        }

        try:
            data_quality_check_spec = DataQualityRootSpec.model_validate(mock_spec)

            passing_rows = spark_session.createDataFrame(mock_dataset["passing_rows"])

            dfs = OrderedDict()
            dfs["input_df"] = input_rows_df

            DataQualityFactory(spark_session, logger, dfs).process_data_quality(
                data_quality_check_spec
            )

            assertDataFrameEqual(dfs["test_dq"], passing_rows)

            if len(mock_dataset["failing_rows"]) > 0:
                failing_rows = spark_session.createDataFrame(
                    mock_dataset["failing_rows"]
                )
                assertDataFrameEqual(dfs["test_dq_fails"], failing_rows)

        except ValidationError as e:
            print(f"Full loaded spec: {mock_spec}")
            for error in e.errors():
                print(
                    "Config Location: {} -> {} -> Found: {}".format(
                        error["loc"], error["msg"], error["input"]
                    )
                )

            pytest.fail(
                f"Failed to load data spec into model for data quality {data_quality_spec_model.__name__}."
            )

        # Try with fail mode
        mock_spec["mode"] = DataQualityModeEnum.FAIL

        try:
            data_quality_check_spec = DataQualityRootSpec.model_validate(mock_spec)

            dfs = OrderedDict()
            dfs["input_df"] = input_rows_df

            if mock_dataset["fails"]:
                with pytest.raises(ValueError):
                    DataQualityFactory(spark_session, logger, dfs).process_data_quality(
                        data_quality_check_spec
                    )
            else:
                DataQualityFactory(spark_session, logger, dfs).process_data_quality(
                    data_quality_check_spec
                )

        except ValidationError as e:
            print(f"Full loaded spec: {mock_spec}")
            for error in e.errors():
                print(
                    "Config Location: {} -> {} -> Found: {}".format(
                        error["loc"], error["msg"], error["input"]
                    )
                )

            pytest.fail(
                f"Failed to load data spec into model for data quality {data_quality_spec_model.__name__}."
            )
