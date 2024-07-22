# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from pyspark.testing import assertDataFrameEqual

from data_rafting_kit.common.pipeline_dataframe_holder import PipelineDataframeHolder
from data_rafting_kit.common.test_utils import (
    env_spec,  # noqa
    extract_and_convert_model_name_to_file_name,
    logger,  # noqa
    spark_session,  # noqa
)
from data_rafting_kit.data_quality.data_quality_factory import DataQualityFactory
from data_rafting_kit.data_quality.data_quality_spec import (
    ALL_DATA_QUALITY_SPECS,
    DataQualityRootSpec,
)
from data_rafting_kit.data_quality.great_expectations import DataQualityModeEnum


def run_data_quality_check(  # noqa: PLR0913
    mode,
    mock_spec,
    mock_dataset,
    spark_session,  # noqa
    logger,  # noqa
    env_spec,  # noqa
):
    """Run the data quality check for the given spec and dataset.

    Args:
    ----
        mode (DataQualityModeEnum): The mode of the data quality check.
        mock_spec (dict): The mock data quality spec.
        mock_dataset (dict): The mock data quality dataset.
        spark_session (SparkSession): The Spark session fixture.
        logger (FakeLogger): The fake logger fixture.
        env_spec (EnvSpec): The environment spec.
    """
    data_quality_check_spec = DataQualityRootSpec.model_validate(mock_spec)

    input_rows_df = spark_session.createDataFrame(mock_dataset["input_rows"])
    passing_rows = spark_session.createDataFrame(
        mock_dataset["passing_rows"], input_rows_df.schema
    )

    failing_rows = spark_session.createDataFrame(
        mock_dataset["failing_rows"], input_rows_df.schema
    )

    dfs = PipelineDataframeHolder()
    dfs["input_df"] = input_rows_df

    DataQualityFactory(spark_session, logger, dfs, env_spec).process_data_quality(
        data_quality_check_spec
    )

    if mode == DataQualityModeEnum.SEPARATE:
        print("Expected DataFrame:")
        passing_rows.show()
        print("Actual DataFrame:")
        dfs["test_dq"].show()

        assertDataFrameEqual(dfs["test_dq"], passing_rows)
        assertDataFrameEqual(dfs["test_dq_fails"], failing_rows)


@pytest.mark.parametrize("data_quality_spec_model", ALL_DATA_QUALITY_SPECS)
def test_data_quality_data(
    data_quality_spec_model,
    spark_session,  # noqa
    logger,  # noqa
    env_spec,  # noqa
):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        data_quality_spec_model (BaseParamSpec): The data quality model to test.
        spark_session (SparkSession): The Spark session fixture.
        logger (FakeLogger): The fake logger fixture.
        env_spec (EnvSpec): The environment spec.
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

    for mode in [DataQualityModeEnum.SEPARATE, DataQualityModeEnum.FAIL]:
        for mock_dataset in mock_data["mock_data"]:
            # Test the data quality spec
            mock_spec = {
                "name": "test_dq",
                "params": {
                    "mode": mode,
                    "unique_column_identifiers": mock_data["unique_column_identifiers"],
                    "checks": [
                        {"type": mock_data_file_name, "params": mock_dataset["spec"]}
                    ],
                },
            }

            try:
                if mode == DataQualityModeEnum.FAIL and mock_dataset["fails"]:
                    with pytest.raises(ValueError):
                        run_data_quality_check(
                            mode,
                            mock_spec,
                            mock_dataset,
                            spark_session,
                            logger,
                            env_spec,
                        )
                else:
                    run_data_quality_check(
                        mode, mock_spec, mock_dataset, spark_session, logger, env_spec
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
