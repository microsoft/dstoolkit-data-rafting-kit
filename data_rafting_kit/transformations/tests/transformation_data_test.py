import json
from collections import OrderedDict
from pathlib import Path

import pytest
from pydantic import ValidationError
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from data_rafting_kit.common.test_utils import (
    env_spec,  # noqa
    extract_and_convert_model_name_to_file_name,
    logger,  # noqa
    spark_session,  # noqa
)
from data_rafting_kit.transformations.transformation_factory import (
    TransformationFactory,
)
from data_rafting_kit.transformations.transformation_spec import (
    ALL_TRANSFORMATION_SPECS,
    TransformationRootSpec,
)


@pytest.mark.parametrize("transformation_spec_model", ALL_TRANSFORMATION_SPECS)
def test_transformation_data(
    transformation_spec_model, spark_session, logger, env_spec  # noqa
):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        transformation_spec_model (Pydantic BaseModel): The transformation model to test.
        spark_session (SparkSession): The Spark session fixture.
        logger (FakeLogger): The fake logger fixture.
        env_spec (EnvSpec): The fake environment spec fixture.
    """
    pattern = r"^(Pyspark|Presido)(.*)TransformationSpec$"
    mock_directory, mock_data_file_name = extract_and_convert_model_name_to_file_name(
        transformation_spec_model.__name__, pattern
    )

    # Check if the file exists
    try:
        with open(
            Path(
                f"./data_rafting_kit/transformations/tests/mock_data/{mock_directory}/mock_{mock_data_file_name}.json"
            )
        ) as file:
            mock_data = json.load(file)
    except FileNotFoundError:
        pytest.fail(
            f"Mock data file not found for transformation {transformation_spec_model.__name__}."
        )

    for mock_dataset in mock_data["mock_data"]:
        # Test the transformation spec
        mock_spec = {
            "name": "test_transformation",
            "type": mock_data_file_name,
        }

        if len(mock_dataset["spec"]) > 0:
            mock_spec["params"] = mock_dataset["spec"]

        try:
            transformation_spec = TransformationRootSpec.model_validate(mock_spec)

            input_rows_df = spark_session.createDataFrame(mock_dataset["input_rows"])
            dfs = OrderedDict()

            if "input_rows_2" in mock_dataset:
                input_rows_df_2 = spark_session.createDataFrame(
                    mock_dataset["input_rows_2"]
                )
                dfs["input_2_df"] = input_rows_df_2

            dfs["input_df"] = input_rows_df

            TransformationFactory(
                spark_session, logger, dfs, env_spec
            ).process_transformation(transformation_spec.root)

            if mock_data_file_name == "window":
                expected_output_rows = spark_session.createDataFrame(
                    mock_dataset["output_rows"],
                    schema=StructType(
                        [
                            StructField("partition_column", StringType(), True),
                            StructField("order_column", LongType(), True),
                            StructField("data_column", StringType(), True),
                            StructField("rank_column", IntegerType(), False),
                        ]
                    ),
                )
            else:
                expected_output_rows = spark_session.createDataFrame(
                    mock_dataset["output_rows"]
                )

            # Alphabetic sort of columns to ensure order is consistent
            output_rows_sorted_columns = sorted(dfs["test_transformation"].columns)
            output_rows = dfs["test_transformation"].select(*output_rows_sorted_columns)

            expected_output_rows_sorted_columns = sorted(expected_output_rows.columns)
            expected_output_rows = expected_output_rows.select(
                *expected_output_rows_sorted_columns
            )

            assertDataFrameEqual(output_rows, expected_output_rows)

        except ValidationError as e:
            print(f"Full loaded spec: {mock_spec}")
            for error in e.errors():
                print(
                    "Config Location: {} -> {} -> Found: {}".format(
                        error["loc"], error["msg"], error["input"]
                    )
                )

            pytest.fail(
                f"Failed to load data spec into model for transformation {transformation_spec_model.__name__}."
            )
