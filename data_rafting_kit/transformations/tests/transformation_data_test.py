import json
from collections import OrderedDict
from pathlib import Path

import pytest
from pydantic import ValidationError
from pyspark.testing import assertDataFrameEqual

from data_rafting_kit.common.test_utils import (
    extract_and_convert_model_name_to_file_name,
)
from data_rafting_kit.transformations.transformation_factory import (
    TransformationFactory,
)
from data_rafting_kit.transformations.transformation_spec import (
    ALL_TRANSFORMATION_SPECS,
    TransformationRootSpec,
)


@pytest.mark.parametrize("transformation_spec_model", ALL_TRANSFORMATION_SPECS)
def test_transformation_data(transformation_spec_model, spark_session, logger):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        transformation_spec_model (Pydantic BaseModel): The transformation model to test.
        spark_session (SparkSession): The Spark session fixture.
        logger (FakeLogger): The fake logger fixture.
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
            "params": mock_dataset["spec"],
        }

        try:
            transformation_spec = TransformationRootSpec.model_validate(mock_spec)

            input_rows_df = spark_session.createDataFrame(mock_dataset["input_rows"])
            dfs = OrderedDict()
            dfs["input_df"] = input_rows_df

            TransformationFactory(spark_session, logger, dfs).process_transformation(
                transformation_spec.root
            )

            output_rows = spark_session.createDataFrame(mock_dataset["output_rows"])

            assertDataFrameEqual(dfs["test_transformation"], output_rows)

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
