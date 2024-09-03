# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from data_rafting_kit.common.test_utils import (
    extract_and_convert_model_name_to_file_name,
)
from data_rafting_kit.data_quality.checks import (
    dynamic_great_expectations_data_quality_models,
)


@pytest.mark.parametrize(
    "data_quality_spec_model", dynamic_great_expectations_data_quality_models
)
def test_data_quality_yaml_spec_loads(data_quality_spec_model):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        data_quality_spec_model (BaseParamSpec): The data quality model to test.
    """
    pattern = r"^(Checks)(.*)DataQualitySpec$"
    mock_directory, mock_spec_file_name = extract_and_convert_model_name_to_file_name(
        data_quality_spec_model.__name__, pattern
    )

    # Check if the file exists
    try:
        with open(
            Path(
                f"./data_rafting_kit/data_quality/tests/mock_specs/{mock_directory}/mock_{mock_spec_file_name}.yaml"
            )
        ) as file:
            mock_spec = yaml.safe_load(file)

    except FileNotFoundError:
        pytest.fail(
            f"Mock spec file not found for data quality {data_quality_spec_model.__name__}."
        )

    try:
        data_quality_spec_model.model_validate(mock_spec)
    except ValidationError as e:
        print(f"Full loaded spec: {mock_spec}")
        for error in e.errors():
            print(
                "Config Location: {} -> {} -> Found: {}".format(
                    error["loc"], error["msg"], error["input"]
                )
            )

        pytest.fail(
            f"Failed to load mock spec into model for data quality {data_quality_spec_model.__name__}."
        )


@pytest.mark.parametrize(
    "data_quality_spec_model", dynamic_great_expectations_data_quality_models
)
def test_data_quality_json_spec_loads(data_quality_spec_model):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
        data_quality_spec_model (Pydantic BaseModel): The data quality model to test.
    """
    pattern = r"^(Checks)(.*)DataQualitySpec$"
    mock_directory, mock_spec_file_name = extract_and_convert_model_name_to_file_name(
        data_quality_spec_model.__name__, pattern
    )

    # Check if the file exists
    try:
        with open(
            Path(
                f"./data_rafting_kit/data_quality/tests/mock_specs/{mock_directory}/mock_{mock_spec_file_name}.json"
            )
        ) as file:
            mock_spec = json.load(file)

    except FileNotFoundError:
        pytest.fail(
            f"Mock spec file not found for data quality {data_quality_spec_model.__name__}."
        )

    try:
        data_quality_spec_model.model_validate(mock_spec)
    except ValidationError as e:
        print(f"Full loaded spec: {mock_spec}")
        for error in e.errors():
            print(
                "Config Location: {} -> {} -> Found: {}".format(
                    error["loc"], error["msg"], error["input"]
                )
            )

        pytest.fail(
            f"Failed to load mock spec into model for data quality {data_quality_spec_model.__name__}."
        )
