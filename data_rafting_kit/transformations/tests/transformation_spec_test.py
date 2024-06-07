from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from data_rafting_kit.common.test_utils import (
    extract_and_convert_model_name_to_file_name,
)
from data_rafting_kit.transformations.transformation_spec import (
    ALL_TRANSFORMATION_SPECS,
)


@pytest.mark.parametrize("transformation_spec_model", ALL_TRANSFORMATION_SPECS)
def test_transformation_spec_loads(transformation_spec_model):
    """Test that the transformation spec can be loaded from the mock spec file.

    Args:
    ----
    transformation_spec_model (Pydantic BaseModel): The transformation model to test.
    """
    pattern = r"^(Pyspark|Presido)(.*)TransformationSpec$"
    mock_directory, mock_spec_file_name = extract_and_convert_model_name_to_file_name(
        transformation_spec_model.__name__, pattern
    )

    # Check if the file exists
    try:
        with open(
            Path(
                f"./data_rafting_kit/transformations/tests/mock_specs/{mock_directory}/{mock_spec_file_name}.yaml"
            )
        ) as file:
            mock_spec = yaml.safe_load(file)
    except FileNotFoundError:
        pytest.fail(
            f"Mock spec file not found for transformation {transformation_spec_model.__name__}."
        )

    try:
        transformation_spec_model.model_validate(mock_spec)
    except ValidationError as e:
        print(f"Full loaded spec: {mock_spec}")
        for error in e.errors():
            print(
                "Config Location: {} -> {} -> Found: {}".format(
                    error["loc"], error["msg"], error["input"]
                )
            )

        pytest.fail(
            f"Failed to load mock spec into model for transformation {transformation_spec_model.__name__}."
        )
