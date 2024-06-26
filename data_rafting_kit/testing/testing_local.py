# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import BaseModel, Field

from data_rafting_kit.io.io_spec import InputRootSpec


class LocalTestingSpec(BaseModel):
    """Local testing specification."""

    expect_failure: bool | None = Field(default=False)
    mock_inputs: list[InputRootSpec]
    expected_outputs: list[InputRootSpec] | None = Field(default_factory=list)
