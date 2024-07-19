# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import Field

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.io.io_spec import InputRootSpec


class LocalTestingSpec(BaseParamSpec):
    """Local testing specification."""

    expect_failure: bool | None = Field(default=False)
    mock_inputs: list[InputRootSpec]
    expected_outputs: list[InputRootSpec] | None = Field(default_factory=list)
