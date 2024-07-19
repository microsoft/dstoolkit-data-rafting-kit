# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import Field

from data_rafting_kit.common.base_spec import BaseParamSpec
from data_rafting_kit.testing.testing_local import LocalTestingSpec


class TestingRootSpec(BaseParamSpec):
    """Root testing specification."""

    local: list[LocalTestingSpec] | None = Field(default_factory=list)
