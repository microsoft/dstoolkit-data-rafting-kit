# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import BaseModel, Field

from data_rafting_kit.testing.testing_local import LocalTestingSpec


class TestingRootSpec(BaseModel):
    """Root testing specification."""

    local: list[LocalTestingSpec] | None = Field(default_factory=list)
