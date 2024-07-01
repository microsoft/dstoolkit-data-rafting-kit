# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import BaseModel, ConfigDict


class BaseSpec(BaseModel):
    """Base class for all spec classes."""

    name: str

    model_config = ConfigDict(
        validate_default=True,
        extra_values="forbid",
    )


class BaseParamSpec(BaseModel):
    """Base class for all parameter spec classes."""

    model_config = ConfigDict(
        validate_default=True, extra_values="forbid", populate_by_name=True
    )
