# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import BaseModel


class BaseSpec(BaseModel):
    """Base class for all spec classes."""

    name: str
