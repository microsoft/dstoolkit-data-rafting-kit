# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from pydantic import Field

from data_rafting_kit.common.base_spec import BaseRootModel
from data_rafting_kit.io.console import ConsoleOutputSpec
from data_rafting_kit.io.delta_table import DeltaTableInputSpec, DeltaTableOutputSpec
from data_rafting_kit.io.event_hub import EventHubInputSpec, EventHubOutputSpec
from data_rafting_kit.io.file import FileInputSpec, FileOutputSpec


# The following classes are used to define the root specifications for the input and output.
class InputRootSpec(BaseRootModel):
    """Root input specification. This class is used to automatically switch between input specs based on the discriminator field."""

    root: DeltaTableInputSpec | FileInputSpec | EventHubInputSpec = Field(
        ..., discriminator="type"
    )


class OutputRootSpec(BaseRootModel):
    """Root output specification. This class is used to automatically switch between output specs based on the discriminator field."""

    root: (
        DeltaTableOutputSpec | FileOutputSpec | EventHubOutputSpec | ConsoleOutputSpec
    ) = Field(..., discriminator="type")
