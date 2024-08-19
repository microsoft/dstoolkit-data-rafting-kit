# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Annotated, Union

from pydantic import Field, create_model

from data_rafting_kit.common.base_spec import BaseRootModel
from data_rafting_kit.data_quality.checks import (
    DataQualityChecksSpec,
)
from data_rafting_kit.data_quality.metrics import DataQualityMetricsSpec

ALL_DATA_QUALITY_SPECS = [DataQualityChecksSpec, DataQualityMetricsSpec]

DataQualityRootSpec = create_model(
    "DataQualityRootSpec",
    root=Annotated[
        Union[tuple(ALL_DATA_QUALITY_SPECS)],
        Field(..., discriminator="type"),
    ],
    __base__=BaseRootModel,
)
