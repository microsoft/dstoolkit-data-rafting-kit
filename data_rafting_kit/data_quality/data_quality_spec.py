# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Annotated, Literal, Union

from pydantic import Field, create_model

from data_rafting_kit.common.base_spec import (
    BaseParamSpec,
    BaseRootModel,
    BaseSpec,
)
from data_rafting_kit.data_quality.checks import (
    GREAT_EXPECTATIONS_DATA_QUALITY_SPECS,
    DataQualityModeEnum,
)

DataQualityChecksRootSpec = create_model(
    "DataQualityChecksRootSpec",
    root=Annotated[
        Union[tuple(GREAT_EXPECTATIONS_DATA_QUALITY_SPECS)],
        Field(..., discriminator="type"),
    ],
    __base__=BaseRootModel,
)

param_fields = {
    "checks": Annotated[list[DataQualityChecksRootSpec], Field(...)],
    "mode": Annotated[
        DataQualityModeEnum | None, Field(default=DataQualityModeEnum.FAIL)
    ],
    "unique_column_identifiers": Annotated[
        list[str] | None, Field(default_factory=list)
    ],
}
DataQualityCheckParamSpec = create_model(
    "DataQualityCheckParamSpec", **param_fields, __base__=BaseParamSpec
)

fields = {
    "input_df": Annotated[str | None, Field(default=None)],
    "params": Annotated[DataQualityCheckParamSpec, Field(...)],
    "type": Annotated[Literal["check"], Field(...)],
}
DataQualityCheckSpec = create_model("DataQualityCheckSpec", **fields, __base__=BaseSpec)

ALL_DATA_QUALITY_SPECS = [DataQualityCheckSpec]

DataQualityRootSpec = create_model(
    "DataQualityRootSpec",
    root=Annotated[
        Union[tuple(ALL_DATA_QUALITY_SPECS)],
        Field(..., discriminator="type"),
    ],
    __base__=BaseRootModel,
)
