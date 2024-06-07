from typing import Annotated, Union

from pydantic import Field, RootModel, create_model

from data_rafting_kit.common.base_spec import BaseSpec
from data_rafting_kit.data_quality.great_expectations import (
    GREAT_EXPECTATIONS_DATA_QUALITY_SPECS,
)

ALL_DATA_QUALITY_SPECS = GREAT_EXPECTATIONS_DATA_QUALITY_SPECS

DataQualityCheckRootSpec = create_model(
    "DataQualityCheckRootSpec",
    root=Annotated[
        Union[tuple(ALL_DATA_QUALITY_SPECS)],  # noqa: UP007
        Field(..., discriminator="type"),
    ],
    __base__=RootModel,
)

fields = {
    "input_df": Annotated[str | None, Field(default=None)],
    "checks": Annotated[list[DataQualityCheckRootSpec], Field(...)],
}
DataQualityRootSpec = create_model("DataQualityRootSpec", **fields, __base__=BaseSpec)
